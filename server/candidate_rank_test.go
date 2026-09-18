// Copyright 2026 gorse Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage/cache"
)

type candidateRankCache struct {
	cache.Database
	getScores func(context.Context, string, string, []string) ([]cache.Score, error)
}

func (d *candidateRankCache) GetScores(ctx context.Context, collection, subset string, ids []string) ([]cache.Score, error) {
	if d.getScores != nil {
		return d.getScores(ctx, collection, subset, ids)
	}
	return d.Database.GetScores(ctx, collection, subset, ids)
}

type changingMarkerCache struct {
	cache.Database
	key       string
	nextValue string
	mu        sync.Mutex
	reads     int
}

func (d *changingMarkerCache) Get(ctx context.Context, name string) *cache.ReturnValue {
	if name == d.key {
		d.mu.Lock()
		d.reads++
		if d.reads == 2 {
			_ = d.Database.Set(ctx, cache.String(name, d.nextValue))
		}
		d.mu.Unlock()
	}
	return d.Database.Get(ctx, name)
}

func (suite *ServerTestSuite) configureCandidateRank(t *testing.T) (config.NonPersonalizedConfig, time.Time) {
	t.Helper()
	recommender := config.NonPersonalizedConfig{
		Name:              "candidate_rank",
		Score:             "len(feedback)",
		CandidateComplete: true,
	}
	suite.Config.Recommend.NonPersonalized = []config.NonPersonalizedConfig{recommender}
	generation := time.Date(2026, 9, 19, 1, 2, 3, 456000000, time.UTC)
	suite.Require().NoError(suite.CacheClient.Set(t.Context(), cache.String(
		cache.Key(cache.NonPersonalizedCandidateGeneration, recommender.Name),
		cache.EncodeCandidateGeneration(generation, recommender.Hash()),
	)))
	return recommender, generation
}

func (suite *ServerTestSuite) candidateRankResponse(ctx context.Context, name, key, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/non-personalized/"+name+"/candidate-rank", strings.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("X-API-Key", key)
	}
	response := httptest.NewRecorder()
	suite.handler.ServeHTTP(response, request)
	return response
}

func (suite *ServerTestSuite) TestCandidateRank() {
	t := suite.T()
	recommender, generation := suite.configureCandidateRank(t)
	suite.Require().NoError(suite.CacheClient.AddScores(t.Context(), cache.NonPersonalizedCandidateScores, recommender.Name, []cache.Score{
		{Id: "b", Score: 1, Timestamp: generation},
		{Id: "a", Score: 1, Timestamp: generation},
		{Id: "zero", Score: 0, Timestamp: generation},
		{Id: "hidden", Score: 100, IsHidden: true, Timestamp: generation},
		{Id: "old", Score: 2, Timestamp: generation.Add(-time.Millisecond)},
	}))

	response := suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["zero","b","a"]}`)
	suite.Equal(http.StatusOK, response.Code)
	suite.JSONEq(`[{"Id":"a","Score":1},{"Id":"b","Score":1},{"Id":"zero","Score":0}]`, response.Body.String())

	for _, id := range []string{"missing", "hidden", "old"} {
		response = suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["`+id+`"]}`)
		suite.Equal(http.StatusServiceUnavailable, response.Code)
	}

	response = suite.candidateRankResponse(t.Context(), "missing", apiKey, `{"item_ids":["a"]}`)
	suite.Equal(http.StatusNotFound, response.Code)
	suite.Config.Recommend.NonPersonalized[0].CandidateComplete = false
	response = suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["a"]}`)
	suite.Equal(http.StatusNotFound, response.Code)
	suite.Config.Recommend.NonPersonalized[0].CandidateComplete = true

	suite.Config.Recommend.NonPersonalized[0].Score = "1"
	response = suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["a"]}`)
	suite.Equal(http.StatusServiceUnavailable, response.Code)
}

func (suite *ServerTestSuite) TestCandidateRankRequestValidationAndAuthentication() {
	t := suite.T()
	recommender, _ := suite.configureCandidateRank(t)
	tests := []struct {
		name   string
		body   string
		status int
	}{
		{name: "empty", body: `{"item_ids":[]}`, status: http.StatusBadRequest},
		{name: "duplicate", body: `{"item_ids":["a","a"]}`, status: http.StatusBadRequest},
		{name: "blank", body: `{"item_ids":[" "]}`, status: http.StatusBadRequest},
		{name: "unknown field", body: `{"item_ids":["a"],"extra":true}`, status: http.StatusBadRequest},
		{name: "multiple values", body: `{"item_ids":["a"]}{}`, status: http.StatusBadRequest},
		{name: "one valid item", body: `{"item_ids":["a"]}`, status: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, test.body)
			suite.Equal(test.status, response.Code)
		})
	}

	ids := make([]string, candidateRankMaxItems)
	for i := range ids {
		ids[i] = fmt.Sprintf("item-%03d", i)
	}
	body, err := json.Marshal(candidateRankRequest{ItemIDs: ids})
	suite.NoError(err)
	response := suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, string(body))
	suite.Equal(http.StatusServiceUnavailable, response.Code)
	ids = append(ids, "overflow")
	body, err = json.Marshal(candidateRankRequest{ItemIDs: ids})
	suite.NoError(err)
	response = suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, string(body))
	suite.Equal(http.StatusBadRequest, response.Code)

	response = suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["`+strings.Repeat("x", candidateRankMaxBodyBytes)+`"]}`)
	suite.Equal(http.StatusRequestEntityTooLarge, response.Code)

	response = suite.candidateRankResponse(t.Context(), recommender.Name, "", `{"item_ids":["a"]}`)
	suite.Equal(http.StatusUnauthorized, response.Code)
	response = suite.candidateRankResponse(t.Context(), recommender.Name, "wrong-secret", `{"item_ids":["a"]}`)
	suite.Equal(http.StatusUnauthorized, response.Code)
	suite.Config.Server.APIKey = ""
	response = suite.candidateRankResponse(t.Context(), recommender.Name, "", `{"item_ids":["a"]}`)
	suite.Equal(http.StatusServiceUnavailable, response.Code)
}

func (suite *ServerTestSuite) TestCandidateRankConcurrentIsolation() {
	t := suite.T()
	recommender, generation := suite.configureCandidateRank(t)
	suite.Require().NoError(suite.CacheClient.AddScores(t.Context(), cache.NonPersonalizedCandidateScores, recommender.Name, []cache.Score{
		{Id: "a", Score: 4, Timestamp: generation},
		{Id: "b", Score: 3, Timestamp: generation},
		{Id: "c", Score: 2, Timestamp: generation},
		{Id: "d", Score: 1, Timestamp: generation},
	}))

	requests := []string{
		`{"item_ids":["a","b"]}`,
		`{"item_ids":["c","d"]}`,
		`{"item_ids":["b","c"]}`,
	}
	wanted := [][]string{{"a", "b"}, {"c", "d"}, {"b", "c"}}
	type result struct {
		index    int
		response *httptest.ResponseRecorder
	}
	results := make(chan result, len(requests))
	var wait sync.WaitGroup
	for i := range requests {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			results <- result{index: i, response: suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, requests[i])}
		}(i)
	}
	wait.Wait()
	close(results)
	for result := range results {
		suite.Equal(http.StatusOK, result.response.Code)
		var scores []cache.Score
		suite.NoError(json.Unmarshal(result.response.Body.Bytes(), &scores))
		suite.Equal(wanted[result.index], cache.ConvertDocumentsToValues(scores))
	}
}

func (suite *ServerTestSuite) TestCandidateRankRejectsNonFiniteAndMarkerChange() {
	t := suite.T()
	recommender, generation := suite.configureCandidateRank(t)
	original := suite.CacheClient
	invalidScore := math.NaN()
	suite.CacheClient = &candidateRankCache{
		Database: original,
		getScores: func(context.Context, string, string, []string) ([]cache.Score, error) {
			return []cache.Score{{Id: "a", Score: invalidScore, Timestamp: generation}}, nil
		},
	}
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		invalidScore = invalid
		response := suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["a"]}`)
		suite.Equal(http.StatusServiceUnavailable, response.Code)
	}

	suite.CacheClient = &candidateRankCache{
		Database: original,
		getScores: func(context.Context, string, string, []string) ([]cache.Score, error) {
			return []cache.Score{
				{Id: "a", Score: 2, Timestamp: generation},
				{Id: "a", Score: 1, Timestamp: generation},
			}, nil
		},
	}
	response := suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["a","b"]}`)
	suite.Equal(http.StatusServiceUnavailable, response.Code)
	suite.CacheClient = &candidateRankCache{
		Database: original,
		getScores: func(context.Context, string, string, []string) ([]cache.Score, error) {
			return []cache.Score{{Id: "unexpected", Score: 1, Timestamp: generation}}, nil
		},
	}
	response = suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["a"]}`)
	suite.Equal(http.StatusServiceUnavailable, response.Code)

	suite.CacheClient = original
	suite.Require().NoError(original.AddScores(t.Context(), cache.NonPersonalizedCandidateScores, recommender.Name, []cache.Score{
		{Id: "a", Score: 1, Timestamp: generation},
	}))
	suite.CacheClient = &changingMarkerCache{
		Database:  original,
		key:       cache.Key(cache.NonPersonalizedCandidateGeneration, recommender.Name),
		nextValue: cache.EncodeCandidateGeneration(generation.Add(time.Millisecond), recommender.Hash()),
	}
	t.Cleanup(func() { suite.CacheClient = original })
	response = suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["a"]}`)
	suite.Equal(http.StatusServiceUnavailable, response.Code)
}

func (suite *ServerTestSuite) TestCandidateRankCancellation() {
	t := suite.T()
	recommender, _ := suite.configureCandidateRank(t)
	original := suite.CacheClient
	entered := make(chan struct{})
	suite.CacheClient = &candidateRankCache{
		Database: original,
		getScores: func(ctx context.Context, _, _ string, _ []string) ([]cache.Score, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	t.Cleanup(func() { suite.CacheClient = original })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- suite.candidateRankResponse(ctx, recommender.Name, apiKey, `{"item_ids":["a"]}`)
	}()
	<-entered
	cancel()
	select {
	case response := <-done:
		suite.Equal(http.StatusServiceUnavailable, response.Code)
	case <-time.After(time.Second):
		t.Fatal("candidate rank request did not stop after cancellation")
	}
}

func (suite *ServerTestSuite) TestCandidateRankDeadline() {
	t := suite.T()
	recommender, _ := suite.configureCandidateRank(t)
	original := suite.CacheClient
	var deadline time.Time
	suite.CacheClient = &candidateRankCache{
		Database: original,
		getScores: func(ctx context.Context, _, _ string, _ []string) ([]cache.Score, error) {
			var ok bool
			deadline, ok = ctx.Deadline()
			suite.True(ok)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	t.Cleanup(func() { suite.CacheClient = original })
	started := time.Now()
	response := suite.candidateRankResponse(t.Context(), recommender.Name, apiKey, `{"item_ids":["a"]}`)
	suite.Equal(http.StatusServiceUnavailable, response.Code)
	suite.WithinDuration(started.Add(candidateRankTimeout), deadline, 50*time.Millisecond)
	suite.Less(time.Since(started), time.Second)
}
