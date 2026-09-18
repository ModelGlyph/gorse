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
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/pkg/errors"
)

const (
	candidateRankMaxItems     = 500
	candidateRankMaxBodyBytes = 64 << 10
	candidateRankTimeout      = 300 * time.Millisecond
)

type candidateRankRequest struct {
	ItemIDs []string `json:"item_ids"`
}

func (s *RestServer) rankNonPersonalizedCandidates(request *restful.Request, response *restful.Response) {
	if strings.TrimSpace(s.Config.Server.APIKey) == "" {
		Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
		return
	}

	name := request.PathParameter("name")
	recommender := findCandidateCompleteRecommender(s.Config, name)
	if recommender == nil {
		PageNotFound(response, errors.New("candidate-complete recommender not found"))
		return
	}

	payload, status, err := readCandidateRankRequest(request, response)
	if err != nil {
		Error(response, status, err)
		return
	}
	requestCtx := request.Request.Context()
	ctx, cancel := context.WithTimeout(requestCtx, candidateRankTimeout)
	defer cancel()
	markerKey := cache.Key(cache.NonPersonalizedCandidateGeneration, name)
	markerValue, err := s.CacheClient.Get(ctx, markerKey).String()
	if err != nil || markerValue == "" {
		Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
		return
	}
	generation, err := cache.DecodeCandidateGeneration(markerValue)
	if err != nil || generation.Digest != recommender.Hash() {
		Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
		return
	}

	scores, err := s.CacheClient.GetScores(ctx, cache.NonPersonalizedCandidateScores, name, payload.ItemIDs)
	if err != nil || len(scores) != len(payload.ItemIDs) {
		Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
		return
	}
	requested := make(map[string]struct{}, len(payload.ItemIDs))
	for _, id := range payload.ItemIDs {
		requested[id] = struct{}{}
	}
	returned := make(map[string]struct{}, len(scores))
	for _, score := range scores {
		if _, ok := requested[score.Id]; !ok {
			Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
			return
		}
		if _, duplicated := returned[score.Id]; duplicated {
			Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
			return
		}
		if math.IsNaN(score.Score) || math.IsInf(score.Score, 0) || !score.Timestamp.Equal(generation.Timestamp) {
			Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
			return
		}
		returned[score.Id] = struct{}{}
	}

	current := findCandidateCompleteRecommender(s.Config, name)
	currentMarker, markerErr := s.CacheClient.Get(ctx, markerKey).String()
	if markerErr != nil || current == nil || current.Hash() != generation.Digest || currentMarker != markerValue || ctx.Err() != nil || requestCtx.Err() != nil {
		Error(response, http.StatusServiceUnavailable, errors.New("candidate ranking is unavailable"))
		return
	}
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].Score == scores[j].Score {
			return scores[i].Id < scores[j].Id
		}
		return scores[i].Score > scores[j].Score
	})
	Ok(response, scores)
}

func readCandidateRankRequest(request *restful.Request, response *restful.Response) (candidateRankRequest, int, error) {
	request.Request.Body = http.MaxBytesReader(response.ResponseWriter, request.Request.Body, candidateRankMaxBodyBytes)
	decoder := json.NewDecoder(request.Request.Body)
	decoder.DisallowUnknownFields()
	var payload candidateRankRequest
	if err := decoder.Decode(&payload); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return candidateRankRequest{}, http.StatusRequestEntityTooLarge, errors.New("request body is too large")
		}
		return candidateRankRequest{}, http.StatusBadRequest, errors.New("invalid candidate rank request")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return candidateRankRequest{}, http.StatusBadRequest, errors.New("invalid candidate rank request")
	}
	if len(payload.ItemIDs) == 0 || len(payload.ItemIDs) > candidateRankMaxItems {
		return candidateRankRequest{}, http.StatusBadRequest, errors.New("item_ids must contain between 1 and 500 items")
	}
	seen := make(map[string]struct{}, len(payload.ItemIDs))
	for _, id := range payload.ItemIDs {
		if strings.TrimSpace(id) == "" {
			return candidateRankRequest{}, http.StatusBadRequest, errors.New("item_ids must not contain empty IDs")
		}
		if _, exists := seen[id]; exists {
			return candidateRankRequest{}, http.StatusBadRequest, errors.New("item_ids must be unique")
		}
		seen[id] = struct{}{}
	}
	return payload, http.StatusOK, nil
}

func findCandidateCompleteRecommender(cfg *config.Config, name string) *config.NonPersonalizedConfig {
	for i := range cfg.Recommend.NonPersonalized {
		if cfg.Recommend.NonPersonalized[i].Name == name && cfg.Recommend.NonPersonalized[i].CandidateComplete {
			return &cfg.Recommend.NonPersonalized[i]
		}
	}
	return nil
}
