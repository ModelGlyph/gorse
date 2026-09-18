// Copyright 2020 gorse Project Authors
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

package cache

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorse-io/gorse/common/log"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var (
	redisDSN string
)

func init() {
	// os.Setenv("REDIS_URI", "redis://127.0.0.1:6379/")
	redisDSN = os.Getenv("REDIS_URI")
}

type RedisTestSuite struct {
	baseTestSuite
}

func (suite *RedisTestSuite) SetupSuite() {
	log.SetTestLogger(suite.T())
	var err error
	suite.Database, err = Open(redisDSN, "gorse_")
	suite.NoError(err)
	// flush db
	redisClient, ok := suite.Database.(*Redis)
	suite.True(ok)
	if clusterClient, ok := redisClient.client.(*redis.ClusterClient); ok {
		err = clusterClient.ForEachMaster(suite.T().Context(), func(ctx context.Context, client *redis.Client) error {
			return client.FlushDB(ctx).Err()
		})
		suite.NoError(err)
	} else {
		err = redisClient.client.FlushDB(suite.T().Context()).Err()
		suite.NoError(err)
	}
	// create schema
	err = suite.Database.Init()
	suite.NoError(err)
}

func (suite *RedisTestSuite) TestEscapeCharacters() {
	ts := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx := suite.T().Context()
	for _, c := range []string{"-", ":", ".", "/"} {
		suite.Run(c, func() {
			collection := fmt.Sprintf("a%s1", c)
			subset := fmt.Sprintf("b%s2", c)
			id := fmt.Sprintf("c%s3", c)
			err := suite.AddScores(ctx, collection, subset, []Score{{
				Id:         id,
				Score:      math.MaxFloat64,
				Categories: []string{"a", "b"},
				Timestamp:  ts,
			}})
			suite.NoError(err)
			documents, err := suite.SearchScores(ctx, collection, subset, []string{"b"}, 0, -1)
			suite.NoError(err)
			suite.Equal([]Score{{Id: id, Score: math.MaxFloat64, Categories: []string{"a", "b"}, Timestamp: ts}}, documents)

			err = suite.UpdateScores(ctx, []string{collection}, nil, id, ScorePatch{Score: new(float64(1))})
			suite.NoError(err)
			documents, err = suite.SearchScores(ctx, collection, subset, []string{"b"}, 0, -1)
			suite.NoError(err)
			suite.Equal([]Score{{Id: id, Score: 1, Categories: []string{"a", "b"}, Timestamp: ts}}, documents)

			err = suite.DeleteScores(ctx, []string{collection}, ScoreCondition{
				Subset: new(subset),
				Id:     new(id),
			})
			suite.NoError(err)
			documents, err = suite.SearchScores(ctx, collection, subset, []string{"b"}, 0, -1)
			suite.NoError(err)
			suite.Empty(documents)
		})
	}
}

func (suite *RedisTestSuite) TestUpdateScoresWithPagination() {
	ctx := suite.T().Context()
	db, ok := suite.Database.(*Redis)
	suite.True(ok)
	limit := db.maxSearchResults
	db.maxSearchResults = 2
	defer func() {
		db.maxSearchResults = limit
	}()

	for i := range 5 {
		subset := fmt.Sprintf("subset-%d", i)
		err := suite.AddScores(ctx, "collection-a", subset, []Score{{
			Id:         "shared-item",
			Score:      float64(i),
			Categories: []string{"old"},
			Timestamp:  time.Now().UTC(),
		}})
		suite.NoError(err)
	}

	err := suite.UpdateScores(ctx, []string{"collection-a"}, nil, "shared-item", ScorePatch{
		Categories: []string{"new"},
	})
	suite.NoError(err)

	for i := range 5 {
		subset := fmt.Sprintf("subset-%d", i)
		docs, err := suite.SearchScores(ctx, "collection-a", subset, []string{"new"}, 0, -1)
		suite.NoError(err)
		suite.Require().Len(docs, 1)
		suite.Equal("shared-item", docs[0].Id)
	}
}

func (suite *RedisTestSuite) TestUpdateScoresWithPaginationAndScorePatch() {
	ctx := suite.T().Context()
	db, ok := suite.Database.(*Redis)
	suite.True(ok)
	limit := db.maxSearchResults
	db.maxSearchResults = 1
	defer func() {
		db.maxSearchResults = limit
	}()

	initialScores := []float64{3, 2, 1}
	for i, score := range initialScores {
		subset := fmt.Sprintf("score-subset-%d", i)
		err := suite.AddScores(ctx, "collection-b", subset, []Score{{
			Id:         "shared-item",
			Score:      score,
			Categories: []string{"score-old"},
			Timestamp:  time.Now().UTC(),
		}})
		suite.NoError(err)
	}

	targetScore := float64(0)
	err := suite.UpdateScores(ctx, []string{"collection-b"}, nil, "shared-item", ScorePatch{
		Score: &targetScore,
	})
	suite.NoError(err)

	for i := range initialScores {
		subset := fmt.Sprintf("score-subset-%d", i)
		docs, err := suite.SearchScores(ctx, "collection-b", subset, nil, 0, -1)
		suite.NoError(err)
		suite.Require().Len(docs, 1)
		suite.Equal(targetScore, docs[0].Score)
	}
}

func (suite *RedisTestSuite) TestUpdateScoresWithPaginationAndTiedScores() {
	ctx := suite.T().Context()
	db, ok := suite.Database.(*Redis)
	suite.True(ok)
	limit := db.maxSearchResults
	db.maxSearchResults = 2
	defer func() {
		db.maxSearchResults = limit
	}()

	for i := range 5 {
		subset := fmt.Sprintf("tie-subset-%d", i)
		err := suite.AddScores(ctx, "collection-c", subset, []Score{{
			Id:         "shared-item",
			Score:      1,
			Categories: []string{"tie-old"},
			Timestamp:  time.Now().UTC(),
		}})
		suite.NoError(err)
	}

	err := suite.UpdateScores(ctx, []string{"collection-c"}, nil, "shared-item", ScorePatch{
		Categories: []string{"tie-new"},
	})
	suite.NoError(err)

	for i := range 5 {
		subset := fmt.Sprintf("tie-subset-%d", i)
		docs, err := suite.SearchScores(ctx, "collection-c", subset, []string{"tie-new"}, 0, -1)
		suite.NoError(err)
		suite.Require().Len(docs, 1)
		suite.Equal("shared-item", docs[0].Id)
	}
}

func TestRedis(t *testing.T) {
	if redisDSN == "" {
		t.Skip("REDIS_URI is not set, skipping Redis test")
	}
	suite.Run(t, new(RedisTestSuite))
}

func TestEncodeDecodeCategories(t *testing.T) {
	encoded := encodeCategories([]string{"z", "h"})
	decoded, err := decodeCategories(encoded)
	assert.NoError(t, err)
	assert.Equal(t, []string{"z", "h"}, decoded)

	encoded = encodeCategories(nil)
	decoded, err = decodeCategories(encoded)
	assert.NoError(t, err)
	assert.Equal(t, []string{}, decoded)
}

func TestDecodeRedisScoreTuple(t *testing.T) {
	timestamp := time.Date(2026, 9, 19, 1, 2, 3, 456000000, time.UTC)
	document, found, err := decodeRedisScoreTuple("collection", "subset", "item", []any{
		"collection", "subset", "item", "0", "0", encodeCategories([]string{"work"}), strconv.FormatInt(timestamp.UnixMicro(), 10),
	})
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, Score{Id: "item", Score: 0, Categories: []string{"work"}, Timestamp: timestamp}, document)

	_, found, err = decodeRedisScoreTuple("collection", "subset", "missing", []any{nil, nil, nil, nil, nil, nil, nil})
	assert.NoError(t, err)
	assert.False(t, found)

	_, found, err = decodeRedisScoreTuple("collection", "subset", "hidden", []any{
		"collection", "subset", "hidden", "1", "1", encodeCategories(nil), strconv.FormatInt(timestamp.UnixMicro(), 10),
	})
	assert.NoError(t, err)
	assert.False(t, found)

	_, _, err = decodeRedisScoreTuple("collection", "subset", "item", []any{"collection"})
	assert.Error(t, err)
	_, _, err = decodeRedisScoreTuple("collection", "subset", "item", []any{
		"other", "subset", "item", "1", "0", encodeCategories(nil), strconv.FormatInt(timestamp.UnixMicro(), 10),
	})
	assert.Error(t, err)
	_, _, err = decodeRedisScoreTuple("collection", "subset", "item", []any{
		"collection", "subset", "item", nil, "0", encodeCategories(nil), strconv.FormatInt(timestamp.UnixMicro(), 10),
	})
	assert.Error(t, err)
}

func TestRedisOpenEnablesContextTimeout(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "standalone", path: "redis://127.0.0.1:6379"},
		{name: "cluster", path: "redis+cluster://127.0.0.1:6379?addr=127.0.0.1:6380"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, err := Open(test.path, "gorse_")
			assert.NoError(t, err)
			if err != nil {
				return
			}
			defer database.Close()
			redisDatabase := database.(*Redis)
			switch client := redisDatabase.client.(type) {
			case *redis.Client:
				assert.True(t, client.Options().ContextTimeoutEnabled)
			case *redis.ClusterClient:
				assert.True(t, client.Options().ContextTimeoutEnabled)
			default:
				t.Fatalf("unexpected redis client type %T", client)
			}
		})
	}
}

func TestRedisGetScoresRespectsDeadlineAfterRequest(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	releaseServer := make(chan struct{})
	requestReceived := make(chan struct{})
	serverResult := make(chan error, 1)
	defer close(releaseServer)
	defer serverConn.Close()

	go func() {
		reader := bufio.NewReader(serverConn)
		command, err := readRESPCommand(reader)
		if err != nil {
			serverResult <- err
			return
		}
		if len(command) == 0 || !strings.EqualFold(command[0], "hello") {
			serverResult <- fmt.Errorf("unexpected handshake command %q", command)
			return
		}
		if _, err = io.WriteString(serverConn, "-ERR unknown command 'hello'\r\n"); err != nil {
			serverResult <- err
			return
		}
		command, err = readRESPCommand(reader)
		if err != nil {
			serverResult <- err
			return
		}
		if len(command) == 0 || !strings.EqualFold(command[0], "hmget") {
			serverResult <- fmt.Errorf("unexpected score command %q", command)
			return
		}
		close(requestReceived)
		<-releaseServer
		serverResult <- nil
	}()

	client := redis.NewClient(&redis.Options{
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return clientConn, nil
		},
		Protocol:              2,
		ContextTimeoutEnabled: true,
		DisableIdentity:       true,
		MaxRetries:            -1,
	})
	defer client.Close()
	database := &Redis{TablePrefix: "gorse_", client: client}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := database.GetScores(ctx, "collection", "subset", []string{"item"})
		result <- err
	}()

	select {
	case <-requestReceived:
	case err := <-serverResult:
		t.Fatalf("fake redis failed before receiving HMGET: %v", err)
	case <-time.After(time.Second):
		t.Fatal("HMGET was not sent")
	}
	select {
	case err := <-result:
		var timeoutError net.Error
		assert.ErrorAs(t, err, &timeoutError)
		assert.True(t, timeoutError.Timeout())
		assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("GetScores ignored the context deadline after sending HMGET")
	}
}

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	header = strings.TrimSpace(header)
	if len(header) < 2 || header[0] != '*' {
		return nil, fmt.Errorf("invalid RESP array header %q", header)
	}
	count, err := strconv.Atoi(header[1:])
	if err != nil {
		return nil, err
	}
	command := make([]string, count)
	for i := range count {
		bulkHeader, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bulkHeader = strings.TrimSpace(bulkHeader)
		if len(bulkHeader) < 2 || bulkHeader[0] != '$' {
			return nil, fmt.Errorf("invalid RESP bulk header %q", bulkHeader)
		}
		size, err := strconv.Atoi(bulkHeader[1:])
		if err != nil {
			return nil, err
		}
		value := make([]byte, size+2)
		if _, err = io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		if value[size] != '\r' || value[size+1] != '\n' {
			return nil, fmt.Errorf("invalid RESP bulk terminator")
		}
		command[i] = string(value[:size])
	}
	return command, nil
}

func BenchmarkRedis(b *testing.B) {
	log.CloseLogger()
	// open db
	database, err := Open(redisDSN, "gorse_")
	assert.NoError(b, err)
	// flush db
	err = database.(*Redis).client.FlushDB(b.Context()).Err()
	assert.NoError(b, err)
	// create schema
	err = database.Init()
	assert.NoError(b, err)
	// benchmark
	benchmark(b, database)
}
