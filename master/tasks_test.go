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

package master

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/gorse-io/gorse/common/event"
	"github.com/gorse-io/gorse/common/expression"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/dataset"
	"github.com/gorse-io/gorse/logics"
	"github.com/gorse-io/gorse/model/cf"
	"github.com/gorse-io/gorse/model/ctr"
	"github.com/gorse-io/gorse/storage/blob"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/storage/meta"
	"github.com/gorse-io/gorse/storage/vectors"
	"github.com/samber/lo"
)

type failOnceBlobStore struct {
	blob.Store
	name   string
	failed bool
}

func (s *failOnceBlobStore) Remove(name string) error {
	if name == s.name && !s.failed {
		s.failed = true
		return fmt.Errorf("failed to remove %s", name)
	}
	return s.Store.Remove(name)
}

type failOnceVectorDatabase struct {
	vectors.Database
	name   string
	failed bool
}

type failingNonPersonalizedCache struct {
	cache.Database
	failAt   string
	addCalls int
	setCalls int
}

type concurrentScanCache struct {
	cache.Database
	keys []string
}

func (d *concurrentScanCache) Scan(work func(string) error) error {
	start := make(chan struct{})
	errs := make(chan error, len(d.keys))
	var waitGroup sync.WaitGroup
	for _, key := range d.keys {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			if err := work(key); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func (d *failingNonPersonalizedCache) AddScores(ctx context.Context, collection, subset string, documents []cache.Score) error {
	d.addCalls++
	if d.failAt == "standard add" && d.addCalls == 1 {
		return fmt.Errorf("standard add scores failed")
	}
	if d.failAt == "candidate add" && d.addCalls == 2 {
		return fmt.Errorf("add scores failed")
	}
	return d.Database.AddScores(ctx, collection, subset, documents)
}

func (d *failingNonPersonalizedCache) DeleteScores(ctx context.Context, collections []string, condition cache.ScoreCondition) error {
	if d.failAt == "delete" {
		return fmt.Errorf("delete scores failed")
	}
	return d.Database.DeleteScores(ctx, collections, condition)
}

func (d *failingNonPersonalizedCache) Set(ctx context.Context, values ...cache.Value) error {
	d.setCalls++
	if d.failAt == "metadata" && d.setCalls == 1 {
		return fmt.Errorf("metadata failed")
	}
	if d.failAt == "generation" && d.setCalls == 2 {
		return fmt.Errorf("generation failed")
	}
	return d.Database.Set(ctx, values...)
}

func (d *failOnceVectorDatabase) DeleteCollection(ctx context.Context, name string) error {
	if name == d.name && !d.failed {
		d.failed = true
		return fmt.Errorf("failed to delete %s", name)
	}
	return d.Database.DeleteCollection(ctx, name)
}

func (s *MasterTestSuite) TestRemoveOutOfDateModels() {
	ctx := s.T().Context()
	s.blobStore = &failOnceBlobStore{Store: blob.NewPOSIX(s.T().TempDir()), name: "50"}
	s.collaborativeFilteringMeta = meta.Model[cf.Score]{ID: 100}
	s.clickThroughRateMeta = meta.Model[ctr.Score]{ID: 150}

	for _, id := range []int64{50, 100, 150, 200, 300, 400} {
		w, done, err := s.blobStore.Create(strconv.FormatInt(id, 10))
		s.Require().NoError(err)
		_, err = w.Write([]byte("model"))
		s.Require().NoError(err)
		s.Require().NoError(w.Close())
		<-done
	}
	w, done, err := s.blobStore.Create("invalid")
	s.Require().NoError(err)
	_, err = w.Write([]byte("model"))
	s.Require().NoError(err)
	s.Require().NoError(w.Close())
	<-done
	for _, id := range []int64{100, 200, 300, 400, 500} {
		s.Require().NoError(s.VectorClient.AddCollection(ctx,
			vectors.CollaborativeFilteringCollection(id), 2, vectors.Dot, vectors.VectorConfig{}))
	}
	s.Require().NoError(s.VectorClient.AddCollection(ctx, "collaborative_filtering_invalid", 2, vectors.Dot, vectors.VectorConfig{}))
	s.Require().NoError(s.VectorClient.AddCollection(ctx, "unrelated", 2, vectors.Dot, vectors.VectorConfig{}))

	s.removeOutOfDateModels(ctx)
	s.removeOutOfDateModels(ctx)

	collections, err := s.VectorClient.ListCollections(ctx)
	s.Require().NoError(err)
	s.ElementsMatch([]string{
		vectors.CollaborativeFilteringCollection(100),
		vectors.CollaborativeFilteringCollection(300),
		vectors.CollaborativeFilteringCollection(400),
		"collaborative_filtering_invalid",
		"unrelated",
	}, collections)
	files, err := s.blobStore.List()
	s.Require().NoError(err)
	s.ElementsMatch([]string{"100", "150", "300", "400", "invalid"}, files)
}

func (s *MasterTestSuite) TestRemoveOutOfDateModelsRetry() {
	ctx := s.T().Context()
	s.blobStore = &failOnceBlobStore{Store: blob.NewPOSIX(s.T().TempDir()), name: "200"}
	s.VectorClient = &failOnceVectorDatabase{
		Database: s.VectorClient,
		name:     vectors.CollaborativeFilteringCollection(200),
	}
	s.collaborativeFilteringMeta = meta.Model[cf.Score]{ID: 100}
	s.clickThroughRateMeta = meta.Model[ctr.Score]{ID: 150}

	for _, id := range []int64{100, 150, 200, 300, 400} {
		w, done, err := s.blobStore.Create(strconv.FormatInt(id, 10))
		s.Require().NoError(err)
		_, err = w.Write([]byte("model"))
		s.Require().NoError(err)
		s.Require().NoError(w.Close())
		<-done
	}
	for _, id := range []int64{100, 200, 300, 400} {
		s.Require().NoError(s.VectorClient.AddCollection(ctx,
			vectors.CollaborativeFilteringCollection(id), 2, vectors.Dot, vectors.VectorConfig{}))
	}

	s.removeOutOfDateModels(ctx)
	s.removeOutOfDateModels(ctx)
	s.removeOutOfDateModels(ctx)

	collections, err := s.VectorClient.ListCollections(ctx)
	s.Require().NoError(err)
	s.NotContains(collections, vectors.CollaborativeFilteringCollection(200))
	files, err := s.blobStore.List()
	s.Require().NoError(err)
	s.NotContains(files, "200")
}

func (s *MasterTestSuite) TestFindItemToItem() {
	ctx := s.T().Context()
	// create config
	s.Config = &config.Config{}
	s.Config.Recommend.CacheSize = 3
	s.Config.Master.NumJobs = 4
	// collect similar
	items := []data.Item{
		{ItemId: "0", IsHidden: false, Categories: []string{"*"}, Timestamp: time.Now(), Labels: []string{"a", "b", "c", "d"}, Comment: ""},
		{ItemId: "1", IsHidden: false, Categories: []string{"*"}, Timestamp: time.Now(), Labels: []string{}, Comment: ""},
		{ItemId: "2", IsHidden: false, Categories: []string{"*"}, Timestamp: time.Now(), Labels: []string{"b", "c", "d"}, Comment: ""},
		{ItemId: "3", IsHidden: false, Categories: nil, Timestamp: time.Now(), Labels: []string{}, Comment: ""},
		{ItemId: "4", IsHidden: false, Categories: nil, Timestamp: time.Now(), Labels: []string{"b", "c"}, Comment: ""},
		{ItemId: "5", IsHidden: false, Categories: []string{"*"}, Timestamp: time.Now(), Labels: []string{}, Comment: ""},
		{ItemId: "6", IsHidden: false, Categories: []string{"*"}, Timestamp: time.Now(), Labels: []string{"c"}, Comment: ""},
		{ItemId: "7", IsHidden: false, Categories: []string{"*"}, Timestamp: time.Now(), Labels: []string{}, Comment: ""},
		{ItemId: "8", IsHidden: false, Categories: []string{"*"}, Timestamp: time.Now(), Labels: []string{"a", "b", "c", "d", "e"}, Comment: ""},
		{ItemId: "9", IsHidden: false, Categories: nil, Timestamp: time.Now(), Labels: []string{}, Comment: ""},
	}
	feedbacks := make([]data.Feedback, 0)
	for i := range 10 {
		for j := 0; j <= i; j++ {
			if i%2 == 1 {
				feedbacks = append(feedbacks, data.Feedback{
					FeedbackKey: data.FeedbackKey{
						ItemId:       strconv.Itoa(i),
						UserId:       strconv.Itoa(j),
						FeedbackType: "FeedbackType",
					},
					Timestamp: time.Now(),
				})
			}
		}
	}
	var err error
	err = s.DataClient.BatchInsertItems(ctx, items)
	s.NoError(err)
	err = s.DataClient.BatchInsertFeedback(ctx, feedbacks, true, true, true)
	s.NoError(err)

	// insert hidden item
	err = s.DataClient.BatchInsertItems(ctx, []data.Item{{
		ItemId:   "10",
		Labels:   []string{"a", "b", "c", "d", "e"},
		IsHidden: true,
	}})
	s.NoError(err)
	for i := 0; i <= 10; i++ {
		err = s.DataClient.BatchInsertFeedback(ctx, []data.Feedback{{
			FeedbackKey: data.FeedbackKey{UserId: strconv.Itoa(i), ItemId: "10", FeedbackType: "FeedbackType"},
		}}, true, true, true)
		s.NoError(err)
	}

	// load mock dataset
	_, dataSet, _, err := s.LoadDataFromDatabase(s.T().Context(), s.DataClient,
		[]expression.FeedbackTypeExpression{expression.MustParseFeedbackTypeExpression("FeedbackType")},
		nil, nil, 0, 0, NewOnlineEvaluator(nil, nil), nil)
	s.NoError(err)

	// similar items (common users)
	s.Config.Recommend.ItemToItem = []config.ItemToItemConfig{{Name: "users", Type: "users"}}
	s.NoError(s.updateItemToItem(s.T().Context(), dataSet))
	similar, err := logics.QueryItemToItem(ctx, s.VectorClient, s.Config.Recommend.ItemToItem[0], "9", nil, s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"7", "5", "3"}, cache.ConvertDocumentsToValues(similar))
	// similar items in category (common users)
	similar, err = logics.QueryItemToItem(ctx, s.VectorClient, s.Config.Recommend.ItemToItem[0], "9", []string{"*"}, s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"7", "5", "1"}, cache.ConvertDocumentsToValues(similar))

	// similar items (common labels)
	err = s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyItemTime, "8"), time.Now()))
	s.NoError(err)
	s.Config.Recommend.ItemToItem = []config.ItemToItemConfig{{Name: "tags", Type: "tags", Column: "item.Labels"}}
	s.NoError(s.updateItemToItem(s.T().Context(), dataSet))
	similar, err = logics.QueryItemToItem(ctx, s.VectorClient, s.Config.Recommend.ItemToItem[0], "8", nil, s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"0", "2", "4"}, cache.ConvertDocumentsToValues(similar))
	// similar items in category (common labels)
	similar, err = logics.QueryItemToItem(ctx, s.VectorClient, s.Config.Recommend.ItemToItem[0], "8", []string{"*"}, s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"0", "2", "6"}, cache.ConvertDocumentsToValues(similar))

	// similar items (auto)
	err = s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyItemTime, "8"), time.Now()))
	s.NoError(err)
	err = s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyItemTime, "9"), time.Now()))
	s.NoError(err)
	s.Config.Recommend.ItemToItem = []config.ItemToItemConfig{{Name: "auto", Type: "auto"}}
	s.NoError(s.updateItemToItem(s.T().Context(), dataSet))
	similar, err = logics.QueryItemToItem(ctx, s.VectorClient, s.Config.Recommend.ItemToItem[0], "8", nil, s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"0", "2", "4"}, cache.ConvertDocumentsToValues(similar))
	similar, err = logics.QueryItemToItem(ctx, s.VectorClient, s.Config.Recommend.ItemToItem[0], "9", nil, s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"7", "5", "3"}, cache.ConvertDocumentsToValues(similar))
}

func (s *MasterTestSuite) TestUserToUser() {
	ctx := s.T().Context()
	// create config
	s.Config = &config.Config{}
	s.Config.Recommend.CacheSize = 3
	s.Config.Master.NumJobs = 4
	// collect similar
	users := []data.User{
		{UserId: "0", Labels: []string{"a", "b", "c", "d"}, Comment: ""},
		{UserId: "1", Labels: []string{}, Comment: ""},
		{UserId: "2", Labels: []string{"b", "c", "d"}, Comment: ""},
		{UserId: "3", Labels: []string{}, Comment: ""},
		{UserId: "4", Labels: []string{"b", "c"}, Comment: ""},
		{UserId: "5", Labels: []string{}, Comment: ""},
		{UserId: "6", Labels: []string{"c"}, Comment: ""},
		{UserId: "7", Labels: []string{}, Comment: ""},
		{UserId: "8", Labels: []string{"a", "b", "c", "d", "e"}, Comment: ""},
		{UserId: "9", Labels: []string{}, Comment: ""},
	}
	feedbacks := make([]data.Feedback, 0)
	for i := range 10 {
		for j := 0; j <= i; j++ {
			if i%2 == 1 {
				feedbacks = append(feedbacks, data.Feedback{
					FeedbackKey: data.FeedbackKey{
						ItemId:       strconv.Itoa(j),
						UserId:       strconv.Itoa(i),
						FeedbackType: "FeedbackType",
					},
					Timestamp: time.Now(),
				})
			}
		}
	}
	var err error
	err = s.DataClient.BatchInsertUsers(ctx, users)
	s.NoError(err)
	err = s.DataClient.BatchInsertFeedback(ctx, feedbacks, true, true, true)
	s.NoError(err)
	_, dataSet, _, err := s.LoadDataFromDatabase(s.T().Context(), s.DataClient,
		[]expression.FeedbackTypeExpression{expression.MustParseFeedbackTypeExpression("FeedbackType")},
		nil, nil, 0, 0, NewOnlineEvaluator(nil, nil), nil)
	s.NoError(err)

	// similar items (common users)
	s.Config.Recommend.UserToUser = []config.UserToUserConfig{{Name: "items", Type: "items"}}
	collection := vectors.UserToUserCollection("items")
	s.NoError(s.VectorClient.AddCollection(ctx, collection, 0, vectors.Dot, vectors.VectorConfig{}))
	s.NoError(s.VectorClient.AddVectors(ctx, collection, []vectors.Vector{{Id: "stale", Indices: []uint32{0}, Values: []float32{1}, Timestamp: dataSet.GetTimestamp().Add(-time.Hour)}}))
	s.NoError(s.updateUserToUser(s.T().Context(), dataSet))
	stale, err := s.VectorClient.GetVectors(ctx, collection, []string{"stale"})
	s.NoError(err)
	s.Empty(stale)
	similar, err := logics.QueryUserToUser(ctx, s.VectorClient, s.Config.Recommend.UserToUser[0], "9", s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"7", "5", "3"}, cache.ConvertDocumentsToValues(similar))

	// similar items (common labels)
	err = s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyUserTime, "8"), time.Now()))
	s.NoError(err)
	s.Config.Recommend.UserToUser = []config.UserToUserConfig{{Name: "tags", Type: "tags", Column: "user.Labels"}}
	s.NoError(s.updateUserToUser(s.T().Context(), dataSet))
	similar, err = logics.QueryUserToUser(ctx, s.VectorClient, s.Config.Recommend.UserToUser[0], "8", s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"0", "2", "4"}, cache.ConvertDocumentsToValues(similar))

	// similar items (auto)
	err = s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyUserTime, "8"), time.Now()))
	s.NoError(err)
	err = s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyUserTime, "9"), time.Now()))
	s.NoError(err)
	s.Config.Recommend.UserToUser = []config.UserToUserConfig{{Name: "auto", Type: "auto"}}
	s.NoError(s.updateUserToUser(s.T().Context(), dataSet))
	similar, err = logics.QueryUserToUser(ctx, s.VectorClient, s.Config.Recommend.UserToUser[0], "8", s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"0", "2", "4"}, cache.ConvertDocumentsToValues(similar))
	similar, err = logics.QueryUserToUser(ctx, s.VectorClient, s.Config.Recommend.UserToUser[0], "9", s.Config.Recommend.CacheSize)
	s.NoError(err)
	s.Equal([]string{"7", "5", "3"}, cache.ConvertDocumentsToValues(similar))
}

type snapshotHandler struct {
	snapshots chan event.Snapshot
}

func (h *snapshotHandler) EmitRequest(context.Context, event.Request) {}

func (h *snapshotHandler) EmitSnapshot(_ context.Context, snapshot event.Snapshot) {
	h.snapshots <- snapshot
}

func (s *MasterTestSuite) TestEmitSnapshot() {
	ctx := s.T().Context()
	s.Config = &config.Config{}
	s.Config.Master.NumJobs = 1
	s.Config.Recommend.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("positive"),
	}
	s.Config.Recommend.DataSource.NegativeFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("negative"),
	}

	users := []data.User{{UserId: "0"}, {UserId: "1"}}
	items := []data.Item{{ItemId: "0"}, {ItemId: "1"}}
	feedbacks := []data.Feedback{
		{FeedbackKey: data.FeedbackKey{FeedbackType: "positive", UserId: "0", ItemId: "0"}},
		{FeedbackKey: data.FeedbackKey{FeedbackType: "negative", UserId: "0", ItemId: "0"}},
	}
	s.NoError(s.DataClient.BatchInsertUsers(ctx, users))
	s.NoError(s.DataClient.BatchInsertItems(ctx, items))
	s.NoError(s.DataClient.BatchInsertFeedback(ctx, feedbacks, false, false, false))

	handler := &snapshotHandler{snapshots: make(chan event.Snapshot, 1)}
	event.SetEventHandler(handler)
	s.T().Cleanup(func() { event.SetEventHandler(&event.NopHandler{}) })

	datasets, err := s.loadDataset(ctx)
	s.Require().NoError(err)
	s.Equal(1, datasets.clickTrainSet.Count()+datasets.clickTestSet.Count())

	select {
	case snapshot := <-handler.snapshots:
		s.Equal(int64(len(users)), snapshot.UserCount)
		s.Equal(deepSize(users), snapshot.UserBytes)
		s.Equal(int64(len(items)), snapshot.ItemCount)
		s.Equal(deepSize(items), snapshot.ItemBytes)
		s.Equal(int64(len(feedbacks)), snapshot.FeedbackCount)
		s.Equal(deepSize(feedbacks), snapshot.FeedbackBytes)
		s.False(snapshot.Timestamp.IsZero())
	case <-time.After(time.Second):
		s.Fail("snapshot was not emitted")
	}
}

func (s *MasterTestSuite) TestLoadDataFromDatabase() {
	ctx := s.T().Context()
	// create config
	s.Config = &config.Config{}
	s.Config.Recommend.CacheSize = 3
	s.Config.Recommend.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("positive")}
	s.Config.Recommend.DataSource.ReadFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("negative")}
	s.Config.Master.NumJobs = runtime.NumCPU()

	// insert items
	var items []data.Item
	for i := range 9 {
		items = append(items, data.Item{
			ItemId:     strconv.Itoa(i),
			Timestamp:  time.Date(2000+i, 1, 1, 1, 1, 0, 0, time.UTC),
			Labels:     []any{strconv.Itoa(i % 3), strconv.Itoa(i*10 + 10)},
			Categories: []string{strconv.Itoa(i % 3)},
		})
	}
	err := s.DataClient.BatchInsertItems(ctx, items)
	s.NoError(err)
	err = s.DataClient.BatchInsertItems(ctx, []data.Item{{
		ItemId:    "9",
		Timestamp: time.Date(2020, 1, 1, 1, 1, 0, 0, time.UTC),
		IsHidden:  true,
	}})
	s.NoError(err)

	// insert users
	var users []data.User
	for i := 0; i <= 10; i++ {
		users = append(users, data.User{
			UserId: strconv.Itoa(i),
			Labels: []string{strconv.Itoa(i % 5), strconv.Itoa(i*10 + 10)},
		})
	}
	err = s.DataClient.BatchInsertUsers(ctx, users)
	s.NoError(err)

	// insert feedback
	feedbacks := make([]data.Feedback, 0)
	for i := range 10 {
		// positive feedback
		// item 0: user 0
		// ...
		// item 9: user 0 ... user 9
		for j := 0; j <= i; j++ {
			feedbacks = append(feedbacks, data.Feedback{
				FeedbackKey: data.FeedbackKey{
					ItemId:       strconv.Itoa(i),
					UserId:       strconv.Itoa(j),
					FeedbackType: "positive",
				},
				Timestamp: time.Now(),
			})
		}
		// negative feedback
		// item 0: user 1 .. user 10
		// ...
		// item 9: user 10
		for j := i + 1; j < 11; j++ {
			feedbacks = append(feedbacks, data.Feedback{
				FeedbackKey: data.FeedbackKey{
					ItemId:       strconv.Itoa(i),
					UserId:       strconv.Itoa(j),
					FeedbackType: "negative",
				},
				Timestamp: time.Now(),
			})
		}
	}
	err = s.DataClient.BatchInsertFeedback(ctx, feedbacks, false, false, true)
	s.NoError(err)

	// load dataset
	datasets, err := s.loadDataset(ctx)
	s.NoError(err)
	s.Equal(11, datasets.rankingTrainSet.CountUsers())
	s.Equal(10, datasets.rankingTrainSet.CountItems())
	s.Equal(11, datasets.rankingTestSet.CountUsers())
	s.Equal(10, datasets.rankingTestSet.CountItems())
	s.Equal(55, datasets.rankingTrainSet.CountFeedback()+datasets.rankingTestSet.CountFeedback())
	s.Equal(11, datasets.clickTrainSet.CountUsers())
	s.Equal(10, datasets.clickTrainSet.CountItems())
	s.Equal(11, datasets.clickTestSet.CountUsers())
	s.Equal(10, datasets.clickTestSet.CountItems())
	s.Equal(int32(3), datasets.clickTrainSet.Index.CountItemLabels())
	s.Equal(int32(5), datasets.clickTrainSet.Index.CountUserLabels())
	s.Equal(int32(3), datasets.clickTestSet.Index.CountItemLabels())
	s.Equal(int32(5), datasets.clickTestSet.Index.CountUserLabels())
	s.Equal(110, datasets.clickTrainSet.Count()+datasets.clickTestSet.Count())
	s.Equal(55, datasets.clickTrainSet.PositiveCount+datasets.clickTestSet.PositiveCount)
	s.Equal(55, datasets.clickTrainSet.NegativeCount+datasets.clickTestSet.NegativeCount)

	// check latest items
	latest, err := s.DataClient.GetLatestItems(ctx, 3, nil, nil)
	s.NoError(err)
	s.Equal([]data.Item{
		items[8],
		items[7],
		items[6],
	}, latest)
	latest, err = s.DataClient.GetLatestItems(ctx, 3, []string{"2"}, nil)
	s.NoError(err)
	s.Equal([]data.Item{
		items[8],
		items[5],
		items[2],
	}, latest)

	// check categories
	categoryScores, err := s.CacheClient.SearchScores(ctx, cache.ItemCategories, "", nil, 0, -1)
	s.NoError(err)
	categories := make([]string, len(categoryScores))
	for i, score := range categoryScores {
		categories[i] = score.Id
	}
	s.Equal([]string{"0", "1", "2"}, categories)
}

func (s *MasterTestSuite) TestNegativeFeedbackPriority() {
	ctx := s.T().Context()
	// create config
	s.Config = &config.Config{}
	s.Config.Recommend.CacheSize = 3
	s.Config.Recommend.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("positive")}
	s.Config.Recommend.DataSource.NegativeFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("dislike")}
	s.Config.Recommend.DataSource.ReadFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("read")}
	s.Config.Master.NumJobs = runtime.NumCPU()

	// insert items
	var items []data.Item
	for i := range 5 {
		items = append(items, data.Item{
			ItemId:    strconv.Itoa(i),
			Timestamp: time.Date(2000+i, 1, 1, 1, 1, 0, 0, time.UTC),
		})
	}
	err := s.DataClient.BatchInsertItems(ctx, items)
	s.NoError(err)

	// insert users
	var users []data.User
	for i := 0; i < 3; i++ {
		users = append(users, data.User{
			UserId: strconv.Itoa(i),
		})
	}
	err = s.DataClient.BatchInsertUsers(ctx, users)
	s.NoError(err)

	// insert feedback
	feedbacks := []data.Feedback{
		// User 0: positive on item 0, 1; negative on item 2; read on item 3, 4
		{FeedbackKey: data.FeedbackKey{UserId: "0", ItemId: "0", FeedbackType: "positive"}, Timestamp: time.Now()},
		{FeedbackKey: data.FeedbackKey{UserId: "0", ItemId: "1", FeedbackType: "positive"}, Timestamp: time.Now()},
		{FeedbackKey: data.FeedbackKey{UserId: "0", ItemId: "2", FeedbackType: "dislike"}, Timestamp: time.Now()},
		{FeedbackKey: data.FeedbackKey{UserId: "0", ItemId: "3", FeedbackType: "read"}, Timestamp: time.Now()},
		{FeedbackKey: data.FeedbackKey{UserId: "0", ItemId: "4", FeedbackType: "read"}, Timestamp: time.Now()},
		// User 1: positive AND negative on item 0 (should be negative due to priority)
		{FeedbackKey: data.FeedbackKey{UserId: "1", ItemId: "0", FeedbackType: "positive"}, Timestamp: time.Now()},
		{FeedbackKey: data.FeedbackKey{UserId: "1", ItemId: "0", FeedbackType: "dislike"}, Timestamp: time.Now()},
		{FeedbackKey: data.FeedbackKey{UserId: "1", ItemId: "1", FeedbackType: "positive"}, Timestamp: time.Now()},
		// User 2: positive, then negative on item 2 (should be negative due to priority)
		{FeedbackKey: data.FeedbackKey{UserId: "2", ItemId: "2", FeedbackType: "positive"}, Timestamp: time.Now().Add(-time.Hour)},
		{FeedbackKey: data.FeedbackKey{UserId: "2", ItemId: "2", FeedbackType: "dislike"}, Timestamp: time.Now()},
		{FeedbackKey: data.FeedbackKey{UserId: "2", ItemId: "3", FeedbackType: "positive"}, Timestamp: time.Now()},
	}
	err = s.DataClient.BatchInsertFeedback(ctx, feedbacks, false, false, true)
	s.NoError(err)

	// load dataset
	datasets, err := s.loadDataset(ctx)
	s.NoError(err)

	// Verify the dataset
	// User 0: 2 positive (0,1), 1 negative feedback (2), 2 read (3,4) = 2 pos + 3 neg = 5 samples
	// User 1: item 0 is negative (due to dislike priority), item 1 is positive = 1 pos + 1 neg = 2 samples
	// User 2: item 2 is negative (due to dislike priority), item 3 is positive = 1 pos + 1 neg = 2 samples
	s.Equal(9, datasets.clickTrainSet.Count()+datasets.clickTestSet.Count())
	s.Equal(4, datasets.clickTrainSet.PositiveCount+datasets.clickTestSet.PositiveCount)
	s.Equal(5, datasets.clickTrainSet.NegativeCount+datasets.clickTestSet.NegativeCount)

	// Verify negative feedback items are excluded from recommendations
	recommender, err := logics.NewRecommender(s.Config.Recommend, s.CacheClient, s.DataClient, s.VectorClient, true, "1", nil)
	s.NoError(err)
	excludeSet := recommender.ExcludeSet()
	// User 1 should have item 0 in exclude set (due to dislike)
	s.True(excludeSet.Contains("0"), "item 0 should be excluded due to dislike feedback")
}

func (s *MasterTestSuite) TestNonPersonalizedRecommend() {
	ctx := s.T().Context()
	// create config
	s.Config = &config.Config{}
	s.Config.Recommend.CacheSize = 3
	s.Config.Recommend.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("positive")}
	s.Config.Recommend.DataSource.ReadFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("negative")}
	s.Config.Server.APIKey = "test_api_key"
	s.Config.Recommend.NonPersonalized = []config.NonPersonalizedConfig{
		{Name: "latest", Score: "item.Timestamp.Unix()"},
		{Name: "candidate_latest", Score: "item.Timestamp.Unix()", CandidateComplete: true},
	}
	s.Config.Master.NumJobs = runtime.NumCPU()

	// insert items
	var items []data.Item
	for i := range 10 {
		items = append(items, data.Item{
			ItemId:    strconv.Itoa(i),
			Timestamp: time.Date(2000+i%2, 1, 1, i, 1, 0, 0, time.UTC),
		})
	}
	err := s.DataClient.BatchInsertItems(ctx, items)
	s.NoError(err)

	// insert users
	var users []data.User
	for i := range 10 {
		users = append(users, data.User{
			UserId: strconv.Itoa(i),
		})
	}
	err = s.DataClient.BatchInsertUsers(ctx, users)
	s.NoError(err)

	// insert feedback
	feedbacks := make([]data.Feedback, 0)
	for i := range 10 {
		// positive feedback
		// item 0: user 0
		// ...
		// item 8: user 0 ... user 8
		if i%2 == 0 {
			for j := 0; j <= i; j++ {
				feedbacks = append(feedbacks, data.Feedback{
					FeedbackKey: data.FeedbackKey{
						ItemId:       strconv.Itoa(i),
						UserId:       strconv.Itoa(j),
						FeedbackType: "positive",
					},
					Timestamp: time.Now(),
				})
			}
		}
	}
	err = s.DataClient.BatchInsertFeedback(ctx, feedbacks, false, false, true)
	s.NoError(err)

	// load dataset
	_, err = s.loadDataset(ctx)
	s.NoError(err)

	// check latest items
	latest, err := s.CacheClient.SearchScores(ctx, cache.NonPersonalized, "latest", []string{""}, 0, len(items))
	s.NoError(err)
	s.Equal([]cache.Score{
		{Id: items[9].ItemId, Score: float64(items[9].Timestamp.Unix())},
		{Id: items[7].ItemId, Score: float64(items[7].Timestamp.Unix())},
		{Id: items[5].ItemId, Score: float64(items[5].Timestamp.Unix())},
	}, lo.Map(latest, func(document cache.Score, _ int) cache.Score {
		return cache.Score{Id: document.Id, Score: document.Score}
	}))

	// check candidate-complete scores beyond cache size
	itemIDs := lo.Map(items, func(item data.Item, _ int) string { return item.ItemId })
	candidateScores, err := s.CacheClient.GetScores(ctx, cache.NonPersonalizedCandidateScores, "candidate_latest", itemIDs)
	s.NoError(err)
	s.Len(candidateScores, len(items))
	markerValue, err := s.CacheClient.Get(ctx, cache.Key(cache.NonPersonalizedCandidateGeneration, "candidate_latest")).String()
	s.NoError(err)
	generation, err := cache.DecodeCandidateGeneration(markerValue)
	s.NoError(err)
	s.Equal(s.Config.Recommend.NonPersonalized[1].Hash(), generation.Digest)
	for _, score := range candidateScores {
		s.Equal(generation.Timestamp, score.Timestamp)
	}
	candidateTop, err := s.CacheClient.SearchScores(ctx, cache.NonPersonalized, "candidate_latest", []string{""}, 0, len(items))
	s.NoError(err)
	s.Equal([]string{items[9].ItemId, items[7].ItemId, items[5].ItemId}, cache.ConvertDocumentsToValues(candidateTop))

	// check digest
	digest, err := s.CacheClient.Get(ctx, cache.Key(cache.NonPersonalizedDigest, "latest")).String()
	s.NoError(err)
	s.Equal(s.Config.Recommend.NonPersonalized[0].Hash(), digest)
}

func (s *MasterTestSuite) TestPersistCandidateCompleteGeneration() {
	ctx := s.T().Context()
	cfg := config.NonPersonalizedConfig{
		Name:              "candidate_rank",
		Score:             "len(feedback)",
		CandidateComplete: true,
	}
	timestamp := time.Date(2026, 9, 19, 1, 2, 3, 456000000, time.UTC)
	tests := []struct {
		name       string
		failAt     string
		wantMarker bool
	}{
		{name: "add standard scores", failAt: "standard add"},
		{name: "add candidate scores", failAt: "candidate add"},
		{name: "delete stale scores", failAt: "delete"},
		{name: "write metadata", failAt: "metadata"},
		{name: "publish generation", failAt: "generation"},
		{name: "success", wantMarker: true},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			s.NoError(s.CacheClient.Purge())
			oldMarker := cache.EncodeCandidateGeneration(timestamp.Add(-time.Millisecond), "old-digest")
			s.NoError(s.CacheClient.Set(ctx, cache.String(
				cache.Key(cache.NonPersonalizedCandidateGeneration, cfg.Name), oldMarker,
			)))
			database := &failingNonPersonalizedCache{Database: s.CacheClient, failAt: test.failAt}
			recommender, err := logics.NewNonPersonalized(cfg, 1, timestamp)
			s.NoError(err)
			recommender.Push(data.Item{ItemId: "zero"}, nil)
			err = persistNonPersonalized(ctx, database, cfg, recommender)
			if test.wantMarker {
				s.NoError(err)
			} else {
				s.Error(err)
			}
			value, getErr := s.CacheClient.Get(ctx, cache.Key(cache.NonPersonalizedCandidateGeneration, cfg.Name)).String()
			s.NoError(getErr)
			if !test.wantMarker {
				s.Equal(oldMarker, value)
				return
			}
			generation, decodeErr := cache.DecodeCandidateGeneration(value)
			s.NoError(decodeErr)
			s.Equal(timestamp, generation.Timestamp)
			s.Equal(cfg.Hash(), generation.Digest)
			scores, getScoresErr := s.CacheClient.GetScores(ctx, cache.NonPersonalizedCandidateScores, cfg.Name, []string{"zero"})
			s.NoError(getScoresErr)
			s.Equal([]cache.Score{{Id: "zero", Categories: []string{""}, Timestamp: timestamp}}, scores)
		})
	}
}

func (s *MasterTestSuite) TestPersistEmptyCandidateCompleteGeneration() {
	ctx := s.T().Context()
	s.NoError(s.CacheClient.Purge())
	cfg := config.NonPersonalizedConfig{
		Name:              "candidate_rank",
		Score:             "1",
		Filter:            "false",
		CandidateComplete: true,
	}
	oldTimestamp := time.Date(2026, 9, 19, 1, 2, 3, 455000000, time.UTC)
	newTimestamp := oldTimestamp.Add(time.Millisecond)
	s.NoError(s.CacheClient.AddScores(ctx, cache.NonPersonalizedCandidateScores, cfg.Name, []cache.Score{{
		Id: "old", Score: 1, Timestamp: oldTimestamp,
	}}))
	recommender, err := logics.NewNonPersonalized(cfg, 1, newTimestamp)
	s.NoError(err)
	recommender.Push(data.Item{ItemId: "filtered"}, nil)
	s.NoError(persistNonPersonalized(ctx, s.CacheClient, cfg, recommender))

	scores, err := s.CacheClient.GetScores(ctx, cache.NonPersonalizedCandidateScores, cfg.Name, []string{"old", "filtered"})
	s.NoError(err)
	s.Empty(scores)
	value, err := s.CacheClient.Get(ctx, cache.Key(cache.NonPersonalizedCandidateGeneration, cfg.Name)).String()
	s.NoError(err)
	generation, err := cache.DecodeCandidateGeneration(value)
	s.NoError(err)
	s.Equal(newTimestamp, generation.Timestamp)
	s.Equal(cfg.Hash(), generation.Digest)
}

func (s *MasterTestSuite) TestNextCandidateGeneration() {
	ctx := s.T().Context()
	s.NoError(s.CacheClient.Purge())
	now := time.Date(2026, 9, 19, 1, 2, 3, 456789123, time.FixedZone("test", 8*60*60))
	generation, err := nextCandidateGeneration(ctx, s.CacheClient, "candidate_rank", now)
	s.NoError(err)
	s.Equal(now.UTC().Truncate(time.Millisecond), generation)
	watermark, err := s.CacheClient.Get(ctx, cache.Key(
		cache.NonPersonalizedCandidateGenerationWatermark, "candidate_rank",
	)).Time()
	s.NoError(err)
	s.Equal(generation, watermark)

	previous := generation.Add(10 * time.Millisecond)
	s.NoError(s.CacheClient.Set(ctx, cache.String(
		cache.Key(cache.NonPersonalizedCandidateGeneration, "candidate_rank"),
		cache.EncodeCandidateGeneration(previous, "digest"),
	)))
	generation, err = nextCandidateGeneration(ctx, s.CacheClient, "candidate_rank", now)
	s.NoError(err)
	s.Equal(previous.Add(time.Millisecond), generation)
}

func (s *MasterTestSuite) TestFailedCandidateGenerationIsNotReused() {
	ctx := s.T().Context()
	s.NoError(s.CacheClient.Purge())
	cfg := config.NonPersonalizedConfig{
		Name:              "candidate_rank",
		Score:             "1",
		CandidateComplete: true,
	}
	now := time.Date(2026, 9, 19, 1, 2, 3, 456789123, time.UTC)
	firstGeneration, err := nextCandidateGeneration(ctx, s.CacheClient, cfg.Name, now)
	s.NoError(err)
	first, err := logics.NewNonPersonalized(cfg, 1, firstGeneration)
	s.NoError(err)
	first.Push(data.Item{ItemId: "first"}, nil)
	s.Error(persistNonPersonalized(ctx, &failingNonPersonalizedCache{
		Database: s.CacheClient,
		failAt:   "delete",
	}, cfg, first))

	secondGeneration, err := nextCandidateGeneration(ctx, s.CacheClient, cfg.Name, now.Add(-time.Hour))
	s.NoError(err)
	s.Equal(firstGeneration.Add(time.Millisecond), secondGeneration)
	second, err := logics.NewNonPersonalized(cfg, 1, secondGeneration)
	s.NoError(err)
	second.Push(data.Item{ItemId: "second"}, nil)
	s.NoError(persistNonPersonalized(ctx, s.CacheClient, cfg, second))

	scores, err := s.CacheClient.GetScores(ctx, cache.NonPersonalizedCandidateScores, cfg.Name, []string{"first", "second"})
	s.NoError(err)
	s.Equal([]cache.Score{{
		Id:         "second",
		Score:      1,
		Categories: []string{""},
		Timestamp:  secondGeneration,
	}}, scores)
	value, err := s.CacheClient.Get(ctx, cache.Key(cache.NonPersonalizedCandidateGeneration, cfg.Name)).String()
	s.NoError(err)
	completed, err := cache.DecodeCandidateGeneration(value)
	s.NoError(err)
	s.Equal(secondGeneration, completed.Timestamp)
}

func (s *MasterTestSuite) TestGarbageCollection() {
	// create config
	s.Config = &config.Config{}
	s.Config.Master.NumJobs = 1
	s.Config.Server.APIKey = "test_api_key"
	s.Config.Recommend.NonPersonalized = []config.NonPersonalizedConfig{
		{Name: "custom", Score: "1", CandidateComplete: true},
		{Name: "disabled", Score: "1"},
	}

	// insert items
	ctx := s.T().Context()
	err := s.DataClient.BatchInsertItems(ctx, []data.Item{
		{ItemId: "1", Timestamp: time.Now(), Categories: []string{"*"}, Labels: []string{"a", "b", "c", "d"}, Comment: ""},
		{ItemId: "2", Timestamp: time.Now(), Categories: []string{"*"}, Labels: []string{}, Comment: ""},
	})
	s.NoError(err)

	// insert users
	err = s.DataClient.BatchInsertUsers(ctx, []data.User{
		{UserId: "1", Labels: []string{"a", "b", "c", "d"}, Comment: ""},
		{UserId: "2", Labels: []string{}, Comment: ""},
	})
	s.NoError(err)

	// insert non-personalized cache
	timestamp := time.Now().Add(time.Hour)
	err = s.CacheClient.AddScores(ctx, cache.NonPersonalized, "custom", []cache.Score{
		{Id: "1", Score: 1, Categories: []string{""}, Timestamp: timestamp},
		{Id: "2", Score: 2, Categories: []string{""}, Timestamp: timestamp},
	})
	s.NoError(err)
	err = s.CacheClient.AddScores(ctx, cache.NonPersonalized, "unknown", []cache.Score{
		{Id: "1", Score: 1, Categories: []string{""}, Timestamp: timestamp},
		{Id: "2", Score: 2, Categories: []string{""}, Timestamp: timestamp},
	})
	s.NoError(err)
	for _, subset := range []string{"custom", "disabled", "unknown"} {
		err = s.CacheClient.AddScores(ctx, cache.NonPersonalizedCandidateScores, subset, []cache.Score{
			{Id: "1", Score: 1, Categories: []string{""}, Timestamp: timestamp},
			{Id: "2", Score: 2, Categories: []string{""}, Timestamp: timestamp},
		})
		s.NoError(err)
		s.NoError(s.CacheClient.Set(ctx,
			cache.String(cache.Key(cache.NonPersonalizedCandidateGeneration, subset), cache.EncodeCandidateGeneration(timestamp, "digest")),
			cache.Time(cache.Key(cache.NonPersonalizedCandidateGenerationWatermark, subset), timestamp),
		))
	}
	s.NoError(s.CacheClient.Set(ctx,
		cache.String(cache.Key(cache.NonPersonalizedCandidateGeneration, "empty"), cache.EncodeCandidateGeneration(timestamp, "digest")),
		cache.Time(cache.Key(cache.NonPersonalizedCandidateGenerationWatermark, "empty"), timestamp),
	))

	// insert collaborative filtering cache
	err = s.CacheClient.AddScores(ctx, cache.CollaborativeFiltering, "1", []cache.Score{
		{Id: "1", Score: 1, Categories: []string{""}},
		{Id: "2", Score: 2, Categories: []string{""}},
	})
	s.NoError(err)
	err = s.CacheClient.AddScores(ctx, cache.CollaborativeFiltering, "3", []cache.Score{
		{Id: "1", Score: 1, Categories: []string{""}},
		{Id: "2", Score: 2, Categories: []string{""}},
	})
	s.NoError(err)

	// load dataset and run garbage collection
	datasets, err := s.loadDataset(ctx)
	s.NoError(err)
	err = s.collectGarbage(ctx, datasets.rankingDataset)
	s.NoError(err)

	// check non-personalized cache
	np, err := s.CacheClient.SearchScores(ctx, cache.NonPersonalized, "custom", nil, 0, 100)
	s.NoError(err)
	s.Equal([]string{"2", "1"}, cache.ConvertDocumentsToValues(np))
	np, err = s.CacheClient.SearchScores(ctx, cache.NonPersonalized, "unknown", nil, 0, 100)
	s.NoError(err)
	s.Empty(np)
	for _, subset := range []string{"disabled", "unknown"} {
		candidateScores, getErr := s.CacheClient.SearchScores(ctx, cache.NonPersonalizedCandidateScores, subset, nil, 0, 100)
		s.NoError(getErr)
		s.Empty(candidateScores)
	}
	candidateScores, err := s.CacheClient.SearchScores(ctx, cache.NonPersonalizedCandidateScores, "custom", nil, 0, 100)
	s.NoError(err)
	s.ElementsMatch([]string{"1", "2"}, cache.ConvertDocumentsToValues(candidateScores))
	for _, subset := range []string{"disabled", "unknown", "empty"} {
		for _, prefix := range []string{
			cache.NonPersonalizedCandidateGeneration,
			cache.NonPersonalizedCandidateGenerationWatermark,
		} {
			value, getErr := s.CacheClient.Get(ctx, cache.Key(prefix, subset)).String()
			s.NoError(getErr)
			s.Empty(value)
		}
	}
	marker, err := s.CacheClient.Get(ctx, cache.Key(cache.NonPersonalizedCandidateGeneration, "custom")).String()
	s.NoError(err)
	s.NotEmpty(marker)

	// check collaborative filtering cache
	cf, err := s.CacheClient.SearchScores(ctx, cache.CollaborativeFiltering, "1", nil, 0, 100)
	s.NoError(err)
	s.Equal([]string{"2", "1"}, cache.ConvertDocumentsToValues(cf))
	cf, err = s.CacheClient.SearchScores(ctx, cache.CollaborativeFiltering, "3", nil, 0, 100)
	s.NoError(err)
	s.Empty(cf)
}

func (s *MasterTestSuite) TestGarbageCollectionWithConcurrentValueScan() {
	ctx := s.T().Context()
	s.NoError(s.CacheClient.Purge())
	s.Config = &config.Config{}
	s.Config.Recommend.NonPersonalized = []config.NonPersonalizedConfig{{
		Name:              "active",
		Score:             "1",
		CandidateComplete: true,
	}}

	timestamp := time.Date(2026, 9, 19, 1, 2, 3, 456000000, time.UTC)
	values := []cache.Value{
		cache.String(cache.Key(cache.NonPersonalizedCandidateGeneration, "active"), cache.EncodeCandidateGeneration(timestamp, "digest")),
		cache.Time(cache.Key(cache.NonPersonalizedCandidateGenerationWatermark, "active"), timestamp),
	}
	keys := []string{
		cache.Key(cache.NonPersonalizedCandidateGeneration, "active"),
		cache.Key(cache.NonPersonalizedCandidateGenerationWatermark, "active"),
	}
	for i := range 64 {
		name := fmt.Sprintf("stale-%02d", i)
		markerKey := cache.Key(cache.NonPersonalizedCandidateGeneration, name)
		watermarkKey := cache.Key(cache.NonPersonalizedCandidateGenerationWatermark, name)
		values = append(values,
			cache.String(markerKey, cache.EncodeCandidateGeneration(timestamp, "digest")),
			cache.Time(watermarkKey, timestamp),
		)
		keys = append(keys, markerKey, watermarkKey)
	}
	s.NoError(s.CacheClient.Set(ctx, values...))

	underlying := s.CacheClient
	s.CacheClient = &concurrentScanCache{Database: underlying, keys: keys}
	defer func() { s.CacheClient = underlying }()
	s.NoError(s.collectGarbage(ctx, dataset.NewDataset(timestamp, 0, 0)))

	for _, key := range keys[:2] {
		value, err := underlying.Get(ctx, key).String()
		s.NoError(err)
		s.NotEmpty(value)
	}
	for _, key := range keys[2:] {
		value, err := underlying.Get(ctx, key).String()
		s.NoError(err)
		s.Empty(value)
	}
}

func (s *MasterTestSuite) TestLoadDataFromDatabaseInParallel() {
	ctx := s.T().Context()
	s.Config = config.GetDefaultConfig()
	s.Config.Master.NumJobs = 16
	s.Config.Recommend.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		expression.MustParseFeedbackTypeExpression("positive"),
	}
	s.Config.Recommend.DataSource.NegativeFeedbackTypes = nil
	s.Config.Recommend.DataSource.ReadFeedbackTypes = nil

	const numItems = 12000
	items := make([]data.Item, 0, numItems)
	feedbacks := make([]data.Feedback, 0, numItems)
	for i := range numItems {
		itemID := fmt.Sprintf("item-%05d", i)
		items = append(items, data.Item{
			ItemId:    itemID,
			Timestamp: time.Unix(int64(i), 0),
		})
		feedbacks = append(feedbacks, data.Feedback{
			FeedbackKey: data.FeedbackKey{
				FeedbackType: "positive",
				UserId:       "hot-user",
				ItemId:       itemID,
			},
			Timestamp: time.Unix(int64(i), 0),
		})
	}

	err := s.DataClient.BatchInsertUsers(ctx, []data.User{{UserId: "hot-user"}})
	s.NoError(err)
	err = s.DataClient.BatchInsertItems(ctx, items)
	s.NoError(err)
	err = s.DataClient.BatchInsertFeedback(ctx, feedbacks, false, false, true)
	s.NoError(err)

	datasets, err := s.loadDataset(ctx)
	s.NoError(err)
	s.Equal(1, datasets.rankingTrainSet.CountUsers())
	s.Equal(1, datasets.rankingTestSet.CountUsers())
	s.Equal(numItems, datasets.rankingTrainSet.CountFeedback()+datasets.rankingTestSet.CountFeedback())
	s.Equal(numItems, datasets.clickTrainSet.Count()+datasets.clickTestSet.Count())
	s.Equal(numItems, datasets.clickTrainSet.PositiveCount+datasets.clickTestSet.PositiveCount)
	s.Equal(0, datasets.clickTrainSet.NegativeCount+datasets.clickTestSet.NegativeCount)
}
