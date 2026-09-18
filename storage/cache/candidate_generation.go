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

package cache

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	NonPersonalizedCandidateGeneration          = "non-personalized_candidate_generation"
	NonPersonalizedCandidateGenerationWatermark = "non-personalized_candidate_generation_watermark"
)

// CandidateGeneration 描述候选完整评分的已发布完成代。
type CandidateGeneration struct {
	Timestamp time.Time
	Digest    string
}

// EncodeCandidateGeneration 编码可持久化的候选完整评分完成代。
func EncodeCandidateGeneration(timestamp time.Time, digest string) string {
	return fmt.Sprintf("v1:%d:%s", timestamp.UTC().Truncate(time.Millisecond).UnixMilli(), digest)
}

// DecodeCandidateGeneration 解码候选完整评分完成代。
func DecodeCandidateGeneration(value string) (CandidateGeneration, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 3 || parts[0] != "v1" || parts[2] == "" {
		return CandidateGeneration{}, fmt.Errorf("invalid candidate generation")
	}
	milliseconds, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return CandidateGeneration{}, errors.WithStack(err)
	}
	return CandidateGeneration{
		Timestamp: time.UnixMilli(milliseconds).UTC(),
		Digest:    parts[2],
	}, nil
}
