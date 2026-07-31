// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "retry_utils.h"

namespace mooncake {
namespace integration {
namespace {

TEST(RetryUtilsTest, AdvancesEveryEntryOnEachRetry) {
    std::vector<TransferRequest> entries(3);
    for (int retry = 0; retry < 4; ++retry) {
        updateRetryHints(entries, retry);
        for (const auto& entry : entries) {
            EXPECT_EQ(entry.advise_retry_cnt, retry);
        }
    }
}

TEST(RetryUtilsTest, EmptyBatchIsSafe) {
    std::vector<TransferRequest> entries;
    updateRetryHints(entries, 2);
    EXPECT_TRUE(entries.empty());
}

}  // namespace
}  // namespace integration
}  // namespace mooncake
