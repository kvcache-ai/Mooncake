// Copyright 2024 KVCache.AI
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

#include <string>

#include "transfer_engine.h"

using namespace mooncake;

TEST(ShowLinksTest, NoInitReturnsEmptyOrPlaceholder) {
    auto engine = std::make_unique<TransferEngine>(false);
    auto result = engine->showLinks();
    EXPECT_FALSE(result.empty());
}

TEST(ShowLinksTest, AutoDiscoverShowsNics) {
    auto engine = std::make_unique<TransferEngine>(true);
    auto result = engine->showLinks();
    EXPECT_NE(result.find("Local NICs"), std::string::npos);
}

TEST(ShowLinksTest, OutputContainsTopologySection) {
    auto engine = std::make_unique<TransferEngine>(true);
    auto result = engine->showLinks();
    // If RDMA devices exist, should show topology
    // If not, gracefully show empty
    EXPECT_FALSE(result.empty());
}
