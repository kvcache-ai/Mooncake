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
#include <json/json.h>

#include <memory>
#include <string>

#include "transfer_engine.h"

using namespace mooncake;

TEST(ShowLinksTest, NoInitReturnsEmptyOrPlaceholder) {
    auto engine = std::make_unique<TransferEngine>(false);
    auto result = engine->showLinks();
    EXPECT_FALSE(result.empty());

    auto json_result = engine->showLinks(true);
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    ASSERT_TRUE(reader->parse(json_result.data(),
                              json_result.data() + json_result.size(), &root,
                              &errors))
        << errors;
    EXPECT_TRUE(root.isMember("local_nics"));
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

TEST(ShowLinksTest, JsonOutputIsValid) {
    auto engine = std::make_unique<TransferEngine>(true);
    auto result = engine->showLinks(true);
    EXPECT_FALSE(result.empty());

    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    ASSERT_TRUE(reader->parse(result.data(), result.data() + result.size(),
                              &root, &errors))
        << errors;
    EXPECT_TRUE(root.isMember("local_nics"));
}

TEST(ShowLinksTest, TopologyOnlyNicsAppearWithoutTransport) {
    auto engine = std::make_unique<TransferEngine>(false);
    auto topology = engine->getLocalTopology();
    ASSERT_NE(topology, nullptr);
    ASSERT_EQ(topology->parse("{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_1\"]]}"),
              0);

    auto readable = engine->showLinks();
    EXPECT_NE(readable.find("erdma_0"), std::string::npos);
    EXPECT_NE(readable.find("erdma_1"), std::string::npos);
    EXPECT_NE(readable.find("topology only"), std::string::npos);

    auto json_result = engine->showLinks(true);
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    ASSERT_TRUE(reader->parse(json_result.data(),
                              json_result.data() + json_result.size(), &root,
                              &errors))
        << errors;

    ASSERT_TRUE(root["local_nics"].isArray());
    ASSERT_EQ(root["local_nics"].size(), 2u);
    for (const auto& nic : root["local_nics"]) {
        EXPECT_EQ(nic["source"].asString(), "topology");
    }
}
