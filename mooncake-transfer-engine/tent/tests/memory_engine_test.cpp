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

#include "tent/common/config.h"
#include "tent/common/config_lifecycle.h"
#include "tent/memory_engine.h"

namespace mooncake {
namespace tent {
namespace {

TEST(MemoryEngineConfigTest, ReadsDefaultsAndOverrides) {
    Config config;
    MemoryEngineConfig engine_config;
    ASSERT_TRUE(MemoryEngineConfig::fromConfig(config, &engine_config).ok());
    EXPECT_EQ(engine_config.world_size, 0u);
    EXPECT_EQ(engine_config.local_rank, 0u);
    EXPECT_EQ(engine_config.host_mem_max, 0u);
    EXPECT_EQ(engine_config.device_mem_max, 0u);
    EXPECT_EQ(engine_config.join_timeout_s, 120u);
    EXPECT_TRUE(engine_config.enabled_server);
    EXPECT_TRUE(engine_config.auto_ranking);
    EXPECT_TRUE(engine_config.store_url.empty());

    config.set("metadata_type", "etcd");
    config.set("metadata_servers", "127.0.0.1:2379");
    ASSERT_TRUE(MemoryEngineConfig::fromConfig(config, &engine_config).ok());
    EXPECT_EQ(engine_config.store_url, "etcd://127.0.0.1:2379");

    config.set("memory_engine/store_url", "tcp://127.0.0.1:9999");
    ASSERT_TRUE(MemoryEngineConfig::fromConfig(config, &engine_config).ok());
    EXPECT_EQ(engine_config.store_url, "tcp://127.0.0.1:9999");

    config.set("memory_engine/enabled_server", false);
    config.set("memory_engine/world_size", 4u);
    config.set("memory_engine/auto_ranking", false);
    config.set("memory_engine/local_rank", 3u);
    config.set("memory_engine/host_mem_max", 1ULL << 30);
    config.set("memory_engine/device_mem_max", 2ULL << 30);
    config.set("memory_engine/join_timeout_s", 5u);
    ASSERT_TRUE(MemoryEngineConfig::fromConfig(config, &engine_config).ok());
    EXPECT_FALSE(engine_config.enabled_server);
    EXPECT_EQ(engine_config.world_size, 4u);
    EXPECT_FALSE(engine_config.auto_ranking);
    EXPECT_EQ(engine_config.local_rank, 3u);
    EXPECT_EQ(engine_config.host_mem_max, 1ULL << 30);
    EXPECT_EQ(engine_config.device_mem_max, 2ULL << 30);
    EXPECT_EQ(engine_config.join_timeout_s, 5u);

    EXPECT_TRUE(
        MemoryEngineConfig::fromConfig(config, nullptr).IsInvalidArgument());
}

TEST(MemoryEngineConfigTest, JoinTimeoutIsRuntimeCandidate) {
    EXPECT_EQ(classifyConfigPath("memory_engine/join_timeout_s"),
              ConfigLifecycle::kRuntimeCandidate);
    EXPECT_EQ(classifyConfigPath("memory_engine/world_size"),
              ConfigLifecycle::kBootstrapOnly);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
