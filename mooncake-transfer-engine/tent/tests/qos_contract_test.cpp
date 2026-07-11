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
#include "tent/runtime/qos_contract.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

void loadConfig(Config* config, const std::string& json_text) {
    ASSERT_NE(config, nullptr);
    auto status = config->load(json_text);
    ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(QosContractTest, EmptyConfigIsDisabledAndPreservesRequestPriority) {
    Config config;
    QosContractResolver resolver;
    auto status = resolver.loadFromConfig(config);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_FALSE(resolver.enabled());

    EffectiveQosPolicy effective;
    status = resolver.resolve({.tenant = "tenant-a",
                               .intent = "foreground_get",
                               .policy_name = std::nullopt,
                               .requested_priority = PRIO_LOW},
                              &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_FALSE(effective.enabled);
    EXPECT_EQ(effective.effective_priority, PRIO_LOW);
    EXPECT_EQ(effective.matched_contract, "compatibility_default");
}

TEST(QosContractTest, ResolvesTenantIntentContractWithInheritedDefaults) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "version": 1,
    "defaults": {
      "priority": "medium",
      "weight": 1,
      "max_inflight_requests": 128,
      "allowed_degraded_actions": ["fallback_transport", "reject"]
    },
    "intent_defaults": {
      "foreground_get": {
        "deadline_profile": "interactive",
        "weight": 4
      }
    },
    "tenants": [
      {
        "name": "tenant-a",
        "defaults": {
          "max_inflight_bytes": "1GiB"
        },
        "intents": {
          "foreground_get": {
            "name": "tenant-a-fg",
            "priority": "high",
            "min_bandwidth_gbps": 40,
            "max_bandwidth_gbps": 100,
            "weight": 8,
            "burst_bytes": "2GiB",
            "max_inflight_bytes": "4GiB",
            "max_inflight_requests": 4096,
            "allowed_degraded_actions": ["fallback_transport", "local_recompute", "reject"]
          }
        }
      }
    ]
  }
}
)json");

    QosContractResolver resolver;
    auto status = resolver.loadFromConfig(config);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(resolver.enabled());

    EffectiveQosPolicy effective;
    status = resolver.resolve({.tenant = "Tenant-A",
                               .intent = "Foreground_Get",
                               .policy_name = std::nullopt,
                               .requested_priority = PRIO_LOW},
                              &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(effective.enabled);
    EXPECT_TRUE(effective.matched);
    EXPECT_EQ(effective.tenant, "tenant-a");
    EXPECT_EQ(effective.intent, "foreground_get");
    EXPECT_EQ(effective.matched_contract, "tenant-a.foreground_get");
    ASSERT_TRUE(effective.priority.has_value());
    EXPECT_EQ(*effective.priority, PRIO_HIGH);
    EXPECT_EQ(effective.effective_priority, PRIO_HIGH);
    ASSERT_TRUE(effective.min_bandwidth_gbps.has_value());
    EXPECT_DOUBLE_EQ(*effective.min_bandwidth_gbps, 40.0);
    ASSERT_TRUE(effective.max_bandwidth_gbps.has_value());
    EXPECT_DOUBLE_EQ(*effective.max_bandwidth_gbps, 100.0);
    ASSERT_TRUE(effective.weight.has_value());
    EXPECT_EQ(*effective.weight, 8u);
    ASSERT_TRUE(effective.burst_bytes.has_value());
    EXPECT_EQ(*effective.burst_bytes, 2ull * 1024 * 1024 * 1024);
    ASSERT_TRUE(effective.max_inflight_bytes.has_value());
    EXPECT_EQ(*effective.max_inflight_bytes, 4ull * 1024 * 1024 * 1024);
    ASSERT_TRUE(effective.max_inflight_requests.has_value());
    EXPECT_EQ(*effective.max_inflight_requests, 4096u);
    ASSERT_TRUE(effective.deadline_profile.has_value());
    EXPECT_EQ(*effective.deadline_profile, "interactive");
    ASSERT_TRUE(effective.allowed_degraded_actions.has_value());
    EXPECT_EQ(effective.allowed_degraded_actions->size(), 3u);

    auto explain = nlohmann::json::parse(resolver.explainJson(effective));
    EXPECT_EQ(explain["matched_contract"], "tenant-a.foreground_get");
    EXPECT_EQ(explain["effective_priority"], PRIO_HIGH);
    EXPECT_EQ(explain["enforcement"]["bandwidth_cap"], "planned");
}

TEST(QosContractTest, ExplicitPolicyNameCanResolveNamedContract) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "version": 1,
    "defaults": {"priority": "low"},
    "tenants": [
      {
        "name": "tenant-a",
        "intents": {
          "checkpoint": {
            "name": "bulk-checkpoint",
            "priority": "low",
            "max_bandwidth_gbps": 10,
            "burst_bytes": "256MiB"
          }
        }
      }
    ]
  }
}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant = "other",
                                    .intent = "foreground_get",
                                    .policy_name = "bulk-checkpoint",
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(effective.matched);
    EXPECT_EQ(effective.matched_contract, "bulk-checkpoint");
    EXPECT_EQ(effective.effective_priority, PRIO_LOW);
    ASSERT_TRUE(effective.max_bandwidth_gbps.has_value());
    EXPECT_DOUBLE_EQ(*effective.max_bandwidth_gbps, 10.0);
}

TEST(QosContractTest, UnknownTenantFallsBackInCompatibilityMode) {
    Config config;
    loadConfig(&config, R"json(
{"qos":{"version":1,"defaults":{"priority":"medium","max_inflight_bytes":"64MiB"}}}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant = "missing",
                                    .intent = "foreground_get",
                                    .policy_name = std::nullopt,
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_FALSE(effective.matched);
    EXPECT_EQ(effective.matched_contract, "global_default");
    EXPECT_EQ(effective.effective_priority, PRIO_MEDIUM);
    ASSERT_TRUE(effective.max_inflight_bytes.has_value());
    EXPECT_EQ(*effective.max_inflight_bytes, 64ull * 1024 * 1024);
}

TEST(QosContractTest, StrictModeRejectsUnknownTenant) {
    Config config;
    loadConfig(&config, R"json(
{"qos":{"version":1,"strict_mode":true,"defaults":{"priority":"medium"}}}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant = "missing",
                                    .intent = "foreground_get",
                                    .policy_name = std::nullopt,
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());
}

TEST(QosContractTest, EmptyAllowedDegradedActionsOverridesInherited) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "version": 1,
    "defaults": {
      "allowed_degraded_actions": ["fallback_transport", "reject"]
    },
    "tenants": [
      {
        "name": "tenant-a",
        "intents": {
          "foreground_get": {
            "allowed_degraded_actions": []
          }
        }
      }
    ]
  }
}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant = "tenant-a",
                                    .intent = "foreground_get",
                                    .policy_name = std::nullopt,
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(effective.allowed_degraded_actions.has_value());
    EXPECT_TRUE(effective.allowed_degraded_actions->empty());
}

TEST(QosContractTest, MergedBandwidthRangeFailsClosed) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "version": 1,
    "defaults": {"max_bandwidth_gbps": 10},
    "intent_defaults": {"foreground_get": {"min_bandwidth_gbps": 20}}
  }
}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant = "tenant-a",
                                    .intent = "foreground_get",
                                    .policy_name = std::nullopt,
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());
}

TEST(QosContractTest, InvalidSchemaFailsClosed) {
    Config config;
    loadConfig(&config, R"json(
{"qos":{"version":1,"defaults":{"priority":"urgent"}}}
)json");
    QosContractResolver resolver;
    auto status = resolver.loadFromConfig(config);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());

    Config bad_bw;
    loadConfig(&bad_bw, R"json(
{"qos":{"version":1,"defaults":{"min_bandwidth_gbps":20,"max_bandwidth_gbps":10}}}
)json");
    status = resolver.loadFromConfig(bad_bw);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());

    Config bad_action;
    loadConfig(&bad_action, R"json(
{"qos":{"version":1,"defaults":{"allowed_degraded_actions":["teleport"]}}}
)json");
    status = resolver.loadFromConfig(bad_action);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());

    Config bad_request_count;
    loadConfig(&bad_request_count, R"json(
{"qos":{"version":1,"defaults":{"max_inflight_requests":"64MiB"}}}
)json");
    status = resolver.loadFromConfig(bad_request_count);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());

    Config unknown_key;
    loadConfig(&unknown_key, R"json(
{"qos":{"version":1,"defaults":{"unexpected_qos_key":10}}}
)json");
    status = resolver.loadFromConfig(unknown_key);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());

    Config bad_version_type;
    loadConfig(&bad_version_type, R"json(
{"qos":{"version":"1","defaults":{"priority":"high"}}}
)json");
    status = resolver.loadFromConfig(bad_version_type);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());
}

TEST(QosContractTest, IntentTypeNamesAreStable) {
    EXPECT_EQ(QosContractResolver::intentTypeName(IntentType::INTENT_UNSPEC),
              "unspec");
    EXPECT_EQ(QosContractResolver::intentTypeName(IntentType::FOREGROUND_GET),
              "foreground_get");
    EXPECT_EQ(
        QosContractResolver::intentTypeName(IntentType::BACKGROUND_PREFETCH),
        "background_prefetch");
    EXPECT_EQ(QosContractResolver::intentTypeName(IntentType::MIGRATION),
              "migration");
    EXPECT_EQ(QosContractResolver::intentTypeName(IntentType::CHECKPOINT),
              "checkpoint");
    EXPECT_EQ(QosContractResolver::intentTypeName(IntentType::WEIGHT_LOADING),
              "weight_loading");
    EXPECT_EQ(QosContractResolver::intentTypeName(IntentType::STAGING_INTERNAL),
              "staging_internal");
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
