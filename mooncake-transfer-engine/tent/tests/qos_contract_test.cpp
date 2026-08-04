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
    status = resolver.resolve({.tenant_id = "tenant-a",
                               .intent = "foreground_get",
                               .requested_priority = PRIO_LOW},
                              &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_FALSE(effective.enabled);
    EXPECT_EQ(effective.effective_priority, PRIO_LOW);
    EXPECT_EQ(effective.matched_contract, "compatibility_default");
}

TEST(QosContractTest, ResolvesGlobalTenantAndIntentLayers) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "version": 1,
    "defaults": {
      "priority": "medium",
      "max_inflight_requests": 128,
      "allowed_degraded_actions": ["fallback_transport", "reject"]
    },
    "tenants": [
      {
        "name": "tenant-a",
        "defaults": {"max_inflight_bytes": "1GiB"},
        "intents": {
          "foreground_get": {
            "priority": "high",
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
    status = resolver.resolve({.tenant_id = " Tenant-A ",
                               .intent = "Foreground_Get",
                               .requested_priority = PRIO_LOW},
                              &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(effective.enabled);
    EXPECT_TRUE(effective.matched);
    EXPECT_EQ(effective.tenant_id, "tenant-a");
    EXPECT_EQ(effective.intent, "foreground_get");
    EXPECT_EQ(effective.matched_contract, "tenant-a.foreground_get");
    ASSERT_TRUE(effective.priority.has_value());
    EXPECT_EQ(*effective.priority, PRIO_HIGH);
    EXPECT_EQ(effective.effective_priority, PRIO_HIGH);
    ASSERT_TRUE(effective.max_inflight_bytes.has_value());
    EXPECT_EQ(*effective.max_inflight_bytes, 4ull * 1024 * 1024 * 1024);
    ASSERT_TRUE(effective.max_inflight_requests.has_value());
    EXPECT_EQ(*effective.max_inflight_requests, 4096u);
    ASSERT_TRUE(effective.allowed_degraded_actions.has_value());
    EXPECT_EQ(effective.allowed_degraded_actions->size(), 3u);

    auto explain = nlohmann::json::parse(resolver.explainJson(effective));
    EXPECT_EQ(explain["tenant_id"], "tenant-a");
    EXPECT_EQ(explain["matched_contract"], "tenant-a.foreground_get");
    EXPECT_EQ(explain["effective_priority"], PRIO_HIGH);
    EXPECT_EQ(explain["diagnostic_scope"], "resolution_only");
    EXPECT_FALSE(explain.contains("enforcement"));
    EXPECT_FALSE(explain.contains("max_bandwidth_gbps"));
    EXPECT_FALSE(explain.contains("weight"));
    EXPECT_FALSE(explain.contains("deadline_profile"));
}

TEST(QosContractTest, TenantDefaultAppliesWithoutIntentMatch) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "defaults": {"priority": "low", "max_inflight_requests": 16},
    "tenants": [
      {
        "name": "tenant-a",
        "defaults": {"priority": "medium", "max_inflight_bytes": "64MiB"}
      }
    ]
  }
}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant_id = "tenant-a",
                                    .intent = "checkpoint",
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_FALSE(effective.matched);
    EXPECT_EQ(effective.matched_contract, "tenant-a.default");
    EXPECT_EQ(effective.effective_priority, PRIO_MEDIUM);
    EXPECT_EQ(*effective.max_inflight_bytes, 64ull * 1024 * 1024);
    EXPECT_EQ(*effective.max_inflight_requests, 16u);
}

TEST(QosContractTest, UnknownTenantFallsBackToGlobalDefaults) {
    Config config;
    loadConfig(&config, R"json(
{"qos":{"version":1,"defaults":{"priority":"medium","max_inflight_bytes":"64MiB"}}}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant_id = "missing",
                                    .intent = "foreground_get",
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_FALSE(effective.matched);
    EXPECT_EQ(effective.matched_contract, "global_default");
    EXPECT_EQ(effective.effective_priority, PRIO_MEDIUM);
    ASSERT_TRUE(effective.max_inflight_bytes.has_value());
    EXPECT_EQ(*effective.max_inflight_bytes, 64ull * 1024 * 1024);
}

TEST(QosContractTest, EmptyIdentityNormalizesToCompatibilityKeys) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "defaults": {"priority": "low"},
    "tenants": [
      {"name": "default", "intents": {"unspec": {"priority": "medium"}}}
    ]
  }
}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve(
        {.tenant_id = "", .intent = "", .requested_priority = PRIO_HIGH},
        &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(effective.tenant_id, "default");
    EXPECT_EQ(effective.intent, "unspec");
    EXPECT_EQ(effective.matched_contract, "default.unspec");
    EXPECT_EQ(effective.effective_priority, PRIO_MEDIUM);
}

TEST(QosContractTest, EmptyAllowedDegradedActionsOverridesInherited) {
    Config config;
    loadConfig(&config, R"json(
{
  "qos": {
    "defaults": {
      "allowed_degraded_actions": ["fallback_transport", "reject"]
    },
    "tenants": [
      {
        "name": "tenant-a",
        "intents": {
          "foreground_get": {"allowed_degraded_actions": []}
        }
      }
    ]
  }
}
)json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(config).ok());

    EffectiveQosPolicy effective;
    auto status = resolver.resolve({.tenant_id = "tenant-a",
                                    .intent = "foreground_get",
                                    .requested_priority = PRIO_HIGH},
                                   &effective);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(effective.allowed_degraded_actions.has_value());
    EXPECT_TRUE(effective.allowed_degraded_actions->empty());
}

TEST(QosContractTest, DeferredM3ToM5FieldsFailClosed) {
    static const std::vector<std::string> kDeferredFields = {
        R"json("min_bandwidth_gbps":20)json",
        R"json("max_bandwidth_gbps":100)json",
        R"json("burst_bytes":"1GiB")json", R"json("weight":8)json",
        R"json("deadline_profile":"interactive")json"};

    for (const auto& field : kDeferredFields) {
        Config config;
        loadConfig(&config, "{\"qos\":{\"defaults\":{" + field + "}}}");
        QosContractResolver resolver;
        auto status = resolver.loadFromConfig(config);
        EXPECT_FALSE(status.ok()) << field;
        EXPECT_TRUE(status.IsInvalidArgument()) << field;
        EXPECT_FALSE(resolver.enabled()) << field;
    }
}

TEST(QosContractTest, DeferredResolutionFeaturesFailClosed) {
    static const std::vector<std::string> kConfigs = {
        R"json({"qos":{"strict_mode":true,"defaults":{"priority":"high"}}})json",
        R"json({"qos":{"intent_defaults":{"foreground_get":{"priority":"high"}}}})json",
        R"json({"qos":{"tenants":[{"name":"tenant-a","intents":{"foreground_get":{"name":"named-policy","priority":"high"}}}]}})json"};

    for (const auto& text : kConfigs) {
        Config config;
        loadConfig(&config, text);
        QosContractResolver resolver;
        auto status = resolver.loadFromConfig(config);
        EXPECT_FALSE(status.ok()) << text;
        EXPECT_TRUE(status.IsInvalidArgument()) << text;
        EXPECT_FALSE(resolver.enabled()) << text;
    }
}

TEST(QosContractTest, InvalidSchemaFailsClosedAndClearsPriorState) {
    Config valid;
    loadConfig(&valid, R"json({"qos":{"defaults":{"priority":"medium"}}})json");
    QosContractResolver resolver;
    ASSERT_TRUE(resolver.loadFromConfig(valid).ok());
    ASSERT_TRUE(resolver.enabled());

    static const std::vector<std::string> kInvalidConfigs = {
        R"json({"qos":{"defaults":{"priority":"urgent"}}})json",
        R"json({"qos":{"defaults":{"allowed_degraded_actions":["teleport"]}}})json",
        R"json({"qos":{"defaults":{"max_inflight_requests":"64MiB"}}})json",
        R"json({"qos":{"defaults":{"unexpected_qos_key":10}}})json",
        R"json({"qos":{"version":"1","defaults":{"priority":"high"}}})json"};

    for (const auto& text : kInvalidConfigs) {
        Config config;
        loadConfig(&config, text);
        auto status = resolver.loadFromConfig(config);
        EXPECT_FALSE(status.ok()) << text;
        EXPECT_TRUE(status.IsInvalidArgument()) << text;
        EXPECT_FALSE(resolver.enabled()) << text;
    }
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
