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

#include <cstdlib>
#include <set>
#include <string>

#include "tent/common/config_lifecycle.h"

namespace mooncake {
namespace tent {
namespace {

class EnvVarGuard {
   public:
    EnvVarGuard(const char* name, const char* value) : name_(name) {
        const char* old = std::getenv(name);
        if (old) {
            old_value_ = old;
            had_value_ = true;
        }
        setenv(name, value, 1);
    }

    ~EnvVarGuard() {
        if (had_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string old_value_;
    bool had_value_{false};
};

TEST(ConfigLifecycleTest, RuntimeAllowlistPathsAreUnique) {
    std::set<std::string_view> paths;
    for (const auto& field : configFieldInventory()) {
        EXPECT_EQ(classifyConfigPath(field.path),
                  ConfigLifecycle::kRuntimeCandidate);
        EXPECT_FALSE(field.path.empty());
        EXPECT_TRUE(paths.insert(field.path).second) << field.path;
    }
}

TEST(ConfigLifecycleTest, ClassifiesRepresentativeFields) {
    EXPECT_EQ(classifyConfigPath("rpc_server_port"),
              ConfigLifecycle::kBootstrapOnly);
    EXPECT_EQ(classifyConfigPath("topology/rdma_whitelist"),
              ConfigLifecycle::kBootstrapOnly);
    EXPECT_EQ(classifyConfigPath("transports/tcp/max_retry_count"),
              ConfigLifecycle::kBootstrapOnly);
    EXPECT_EQ(classifyConfigPath("transports/hp_tcp/worker_count"),
              ConfigLifecycle::kBootstrapOnly);
    EXPECT_EQ(classifyConfigPath("staging/shutdown_drain_timeout_ms"),
              ConfigLifecycle::kBootstrapOnly);

    EXPECT_EQ(classifyConfigPath("policy"), ConfigLifecycle::kRuntimeCandidate);
    EXPECT_EQ(classifyConfigPath("qos/tenants"),
              ConfigLifecycle::kRuntimeCandidate);
    EXPECT_EQ(classifyConfigPath("runtime_queue/max_dispatch_bytes"),
              ConfigLifecycle::kRuntimeCandidate);
    EXPECT_EQ(classifyConfigPath("metrics/report_interval_seconds"),
              ConfigLifecycle::kRuntimeCandidate);
    EXPECT_EQ(classifyConfigPath("max_failover_attempts"),
              ConfigLifecycle::kRuntimeCandidate);
    EXPECT_EQ(classifyConfigPath("enable_auto_failover_on_poll"),
              ConfigLifecycle::kRuntimeCandidate);

    EXPECT_EQ(classifyConfigPath("unknown/value"),
              ConfigLifecycle::kBootstrapOnly);
    EXPECT_EQ(classifyConfigPath("policy_extra/value"),
              ConfigLifecycle::kBootstrapOnly);
}

TEST(ConfigLifecycleTest, AllowlistedFieldOverridesBootstrapDefault) {
    EXPECT_EQ(classifyConfigPath("transports/rdma/device/max_cqe"),
              ConfigLifecycle::kBootstrapOnly);
    EXPECT_EQ(classifyConfigPath("transports/rdma/enable_smart_scheduling"),
              ConfigLifecycle::kRuntimeCandidate);
    EXPECT_EQ(
        classifyConfigPath("transports/rdma/enable_smart_scheduling/extra"),
        ConfigLifecycle::kBootstrapOnly);
}

TEST(ConfigLifecycleTest, BundleViewsEnforceLifecycleBoundaries) {
    Config config;
    config.set("rpc_server_port", 18080);
    config.set("merge_requests", true);

    auto bundle = buildTentConfigBundle(config, 17);
    ASSERT_NE(bundle.bootstrap, nullptr);
    ASSERT_NE(bundle.runtime, nullptr);
    ASSERT_NE(bundle.runtime->config, nullptr);

    EXPECT_EQ(bundle.bootstrap->get("rpc_server_port", 0), 18080);
    EXPECT_FALSE(bundle.bootstrap->contains("merge_requests"));
    EXPECT_FALSE(bundle.bootstrap->get("merge_requests", false));

    EXPECT_TRUE(bundle.runtime->config->get("merge_requests", false));
    EXPECT_FALSE(bundle.runtime->config->contains("rpc_server_port"));
    EXPECT_EQ(bundle.runtime->config->get("rpc_server_port", 7), 7);
    EXPECT_EQ(bundle.runtime->generation, 17);
}

TEST(ConfigLifecycleTest, RejectsMixedLifecycleSubtreeReads) {
    Config config;
    ASSERT_TRUE(config
                    .load(R"({
                        "metrics": {
                            "enabled": true,
                            "report_interval_seconds": 5
                        },
                        "staging": {"shutdown_drain_timeout_ms": 1000},
                        "transports": {
                            "rdma": {
                                "device": {"max_cqe": 4096},
                                "strict_local_numa": true
                            }
                        }
                    })")
                    .ok());

    auto bundle = buildTentConfigBundle(config);
    const json rejected_default = {{"rejected", true}};
    std::string subtree = "unchanged";

    EXPECT_FALSE(bundle.bootstrap->contains("metrics"));
    EXPECT_FALSE(bundle.bootstrap->dumpSubtree("metrics", &subtree));
    EXPECT_EQ(subtree, "unchanged");
    EXPECT_EQ(bundle.bootstrap->get<json>("transports/rdma", rejected_default),
              rejected_default);

    EXPECT_TRUE(bundle.bootstrap->get("metrics/enabled", false));
    EXPECT_EQ(bundle.runtime->config->get("metrics/report_interval_seconds", 0),
              5);
    EXPECT_TRUE(bundle.runtime->config->get("transports/rdma/strict_local_numa",
                                            false));

    EXPECT_EQ(bundle.bootstrap->get<json>("staging", json::object()),
              json({{"shutdown_drain_timeout_ms", 1000}}));
}

TEST(ConfigLifecycleTest, RejectsNoncanonicalScalarPaths) {
    Config config;
    config.set("merge_requests", true);
    auto bundle = buildTentConfigBundle(config);

    EXPECT_TRUE(bundle.runtime->config->get("merge_requests", false));
    for (const auto* path : {"/merge_requests", "merge_requests/"}) {
        SCOPED_TRACE(path);
        EXPECT_FALSE(bundle.bootstrap->get(path, false));
        EXPECT_FALSE(bundle.bootstrap->contains(path));
        EXPECT_FALSE(bundle.runtime->config->get(path, false));
        EXPECT_FALSE(bundle.runtime->config->contains(path));
    }
}

TEST(ConfigLifecycleTest, RejectsNoncanonicalMetricsPaths) {
    Config config;
    config.set("metrics/enabled", true);
    config.set("metrics/report_interval_seconds", 5);
    auto bundle = buildTentConfigBundle(config);

    for (const auto* path :
         {"metrics/", "/metrics", "metrics//report_interval_seconds"}) {
        SCOPED_TRACE(path);
        std::string output = "unchanged";
        EXPECT_FALSE(bundle.bootstrap->dumpSubtree(path, &output));
        EXPECT_EQ(output, "unchanged");
        EXPECT_FALSE(bundle.bootstrap->contains(path));
        EXPECT_EQ(bundle.bootstrap->get<json>(path, json::object()),
                  json::object());
    }
}

TEST(ConfigLifecycleTest, RejectsRootAliasesInBothViews) {
    Config config;
    config.set("rpc_server_port", 18080);
    config.set("merge_requests", true);
    auto bundle = buildTentConfigBundle(config);
    const LifecycleConfigView* views[] = {bundle.bootstrap.get(),
                                          bundle.runtime->config.get()};

    for (const auto* view : views) {
        SCOPED_TRACE(configLifecycleName(view->lifecycle()));
        for (const auto* path : {"/", "//", ""}) {
            SCOPED_TRACE(path);
            std::string output = "unchanged";
            EXPECT_FALSE(view->dumpSubtree(path, &output));
            EXPECT_EQ(output, "unchanged");
            EXPECT_FALSE(view->contains(path));
            EXPECT_EQ(view->get<json>(path, json::object()), json::object());
        }
    }
}

TEST(ConfigLifecycleTest, SnapshotDoesNotChangeWithLegacyConfig) {
    Config config;
    config.set("rpc_server_port", 18080);
    config.set("merge_requests", true);

    auto generation_one = buildTentConfigBundle(config, 1);
    config.set("rpc_server_port", 28080);
    config.set("merge_requests", false);
    auto generation_two = buildTentConfigBundle(config, 2);

    EXPECT_EQ(generation_one.bootstrap->get("rpc_server_port", 0), 18080);
    EXPECT_TRUE(generation_one.runtime->config->get("merge_requests", false));
    EXPECT_EQ(generation_one.runtime->generation, 1);

    EXPECT_EQ(generation_two.bootstrap->get("rpc_server_port", 0), 28080);
    EXPECT_FALSE(generation_two.runtime->config->get("merge_requests", true));
    EXPECT_EQ(generation_two.runtime->generation, 2);
}

TEST(ConfigLifecycleTest, PreservesNestedBeforeFlatLookupCompatibility) {
    Config config;
    ASSERT_TRUE(config
                    .load(R"({
                        "metrics": {"http_port": 9100},
                        "metrics/http_port": 9200
                    })")
                    .ok());

    auto bundle = buildTentConfigBundle(config);
    EXPECT_EQ(bundle.bootstrap->get("metrics/http_port", 0), 9100);
}

TEST(ConfigLifecycleTest, UnmatchedFieldsAreBootstrapOnlyWithoutDiagnostics) {
    Config config;
    ASSERT_TRUE(config
                    .load(R"({
                        "mystery": 1,
                        "transports": {"unknown": {"enable": true}}
                    })")
                    .ok());

    auto bundle = buildTentConfigBundle(config);
    EXPECT_TRUE(bundle.diagnostics.empty());
    EXPECT_EQ(bundle.bootstrap->get("mystery", 0), 1);
    EXPECT_TRUE(bundle.bootstrap->get("transports/unknown/enable", false));
    EXPECT_FALSE(bundle.runtime->config->contains("mystery"));
    EXPECT_FALSE(
        bundle.runtime->config->get("transports/unknown/enable", false));
}

TEST(ConfigLifecycleTest, ReportsNonObjectRoot) {
    Config config;
    ASSERT_TRUE(config.load("[]").ok());

    auto bundle = buildTentConfigBundle(config);
    ASSERT_EQ(bundle.diagnostics.size(), 1);
    EXPECT_EQ(bundle.diagnostics[0].code, ConfigDiagnosticCode::kInvalidRoot);
    EXPECT_EQ(bundle.diagnostics[0].path, "$");
}

TEST(ConfigLifecycleTest, DefaultConfigIsAValidEmptyConfiguration) {
    Config config;

    auto bundle = buildTentConfigBundle(config);
    EXPECT_TRUE(bundle.diagnostics.empty());
}

TEST(ConfigLifecycleTest, AcceptsEmptyKnownObjects) {
    Config config;
    ASSERT_TRUE(config.load(R"({"metrics": {}, "transports": {}})").ok());

    auto bundle = buildTentConfigBundle(config);
    EXPECT_TRUE(bundle.diagnostics.empty());
}

TEST(ConfigLifecycleTest, PreservesLegacyEnvironmentPrecedence) {
    EnvVarGuard conf_guard(
        "MC_TENT_CONF",
        R"({"transports":{"rdma":{"bind_address":"10.0.0.1"}}})");
    EnvVarGuard bind_guard("MC_RDMA_BIND_ADDRESS", "10.0.0.2");

    Config config;
    ASSERT_TRUE(ConfigHelper().loadFromEnv(config).ok());
    auto bundle = buildTentConfigBundle(config);

    EXPECT_EQ(bundle.bootstrap->get("transports/rdma/bind_address", ""),
              "10.0.0.2");
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
