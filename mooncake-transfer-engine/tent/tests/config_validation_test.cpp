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

#include <algorithm>
#include <limits>

#include "tent/common/config_validation.h"
#include "tent/common/config_parser.h"

namespace mooncake {
namespace tent {
namespace {

using Disposition = ConfigChangeDisposition;

const ConfigValidationContext kCpuBuild{{"tcp", "hp_tcp", "shm"}, 16};

bool hasError(const Status& status, const std::string& reason) {
    return status.IsInvalidArgument() &&
           status.message().find(reason) != std::string_view::npos;
}
bool hasChange(const ConfigChangePlan& plan, const std::string& path,
               Disposition disposition) {
    return std::any_of(
        plan.changes.begin(), plan.changes.end(), [&](const ConfigChange& c) {
            return c.path == path && c.disposition == disposition;
        });
}

TEST(ConfigValidationTest, EmptyAndExplicitDefaultsHaveNoChanges) {
    Config current, candidate;
    ASSERT_TRUE(candidate
                    .load(R"({"rpc_server_port":0,"rpc_server_threads":1,
      "rpc_server_hostname":"","merge_requests":true,
      "max_failover_attempts":3,"enable_auto_failover_on_poll":true,
      "enable_runtime_queue":false,"enable_progress_worker":false,
      "runtime_queue":{"max_outstanding_owners":1024,
        "max_outstanding_bytes":1073741824,"staging_owner_reserve":0,
        "staging_byte_reserve":0,"deadline_aware":false,
        "mlu_local_threshold":0,"promotion_slack_ns":0,
        "max_dispatch_owners":64,"max_dispatch_bytes":67108864,
        "progress_fallback_interval_us":50000}})")
                    .ok());
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(plan.valid());
    EXPECT_TRUE(plan.changes.empty());
    EXPECT_TRUE(current.toJson().is_null());
    auto copy = candidate.toJson();
    copy["rpc_server_port"] = 9;
    EXPECT_EQ(candidate.get("rpc_server_port", -1), 0);
}

TEST(ConfigValidationTest, SeparatesRuntimeRestartAndUnsupportedChanges) {
    Config current, candidate;
    current.set("rpc_server_port", 18080);
    candidate.set("rpc_server_port", 28080);
    candidate.set("merge_requests", false);
    candidate.set("runtime_queue/typo", 5);
    // PR1 permits the entire policy subtree. That is not validation coverage.
    candidate.set("policy", json::array());
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    ASSERT_TRUE(plan.valid());
    ASSERT_EQ(plan.changes.size(), 4);
    EXPECT_TRUE(
        hasChange(plan, "rpc_server_port", Disposition::kRestartRequired));
    EXPECT_TRUE(
        hasChange(plan, "merge_requests", Disposition::kRuntimeCandidate));
    EXPECT_TRUE(
        hasChange(plan, "runtime_queue/typo", Disposition::kUnsupported));
    EXPECT_TRUE(hasChange(plan, "policy", Disposition::kUnsupported));
    EXPECT_TRUE(std::is_sorted(
        plan.changes.begin(), plan.changes.end(),
        [](const auto& a, const auto& b) { return a.path < b.path; }));
}

TEST(ConfigValidationTest,
     RemovalRestoresDefaultAndUnknownRemovalIsUnsupported) {
    Config current, candidate;
    current.set("max_failover_attempts", 0);
    current.set("rpc_server_port", 1234);
    current.set("custom_backend/token", "do-not-print-this-value");
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    ASSERT_TRUE(plan.valid());
    ASSERT_EQ(plan.changes.size(), 3);
    EXPECT_TRUE(hasChange(plan, "max_failover_attempts",
                          Disposition::kRuntimeCandidate));
    EXPECT_TRUE(
        hasChange(plan, "rpc_server_port", Disposition::kRestartRequired));
    EXPECT_TRUE(
        hasChange(plan, "custom_backend/token", Disposition::kUnsupported));
}

TEST(ConfigValidationTest, UnchangedUnknownFieldsCanBeCarriedThrough) {
    Config current, candidate;
    ASSERT_TRUE(current.load(R"({"plugin":{"custom":[1,2,3]}})").ok());
    ASSERT_TRUE(candidate.load(current.dump()).ok());
    candidate.set("max_failover_attempts", 7);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    ASSERT_TRUE(plan.valid());
    ASSERT_EQ(plan.changes.size(), 1);
    EXPECT_TRUE(hasChange(plan, "max_failover_attempts",
                          Disposition::kRuntimeCandidate));
}

TEST(ConfigValidationTest, DoesNotHideTypeChangesInUnknownArrays) {
    Config current, candidate;
    ASSERT_TRUE(current.load(R"({"policy":[{"value":1}]})").ok());
    ASSERT_TRUE(candidate.load(R"({"policy":[{"value":1.0}]})").ok());
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    ASSERT_TRUE(plan.valid());
    ASSERT_EQ(plan.changes.size(), 1);
    EXPECT_TRUE(hasChange(plan, "policy", Disposition::kUnsupported));
}

TEST(ConfigValidationTest, AcceptsFlatAndNestedRepresentationsAndRpcStrings) {
    Config current, candidate;
    ASSERT_TRUE(current
                    .load(R"({"runtime_queue/max_dispatch_owners":8,
      "rpc_server_port":"  +18080","rpc_server_threads":"4"})")
                    .ok());
    ASSERT_TRUE(candidate
                    .load(R"({"runtime_queue":{"max_dispatch_owners":8},
      "rpc_server_port":18080,"rpc_server_threads":4})")
                    .ok());
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(plan.valid());
    EXPECT_TRUE(plan.changes.empty());
    // Repeating the same alias is unambiguous.
    ASSERT_TRUE(candidate
                    .load(R"({"runtime_queue/max_dispatch_owners":8,
      "runtime_queue":{"max_dispatch_owners":8},
      "rpc_server_port":18080,"rpc_server_threads":4})")
                    .ok());
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
}

TEST(ConfigValidationTest,
     RejectsConflictingAliasesWithoutChangingLegacyLookup) {
    Config current, candidate;
    ASSERT_TRUE(candidate
                    .load(R"({"runtime_queue/max_dispatch_owners":9,
      "runtime_queue":{"max_dispatch_owners":8}})")
                    .ok());
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_FALSE(plan.valid());
    EXPECT_TRUE(plan.changes.empty());
    EXPECT_TRUE(
        hasError(plan.candidate_status, "runtime_queue/max_dispatch_owners"));
    EXPECT_EQ(candidate.get("runtime_queue/max_dispatch_owners", 0), 8);
}

TEST(ConfigValidationTest, RejectsMalformedPathsAndRootShapes) {
    Config current;
    for (const auto* input :
         {R"({"":1})", R"({"/merge_requests":false})",
          R"({"runtime_queue//max_dispatch_owners":1})",
          R"({"runtime_queue/":{}})", R"({"transports":{"tcp/enable":false}})",
          R"({"transports/tcp":{"enable":false}})"}) {
        Config candidate;
        ASSERT_TRUE(candidate.load(input).ok());
        auto plan = planTentConfigChange(current, candidate, kCpuBuild);
        EXPECT_FALSE(plan.valid()) << input;
        EXPECT_TRUE(plan.changes.empty());
        EXPECT_TRUE(plan.candidate_status.IsInvalidArgument());
    }
    for (const auto* input : {"[]", "123", "true", "\"object\""}) {
        Config candidate;
        ASSERT_TRUE(candidate.load(input).ok());
        auto plan = planTentConfigChange(current, candidate, kCpuBuild);
        EXPECT_TRUE(hasError(plan.candidate_status, "$"));
    }
}

TEST(ConfigValidationTest, NullAndEmptyGroupsUseDefaults) {
    Config current, candidate;
    ASSERT_TRUE(candidate
                    .load(R"({"rpc_server_port":null,
      "runtime_queue":{},"transports":null})")
                    .ok());
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(plan.valid());
    EXPECT_TRUE(plan.changes.empty());
    candidate.set("runtime_queue", 123);
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "runtime_queue"));
}

TEST(ConfigValidationTest, RejectsWrongTypesInsteadOfSilentlyDefaulting) {
    Config current;
    const std::vector<std::pair<std::string, json>> bad = {
        {"merge_requests", "false"},
        {"enable_runtime_queue", 1},
        {"enable_auto_failover_on_poll", "true"},
        {"max_failover_attempts", 1.5},
        {"max_failover_attempts", "3"},
        {"rpc_server_hostname", false},
        {"rpc_server_threads", "4junk"},
        {"rpc_server_port", 1.0},
        {"runtime_queue/max_outstanding_bytes", "1024"},
        {"runtime_queue/deadline_aware", json::array()},
        {"runtime_queue/mlu_local_threshold", "1.5"},
        {"transports/rdma/enable", 1},
        {"rpc_server_port", json::object()}};
    for (const auto& [path, value] : bad) {
        Config candidate;
        candidate.set(path, value);
        auto plan = planTentConfigChange(current, candidate, kCpuBuild);
        EXPECT_FALSE(plan.valid()) << path;
        EXPECT_TRUE(hasError(plan.candidate_status, path));
    }
}

TEST(ConfigValidationTest, ChecksRangeBeforeIntegerNarrowing) {
    Config current;
    const std::vector<std::pair<std::string, json>> bad = {
        {"rpc_server_port", -1},
        {"rpc_server_port", 65536},
        {"rpc_server_port", std::numeric_limits<uint64_t>::max()},
        {"rpc_server_threads", 0},
        {"rpc_server_threads", 1025},
        {"max_failover_attempts", -1},
        {"max_failover_attempts",
         static_cast<uint64_t>(std::numeric_limits<int>::max()) + 1},
        {"runtime_queue/max_outstanding_bytes", -1},
        {"runtime_queue/promotion_slack_ns", -1},
        {"runtime_queue/progress_fallback_interval_us",
         std::numeric_limits<uint64_t>::max()}};
    for (const auto& [path, value] : bad) {
        Config candidate;
        candidate.set(path, value);
        auto plan = planTentConfigChange(current, candidate, kCpuBuild);
        EXPECT_TRUE(hasError(plan.candidate_status, path)) << path;
    }
    Config candidate;
    candidate.set("rpc_server_port", 65535);
    candidate.set("rpc_server_threads", 1024);
    candidate.set("max_failover_attempts", 0);
    candidate.set("runtime_queue/promotion_slack_ns",
                  std::numeric_limits<uint64_t>::max());
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
}

TEST(ConfigValidationTest, PreservesNonFiniteNumbersForValidation) {
    Config current;
    for (auto value : {std::numeric_limits<double>::infinity(),
                       std::numeric_limits<double>::quiet_NaN()}) {
        Config candidate;
        candidate.set("runtime_queue/mlu_local_threshold", value);
        auto plan = planTentConfigChange(current, candidate, kCpuBuild);
        EXPECT_TRUE(hasError(plan.candidate_status,
                             "runtime_queue/mlu_local_threshold"));
    }
}

TEST(ConfigValidationTest, QueueReservesCannotExceedCapacity) {
    Config current, candidate;
    candidate.set("runtime_queue/max_outstanding_owners", 1);
    candidate.set("runtime_queue/staging_owner_reserve", 2);
    candidate.set("runtime_queue/max_outstanding_bytes", 1);
    candidate.set("runtime_queue/staging_byte_reserve", 2);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_FALSE(plan.valid());
    EXPECT_TRUE(hasError(plan.candidate_status, "staging owner reserve"));
    candidate.set("runtime_queue/staging_owner_reserve", 1);
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "staging byte reserve"));
    candidate.set("runtime_queue/staging_byte_reserve", 1);
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
}

TEST(ConfigValidationTest, QueueDispatchWindowsAreRequiredOnlyWhenEnabled) {
    Config current, candidate;
    candidate.set("runtime_queue/max_dispatch_owners", 0);
    candidate.set("runtime_queue/max_dispatch_bytes", 0);
    candidate.set("runtime_queue/max_outstanding_owners", 0);
    candidate.set("runtime_queue/max_outstanding_bytes", 0);
    candidate.set("runtime_queue/mlu_local_threshold", -1.0);
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
    candidate.set("enable_runtime_queue", true);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "dispatch window"));
    candidate.set("runtime_queue/max_dispatch_owners", 1);
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "dispatch window"));
    candidate.set("runtime_queue/max_dispatch_bytes", 1);
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
}

TEST(ConfigValidationTest, AccountsForDerivedProgressWorkerAndRpcDefaults) {
    Config current, candidate;
    current.set("enable_runtime_queue", true);
    candidate.set("enable_runtime_queue", true);
    candidate.set("enable_progress_worker", true);
    EXPECT_TRUE(
        planTentConfigChange(current, candidate, kCpuBuild).changes.empty());
    current.set("transports/tcp/enable", true);
    candidate.set("transports/tcp/enable", true);
    candidate.set("rpc_server_threads", 8);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(plan.valid());
    EXPECT_TRUE(plan.changes.empty());
    candidate.set("transports/tcp/enable", false);
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasChange(plan, "transports/tcp/enable",
                          Disposition::kRestartRequired));
    // Explicit 8 preserves the previous effective thread count.
    EXPECT_FALSE(
        hasChange(plan, "rpc_server_threads", Disposition::kRestartRequired));
}

TEST(ConfigValidationTest, RpcDefaultChangesWithTcpEnablement) {
    Config current, candidate;
    candidate.set("transports/tcp/enable", true);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(plan.valid());
    EXPECT_TRUE(
        hasChange(plan, "rpc_server_threads", Disposition::kRestartRequired));
    for (const auto count : {0U, 2U, 6U, 128U}) {
        const auto expected = std::min(8U, std::max(4U, count));
        Config explicit_threads;
        explicit_threads.set("transports/tcp/enable", true);
        explicit_threads.set("rpc_server_threads", expected);
        const ConfigValidationContext context{{"tcp", "hp_tcp", "shm"}, count};
        EXPECT_TRUE(planTentConfigChange(candidate, explicit_threads, context)
                        .changes.empty());
    }
}

TEST(ConfigValidationTest, BuildCapabilitiesApplyOnlyToExplicitEnables) {
    Config current, candidate;
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
    candidate.set("transports/rdma/enable", true);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "transports/rdma/enable"));
    auto rdma_build = kCpuBuild;
    rdma_build.compiled_transports.push_back("rdma");
    plan = planTentConfigChange(current, candidate, rdma_build);
    EXPECT_TRUE(plan.valid());
    EXPECT_TRUE(hasChange(plan, "transports/rdma/enable",
                          Disposition::kRestartRequired));
    candidate.set("transports/rdma/enable", false);
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
    candidate.set("transports/unknown/enable", true);
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasChange(plan, "transports/unknown/enable",
                          Disposition::kUnsupported));
}

TEST(ConfigValidationTest, HpTcpRequiresExplicitlyDisablingDefaultTcp) {
    Config current, candidate;
    candidate.set("transports/hp_tcp/enable", true);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status,
                         "tcp and hp_tcp cannot be enabled together"));
    candidate.set("transports/tcp/enable", false);
    EXPECT_TRUE(planTentConfigChange(current, candidate, kCpuBuild).valid());
    ASSERT_TRUE(candidate
                    .load(R"({"transports/hp_tcp/enable":true,
      "transports/tcp/enable":false})")
                    .ok());
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "transports/hp_tcp/enable"));
    ASSERT_TRUE(
        candidate.load(R"({"transports":{"hp_tcp":{"enable":null}}})").ok());
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "transports/hp_tcp/enable"));
}

TEST(ConfigValidationTest, OwnsInputsAndDoesNotChangePublishedSnapshots) {
    Config current, candidate;
    current.set("max_failover_attempts", 1);
    candidate.set("max_failover_attempts", 5);
    auto active = buildTentConfigBundle(current, 42).runtime;
    auto before = current.dump();
    auto next = candidate.dump();
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    ASSERT_TRUE(plan.valid());
    EXPECT_EQ(current.dump(), before);
    EXPECT_EQ(candidate.dump(), next);
    current.set("max_failover_attempts", 8);
    candidate.set("max_failover_attempts", 9);
    EXPECT_EQ(plan.current->get("max_failover_attempts", -1), 1);
    EXPECT_EQ(plan.candidate->get("max_failover_attempts", -1), 5);
    EXPECT_EQ(active->generation, 42);
    EXPECT_EQ(active->max_failover_attempts, 1);
    candidate.set("rpc_server_port", -1);
    auto rejected = planTentConfigChange(*plan.current, candidate, kCpuBuild);
    EXPECT_FALSE(rejected.valid());
    EXPECT_TRUE(rejected.changes.empty());
    EXPECT_EQ(active->generation, 42);
    EXPECT_EQ(active->max_failover_attempts, 1);
}

TEST(ConfigValidationTest, ReportsInvalidCurrentInputSeparately) {
    Config current, candidate;
    current.set("rpc_server_port", -1);
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_FALSE(plan.valid());
    EXPECT_FALSE(plan.current_status.ok());
    EXPECT_TRUE(plan.candidate_status.ok());
    EXPECT_TRUE(plan.changes.empty());
}

TEST(ConfigValidationTest, ReportsBothInputErrorsUsingStatus) {
    Config current, candidate;
    current.set("rpc_server_port", -1);
    candidate.set("merge_requests", "invalid-boolean");
    const auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.current_status, "rpc_server_port"));
    EXPECT_TRUE(hasError(plan.candidate_status, "merge_requests"));
    EXPECT_FALSE(plan.valid());
    EXPECT_TRUE(plan.changes.empty());
    EXPECT_NE(plan.candidate_status.ToString().find("InvalidArgument"),
              std::string::npos);
}

TEST(ConfigValidationTest, SharedRpcParsersPreserveOutputOnInvalidInput) {
    Config config;
    uint16_t port = 1234;
    size_t threads = 4;
    config.set("rpc_server_port", std::numeric_limits<uint64_t>::max());
    config.set("rpc_server_threads", "1025");
    EXPECT_TRUE(
        getRpcServerPortFromConfig(config, 0, port).IsInvalidArgument());
    EXPECT_TRUE(
        getRpcServerThreadsFromConfig(config, 1, threads).IsInvalidArgument());
    EXPECT_EQ(port, 1234);
    EXPECT_EQ(threads, 4);

    config.set("rpc_server_port", "  +65535");
    config.set("rpc_server_threads", "1024");
    ASSERT_TRUE(getRpcServerPortFromConfig(config, 0, port).ok());
    ASSERT_TRUE(getRpcServerThreadsFromConfig(config, 1, threads).ok());
    EXPECT_EQ(port, 65535);
    EXPECT_EQ(threads, 1024);
    config.set("rpc_server_threads", nullptr);
    ASSERT_TRUE(getRpcServerThreadsFromConfig(config, 8, threads).ok());
    EXPECT_EQ(threads, 8);
}

TEST(ConfigValidationTest, BoundsOutputAndNeverIncludesValues) {
    Config current, candidate;
    candidate.set("rpc_server_port", "SECRET_TOKEN_VALUE");
    auto plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(plan.candidate_status.IsInvalidArgument());
    EXPECT_EQ(plan.candidate_status.message().find("SECRET_TOKEN_VALUE"),
              std::string_view::npos);
    candidate.set(std::string(257, 'x'), true);
    plan = planTentConfigChange(current, candidate, kCpuBuild);
    EXPECT_TRUE(hasError(plan.candidate_status, "$"));

    Config many_changes;
    for (size_t i = 0; i < 129; ++i)
        many_changes.set("key" + std::to_string(i), i);
    plan = planTentConfigChange(current, many_changes, kCpuBuild);
    EXPECT_FALSE(plan.valid());
    EXPECT_TRUE(plan.changes.empty());
    EXPECT_TRUE(hasError(plan.candidate_status, "$"));

    json bad_paths = json::object();
    for (size_t i = 0; i < 100; ++i) bad_paths["/key" + std::to_string(i)] = i;
    Config many_errors;
    ASSERT_TRUE(many_errors.load(bad_paths.dump()).ok());
    plan = planTentConfigChange(current, many_errors, kCpuBuild);
    EXPECT_FALSE(plan.valid());
    EXPECT_TRUE(hasError(plan.candidate_status,
                         "Invalid canonical configuration path"));
    EXPECT_LT(plan.candidate_status.message().size(), 1024);
    EXPECT_TRUE(plan.changes.empty());
}

TEST(ConfigValidationTest, LimitsDepthAndInputCount) {
    Config current, candidate;
    std::string deep = "x";
    for (size_t i = 0; i < 33; ++i) deep += "/x";
    candidate.set(deep, 1);
    EXPECT_FALSE(planTentConfigChange(current, candidate, kCpuBuild).valid());
    Config many_values;
    for (size_t i = 0; i < 4096; ++i)
        many_values.set("key" + std::to_string(i), i);
    auto plan = planTentConfigChange(many_values, many_values, kCpuBuild);
    EXPECT_FALSE(plan.valid());
    EXPECT_TRUE(hasError(plan.current_status, "$"));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
