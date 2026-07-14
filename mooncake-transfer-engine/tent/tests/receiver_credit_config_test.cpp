// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_config.h"

#include <string>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {

constexpr size_t resourceIndex(CreditResource resource) {
    return static_cast<size_t>(resource) - 1;
}

Status load(const std::string& contents, bool runtime_queue_enabled,
            ReceiverCreditRuntimeConfig& output) {
    Config config;
    auto status = config.load(contents);
    if (!status.ok()) return status;
    return loadReceiverCreditConfig(config, runtime_queue_enabled, output);
}

TEST(ReceiverCreditConfig, MissingSectionIsStrictlyDefaultOff) {
    Config config;
    ReceiverCreditRuntimeConfig output;
    output.mode = CreditRolloutMode::Required;

    ASSERT_TRUE(loadReceiverCreditConfig(config, false, output).ok());
    EXPECT_EQ(output.mode, CreditRolloutMode::Disabled);
    EXPECT_EQ(output.capacity, (std::array<uint64_t, kCreditResourceCount>{}));
    EXPECT_EQ(output.max_grant_per_pull,
              (std::array<uint64_t, kCreditResourceCount>{}));
    EXPECT_EQ(output.max_peers, ReceiverCreditRuntimeConfig::kDefaultMaxPeers);
}

TEST(ReceiverCreditConfig, LoadsCompleteNestedRequiredConfiguration) {
    ReceiverCreditRuntimeConfig output;
    ASSERT_TRUE(load(R"({
        "receiver_credit": {
          "mode": "required",
          "default_qos_class": 0,
          "capacity": {"data_bytes": 1048576, "request_slots": 64},
          "grant_batch": {"data_bytes": 262144, "request_slots": 8},
          "control": {
            "freshness_ttl_ms": 500,
            "retry_after_us": 200,
            "poll_interval_us": 50
          },
          "limits": {"max_peers": 4096}
        }
      })",
                     true, output)
                    .ok());

    EXPECT_EQ(output.mode, CreditRolloutMode::Required);
    EXPECT_EQ(output.capacity[resourceIndex(CreditResource::DataBytes)],
              1048576);
    EXPECT_EQ(output.capacity[resourceIndex(CreditResource::RequestSlots)], 64);
    EXPECT_EQ(
        output.max_grant_per_pull[resourceIndex(CreditResource::DataBytes)],
        262144);
    EXPECT_EQ(
        output.max_grant_per_pull[resourceIndex(CreditResource::RequestSlots)],
        8);
    EXPECT_EQ(output.capacity[resourceIndex(CreditResource::StagingSlots)], 0);
    EXPECT_EQ(output.max_peers, 4096);
    EXPECT_EQ(output.freshness_ttl_ms, 500);
    EXPECT_EQ(output.retry_after_us, 200);
    EXPECT_EQ(output.progress_interval_us, 50);
}

TEST(ReceiverCreditConfig, SupportsOptionalModeAndLegacyAliases) {
    ReceiverCreditRuntimeConfig output;
    ASSERT_TRUE(load(R"({
        "receiver_credit": {
          "mode": "optional",
          "capacity": {"data_bytes": 1024, "request_slots": 4},
          "grant_batch": {"data_bytes": 512, "request_slots": 2}
        }
      })",
                     true, output)
                    .ok());
    EXPECT_EQ(output.mode, CreditRolloutMode::Optional);

    ASSERT_TRUE(
        load(R"({"receiver_credit": {"enabled": false}})", false, output).ok());
    EXPECT_EQ(output.mode, CreditRolloutMode::Disabled);

    ASSERT_TRUE(load(R"({
        "receiver_credit": {
          "capacity": {"data_bytes": 1024, "request_slots": 4},
          "grant_batch": {"data_bytes": 512, "request_slots": 2}
        },
        "receiver_credit/enabled": true
      })",
                     true, output)
                    .ok());
    EXPECT_EQ(output.mode, CreditRolloutMode::Required);
}

TEST(ReceiverCreditConfig, RejectsConflictingRolloutSettings) {
    ReceiverCreditRuntimeConfig output;
    EXPECT_TRUE(load(R"({
        "receiver_credit": {"mode": "optional", "enabled": true}
      })",
                     true, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {"enabled": false},
        "receiver_credit/enabled": true
      })",
                     true, output)
                    .IsInvalidArgument());
}

TEST(ReceiverCreditConfig, AllowsConsistentModeAndLegacyAlias) {
    ReceiverCreditRuntimeConfig output;
    ASSERT_TRUE(load(R"({
        "receiver_credit": {
          "mode": "required",
          "enabled": true,
          "capacity": {"data_bytes": 1024, "request_slots": 4},
          "grant_batch": {"data_bytes": 512, "request_slots": 2}
        }
      })",
                     true, output)
                    .ok());
    EXPECT_EQ(output.mode, CreditRolloutMode::Required);
}

TEST(ReceiverCreditConfig, RejectsUnknownKeysAtEverySchemaLevel) {
    ReceiverCreditRuntimeConfig output;
    EXPECT_TRUE(load(R"({"receiver_credit": {"surprise": 1}})", false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {
          "capacity": {"data_bytes": 1, "request_slots": 1, "gpu": 1},
          "grant_batch": {"data_bytes": 1, "request_slots": 1}
        }
      })",
                     false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({"receiver_credit": {"control": {"period_us": 1}}})",
                     false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({"receiver_credit": {"limits": {"entries": 1}}})",
                     false, output)
                    .IsInvalidArgument());
}

TEST(ReceiverCreditConfig, RejectsWrongJsonTypesWithoutDefaulting) {
    ReceiverCreditRuntimeConfig output;
    EXPECT_TRUE(
        load(R"({"receiver_credit": []})", false, output).IsInvalidArgument());
    EXPECT_TRUE(load(R"({"receiver_credit": {"mode": true}})", false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(
        load(R"({"receiver_credit": {"enabled": "true"}})", false, output)
            .IsInvalidArgument());
    EXPECT_TRUE(load(R"({"receiver_credit/enabled": 1})", false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({"receiver_credit": {"default_qos_class": 0.0}})",
                     false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({"receiver_credit": {"limits": {"max_peers": -1}}})",
                     false, output)
                    .IsInvalidArgument());
}

TEST(ReceiverCreditConfig, ActiveModeRequiresRuntimeQueueAndBothResources) {
    ReceiverCreditRuntimeConfig output;
    const std::string valid = R"({
      "receiver_credit": {
        "mode": "required",
        "capacity": {"data_bytes": 1024, "request_slots": 4},
        "grant_batch": {"data_bytes": 512, "request_slots": 2}
      }
    })";
    EXPECT_TRUE(load(valid, false, output).IsInvalidArgument());
    EXPECT_TRUE(load(R"({"receiver_credit/enabled": true})", true, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(
        load(R"({"receiver_credit": {"mode": "optional"}})", true, output)
            .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {
          "mode": "required",
          "capacity": {"data_bytes": 1024, "request_slots": 4}
        }
      })",
                     true, output)
                    .IsInvalidArgument());
}

TEST(ReceiverCreditConfig, RejectsZeroMissingOrOversizedGrants) {
    ReceiverCreditRuntimeConfig output;
    EXPECT_TRUE(load(R"({
        "receiver_credit": {
          "mode": "required",
          "capacity": {"data_bytes": 1024, "request_slots": 0},
          "grant_batch": {"data_bytes": 512, "request_slots": 1}
        }
      })",
                     true, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {
          "mode": "required",
          "capacity": {"data_bytes": 1024, "request_slots": 4},
          "grant_batch": {"data_bytes": 2048, "request_slots": 1}
        }
      })",
                     true, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {
          "mode": "required",
          "capacity": {"data_bytes": 1024},
          "grant_batch": {"data_bytes": 512, "request_slots": 1}
        }
      })",
                     true, output)
                    .IsInvalidArgument());
}

TEST(ReceiverCreditConfig, EnforcesControlAndPeerBounds) {
    ReceiverCreditRuntimeConfig output;
    EXPECT_TRUE(load(R"({
        "receiver_credit": {
          "control": {"freshness_ttl_ms": 1, "poll_interval_us": 1000}
        }
      })",
                     false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {"control": {"retry_after_us": 0}}
      })",
                     false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {"limits": {"max_peers": 65537}}
      })",
                     false, output)
                    .IsInvalidArgument());
    EXPECT_TRUE(load(R"({
        "receiver_credit": {"limits": {"max_peers": 0}}
      })",
                     false, output)
                    .IsInvalidArgument());
}

TEST(ReceiverCreditConfig, MvpOnlyAcceptsQosClassZero) {
    ReceiverCreditRuntimeConfig output;
    EXPECT_TRUE(
        load(R"({"receiver_credit": {"default_qos_class": 1}})", false, output)
            .IsInvalidArgument());
}

TEST(ReceiverCreditConfig, FailureLeavesOutputUnchanged) {
    ReceiverCreditRuntimeConfig output;
    output.mode = CreditRolloutMode::Optional;
    output.capacity.fill(17);
    output.max_grant_per_pull.fill(19);
    output.max_peers = 23;
    output.freshness_ttl_ms = 29;
    output.retry_after_us = 31;
    output.progress_interval_us = 37;
    output.default_qos_class = 41;
    const auto before = output;

    EXPECT_TRUE(load(R"({"receiver_credit": {"mode": "broken"}})", true, output)
                    .IsInvalidArgument());
    EXPECT_EQ(output.mode, before.mode);
    EXPECT_EQ(output.capacity, before.capacity);
    EXPECT_EQ(output.max_grant_per_pull, before.max_grant_per_pull);
    EXPECT_EQ(output.max_peers, before.max_peers);
    EXPECT_EQ(output.freshness_ttl_ms, before.freshness_ttl_ms);
    EXPECT_EQ(output.retry_after_us, before.retry_after_us);
    EXPECT_EQ(output.progress_interval_us, before.progress_interval_us);
    EXPECT_EQ(output.default_qos_class, before.default_qos_class);
}

}  // namespace
}  // namespace mooncake::tent
