#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "real_client.h"
#include "test_server_helpers.h"

namespace mooncake::testing {
namespace {

class WeightManualResidencyE2ETest : public ::testing::Test {
   protected:
    static constexpr size_t kBytes = 256 * 1024;
    virtual bool EnableDisk() const { return true; }

    void SetUp() override {
        auto root = std::filesystem::temp_directory_path();
        std::string pattern = (root / "weight-manual-XXXXXX").string();
        ASSERT_NE(nullptr, ::mkdtemp(pattern.data()));
        directory_ = pattern;
        ASSERT_TRUE(server_.Start(InProcMasterConfigBuilder()
                                      .set_enable_offload(true)
                                      .set_default_kv_lease_ttl(100)
                                      .build()));
        auto ports = getFreeTcpPorts(2);
        ASSERT_EQ(ports.size(), 2u);
        storage_ = std::make_unique<RealClient>();
        ASSERT_EQ(0, storage_->setup_real(
                         "127.0.0.1:" + std::to_string(ports[0]),
                         server_.metadata_url(), 16 * 1024 * 1024,
                         16 * 1024 * 1024, "tcp", "", server_.master_address(),
                         nullptr, "", EnableDisk(), directory_.string()));
        reader_ = std::make_unique<RealClient>();
        ASSERT_EQ(
            0, reader_->setup_real("127.0.0.1:" + std::to_string(ports[1]),
                                   server_.metadata_url(), 0, 16 * 1024 * 1024,
                                   "tcp", "", server_.master_address()));
        buffer_.resize(kBytes);
        ASSERT_EQ(0, reader_->register_buffer(buffer_.data(), buffer_.size()));
        registered_ = true;
    }

    void TearDown() override {
        if (registered_) {
            EXPECT_EQ(0, reader_->unregister_buffer(buffer_.data()));
        }
        reader_.reset();
        storage_.reset();
        server_.Stop();
        if (!directory_.empty()) std::filesystem::remove_all(directory_);
    }

    WeightRevisionIdentity Identity() const {
        return {.tenant_id = "default",
                .name_space = "manual-e2e",
                .resource_id = "weights",
                .revision = "one",
                .weight_generation = 1};
    }

    void Import() {
        auto begin = reader_->begin_weight_import({
            .identity = Identity(),
            .payload_group_id = {},
            .expected_payload_count = 4,
            .expected_logical_bytes = 4 * kBytes,
            .policy = WeightStoragePolicy{.preferred_residency =
                                              WeightResidencyState::HOT},
            .affinity_summary = {.affinity_count = 2,
                                 .affinity_digest = std::string(64, 'c')},
        });
        ASSERT_TRUE(begin.has_value());
        ASSERT_TRUE(begin->has_value());
        EXPECT_EQ((*begin)->policy.migration_mode, WeightMigrationMode::MANUAL);
        ReplicateConfig config;
        config.replica_num = 1;
        config.group_ids = {
            std::vector<std::string>{(*begin)->manifest.payload_group_id}};
        config.data_type = ObjectDataType::WEIGHT;
        for (size_t i = 0; i < 4; ++i) {
            keys_.push_back("manual-payload-" + std::to_string(i));
            payloads_.emplace_back(kBytes, static_cast<char>('a' + i));
            config.residency_affinity_ids = {
                std::vector<std::string>{i < 2 ? "tensor-a" : "tensor-b"}};
            ASSERT_EQ(0, reader_->put(keys_.back(), payloads_.back(), config));
        }
        config.data_type = ObjectDataType::METADATA;
        config.residency_affinity_ids.reset();
        const std::string manifest = "{}";
        ASSERT_EQ(0, reader_->put(MakeWeightManifestKey(Identity()), manifest,
                                  config));
        auto ready = reader_->commit_weight_import({
            .identity = Identity(),
            .expected_metadata_generation = (*begin)->metadata_generation,
            .manifest = {.manifest_key = MakeWeightManifestKey(Identity()),
                         .manifest_sha256 =
                             "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe"
                             "77e8310c060f61caaff8a",
                         .payload_group_id =
                             (*begin)->manifest.payload_group_id,
                         .payload_keys_sha256 =
                             ComputeWeightPayloadKeysSha256(keys_),
                         .payload_count = 4,
                         .logical_bytes = 4 * kBytes},
        });
        ASSERT_TRUE(ready.has_value());
        ASSERT_TRUE(ready->has_value());
        EXPECT_EQ((*ready)->residency, WeightResidencyState::HOT);
    }

    void ExpectPayloads() {
        for (size_t i = 0; i < keys_.size(); ++i) {
            std::fill(buffer_.begin(), buffer_.end(), '\0');
            ASSERT_EQ(
                static_cast<int64_t>(kBytes),
                reader_->get_into(keys_[i], buffer_.data(), buffer_.size()));
            EXPECT_EQ(payloads_[i],
                      std::string(buffer_.data(), buffer_.size()));
        }
    }

    void Migrate(WeightResidencyState target) {
        auto view = reader_->get_weight_revision({.identity = Identity()});
        ASSERT_TRUE(view.has_value());
        ASSERT_TRUE(view->has_value());
        auto operation = reader_->start_weight_residency_operation({
            .identity = Identity(),
            .expected_metadata_generation =
                (*view)->metadata.metadata_generation,
            .target_residency = target,
            .mixed_hot_ratio = target == WeightResidencyState::MIXED
                                   ? std::optional<double>(0.5)
                                   : std::nullopt,
        });
        ASSERT_TRUE(operation.has_value());
        ASSERT_TRUE(operation->has_value());
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(90);
        while (std::chrono::steady_clock::now() < deadline) {
            auto status = reader_->query_weight_operation(
                {.operation_id = (*operation)->operation_id});
            ASSERT_TRUE(status.has_value());
            ASSERT_TRUE(status->has_value());
            if ((*status)->message == kWeightOperationCompletedMessage) {
                auto result =
                    reader_->get_weight_revision({.identity = Identity()});
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(result->has_value());
                EXPECT_EQ((*result)->metadata.residency, target);
                if (target == WeightResidencyState::MIXED)
                    EXPECT_DOUBLE_EQ((*result)->metadata.observed_hot_ratio,
                                     0.5);
                return;
            }
            ASSERT_NE((*status)->message, kWeightOperationCancelledMessage);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        FAIL() << "Manual migration did not complete";
    }

    InProcMaster server_;
    std::unique_ptr<RealClient> storage_, reader_;
    std::filesystem::path directory_;
    std::vector<char> buffer_;
    std::vector<std::string> keys_, payloads_;
    bool registered_ = false;
};

TEST_F(WeightManualResidencyE2ETest,
       ManualMigrationPreservesPayloadsThroughSSD) {
    ASSERT_NO_FATAL_FAILURE(Import());
    ASSERT_NO_FATAL_FAILURE(ExpectPayloads());
    for (auto target : {WeightResidencyState::COLD, WeightResidencyState::MIXED,
                        WeightResidencyState::HOT}) {
        ASSERT_NO_FATAL_FAILURE(Migrate(target));
        ASSERT_NO_FATAL_FAILURE(ExpectPayloads());
    }
}

TEST_F(WeightManualResidencyE2ETest, RejectsAutomaticPolicyAtImport) {
    auto rejected = reader_->begin_weight_import({
        .identity = Identity(),
        .payload_group_id = {},
        .expected_payload_count = 4,
        .expected_logical_bytes = 4 * kBytes,
        .policy =
            WeightStoragePolicy{
                .preferred_residency = WeightResidencyState::HOT,
                .migration_mode = WeightMigrationMode::AUTO},
        .affinity_summary = {.affinity_count = 2,
                             .affinity_digest = std::string(64, 'c')},
    });
    ASSERT_TRUE(rejected.has_value());
    ASSERT_FALSE(rejected->has_value());
    EXPECT_EQ(rejected->error(), WeightManagementError::INVALID_ARGUMENT);
    auto absent = reader_->get_weight_revision({.identity = Identity()});
    ASSERT_TRUE(absent.has_value());
    ASSERT_FALSE(absent->has_value());
    EXPECT_EQ(absent->error(), WeightManagementError::NOT_FOUND);
}

class WeightManualResidencyNoDiskTest : public WeightManualResidencyE2ETest {
   protected:
    bool EnableDisk() const override { return false; }
};

TEST_F(WeightManualResidencyNoDiskTest,
       DeleteCancelsAnObservedPendingMigration) {
    ASSERT_NO_FATAL_FAILURE(Import());
    auto view = reader_->get_weight_revision({.identity = Identity()});
    ASSERT_TRUE(view && *view);
    auto operation = reader_->start_weight_residency_operation({
        .identity = Identity(),
        .expected_metadata_generation = (*view)->metadata.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    });
    ASSERT_TRUE(operation && *operation);
    auto pending = reader_->query_weight_operation(
        {.operation_id = (*operation)->operation_id});
    ASSERT_TRUE(pending && *pending);
    ASSERT_FALSE(IsWeightOperationTerminal((*pending)->message));
    ASSERT_NO_FATAL_FAILURE(ExpectPayloads());
    view = reader_->get_weight_revision({.identity = Identity()});
    ASSERT_TRUE(view && *view);
    auto deleted = reader_->delete_weight_revision({
        .identity = Identity(),
        .expected_metadata_generation = (*view)->metadata.metadata_generation,
    });
    ASSERT_TRUE(deleted && *deleted);
    EXPECT_EQ((*deleted)->availability, WeightAvailabilityState::DELETED);
    auto cancelled = reader_->query_weight_operation(
        {.operation_id = (*operation)->operation_id});
    ASSERT_TRUE(cancelled && *cancelled);
    EXPECT_EQ((*cancelled)->message, kWeightOperationCancelledMessage);
    for (const auto& key : keys_)
        EXPECT_LT(reader_->get_into(key, buffer_.data(), buffer_.size()), 0);
}

}  // namespace
}  // namespace mooncake::testing
