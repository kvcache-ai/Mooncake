#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "real_client.h"
#include "test_server_helpers.h"
#include "weight_residency_planner.h"

namespace mooncake::testing {
namespace {

class WeightColdImportE2ETest : public ::testing::Test {
   protected:
    static constexpr size_t kMiB = 1024 * 1024;

    void Start(size_t segment_bytes) {
        auto pattern =
            (std::filesystem::current_path() / "weight-cold-e2e-XXXXXX")
                .string();
        ASSERT_NE(nullptr, ::mkdtemp(pattern.data()));
        directory_ = pattern;
        ASSERT_TRUE(server_.Start(InProcMasterConfigBuilder()
                                      .set_enable_offload(true)
                                      .set_default_kv_lease_ttl(100)
                                      .build()));
        auto ports = getFreeTcpPorts(2);
        ASSERT_EQ(ports.size(), 2u);
        owner_ = std::make_unique<RealClient>();
        ASSERT_EQ(
            0, owner_->setup_real("127.0.0.1:" + std::to_string(ports[0]),
                                  server_.metadata_url(), segment_bytes, 16 * kMiB,
                                  "tcp", "", server_.master_address(), nullptr,
                                  "", true, directory_.string()));
        source_ = std::make_unique<RealClient>();
        ASSERT_EQ(
            0, source_->setup_real("127.0.0.1:" + std::to_string(ports[1]),
                                   server_.metadata_url(), 0, kMiB, "tcp", "",
                                   server_.master_address()));
    }

    void TearDown() override {
        for (auto* buffer : registered_) {
            EXPECT_EQ(0, source_->unregister_buffer(buffer));
        }
        source_.reset();
        owner_.reset();
        server_.Stop();
        if (!directory_.empty()) std::filesystem::remove_all(directory_);
    }

    void Import(WeightResidencyState target, bool batch, bool local,
                const std::vector<size_t>& sizes) {
        ASSERT_TRUE(owner_);
        const std::string hot_control(64 * 1024, 'h');
        ASSERT_EQ(0, source_->put("hot-control", hot_control));
        WeightRevisionIdentity identity{
            .tenant_id = "default",
            .name_space = "cold-import-e2e",
            .resource_id = "weights",
            .revision =
                ::testing::UnitTest::GetInstance()->current_test_info()->name(),
            .weight_generation = 1};
        std::vector<WeightAffinityMappingEntry> layout;
        std::vector<WeightAffinityUnit> units;
        std::vector<std::string> keys;
        std::vector<void*> addresses;
        std::vector<std::string> affinity_ids;
        uint64_t total = 0;
        payloads_.resize(sizes.size());
        for (size_t index = 0; index < sizes.size(); ++index) {
            const auto key = "payload-" + std::to_string(index);
            const auto affinity = "tensor-" + std::to_string(index);
            keys.push_back(key);
            affinity_ids.push_back(affinity);
            layout.push_back({key, sizes[index], affinity});
            units.push_back({affinity, sizes[index]});
            total += sizes[index];
            payloads_[index].assign(sizes[index],
                                    static_cast<char>('a' + index));
            addresses.push_back(payloads_[index].data());
            ASSERT_EQ(0,
                      source_->register_buffer(addresses.back(), sizes[index]));
            registered_.push_back(addresses.back());
        }
        auto begin = source_->begin_weight_import(
            {.identity = identity,
             .payload_group_id = {},
             .expected_payload_count = sizes.size(),
             .expected_logical_bytes = total,
             .policy = WeightStoragePolicy{.preferred_residency = target},
             .affinity_summary = {.affinity_count = units.size(),
                                  .affinity_digest =
                                      ComputeWeightAffinityMappingSha256(
                                          layout)},
             .payload_layout = layout});
        ASSERT_TRUE(begin && begin->has_value());
        const auto group = (*begin)->manifest.payload_group_id;
        ReplicateConfig config;
        config.replica_num = 1;
        config.data_type = ObjectDataType::WEIGHT;
        auto* writer = local ? owner_.get() : source_.get();
        if (batch) {
            config.group_ids = std::vector<std::string>(keys.size(), group);
            config.residency_affinity_ids = affinity_ids;
            auto result =
                writer->batch_put_from(keys, addresses, sizes, config);
            ASSERT_EQ(result.size(), keys.size());
            for (const auto status : result) ASSERT_EQ(status, 0);
        } else {
            for (size_t index = 0; index < keys.size(); ++index) {
                config.group_ids = std::vector<std::string>{group};
                config.residency_affinity_ids =
                    std::vector<std::string>{affinity_ids[index]};
                ASSERT_EQ(0,
                          writer->put(keys[index], payloads_[index], config));
            }
        }
        config.group_ids = std::vector<std::string>{group};
        config.residency_affinity_ids.reset();
        config.data_type = ObjectDataType::METADATA;
        ASSERT_EQ(0,
                  source_->put(MakeWeightManifestKey(identity), "{}", config));
        auto committed = source_->commit_weight_import(
            {.identity = identity,
             .expected_metadata_generation = (*begin)->metadata_generation,
             .manifest = {
                 .manifest_key = MakeWeightManifestKey(identity),
                 .manifest_sha256 = "44136fa355b3678a1146ad16f7e8649e94fb4fc21f"
                                    "e77e8310c060f61caaff8a",
                 .payload_group_id = group,
                 .payload_keys_sha256 = ComputeWeightPayloadKeysSha256(keys),
                 .payload_count = keys.size(),
                 .logical_bytes = total}});
        ASSERT_TRUE(committed && committed->has_value());
        EXPECT_EQ((*committed)->availability, WeightAvailabilityState::READY);
        EXPECT_EQ((*committed)->residency, target);
        EXPECT_FALSE((*committed)->operation_id.has_value());
        EXPECT_TRUE((*committed)->import_layout.empty());

        std::vector<std::string> hot_affinities;
        if (target == WeightResidencyState::MIXED) {
            auto plan = PlanMixedWeightResidency(units, 0.5);
            ASSERT_TRUE(plan);
            hot_affinities = plan->hot_affinity_ids;
            EXPECT_DOUBLE_EQ(
                (*committed)->observed_hot_ratio,
                static_cast<double>(plan->hot_logical_bytes) / total);
        }
        auto placements = source_->batch_query(keys);
        ASSERT_EQ(placements.size(), keys.size());
        for (size_t index = 0; index < keys.size(); ++index) {
            ASSERT_TRUE(placements[index]);
            const bool hot =
                std::find(hot_affinities.begin(), hot_affinities.end(),
                          affinity_ids[index]) != hot_affinities.end();
            ASSERT_EQ(placements[index]->replicas.size(), 1u);
            EXPECT_EQ(placements[index]->replicas.front().type(),
                      hot ? ReplicaType::MEMORY : ReplicaType::LOCAL_DISK);
            std::string actual(sizes[index], '\0');
            ASSERT_EQ(0,
                      source_->register_buffer(actual.data(), actual.size()));
            EXPECT_EQ(
                static_cast<int64_t>(actual.size()),
                source_->get_into(keys[index], actual.data(), actual.size()));
            EXPECT_EQ(actual, payloads_[index]);
            EXPECT_EQ(0, source_->unregister_buffer(actual.data()));
        }
        auto manifest = source_->batch_query({MakeWeightManifestKey(identity)});
        ASSERT_EQ(manifest.size(), 1u);
        ASSERT_TRUE(manifest.front());
        ASSERT_EQ(manifest.front()->replicas.size(), 1u);
        EXPECT_EQ(manifest.front()->replicas.front().type(),
                  ReplicaType::MEMORY);
        std::string control_readback(hot_control.size(), '\0');
        ASSERT_EQ(0, source_->register_buffer(control_readback.data(),
                                              control_readback.size()));
        EXPECT_EQ(static_cast<int64_t>(control_readback.size()),
                  source_->get_into("hot-control", control_readback.data(),
                                    control_readback.size()));
        EXPECT_EQ(control_readback, hot_control);
        EXPECT_EQ(0, source_->unregister_buffer(control_readback.data()));
    }

    InProcMasterServer server_;
    std::unique_ptr<RealClient> owner_;
    std::unique_ptr<RealClient> source_;
    std::filesystem::path directory_;
    std::vector<std::string> payloads_;
    std::vector<void*> registered_;
};

TEST_F(WeightColdImportE2ETest, RemoteScalarBypassesHotSegmentCapacity) {
    Start(512 * 1024);
    Import(WeightResidencyState::COLD, false, false, {8 * kMiB});
}

TEST_F(WeightColdImportE2ETest, LocalScalarBypassesHotSegmentCapacity) {
    Start(512 * 1024);
    Import(WeightResidencyState::COLD, false, true, {8 * kMiB});
}

TEST_F(WeightColdImportE2ETest, RemoteBatchBypassesHotSegmentCapacity) {
    Start(512 * 1024);
    Import(WeightResidencyState::COLD, true, false, {8 * kMiB, 8 * kMiB});
}

TEST_F(WeightColdImportE2ETest, MixedBatchOnlyKeepsPlannedAffinitiesHot) {
    Start(3 * kMiB);
    Import(WeightResidencyState::MIXED, true, false, {2 * kMiB, 6 * kMiB});
}

}  // namespace
}  // namespace mooncake::testing
