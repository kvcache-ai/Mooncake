#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "metadata_store.h"

namespace mooncake::test {

class MasterServiceEtcdOpLogTest : public ::testing::Test {
   protected:
    struct MountedSegmentContext {
        Segment segment;
        UUID client_id;
    };

    static void SetUpTestSuite() {
        if (!glog_initialized_) {
            google::InitGoogleLogging("MasterServiceEtcdOpLogTest");
            FLAGS_logtostderr = true;
            glog_initialized_ = true;
        }
    }

    static void TearDownTestSuite() {
        if (glog_initialized_) {
            google::ShutdownGoogleLogging();
            glog_initialized_ = false;
        }
    }

    void SetUp() override {
#if !defined(STORE_USE_ETCD)
        GTEST_SKIP() << "STORE_USE_ETCD is disabled.";
#else
        service_ = CreateService();
#endif
    }

    std::unique_ptr<MasterService> CreateService() const {
        auto config = MasterServiceConfig::builder()
                          .set_enable_ha(true)
                          .set_ha_backend_type("etcd")
                          .set_cluster_id("master-service-oplog-test-" +
                                          UuidToString(generate_uuid()))
                          .build();
        return std::make_unique<MasterService>(config);
    }

    MountedSegmentContext MountSegment(std::string name, size_t base) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = kSegmentSize;
        segment.te_endpoint = segment.name;

        UUID client_id = generate_uuid();
        auto result = service_->MountSegment(segment, client_id);
        EXPECT_TRUE(result.has_value());
        return {.segment = std::move(segment), .client_id = client_id};
    }

    std::string NextKey(const std::string& prefix) const {
        return prefix + "-" + std::to_string(next_key_id_.fetch_add(1));
    }

    void PutMemoryReplica(const UUID& client_id, const std::string& key,
                          const std::string& segment_name,
                          uint64_t size = 4096) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name};

        auto result = service_->PutStart(client_id, key, size, config);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(1u, result->size());
        EXPECT_EQ(segment_name, result->front()
                                    .get_memory_descriptor()
                                    .buffer_descriptor.transport_endpoint_);
        ASSERT_TRUE(
            service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    }

    Replica MakeLocalDiskReplica(const UUID& client_id,
                                 const std::string& transport_endpoint,
                                 uint64_t size = 4096) const {
        return Replica(client_id, size, transport_endpoint,
                       ReplicaStatus::COMPLETE);
    }

    const OpLogEntry& LastOpLogEntry() const {
        EXPECT_FALSE(service_->oplog_manager_.buffer_.empty());
        return service_->oplog_manager_.buffer_.back();
    }

    size_t EntryCount() const {
        return service_->oplog_manager_.buffer_.size();
    }

    MetadataPayload DecodePayload(const std::string& payload) const {
        auto decoded = struct_pack::deserialize<MetadataPayload>(payload);
        EXPECT_TRUE(decoded.has_value());
        if (!decoded.has_value()) {
            return {};
        }
        return decoded.value();
    }

    void RunBatchEvict(double evict_ratio_target,
                       double evict_ratio_lowerbound) {
        service_->BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
    }

    static constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    inline static std::atomic<uint64_t> next_key_id_{0};
    inline static bool glog_initialized_{false};
    std::unique_ptr<MasterService> service_;
};

TEST_F(MasterServiceEtcdOpLogTest, AddReplicaAppendsPutEnd) {
    auto mounted = MountSegment("seg-a", 0x300000000ull);
    const auto key = NextKey("add-replica");
    PutMemoryReplica(mounted.client_id, key, mounted.segment.name);

    const auto before = EntryCount();
    auto local_disk =
        MakeLocalDiskReplica(mounted.client_id, "local-disk://seg-a");
    ASSERT_TRUE(
        service_->AddReplica(mounted.client_id, key, local_disk).has_value());

    ASSERT_EQ(before + 1, EntryCount());
    const auto& entry = LastOpLogEntry();
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(key, entry.object_key);

    const auto payload = DecodePayload(entry.payload);
    ASSERT_EQ(2u, payload.replicas.size());
    EXPECT_EQ(1u,
              std::count_if(payload.replicas.begin(), payload.replicas.end(),
                            [](const Replica::Descriptor& replica) {
                                return replica.is_memory_replica();
                            }));
    EXPECT_EQ(1u,
              std::count_if(payload.replicas.begin(), payload.replicas.end(),
                            [](const Replica::Descriptor& replica) {
                                return replica.is_local_disk_replica();
                            }));
}

TEST_F(MasterServiceEtcdOpLogTest, EvictDiskReplicaAppendsUpdatedPutEnd) {
    auto mounted = MountSegment("seg-a", 0x310000000ull);
    const auto key = NextKey("evict-update");
    PutMemoryReplica(mounted.client_id, key, mounted.segment.name);

    auto local_disk =
        MakeLocalDiskReplica(mounted.client_id, "local-disk://seg-a");
    ASSERT_TRUE(
        service_->AddReplica(mounted.client_id, key, local_disk).has_value());

    const auto before = EntryCount();
    ASSERT_TRUE(
        service_
            ->EvictDiskReplica(mounted.client_id, key, ReplicaType::LOCAL_DISK)
            .has_value());

    ASSERT_EQ(before + 1, EntryCount());
    const auto& entry = LastOpLogEntry();
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(key, entry.object_key);

    const auto payload = DecodePayload(entry.payload);
    ASSERT_EQ(1u, payload.replicas.size());
    EXPECT_TRUE(payload.replicas.front().is_memory_replica());
}

TEST_F(MasterServiceEtcdOpLogTest, EvictDiskReplicaAppendsRemoveWhenKeyGone) {
    const UUID client_id = generate_uuid();
    const auto key = NextKey("evict-remove");

    auto local_disk = MakeLocalDiskReplica(client_id, "local-disk://only");
    ASSERT_TRUE(service_->AddReplica(client_id, key, local_disk).has_value());

    const auto before = EntryCount();
    ASSERT_TRUE(
        service_->EvictDiskReplica(client_id, key, ReplicaType::LOCAL_DISK)
            .has_value());

    ASSERT_EQ(before + 1, EntryCount());
    const auto& entry = LastOpLogEntry();
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ(key, entry.object_key);
    EXPECT_TRUE(entry.payload.empty());

    auto exists = service_->ExistKey(key);
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(exists.value());
}

TEST_F(MasterServiceEtcdOpLogTest, CopyEndAppendsExpandedMetadata) {
    auto source = MountSegment("seg-a", 0x320000000ull);
    auto target = MountSegment("seg-b", 0x330000000ull);
    const auto key = NextKey("copy-end");
    PutMemoryReplica(source.client_id, key, source.segment.name);

    auto copy = service_->CopyStart(source.client_id, key, source.segment.name,
                                    {target.segment.name});
    ASSERT_TRUE(copy.has_value());

    const auto before = EntryCount();
    ASSERT_TRUE(service_->CopyEnd(source.client_id, key).has_value());

    ASSERT_EQ(before + 1, EntryCount());
    const auto& entry = LastOpLogEntry();
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(key, entry.object_key);

    const auto payload = DecodePayload(entry.payload);
    ASSERT_EQ(2u, payload.replicas.size());

    std::set<std::string> endpoints;
    for (const auto& replica : payload.replicas) {
        ASSERT_TRUE(replica.is_memory_replica());
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
        endpoints.insert(replica.get_memory_descriptor()
                             .buffer_descriptor.transport_endpoint_);
    }
    EXPECT_EQ(std::set<std::string>({source.segment.name, target.segment.name}),
              endpoints);
}

TEST_F(MasterServiceEtcdOpLogTest, CopyRevokeAppendsSourceOnlyMetadata) {
    auto source = MountSegment("seg-a", 0x340000000ull);
    auto target = MountSegment("seg-b", 0x350000000ull);
    const auto key = NextKey("copy-revoke");
    PutMemoryReplica(source.client_id, key, source.segment.name);

    auto copy = service_->CopyStart(source.client_id, key, source.segment.name,
                                    {target.segment.name});
    ASSERT_TRUE(copy.has_value());

    const auto before = EntryCount();
    ASSERT_TRUE(service_->CopyRevoke(source.client_id, key).has_value());

    ASSERT_EQ(before + 1, EntryCount());
    const auto payload = DecodePayload(LastOpLogEntry().payload);
    ASSERT_EQ(1u, payload.replicas.size());
    EXPECT_TRUE(payload.replicas.front().is_memory_replica());
    EXPECT_EQ(source.segment.name, payload.replicas.front()
                                       .get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_);
}

TEST_F(MasterServiceEtcdOpLogTest, MoveEndAppendsPromotedTargetMetadata) {
    auto source = MountSegment("seg-a", 0x360000000ull);
    auto target = MountSegment("seg-b", 0x370000000ull);
    const auto key = NextKey("move-end");
    PutMemoryReplica(source.client_id, key, source.segment.name);

    auto move = service_->MoveStart(source.client_id, key, source.segment.name,
                                    target.segment.name);
    ASSERT_TRUE(move.has_value());

    const auto before = EntryCount();
    ASSERT_TRUE(service_->MoveEnd(source.client_id, key).has_value());

    ASSERT_EQ(before + 1, EntryCount());
    const auto payload = DecodePayload(LastOpLogEntry().payload);
    ASSERT_EQ(1u, payload.replicas.size());
    EXPECT_TRUE(payload.replicas.front().is_memory_replica());
    EXPECT_EQ(target.segment.name, payload.replicas.front()
                                       .get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_);
}

TEST_F(MasterServiceEtcdOpLogTest, MoveRevokeAppendsSourceOnlyMetadata) {
    auto source = MountSegment("seg-a", 0x380000000ull);
    auto target = MountSegment("seg-b", 0x390000000ull);
    const auto key = NextKey("move-revoke");
    PutMemoryReplica(source.client_id, key, source.segment.name);

    auto move = service_->MoveStart(source.client_id, key, source.segment.name,
                                    target.segment.name);
    ASSERT_TRUE(move.has_value());

    const auto before = EntryCount();
    ASSERT_TRUE(service_->MoveRevoke(source.client_id, key).has_value());

    ASSERT_EQ(before + 1, EntryCount());
    const auto payload = DecodePayload(LastOpLogEntry().payload);
    ASSERT_EQ(1u, payload.replicas.size());
    EXPECT_TRUE(payload.replicas.front().is_memory_replica());
    EXPECT_EQ(source.segment.name, payload.replicas.front()
                                       .get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_);
}

TEST_F(MasterServiceEtcdOpLogTest, BatchEvictAppendsRemoveForMemoryOnlyObject) {
    auto mounted = MountSegment("seg-a", 0x3a0000000ull);
    const auto key = NextKey("batch-evict");
    PutMemoryReplica(mounted.client_id, key, mounted.segment.name);

    const auto before = EntryCount();
    RunBatchEvict(1.0, 1.0);

    ASSERT_EQ(before + 1, EntryCount());
    const auto& entry = LastOpLogEntry();
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ(key, entry.object_key);

    auto exists = service_->ExistKey(key);
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(exists.value());
}

}  // namespace mooncake::test
