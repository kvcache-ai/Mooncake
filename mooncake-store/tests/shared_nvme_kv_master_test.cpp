#include "master_service.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

class SharedNvmeKvMasterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        shared_.replica_num = 0;
        shared_.shared_nvme_kv_replica_num = 1;
        master_ = std::make_unique<MasterService>(config_);
    }

    MasterServiceConfig config_;
    std::unique_ptr<MasterService> master_;
    ReplicateConfig shared_;
    const UUID client_ = generate_uuid();
    const TenantId tenant_ = TenantId::Default();
};

TEST_F(SharedNvmeKvMasterTest, UpsertCannotTakeOverExistingSharedObject) {
    auto start = master_->PutStart(client_, "key", tenant_, 1024, shared_);
    ASSERT_TRUE(start);
    ASSERT_EQ(start->size(), 1);
    const auto replica_id = start->front().id;
    const auto allocation =
        start->front().get_shared_kv_descriptor().allocation_id;
    ASSERT_TRUE(master_->PutEnd(client_,
                                ObjectMeta{"key", std::nullopt, allocation},
                                tenant_, ReplicaType::ALL));

    auto upsert =
        master_->UpsertStart(client_, "key", tenant_, 1024, ReplicateConfig{});
    ASSERT_FALSE(upsert);
    EXPECT_EQ(upsert.error(), ErrorCode::NOT_SUPPORTED);
    auto query = master_->GetReplicaList("key", tenant_);
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1);
    EXPECT_EQ(query->replicas.front().id, replica_id);
    EXPECT_EQ(query->replicas.front().status, ReplicaStatus::COMPLETE);
}

TEST_F(SharedNvmeKvMasterTest, LateEndRevokeAndMissCannotAffectNewAllocation) {
    auto old = master_->PutStart(client_, "key", tenant_, 1024, shared_);
    ASSERT_TRUE(old);
    const auto old_id = old->front().get_shared_kv_descriptor().allocation_id;
    ASSERT_TRUE(
        master_->PutRevoke(client_, "key", tenant_, ReplicaType::ALL, old_id));
    auto current = master_->PutStart(client_, "key", tenant_, 1024, shared_);
    ASSERT_TRUE(current);
    const auto id = current->front().get_shared_kv_descriptor().allocation_id;
    ASSERT_NE(old_id, id);

    for (uint64_t stale : {uint64_t{0}, old_id}) {
        for (auto type :
             {ReplicaType::ALL, ReplicaType::MEMORY, ReplicaType::NOF_SSD,
              ReplicaType::DFS, ReplicaType::SHARED_NVME_KV}) {
            auto end = master_->PutEnd(
                client_, ObjectMeta{"key", std::nullopt, stale}, tenant_, type);
            ASSERT_FALSE(end);
            EXPECT_EQ(end.error(), ErrorCode::INVALID_WRITE);
            auto revoke =
                master_->PutRevoke(client_, "key", tenant_, type, stale);
            ASSERT_FALSE(revoke);
            EXPECT_EQ(revoke.error(), ErrorCode::INVALID_WRITE);
        }
    }
    ASSERT_TRUE(master_->PutEnd(client_, ObjectMeta{"key", std::nullopt, id},
                                tenant_, ReplicaType::ALL));
    ASSERT_TRUE(master_->InvalidateSharedKv("key", tenant_, old_id));
    auto query = master_->GetReplicaList("key", tenant_);
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1);
    EXPECT_EQ(query->replicas.front().get_shared_kv_descriptor().allocation_id,
              id);
    ASSERT_TRUE(master_->InvalidateSharedKv("key", tenant_, id));
    EXPECT_FALSE(master_->GetReplicaList("key", tenant_));
}

TEST_F(SharedNvmeKvMasterTest, StandaloneSoftPinRemainsUnsupported) {
    auto start = master_->PutStart(client_, "key", tenant_, 1024, shared_);
    ASSERT_TRUE(start);
    const auto id = start->front().get_shared_kv_descriptor().allocation_id;
    ASSERT_TRUE(master_->PutEnd(client_, ObjectMeta{"key", std::nullopt, id},
                                tenant_, ReplicaType::ALL));

    for (uint64_t ttl_ms : {uint64_t{0}, uint64_t{10000}}) {
        auto results = master_->BatchUpdateSoftPin({"key"}, tenant_, ttl_ms);
        ASSERT_EQ(results.size(), 1);
        ASSERT_FALSE(results.front());
        EXPECT_EQ(results.front().error(), ErrorCode::NOT_SUPPORTED);
    }

    auto query = master_->GetReplicaList("key", tenant_);
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1);
    EXPECT_EQ(query->replicas.front().get_shared_kv_descriptor().allocation_id,
              id);
    EXPECT_EQ(query->replicas.front().status, ReplicaStatus::COMPLETE);
}

TEST_F(SharedNvmeKvMasterTest, FlexibleWriteCanPublishWithoutMemoryAllocation) {
    shared_.replica_num = 1;
    auto start = master_->PutStart(client_, "key", tenant_, 1024, shared_);
    ASSERT_TRUE(start);
    ASSERT_EQ(start->size(), 1);
    ASSERT_TRUE(start->front().is_shared_kv_replica());
    const auto id = start->front().get_shared_kv_descriptor().allocation_id;
    ASSERT_TRUE(master_->PutEnd(client_, ObjectMeta{"key", std::nullopt, id},
                                tenant_, ReplicaType::SHARED_NVME_KV));
    auto query = master_->GetReplicaList("key", tenant_);
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1);
    EXPECT_EQ(query->replicas.front().status, ReplicaStatus::COMPLETE);
}

}  // namespace
}  // namespace mooncake
