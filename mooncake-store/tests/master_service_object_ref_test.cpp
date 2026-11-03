// The accessor contract: a key held across a write's whole read-modify-write,
// and a publication named by an ObjectRef that a later step comes back to.

#include "master_service_test_fixture.h"

#include <barrier>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::test {
namespace {

using Accessor = MasterServiceTestPeer::MetadataAccessorRW;
using Identity = MasterServiceTestPeer::ObjectIdentity;

std::optional<route::ObjectRef> CurrentRef(MasterService& service,
                                           const std::string& key) {
    Accessor accessor(&service, Identity{TenantId::Default(), key});
    if (!accessor.IsPublished()) {
        return std::nullopt;
    }
    return accessor.GetRef();
}

bool IsPublished(MasterService& service, const route::ObjectRef& ref) {
    Accessor accessor(&service, TenantId::Default(), ref);
    return accessor.IsPublished();
}

ReplicateConfig OneMemoryReplica() {
    ReplicateConfig config;
    config.replica_num = 1;
    return config;
}

}  // namespace

TEST_F(MasterServiceTest, ObjectRefReachesOnlyItsOwnPublication) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const std::string key = "object_ref_key";

    PutCompletedObject(service, context.client_id, key, OneMemoryReplica());
    const auto first = CurrentRef(service, key);
    ASSERT_TRUE(first.has_value());
    EXPECT_TRUE(IsPublished(service, *first));

    ASSERT_TRUE(service.Remove(key, TenantId::Default()).has_value());
    EXPECT_FALSE(IsPublished(service, *first));

    PutCompletedObject(service, context.client_id, key, OneMemoryReplica());
    const auto second = CurrentRef(service, key);
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(second->generation, first->generation);
    EXPECT_FALSE(IsPublished(service, *first));
    EXPECT_TRUE(IsPublished(service, *second));

    // A durable erase decided for the first publication leaves the recreate
    // alone; one decided for the current publication takes it.
    MasterServiceTestPeer peer(service);
    peer.FinalizeMetadataEraseAfterDurable(*first, TenantId::Default());
    EXPECT_TRUE(IsPublished(service, *second));
    peer.FinalizeMetadataEraseAfterDurable(*second, TenantId::Default());
    EXPECT_FALSE(CurrentRef(service, key).has_value());
}

TEST_F(MasterServiceTest, SizeChangingUpsertStartsANewPublication) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const std::string key = "object_ref_upsert_key";

    PutCompletedObject(service, context.client_id, key, OneMemoryReplica(),
                       1024);
    const auto before = CurrentRef(service, key);
    ASSERT_TRUE(before.has_value());

    // The same size is overwritten in place: still the same publication.
    ASSERT_TRUE(service
                    .UpsertStart(context.client_id, key, TenantId::Default(),
                                 1024, OneMemoryReplica())
                    .has_value());
    EXPECT_TRUE(IsPublished(service, *before));
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());

    // A new size replaces the object under the same lock.
    ASSERT_TRUE(service
                    .UpsertStart(context.client_id, key, TenantId::Default(),
                                 2048, OneMemoryReplica())
                    .has_value());
    const auto after = CurrentRef(service, key);
    ASSERT_TRUE(after.has_value());
    EXPECT_NE(after->generation, before->generation);
    EXPECT_FALSE(IsPublished(service, *before));
}

TEST_F(MasterServiceTest, ConcurrentPutStartsAdmitOneWriter) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const std::string key = "object_ref_racing_put";
    constexpr int kWriters = 16;

    std::barrier start_line(kWriters);
    std::vector<std::optional<ErrorCode>> results(kWriters);
    std::vector<std::thread> writers;
    for (int i = 0; i < kWriters; ++i) {
        writers.emplace_back([&, i] {
            start_line.arrive_and_wait();
            auto result =
                service.PutStart(context.client_id, key, TenantId::Default(),
                                 1024, OneMemoryReplica());
            results[i] = result ? ErrorCode::OK : result.error();
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }

    int admitted = 0;
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value());
        if (*result == ErrorCode::OK) {
            ++admitted;
        } else {
            EXPECT_EQ(*result, ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }
    EXPECT_EQ(admitted, 1);

    Accessor accessor(&service, Identity{TenantId::Default(), key});
    ASSERT_TRUE(accessor.IsPublished());
    EXPECT_TRUE(accessor.InProcessing());
    EXPECT_EQ(accessor.Get().CountReplicas(&Replica::fn_is_memory_replica), 1);
}

TEST_F(MasterServiceTest, ConcurrentUpsertStartsLeaveOneObject) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const std::string key = "object_ref_racing_upsert";
    constexpr int kWriters = 16;

    PutCompletedObject(service, context.client_id, key, OneMemoryReplica(),
                       1024);

    std::barrier start_line(kWriters);
    std::vector<std::optional<ErrorCode>> results(kWriters);
    std::vector<std::thread> writers;
    for (int i = 0; i < kWriters; ++i) {
        writers.emplace_back([&, i] {
            start_line.arrive_and_wait();
            // Half keep the size and half change it, so in-place overwrites,
            // preemptions and replacements interleave.
            const uint64_t size = i % 2 == 0 ? 1024 : 2048;
            auto result =
                service.UpsertStart(context.client_id, key, TenantId::Default(),
                                    size, OneMemoryReplica());
            results[i] = result ? ErrorCode::OK : result.error();
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }

    int admitted = 0;
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value());
        if (*result == ErrorCode::OK) {
            ++admitted;
        }
    }
    EXPECT_GE(admitted, 1);

    // Whatever the order, the key ends with one object being written, holding
    // the one replica its last writer was handed.
    {
        Accessor accessor(&service, Identity{TenantId::Default(), key});
        ASSERT_TRUE(accessor.IsPublished());
        EXPECT_TRUE(accessor.InProcessing());
        EXPECT_EQ(accessor.Get().CountReplicas(&Replica::fn_is_memory_replica),
                  1);
    }
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service.GetReplicaList(key, TenantId::Default()).has_value());
}

}  // namespace mooncake::test
