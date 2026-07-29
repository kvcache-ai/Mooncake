#include "local_delete.h"

#include <gtest/gtest.h>

#include "replica.h"
#include "segment.h"
#include "serialize/serializer.h"
#include "utils.h"

namespace mooncake::test {

namespace {

struct LegacyOffloadTaskItem {
    std::string tenant_id;
    std::string key;
    int64_t size;
};
YLT_REFL(LegacyOffloadTaskItem, tenant_id, key, size);

struct LegacyStorageObjectMetadata {
    int64_t bucket_id;
    int64_t offset;
    int64_t key_size;
    int64_t data_size;
    std::string transport_endpoint;
};
YLT_REFL(LegacyStorageObjectMetadata, bucket_id, offset, key_size, data_size,
         transport_endpoint);

struct LegacyLocalDiskDescriptor {
    UUID client_id;
    uint64_t object_size;
    std::string transport_endpoint;
};
YLT_REFL(LegacyLocalDiskDescriptor, client_id, object_size, transport_endpoint);

LocalDeleteTask MakeTask(std::string storage_id, std::string key) {
    return LocalDeleteTask{
        .task_id = GenerateLocalDeleteTaskId(),
        .local_disk_segment_id = std::move(storage_id),
        .tenant_id = "default",
        .key = std::move(key),
        .object_incarnation = GenerateObjectIncarnation(),
        .expected_bucket_id = 7,
    };
}

}  // namespace

TEST(LocalDeleteRegistryTest, MountEpochFencesPreviousOwner) {
    LocalDeleteRegistry registry;
    const UUID first_client{1, 2};
    const UUID second_client{3, 4};

    const auto first = registry.Mount(first_client, "disk-a",
                                      kLocalDiskCapabilityObjectTombstoneV1);
    const auto retry = registry.Mount(first_client, "disk-a",
                                      kLocalDiskCapabilityObjectTombstoneV1);
    const auto replacement = registry.Mount(
        second_client, "disk-a", kLocalDiskCapabilityObjectTombstoneV1);

    EXPECT_NE(first.mount_epoch, 0);
    EXPECT_EQ(retry.mount_epoch, first.mount_epoch);
    EXPECT_NE(replacement.mount_epoch, first.mount_epoch);
    EXPECT_FALSE(registry.Fetch(first_client, "disk-a", first.mount_epoch, 1));
    EXPECT_TRUE(
        registry.Fetch(second_client, "disk-a", replacement.mount_epoch, 1));
}

TEST(LocalDeleteRegistryTest, UnmountRejectsFormerOwnerWithoutDroppingTasks) {
    LocalDeleteRegistry registry;
    const UUID client{1, 2};
    const auto mount =
        registry.Mount(client, "disk-a", kLocalDiskCapabilityObjectTombstoneV1);
    const auto task = MakeTask("disk-a", "key");
    ASSERT_TRUE(registry.ApplyDurableTasks({task}));

    registry.Unmount(client);
    EXPECT_FALSE(registry.Fetch(client, "disk-a", mount.mount_epoch, 1));
    EXPECT_EQ(registry.Size(), 1);

    const auto remount =
        registry.Mount(client, "disk-a", kLocalDiskCapabilityObjectTombstoneV1);
    auto fetched = registry.Fetch(client, "disk-a", remount.mount_epoch, 1);
    ASSERT_TRUE(fetched);
    ASSERT_EQ(fetched->size(), 1);
    EXPECT_EQ(fetched->front(), task);
}

TEST(LocalDeleteRegistryTest, ClientRemountingAnotherDiskLosesOldBinding) {
    LocalDeleteRegistry registry;
    const UUID client{1, 2};
    const auto first =
        registry.Mount(client, "disk-a", kLocalDiskCapabilityObjectTombstoneV1);
    const auto second =
        registry.Mount(client, "disk-b", kLocalDiskCapabilityObjectTombstoneV1);

    EXPECT_FALSE(registry.Fetch(client, "disk-a", first.mount_epoch, 1));
    EXPECT_TRUE(registry.Fetch(client, "disk-b", second.mount_epoch, 1));
}

TEST(LocalDeleteRegistryTest, CapabilityNegotiationIsFailClosed) {
    LocalDeleteRegistry registry;
    const UUID legacy_client{1, 2};
    const auto mount = registry.Mount(legacy_client, "disk-a", 0);
    EXPECT_FALSE(registry.Fetch(legacy_client, "disk-a", mount.mount_epoch, 1));
}

TEST(LocalDeleteRegistryTest, ReservationPublishesOnlyAfterCommit) {
    LocalDeleteRegistry registry(2);
    const UUID client{1, 2};
    const auto mount =
        registry.Mount(client, "disk-a", kLocalDiskCapabilityObjectTombstoneV1);
    auto task = MakeTask("disk-a", "key");

    auto reservation = registry.Reserve({task});
    ASSERT_TRUE(reservation);
    EXPECT_TRUE(
        registry.Fetch(client, "disk-a", mount.mount_epoch, 8)->empty());

    reservation.value()->Publish();
    auto fetched = registry.Fetch(client, "disk-a", mount.mount_epoch, 8);
    ASSERT_TRUE(fetched);
    ASSERT_EQ(fetched->size(), 1);
    EXPECT_EQ(fetched->front(), task);

    registry.Erase("disk-a", {task.task_id});
    EXPECT_EQ(registry.Size(), 0);
}

TEST(LocalDeleteRegistryTest, SnapshotRestoreKeepsPendingIntent) {
    LocalDeleteRegistry source;
    auto first = MakeTask("disk-a", "first");
    auto second = MakeTask("disk-b", "second");
    ASSERT_TRUE(source.ApplyDurableTasks({first, second}));

    LocalDeleteRegistry restored;
    ASSERT_TRUE(restored.Restore(source.Snapshot()));
    EXPECT_EQ(restored.Size(), 2);
    EXPECT_TRUE(restored.ApplyDurableTasks({first}));
    EXPECT_EQ(restored.Size(), 2);
}

TEST(LocalDeleteRegistryTest, CapacityIncludesReservations) {
    LocalDeleteRegistry registry(1);
    {
        auto first = registry.Reserve({MakeTask("disk-a", "first")});
        ASSERT_TRUE(first);
        auto second = registry.Reserve({MakeTask("disk-a", "second")});
        ASSERT_FALSE(second);
        EXPECT_EQ(second.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    }
    EXPECT_TRUE(registry.Reserve({MakeTask("disk-a", "second")}));
}

TEST(LocalDeleteRegistryTest, DuplicateDurableTasksDoNotConsumeCapacity) {
    LocalDeleteRegistry registry(1);
    const auto task = MakeTask("disk-a", "key");

    EXPECT_TRUE(registry.ApplyDurableTasks({task, task}));
    EXPECT_EQ(registry.Size(), 1);
    EXPECT_TRUE(registry.ApplyDurableTasks({task}));
    EXPECT_EQ(registry.Size(), 1);
    EXPECT_FALSE(
        registry.ApplyDurableTasks({MakeTask("disk-a", "another-key")}));
}

TEST(LocalDeleteRegistryTest, InvalidTasksAreRejected) {
    LocalDeleteRegistry registry;
    auto missing_storage = MakeTask("", "key");
    auto missing_task_id = MakeTask("disk-a", "key");
    missing_task_id.task_id = {};

    auto reservation = registry.Reserve({missing_storage});
    ASSERT_FALSE(reservation);
    EXPECT_EQ(reservation.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(registry.ApplyDurableTasks({missing_task_id}));
    EXPECT_FALSE(registry.Restore({missing_storage}));
}

TEST(LocalDeleteWireCompatibilityTest, AdditiveFieldsUseCompatibleEncoding) {
    const LegacyOffloadTaskItem legacy_task{"default", "key", 1024};
    const auto legacy_task_bytes = struct_pack::serialize(legacy_task);
    OffloadTaskItem current_task;
    ASSERT_EQ(struct_pack::deserialize_to(current_task, legacy_task_bytes),
              struct_pack::errc::ok);
    EXPECT_TRUE(current_task.GetObjectIncarnation().IsZero());

    current_task.object_incarnation = ObjectIncarnation{11, 12};
    const auto current_task_bytes = struct_pack::serialize(current_task);
    LegacyOffloadTaskItem decoded_legacy_task;
    ASSERT_EQ(
        struct_pack::deserialize_to(decoded_legacy_task, current_task_bytes),
        struct_pack::errc::ok);
    EXPECT_EQ(decoded_legacy_task.key, legacy_task.key);

    const LegacyStorageObjectMetadata legacy_storage{7, 1, 3, 1024, "endpoint"};
    const auto legacy_storage_bytes = struct_pack::serialize(legacy_storage);
    StorageObjectMetadata current_storage;
    ASSERT_EQ(
        struct_pack::deserialize_to(current_storage, legacy_storage_bytes),
        struct_pack::errc::ok);
    EXPECT_TRUE(current_storage.GetObjectIncarnation().IsZero());
    current_storage.object_incarnation = ObjectIncarnation{11, 12};
    const auto current_storage_bytes = struct_pack::serialize(current_storage);
    LegacyStorageObjectMetadata decoded_legacy_storage;
    ASSERT_EQ(struct_pack::deserialize_to(decoded_legacy_storage,
                                          current_storage_bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded_legacy_storage.bucket_id, legacy_storage.bucket_id);

    const LegacyLocalDiskDescriptor legacy_descriptor{{1, 2}, 1024, "endpoint"};
    const auto legacy_descriptor_bytes =
        struct_pack::serialize(legacy_descriptor);
    LocalDiskDescriptor current_descriptor;
    ASSERT_EQ(struct_pack::deserialize_to(current_descriptor,
                                          legacy_descriptor_bytes),
              struct_pack::errc::ok);
    EXPECT_TRUE(current_descriptor.GetLocalDiskSegmentId().empty());

    current_descriptor.SetDeleteMetadata(
        "disk-a", 3, kLocalDiskCapabilityObjectTombstoneV1, 7, {11, 12});
    const auto current_descriptor_bytes =
        struct_pack::serialize(current_descriptor);
    LegacyLocalDiskDescriptor decoded_legacy_descriptor;
    ASSERT_EQ(struct_pack::deserialize_to(decoded_legacy_descriptor,
                                          current_descriptor_bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded_legacy_descriptor.client_id, legacy_descriptor.client_id);
    EXPECT_EQ(decoded_legacy_descriptor.transport_endpoint,
              legacy_descriptor.transport_endpoint);
}

TEST(LocalDeleteWireCompatibilityTest,
     ReplicaDeserializerAcceptsLegacyAndCurrentLocalDiskPayloads) {
    const UUID client_id{1, 2};
    SegmentManager segment_manager;

    msgpack::sbuffer legacy_buffer;
    MsgpackPacker legacy_packer(&legacy_buffer);
    legacy_packer.pack_array(4);
    legacy_packer.pack(uint64_t{9});
    legacy_packer.pack(static_cast<int16_t>(ReplicaStatus::COMPLETE));
    legacy_packer.pack(static_cast<int8_t>(ReplicaType::LOCAL_DISK));
    legacy_packer.pack_array(3);
    legacy_packer.pack(UuidToString(client_id));
    legacy_packer.pack(uint64_t{1024});
    legacy_packer.pack(std::string("endpoint"));
    auto legacy_object =
        msgpack::unpack(legacy_buffer.data(), legacy_buffer.size());
    auto legacy = Serializer<Replica>::deserialize(legacy_object.get(),
                                                   segment_manager.getView());
    ASSERT_TRUE(legacy);
    auto legacy_descriptor = std::get<LocalDiskDescriptor>(
        legacy.value()->get_descriptor().descriptor_variant);
    EXPECT_TRUE(legacy_descriptor.GetLocalDiskSegmentId().empty());
    EXPECT_TRUE(legacy_descriptor.GetObjectIncarnation().IsZero());

    Replica current(client_id, 1024, "endpoint", ReplicaStatus::COMPLETE,
                    "disk-a", 3, kLocalDiskCapabilityObjectTombstoneV1, 7,
                    {11, 12});
    msgpack::sbuffer current_buffer;
    MsgpackPacker current_packer(&current_buffer);
    ASSERT_TRUE(Serializer<Replica>::serialize(
        current, segment_manager.getView(), current_packer));
    auto current_object =
        msgpack::unpack(current_buffer.data(), current_buffer.size());
    auto decoded = Serializer<Replica>::deserialize(current_object.get(),
                                                    segment_manager.getView());
    ASSERT_TRUE(decoded);
    auto current_descriptor = std::get<LocalDiskDescriptor>(
        decoded.value()->get_descriptor().descriptor_variant);
    EXPECT_EQ(current_descriptor.GetLocalDiskSegmentId(), "disk-a");
    EXPECT_EQ(current_descriptor.GetMountEpoch(), 3);
    EXPECT_EQ(current_descriptor.GetObjectIncarnation(),
              (ObjectIncarnation{11, 12}));
}

}  // namespace mooncake::test
