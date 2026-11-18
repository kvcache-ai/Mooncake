#include "master_client.h"
#include "storage_backend.h"
#include "tenant_id.h"

#include <algorithm>
#include <iostream>
#include <string>

#include <glog/logging.h>

namespace mooncake {
namespace {

const std::string kKey = "offset-ha-same-key";
constexpr size_t kSize = 8192;

LocalDeleteTask FindTask(const std::vector<LocalDeleteTask>& tasks,
                         const UUID& id) {
    const auto it = std::ranges::find(tasks, id, &LocalDeleteTask::task_id);
    CHECK(it != tasks.end());
    return *it;
}

ObjectIncarnation PutOnDisk(MasterClient& master, const UUID& client_id,
                            OffsetAllocatorStorageBackend& disk, char value) {
    auto put = master.PutStart(kKey, {kSize}, ReplicateConfig{});
    CHECK(put);
    ObjectMeta object;
    object.key = kKey;
    CHECK(master.PutEnd(object, ReplicaType::MEMORY));
    auto work = master.OffloadObjectHeartbeat(client_id, true);
    CHECK(work);
    CHECK_EQ(work->size(), 1);
    CHECK_EQ(work->front().key, kKey);
    const auto incarnation = work->front().GetObjectIncarnation();
    CHECK(!incarnation.IsZero());

    // This fixture drives the offload protocol using known bytes, without
    // requiring a transfer-engine data source for the test memory descriptor.
    const auto key = TenantId::Default().MakeScopedKey(kKey);
    std::string bytes(kSize, value);
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {key, {{bytes.data(), bytes.size()}}}};
    StorageBackendInterface::ObjectVersions versions{{key, incarnation}};
    auto written = disk.BatchOffload(
        batch,
        [&](const auto&, auto& metadata) {
            metadata.front().transport_endpoint = "127.0.0.1:19000";
            auto reported =
                master.NotifyOffloadSuccess(client_id, *work, metadata);
            return reported ? ErrorCode::OK : reported.error();
        },
        nullptr, &versions);
    CHECK(written && *written == 1);
    auto replicas = master.GetReplicaList(kKey);
    CHECK(replicas);
    CHECK(std::ranges::any_of(replicas->replicas, [](const auto& replica) {
        return replica.is_local_disk_replica();
    }));
    return incarnation;
}

void Apply(OffsetAllocatorStorageBackend& disk, const LocalDeleteTask& task) {
    const auto scoped = TenantId(task.tenant_id).MakeScopedKey(task.key);
    CHECK(disk.DeleteVersions({{scoped, task.incarnation}}));
}

int Run(int argc, char** argv) {
    CHECK_GE(argc, 4);
    const std::string mode = argv[1];
    const auto client_id = generate_uuid();
    MasterClient master(client_id);
    CHECK(master.Connect(argv[2]) == ErrorCode::OK);
    FileStorageConfig config;
    config.storage_filepath = argv[3];
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 1 << 20;
    config.total_keys_limit = 32;
    config.use_uring = false;
    OffsetAllocatorBackendConfig offset;
    offset.persist_mode = OffsetPersistMode::kStrict;
    offset.eviction_policy = OffsetEvictionPolicy::NONE;
    OffsetAllocatorStorageBackend disk(config, offset);
    CHECK(disk.Init());
    auto identity = disk.GetDeleteStorageId(true);
    CHECK(identity && identity->has_value());
    const auto storage_id = **identity;
    auto mount = master.MountOffsetDeleteStorage(client_id, true, storage_id);
    CHECK(mount);
    const auto epoch = *mount;
    auto fetch = [&] {
        auto tasks = master.FetchLocalDeletes(client_id, storage_id, epoch,
                                              kMaxLocalDeleteTasks);
        CHECK(tasks);
        for (const auto& task : *tasks) {
            CHECK(task.storage_id == storage_id);
            CHECK_EQ(task.key, kKey);
        }
        return *tasks;
    };
    auto ack = [&](const UUID& id) {
        CHECK(master.AckLocalDeletes(client_id, storage_id, epoch, {id}));
    };

    if (mode == "seed") {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "offset-delete-ha-memory";
        segment.base = 0x100000;
        segment.size = 64 << 20;
        segment.te_endpoint = "127.0.0.1:19001";
        segment.protocol = "tcp";
        CHECK(master.MountSegment(segment));
        const auto old_version = PutOnDisk(master, client_id, disk, 'a');
        CHECK(master.Remove(kKey, true));
        auto exists = master.ProbeKey(kKey);
        CHECK(exists && !*exists);
        const auto before_loss = fetch();
        CHECK_EQ(before_loss.size(), 1);
        const auto old_task = before_loss.front();
        CHECK(old_task.incarnation == old_version);
        Apply(disk, old_task);
        CHECK(fetch() == before_loss);
        Apply(disk, old_task);

        const auto new_version = PutOnDisk(master, client_id, disk, 'b');
        CHECK(new_version != old_version);
        auto result = master.BatchRemove({kKey}, true);
        CHECK(result.size() == 1 && result.front());
        exists = master.ProbeKey(kKey);
        CHECK(exists && !*exists);
        const auto pending = fetch();
        CHECK_EQ(pending.size(), 2);
        CHECK(FindTask(pending, old_task.task_id) == old_task);
        const auto new_task = std::ranges::find(pending, new_version,
                                                &LocalDeleteTask::incarnation);
        CHECK(new_task != pending.end());
        CHECK(new_task->task_id != old_task.task_id);
        std::cout << UuidToString(old_task.task_id) << ' '
                  << UuidToString(new_task->task_id) << '\n';
        return 0;
    }

    CHECK_EQ(argc, 6);
    UUID old_id{}, new_id{};
    CHECK(StringToUuid(argv[4], old_id));
    CHECK(StringToUuid(argv[5], new_id));
    const auto pending = fetch();
    const auto active = FindTask(pending, new_id);
    if (mode == "promoted") {
        CHECK_EQ(pending.size(), 2);
        const auto old = FindTask(pending, old_id);
        CHECK(old.incarnation != active.incarnation);
        // Reapply the original request after leader promotion while the new
        // same-name version still lives on SSD and has its own pending task.
        Apply(disk, old);
        auto key = TenantId(active.tenant_id).MakeScopedKey(active.key);
        std::string bytes(kSize, '\0');
        std::unordered_map<std::string, StorageValueSpan> spans{
            {key,
             {0, kSize, {bytes.data(), bytes.size()}, active.incarnation}}};
        CHECK(disk.BatchLoadSpans(spans));
        CHECK_EQ(bytes, std::string(kSize, 'b'));
        ack(old_id);
        CHECK(fetch() == std::vector<LocalDeleteTask>{active});
        ack(old_id);
        CHECK(fetch() == std::vector<LocalDeleteTask>{active});
        std::cout << "old ACK consumed only its task after promotion\n";
        return 0;
    }
    CHECK_EQ(mode, "cold");
    CHECK(pending == std::vector<LocalDeleteTask>{active});
    ack(old_id);
    CHECK(fetch() == pending);
    Apply(disk, active);
    CHECK(fetch() == pending);
    ack(new_id);
    CHECK(fetch().empty());
    CHECK(
        !disk.IsExist(TenantId(active.tenant_id).MakeScopedKey(kKey)).value());
    std::cout
        << "snapshot restored the pending successor and ACK remained exact\n";
    return 0;
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    return mooncake::Run(argc, argv);
}
