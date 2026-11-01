#include "candidate_codecs.h"

#include <stdexcept>

#include "ha/oplog/oplog_applier.h"
#include "ha/standby_metadata_store.h"
#include "metadata_store.h"

namespace mooncake::codec_bench {
namespace {
constexpr const char* kMemoryEndpoint = "127.0.0.1:7101";
constexpr const char* kDiskEndpoint = "127.0.0.1:7102";
constexpr uint64_t kGiB = 1ULL << 30;

template <typename T>
std::string Serialize(const T& value) {
    const auto bytes = struct_pack::serialize(value);
    return std::string(bytes.begin(), bytes.end());
}

MetadataPayload Metadata(size_t index, uint64_t size, size_t replicas,
                         bool pinned, ObjectDataType type) {
    MetadataPayload payload;
    payload.client_id = {index + 1, index + 2};
    payload.size = size;
    payload.group_id = "group-" + std::to_string(index);
    payload.data_type = type;
    payload.hard_pinned = pinned;
    for (size_t r = 0; r < replicas; ++r) {
        Replica::Descriptor descriptor;
        descriptor.id = index * 100 + r + 1;
        descriptor.status = ReplicaStatus::COMPLETE;
        if (r % 2 == 0) {
            descriptor.descriptor_variant =
                MemoryDescriptor{{size, 0x10000 + index * 0x10000 + r * 8192,
                                  "tcp", kMemoryEndpoint}};
        } else {
            descriptor.descriptor_variant = DiskDescriptor{
                "/var/lib/mooncake/object-" + std::to_string(index) +
                    "-replica-" + std::to_string(r),
                size};
        }
        payload.replicas.push_back(std::move(descriptor));
    }
    return payload;
}

void Add(OpLogBatchRecord& batch, OpType type, const std::string& tenant,
         const std::string& key, std::string payload) {
    OpLogEntry entry;
    entry.sequence_id = batch.entries.size() + 1;
    entry.op_type = type;
    entry.tenant_id = tenant;
    entry.object_key = key;
    entry.payload = std::move(payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    batch.entries.push_back(std::move(entry));
    batch.batch_id = 1;
    batch.first_seq = 1;
    batch.last_seq = batch.entries.size();
}

void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
}  // namespace

Workload ReplayWorkload() {
    OpLogBatchRecord batch;
    Add(batch, OpType::SEGMENT_MOUNT, "default", "memory-segment",
        Serialize(
            SegmentMountOp{"memory-segment", kMemoryEndpoint, kGiB, true, ""}));
    Add(batch, OpType::SEGMENT_MOUNT, "default", "disk-segment",
        Serialize(SegmentMountOp{"disk-segment", kDiskEndpoint, kGiB, false,
                                 "/var/lib/mooncake/segment-data"}));
    Add(batch, OpType::PUT_END, "tenant-a", "shared-object",
        Serialize(Metadata(1, 4096, 2, false, ObjectDataType::KVCACHE)));
    Add(batch, OpType::PUT_END, "tenant-b", "shared-object",
        Serialize(Metadata(2, 2048, 2, true, ObjectDataType::TENSOR)));
    Add(batch, OpType::PUT_END, "tenant-a", "temporary",
        Serialize(Metadata(3, 1024, 2, false, ObjectDataType::METADATA)));
    Add(batch, OpType::REMOVE, "tenant-a", "temporary", "");
    Add(batch, OpType::PUT_END, "tenant-a", "revoked",
        Serialize(Metadata(4, 1024, 2, false, ObjectDataType::METADATA)));
    Add(batch, OpType::PUT_REVOKE, "tenant-a", "revoked", "");
    Add(batch, OpType::SEGMENT_UPDATE, "default", "memory-segment",
        Serialize(SegmentUpdateOp{"memory-segment", kMemoryEndpoint, 2 * kGiB,
                                  true, ""}));
    Add(batch, OpType::SEGMENT_UNMOUNT, "default", "disk-segment",
        Serialize(SegmentUnmountOp{kDiskEndpoint}));
    Add(batch, OpType::PUT_END, "tenant-a", "shared-object",
        Serialize(Metadata(5, 8192, 2, true, ObjectDataType::KVCACHE)));
    return {"typed_replay_trace", std::move(batch)};
}

std::vector<Workload> TypedWorkloads() {
    std::vector<Workload> workloads;
    for (auto [count, replicas] :
         {std::pair{32, 2}, std::pair{256, 8}, std::pair{1024, 2}}) {
        OpLogBatchRecord batch;
        for (int i = 0; i < count; ++i) {
            Add(batch, OpType::PUT_END, "tenant-" + std::to_string(i % 8),
                "kv-cache-object-" + std::to_string(i),
                Serialize(Metadata(i + 1, (i % 8 + 1) * 4096, replicas,
                                   i % 3 == 0, ObjectDataType::KVCACHE)));
        }
        workloads.push_back({"typed_put_" + std::to_string(count) + "_r" +
                                 std::to_string(replicas),
                             std::move(batch)});
    }
    OpLogBatchRecord segments;
    for (int i = 0; i < 256; ++i) {
        const std::string name = "segment-" + std::to_string(i);
        const std::string endpoint = "127.0.0.1:" + std::to_string(7101 + i);
        Add(segments, OpType::SEGMENT_MOUNT, "default", name,
            Serialize(
                SegmentMountOp{name, endpoint, kGiB + i * 4096, i % 2 == 0,
                               i % 2 == 0 ? "" : "/var/lib/mooncake/" + name}));
    }
    workloads.push_back({"typed_segment_256", std::move(segments)});
    workloads.push_back(ReplayWorkload());
    return workloads;
}

void VerifyReplay(const OpLogBatchRecord& batch) {
    StandbyMetadataStore store;
    OpLogApplier applier(&store, "p01-semantic-replay");
    const auto split = batch.entries.size() / 2;
    // Exercise an initial prefix and suffix, then duplicate history. The
    // metadata/registry assertions catch the applier's empty-payload fallback.
    for (size_t i = 0; i < split; ++i) {
        Require(applier.ApplyOpLogEntry(batch.entries[i]),
                "prefix apply failed");
    }
    for (size_t i = split; i < batch.entries.size(); ++i) {
        Require(applier.ApplyOpLogEntry(batch.entries[i]),
                "suffix apply failed");
    }
    auto verify = [&] {
        Require(applier.GetExpectedSequenceId() == batch.last_seq + 1,
                "replay cursor changed");
        Require(store.GetKeyCount() == 2, "replay object count changed");
        const auto a = store.GetMetadata("tenant-a", "shared-object");
        const auto b = store.GetMetadata("tenant-b", "shared-object");
        Require(a && b, "tenant isolation lost object");
        Require(
            Serialize(*a) ==
                Serialize(Metadata(5, 8192, 2, true, ObjectDataType::KVCACHE)
                              .ToStandbyMetadata()),
            "upsert UUID/size/replica IDs/descriptors/group/type/pin changed");
        Require(Serialize(*b) ==
                    Serialize(Metadata(2, 2048, 2, true, ObjectDataType::TENSOR)
                                  .ToStandbyMetadata()),
                "other tenant's metadata changed");
        Require(!store.Exists("tenant-a", "temporary") &&
                    !store.Exists("tenant-a", "revoked"),
                "REMOVE or PUT_REVOKE did not replay");
        const auto& registry = applier.GetSegmentRegistry();
        Require(registry.GetAllSegments().size() == 1 &&
                    !registry.HasSegment(kDiskEndpoint),
                "Segment unmount did not replay");
        const auto memory = registry.GetSegment(kMemoryEndpoint);
        Require(memory && memory->segment_name == "memory-segment" &&
                    memory->transport_endpoint == kMemoryEndpoint &&
                    memory->capacity == 2 * kGiB && memory->is_memory_segment &&
                    memory->file_path.empty(),
                "Segment descriptor/update changed");
        ReplicaID max_id = 0;
        Require(store.ValidateReplicaIds(max_id) && max_id == 502,
                "ReplicaID identity changed");
    };
    verify();
    Require(applier.ApplyOpLogEntries(batch.entries) == batch.entries.size(),
            "duplicate apply failed");
    verify();
}
}  // namespace mooncake::codec_bench
