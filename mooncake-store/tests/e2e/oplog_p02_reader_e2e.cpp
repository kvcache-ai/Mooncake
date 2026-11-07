// P02 real-etcd reader integration harness.
//
// This is a test-only producer/consumer pair. It is NOT a production writer:
// `seed` writes the historical JSON records through the production storage
// transaction path and then, for the batches the scenario marks as binary,
// overwrites the already-committed etcd value with raw binary bytes to emulate
// a future P03 writer. `verify` then runs on the production read path only:
//
//   etcd raw value -> EtcdHaKvBackend -> OpLogBatchStorage::ReadBatch(s)
//                  -> DecodeOpLogBatchRecord (dual-format dispatch)
//                  -> OpLogBatchStandbyReader::PollOnce -> OpLogApplier
//                  -> StandbyMetadataStore
//
// No read-side transcoding exists between etcd and the decoder.
//
// Usage:
//   oplog_p02_reader_e2e --etcd=<endpoint> --mode=seed|verify
//                        --cluster=<id> --variant=control|mixed_a|mixed_b
//                        [--dump=PATH] [--append-json] [--expect-dump=PATH]

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <xxhash.h>

#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_binary_codec.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/standby_metadata_store.h"
#include "metadata_store.h"
#include "types.h"

namespace {

// main() lives outside this anonymous namespace, so the logical types it uses
// are brought in explicitly.
using mooncake::BuildBatchRecordKey;
using mooncake::BuildBatchRecordRange;
using mooncake::ComputeOpLogChecksum;
using mooncake::DiskDescriptor;
using mooncake::DurablePrefix;
using mooncake::ErrorCode;
using mooncake::EtcdHaKvBackend;
using mooncake::EtcdHelper;
using mooncake::HasOpLogBatchBinaryMagic;
using mooncake::KvPair;
using mooncake::LocalDiskDescriptor;
using mooncake::MemoryDescriptor;
using mooncake::MetadataPayload;
using mooncake::ObjectDataType;
using mooncake::OpLogApplier;
using mooncake::OpLogBatchRecord;
using mooncake::OpLogBatchStandbyPollDisposition;
using mooncake::OpLogBatchStandbyReader;
using mooncake::OpLogBatchStorage;
using mooncake::OpLogEntry;
using mooncake::OpType;
using mooncake::Replica;
using mooncake::ReplicaStatus;
using mooncake::StandbyMetadataStore;
using mooncake::StandbyObjectEntry;
using mooncake::StandbyObjectMetadata;
using mooncake::UUID;

struct Options {
    std::string etcd_endpoints;
    std::string mode;
    std::string cluster;
    std::string variant{"control"};
    std::string dump_path;
    std::string expect_dump_path;
    std::string writer_continuation_path;
    std::string edge_case;
    uint64_t probe_batch{2};
    bool append_json{false};
};

struct Step {
    OpType op_type;
    std::string tenant;
    std::string key;
    uint64_t payload_size;
};

// The logical history is identical for every variant; only the physical wire
// encoding of the batches named in BinaryBatchIds() differs.
std::vector<Step> HistorySteps() {
    return {
        {OpType::PUT_END, "tenant-a", "obj/1", 1024},
        {OpType::PUT_END, "tenant-a", "obj/2", 2048},
        {OpType::PUT_END, "tenant-b", "obj/3", 4096},
        {OpType::REMOVE, "tenant-a", "obj/2", 0},
        {OpType::PUT_END, "tenant-a", "obj/4", 512},
        {OpType::PUT_END, "tenant-b", "obj/5", 8192},
        {OpType::REMOVE, "tenant-b", "obj/3", 0},
        {OpType::PUT_END, "tenant-c", "obj/6", 256},
        {OpType::PUT_END, "tenant-c", "obj/7", 65536},
    };
}

std::vector<uint64_t> BinaryBatchIds(const std::string& variant) {
    if (variant == "mixed_a") {
        return {2, 4};
    }
    if (variant == "mixed_b") {
        return {1, 3};
    }
    return {};
}

std::string MakeMetadataPayload(const Step& step) {
    MetadataPayload payload;
    payload.client_id = UUID{0x1111, 0x2222};
    payload.size = step.payload_size;
    Replica::Descriptor descriptor;
    descriptor.id = 7;
    descriptor.status = ReplicaStatus::COMPLETE;
    LocalDiskDescriptor local;
    local.client_id = UUID{0x3333, 0x4444};
    local.object_size = step.payload_size;
    local.transport_endpoint = "127.0.0.1:12345";
    descriptor.descriptor_variant = local;
    payload.replicas.push_back(std::move(descriptor));
    payload.group_id = "group-" + step.tenant;
    payload.data_type = ObjectDataType::KVCACHE;
    payload.hard_pinned = false;
    auto serialized = struct_pack::serialize(payload);
    return std::string(serialized.begin(), serialized.end());
}

OpLogBatchRecord BuildBatch(uint64_t batch_id, uint64_t first_seq,
                            const std::vector<Step>& steps, size_t begin,
                            size_t end) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = first_seq;
    batch.last_seq = first_seq + (end - begin) - 1;
    for (size_t i = begin; i < end; ++i) {
        OpLogEntry entry;
        entry.sequence_id = first_seq + (i - begin);
        entry.timestamp_ms = 0;
        entry.op_type = steps[i].op_type;
        entry.tenant_id = steps[i].tenant;
        entry.object_key = steps[i].key;
        entry.payload = steps[i].op_type == OpType::PUT_END
                            ? MakeMetadataPayload(steps[i])
                            : std::string();
        entry.checksum = ComputeOpLogChecksum(entry.payload);
        entry.prefix_hash = 0;
        batch.entries.push_back(std::move(entry));
    }
    return batch;
}

std::vector<OpLogBatchRecord> BuildHistory(const std::string& variant) {
    const auto steps = HistorySteps();
    std::vector<std::pair<size_t, size_t>> slices = {
        {0, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 9}};
    std::vector<OpLogBatchRecord> batches;
    uint64_t batch_id = 1;
    uint64_t first_seq = 1;
    for (const auto& [begin, end] : slices) {
        auto batch = BuildBatch(batch_id, first_seq, steps, begin, end);
        first_seq = batch.last_seq + 1;
        batches.push_back(std::move(batch));
        ++batch_id;
    }
    (void)variant;
    return batches;
}

std::string FormatMetadata(const StandbyObjectMetadata& metadata) {
    std::ostringstream out;
    out << "client=" << metadata.client_id.first << "-"
        << metadata.client_id.second << " size=" << metadata.size;
    out << " replicas=" << metadata.replicas.size();
    for (const auto& replica : metadata.replicas) {
        out << " [id=" << replica.id;
        if (std::holds_alternative<LocalDiskDescriptor>(
                replica.descriptor_variant)) {
            const auto& local =
                std::get<LocalDiskDescriptor>(replica.descriptor_variant);
            out << " local=" << local.client_id.first << "-"
                << local.client_id.second << "@" << local.transport_endpoint
                << " bytes=" << local.object_size;
        } else if (std::holds_alternative<MemoryDescriptor>(
                       replica.descriptor_variant)) {
            out << " memory";
        } else if (std::holds_alternative<DiskDescriptor>(
                       replica.descriptor_variant)) {
            out << " disk";
        } else {
            out << " other";
        }
        out << "]";
    }
    out << " group=" << metadata.group_id;
    return out.str();
}

std::string DumpMetadata(StandbyMetadataStore& store) {
    std::vector<StandbyObjectEntry> entries;
    store.Snapshot(entries);
    std::sort(entries.begin(), entries.end(),
              [](const StandbyObjectEntry& lhs, const StandbyObjectEntry& rhs) {
                  if (lhs.tenant_id != rhs.tenant_id) {
                      return lhs.tenant_id < rhs.tenant_id;
                  }
                  return lhs.key < rhs.key;
              });
    std::ostringstream out;
    for (const auto& entry : entries) {
        out << entry.tenant_id << " " << entry.key << " "
            << FormatMetadata(entry.metadata) << "\n";
    }
    return out.str();
}

Options ParseOptions(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        const auto value_of = [&arg](const char* name) {
            const std::string prefix = std::string("--") + name + "=";
            return arg.rfind(prefix, 0) == 0 ? arg.substr(prefix.size())
                                             : std::string();
        };
        if (arg.rfind("--etcd=", 0) == 0) {
            options.etcd_endpoints = value_of("etcd");
        } else if (arg.rfind("--mode=", 0) == 0) {
            options.mode = value_of("mode");
        } else if (arg.rfind("--cluster=", 0) == 0) {
            options.cluster = value_of("cluster");
        } else if (arg.rfind("--variant=", 0) == 0) {
            options.variant = value_of("variant");
        } else if (arg.rfind("--dump=", 0) == 0) {
            options.dump_path = value_of("dump");
        } else if (arg.rfind("--expect-dump=", 0) == 0) {
            options.expect_dump_path = value_of("expect-dump");
        } else if (arg.rfind("--edge-case=", 0) == 0) {
            options.edge_case = value_of("edge-case");
        } else if (arg.rfind("--probe-batch=", 0) == 0) {
            options.probe_batch = static_cast<uint64_t>(
                std::strtoull(value_of("probe-batch").c_str(), nullptr, 10));
        } else if (arg.rfind("--writer-continuation=", 0) == 0) {
            options.writer_continuation_path = value_of("writer-continuation");
        } else if (arg == "--append-json") {
            options.append_json = true;
        } else {
            std::fprintf(stderr, "unknown argument: %s\n", arg.c_str());
            std::exit(2);
        }
    }
    if (options.etcd_endpoints.empty() || options.mode.empty() ||
        options.cluster.empty()) {
        std::fprintf(stderr, "--etcd, --mode and --cluster are required\n");
        std::exit(2);
    }
    return options;
}

int RunSeed(const Options& options, EtcdHaKvBackend& backend) {
    OpLogBatchStorage storage(options.cluster, backend);
    DurablePrefix prefix;
    auto err = storage.InitDurablePrefix(prefix);
    if (err != ErrorCode::OK) {
        std::fprintf(stderr, "InitDurablePrefix failed: %d\n",
                     static_cast<int>(err));
        return 1;
    }
    err = storage.ClaimProducerView(1);
    if (err != ErrorCode::OK) {
        std::fprintf(stderr, "ClaimProducerView failed: %d\n",
                     static_cast<int>(err));
        return 1;
    }

    const auto batches = BuildHistory(options.variant);
    for (const auto& batch : batches) {
        err = storage.WriteBatchAndAdvancePrefix(batch, prefix,
                                                 /*producer_view_version=*/1);
        if (err != ErrorCode::OK) {
            std::fprintf(stderr,
                         "WriteBatchAndAdvancePrefix(%llu) failed: %d\n",
                         static_cast<unsigned long long>(batch.batch_id),
                         static_cast<int>(err));
            return 1;
        }
        prefix = DurablePrefix{.batch_id = batch.batch_id,
                               .last_seq = batch.last_seq};
        std::printf("seeded batch %llu (json) entries=%zu bytes=%zu\n",
                    static_cast<unsigned long long>(batch.batch_id),
                    batch.entries.size(), EncodeOpLogBatchRecord(batch).size());
    }

    // Emulate a future binary writer for the batches this variant marks as
    // binary by replacing the committed value with raw binary bytes.
    for (uint64_t batch_id : BinaryBatchIds(options.variant)) {
        const auto it = std::find_if(batches.begin(), batches.end(),
                                     [batch_id](const OpLogBatchRecord& batch) {
                                         return batch.batch_id == batch_id;
                                     });
        if (it == batches.end()) {
            std::fprintf(stderr, "variant names unknown batch %llu\n",
                         static_cast<unsigned long long>(batch_id));
            return 1;
        }
        const std::string wire = EncodeOpLogBatchRecordBinaryForTest(*it);
        err = backend.Put(BuildBatchRecordKey(options.cluster, batch_id), wire);
        if (err != ErrorCode::OK) {
            std::fprintf(stderr, "binary Put(%llu) failed: %d\n",
                         static_cast<unsigned long long>(batch_id),
                         static_cast<int>(err));
            return 1;
        }
        std::printf(
            "seeded batch %llu (binary) entries=%zu bytes=%zu magic=%s\n",
            static_cast<unsigned long long>(batch_id), it->entries.size(),
            wire.size(), HasOpLogBatchBinaryMagic(wire) ? "yes" : "no");
    }
    std::printf("seed complete cluster=%s variant=%s final_batch=%llu\n",
                options.cluster.c_str(), options.variant.c_str(),
                static_cast<unsigned long long>(prefix.batch_id));
    return 0;
}

int RunVerify(const Options& options, EtcdHaKvBackend& backend) {
    OpLogBatchStorage storage(options.cluster, backend);
    DurablePrefix prefix;
    auto err = storage.ReadDurablePrefix(prefix);
    if (err != ErrorCode::OK) {
        std::fprintf(stderr, "ReadDurablePrefix failed: %d\n",
                     static_cast<int>(err));
        return 1;
    }
    // InitDurablePrefix reruns the private startup validation; this is the same
    // public entry point production uses at start-up.
    err = storage.InitDurablePrefix(prefix);
    if (err != ErrorCode::OK) {
        std::fprintf(stderr, "ValidateDurablePrefixAtStartup failed: %d\n",
                     static_cast<int>(err));
        return 1;
    }
    std::printf("verify: durable_prefix batch=%llu last_seq=%llu\n",
                static_cast<unsigned long long>(prefix.batch_id),
                static_cast<unsigned long long>(prefix.last_seq));

    StandbyMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, options.cluster);
    OpLogBatchStandbyReader reader(options.cluster, backend, applier);

    size_t applied = 0;
    for (int poll = 0; poll < 64; ++poll) {
        auto result = reader.PollOnce(/*max_batches=*/3);
        if (result.disposition != OpLogBatchStandbyPollDisposition::OK) {
            std::fprintf(stderr, "poll failed: disposition=%d error=%d\n",
                         static_cast<int>(result.disposition),
                         static_cast<int>(result.error));
            return 1;
        }
        applied += result.applied_entries;
        const auto applied_prefix = reader.GetLastAppliedDurablePrefix();
        if (applied_prefix && applied_prefix->batch_id == prefix.batch_id &&
            applied_prefix->last_seq == prefix.last_seq) {
            break;
        }
    }
    const auto final_prefix = reader.GetLastAppliedDurablePrefix();
    if (!final_prefix || final_prefix->batch_id != prefix.batch_id ||
        final_prefix->last_seq != prefix.last_seq) {
        std::fprintf(stderr,
                     "reader cursor did not reach the durable prefix\n");
        return 1;
    }
    std::printf("verify: applied_entries=%zu key_count=%zu\n", applied,
                metadata_store.GetKeyCount());

    const std::string dump = DumpMetadata(metadata_store);
    if (!options.dump_path.empty()) {
        std::ofstream out(options.dump_path, std::ios::binary);
        out << dump;
    }

    // Writer continuation: history may end with a binary terminal batch, and
    // the unchanged production writer must still append JSON afterwards.
    bool continuation_written = false;
    if (options.append_json) {
        uint64_t next_batch = prefix.batch_id + 1;
        auto batch =
            BuildBatch(next_batch, prefix.last_seq + 1, HistorySteps(), 0, 1);
        // Replace the payload with a marker so the continuation is
        // identifiable.
        batch.entries.front().op_type = OpType::PUT_END;
        batch.entries.front().tenant_id = "tenant-continuation";
        batch.entries.front().object_key = "obj/continuation";
        batch.entries.front().payload = MakeMetadataPayload(
            {OpType::PUT_END, "tenant-continuation", "obj/continuation", 128});
        batch.entries.front().checksum =
            ComputeOpLogChecksum(batch.entries.front().payload);
        err = storage.WriteBatchAndAdvancePrefix(batch, prefix, 1);
        if (err != ErrorCode::OK) {
            std::fprintf(stderr, "writer continuation failed: %d\n",
                         static_cast<int>(err));
            return 1;
        }
        std::string stored;
        err = backend.Get(BuildBatchRecordKey(options.cluster, next_batch),
                          stored);
        if (err != ErrorCode::OK) {
            std::fprintf(stderr, "readback failed: %d\n",
                         static_cast<int>(err));
            return 1;
        }
        const bool is_json = stored.find("\x89MCOPLG\n") == std::string::npos;
        std::printf(
            "verify: writer continuation batch=%llu format=%s bytes=%zu\n",
            static_cast<unsigned long long>(next_batch),
            is_json ? "json" : "binary", stored.size());
        continuation_written = true;
    }
    if (continuation_written && !options.writer_continuation_path.empty()) {
        std::ofstream out(options.writer_continuation_path);
        out << "continuation_batch=" << prefix.batch_id + 1 << "\n";
        out << "continuation_last_seq=" << prefix.last_seq + 1 << "\n";
    }

    if (!options.expect_dump_path.empty()) {
        std::ifstream expected(options.expect_dump_path, std::ios::binary);
        std::ostringstream buffer;
        buffer << expected.rdbuf();
        if (buffer.str() != dump) {
            std::fprintf(stderr,
                         "materialized metadata differs from the control dump\n"
                         "--- expected ---\n%s--- actual ---\n%s",
                         buffer.str().c_str(), dump.c_str());
            return 1;
        }
        std::printf("verify: metadata matches control dump\n");
    }
    return 0;
}

}  // namespace

// Corrupt-history scenarios. Each one seeds a namespace whose read must fail
// closed; the runner asserts a non-zero exit status.
int RunEdgeCase(const Options& options, EtcdHaKvBackend& backend) {
    const std::string& edge = options.edge_case;
    OpLogBatchStorage storage(options.cluster, backend);
    DurablePrefix prefix;
    auto err = storage.InitDurablePrefix(prefix);
    if (err != ErrorCode::OK) {
        std::fprintf(stderr, "InitDurablePrefix failed: %d\n",
                     static_cast<int>(err));
        return 1;
    }
    err = storage.ClaimProducerView(1);
    if (err != ErrorCode::OK) {
        std::fprintf(stderr, "ClaimProducerView failed: %d\n",
                     static_cast<int>(err));
        return 1;
    }
    const auto batches = BuildHistory("control");
    for (const auto& batch : batches) {
        err = storage.WriteBatchAndAdvancePrefix(batch, prefix, 1);
        if (err != ErrorCode::OK) {
            std::fprintf(stderr, "seed write failed: %d\n",
                         static_cast<int>(err));
            return 1;
        }
        prefix = DurablePrefix{.batch_id = batch.batch_id,
                               .last_seq = batch.last_seq};
    }

    auto record = [&batches](uint64_t batch_id) {
        return *std::find_if(batches.begin(), batches.end(),
                             [batch_id](const OpLogBatchRecord& batch) {
                                 return batch.batch_id == batch_id;
                             });
    };

    if (edge == "unknown_envelope_version") {
        auto wire = EncodeOpLogBatchRecordBinaryForTest(record(2));
        wire[8] = 9;
        if (backend.Put(BuildBatchRecordKey(options.cluster, 2), wire) !=
            ErrorCode::OK) {
            return 1;
        }
    } else if (edge == "bad_checksum") {
        auto wire = EncodeOpLogBatchRecordBinaryForTest(record(2));
        wire.back() =
            static_cast<char>(static_cast<unsigned char>(wire.back()) ^ 0x01);
        if (backend.Put(BuildBatchRecordKey(options.cluster, 2), wire) !=
            ErrorCode::OK) {
            return 1;
        }
    } else if (edge == "missing_terminal") {
        // Delete the batch the durable prefix points at.
        if (backend.DeleteRange(
                BuildBatchRecordKey(options.cluster, prefix.batch_id),
                BuildBatchRecordKey(options.cluster, prefix.batch_id) +
                    "\x01") != ErrorCode::OK) {
            return 1;
        }
    } else if (edge == "key_body_batch_id_mismatch") {
        if (backend.Put(BuildBatchRecordKey(options.cluster, 2),
                        EncodeOpLogBatchRecordBinaryForTest(record(3))) !=
            ErrorCode::OK) {
            return 1;
        }
    } else if (edge == "sequence_gap") {
        // Remove the middle batch so the surviving sequence is not contiguous.
        if (backend.DeleteRange(BuildBatchRecordKey(options.cluster, 3),
                                BuildBatchRecordKey(options.cluster, 4)) !=
            ErrorCode::OK) {
            return 1;
        }
    } else {
        std::fprintf(stderr, "unknown edge case: %s\n", edge.c_str());
        return 2;
    }

    std::printf("edge-case seeded: %s cluster=%s final_batch=%llu\n",
                edge.c_str(), options.cluster.c_str(),
                static_cast<unsigned long long>(prefix.batch_id));
    return 0;
}

// Diagnostic split: read the same committed binary batch through the
// single-key Get path and through the Range path. This isolates whether a
// binary-read failure belongs to the codec or to the etcd bridge.
int RunProbeRead(const Options& options, EtcdHaKvBackend& backend) {
    OpLogBatchStorage storage(options.cluster, backend);
    DurablePrefix prefix;
    auto err = storage.ReadDurablePrefix(prefix);
    if (err != ErrorCode::OK) {
        std::fprintf(stderr, "ReadDurablePrefix failed: %d\n",
                     static_cast<int>(err));
        return 1;
    }
    const std::string key =
        BuildBatchRecordKey(options.cluster, options.probe_batch);

    std::string raw;
    err = backend.Get(key, raw);
    std::printf("probe get err=%d bytes=%zu magic=%s\n", static_cast<int>(err),
                raw.size(), HasOpLogBatchBinaryMagic(raw) ? "yes" : "no");
    if (err != ErrorCode::OK) {
        return 1;
    }
    OpLogBatchRecord decoded;
    std::string reason;
    const bool ok = DecodeOpLogBatchRecord(raw, &decoded, &reason);
    std::printf("probe get decode=%d reason=%s entries=%zu\n",
                static_cast<int>(ok), reason.c_str(),
                ok ? decoded.entries.size() : 0);
    if (!ok) {
        return 1;
    }
    std::printf("probe get payload_sha256=%llu\n",
                static_cast<unsigned long long>(
                    ComputeOpLogChecksum(decoded.entries.front().payload)));
    return 0;
}

int RunProbeRange(const Options& options, EtcdHaKvBackend& backend) {
    OpLogBatchStorage storage(options.cluster, backend);
    std::vector<OpLogBatchRecord> batches;
    std::printf("probe range requesting after=0 limit=0\n");
    const auto err = storage.ReadBatchesAfter(0, 0, batches);
    std::printf("probe range err=%d count=%zu\n", static_cast<int>(err),
                batches.size());
    if (err != ErrorCode::OK) {
        // Surface the raw bytes for the first batch so the corruption is
        // visible in the evidence rather than only inferred.
        OpLogBatchStorage direct(options.cluster, backend);
        std::vector<KvPair> kvs;
        const auto range = BuildBatchRecordRange(options.cluster, 0);
        backend.Range(range.begin_key, range.end_key, 1, kvs);
        if (!kvs.empty()) {
            std::printf("probe range raw_bytes=%zu magic=%s\n",
                        kvs[0].value.size(),
                        HasOpLogBatchBinaryMagic(kvs[0].value) ? "yes" : "no");
        }
        return 1;
    }
    return 0;
}

int main(int argc, char** argv) {
    google::InitGoogleLogging("oplog_p02_reader_e2e");
    FLAGS_logtostderr = 0;
    FLAGS_minloglevel = 2;

    const Options options = ParseOptions(argc, argv);
    const auto connect =
        EtcdHelper::ConnectToEtcdStoreClient(options.etcd_endpoints);
    if (connect != ErrorCode::OK) {
        std::fprintf(stderr, "ConnectToEtcdStoreClient failed: %d\n",
                     static_cast<int>(connect));
        return 1;
    }
    EtcdHaKvBackend backend;

    int rc = 0;
    if (options.mode == "seed") {
        rc = RunSeed(options, backend);
    } else if (options.mode == "edge-case") {
        rc = RunEdgeCase(options, backend);
    } else if (options.mode == "verify") {
        rc = RunVerify(options, backend);
    } else if (options.mode == "probe-get") {
        rc = RunProbeRead(options, backend);
    } else if (options.mode == "probe-range") {
        rc = RunProbeRange(options, backend);
    } else {
        std::fprintf(stderr, "unknown mode: %s\n", options.mode.c_str());
        rc = 2;
    }
    google::ShutdownGoogleLogging();
    return rc;
}
