#include "master_client.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "types.h"

DEFINE_string(master_server, "127.0.0.1:50051", "Mooncake master address");
DEFINE_int32(within_tasks, 4, "Promotion tasks completed within budget");
DEFINE_int32(late_tasks, 4, "Promotion tasks left to exceed the budget");
DEFINE_int32(late_wait_ms, 120, "Milliseconds to wait before late heartbeat");
DEFINE_uint64(object_size, 1024, "Synthetic object size in bytes");
DEFINE_uint64(segment_size, 128ULL * 1024 * 1024,
              "Mounted memory segment size in bytes");
DEFINE_string(segment_name, "service_probe_segment",
              "Synthetic memory segment name");
DEFINE_string(out_csv, "", "Optional CSV output path");

namespace mooncake::test {
namespace {

struct PhaseResult {
    std::string phase;
    int injected = 0;
    int admitted_gets = 0;
    int heartbeat_returned = 0;
    int alloc_started = 0;
    int completed = 0;
};

template <typename T>
const T& RequireExpected(const tl::expected<T, ErrorCode>& result,
                         const std::string& operation) {
    CHECK(result.has_value()) << operation << " failed: "
                              << toString(result.error());
    return result.value();
}

void RequireExpected(const tl::expected<void, ErrorCode>& result,
                     const std::string& operation) {
    CHECK(result.has_value()) << operation << " failed: "
                              << toString(result.error());
}

std::vector<std::string> MakeKeys(const std::string& phase, int count) {
    std::vector<std::string> keys;
    keys.reserve(count);
    const auto suffix =
        std::chrono::steady_clock::now().time_since_epoch().count();
    for (int i = 0; i < count; ++i) {
        keys.push_back("e3_" + phase + "_" + std::to_string(suffix) + "_" +
                       std::to_string(i));
    }
    return keys;
}

void InjectLocalDiskOnlyKeys(MasterClient& client, const UUID& client_id,
                             const std::vector<std::string>& keys,
                             const std::string& transport_endpoint) {
    std::vector<OffloadTaskItem> tasks;
    std::vector<StorageObjectMetadata> metas;
    tasks.reserve(keys.size());
    metas.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        tasks.push_back(OffloadTaskItem{.tenant_id = "default",
                                        .key = keys[i],
                                        .size = static_cast<int64_t>(
                                            FLAGS_object_size)});
        StorageObjectMetadata metadata;
        metadata.bucket_id = 0;
        metadata.offset = static_cast<int64_t>(i * FLAGS_object_size);
        metadata.key_size = static_cast<int64_t>(keys[i].size());
        metadata.data_size = static_cast<int64_t>(FLAGS_object_size);
        metadata.transport_endpoint = transport_endpoint;
        metas.push_back(metadata);
    }
    RequireExpected(client.NotifyOffloadSuccess(client_id, tasks, metas),
                    "NotifyOffloadSuccess");
}

PhaseResult RunPhase(MasterClient& client, const UUID& client_id,
                     const std::string& phase, int count, int wait_ms,
                     bool complete_tasks,
                     const std::vector<std::string>& preferred_segments) {
    PhaseResult result;
    result.phase = phase;
    const auto keys = MakeKeys(phase, count);
    InjectLocalDiskOnlyKeys(client, client_id, keys, preferred_segments[0]);
    result.injected = static_cast<int>(keys.size());

    for (const auto& key : keys) {
        (void)RequireExpected(client.GetReplicaList(key), "GetReplicaList");
        ++result.admitted_gets;
    }

    if (wait_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
    }

    auto heartbeat = RequireExpected(client.PromotionObjectHeartbeat(client_id),
                                     "PromotionObjectHeartbeat");
    result.heartbeat_returned = static_cast<int>(heartbeat.size());

    if (complete_tasks) {
        for (const auto& task : heartbeat) {
            (void)RequireExpected(
                client.PromotionAllocStart(client_id, task.key, task.tenant_id,
                                           task.size, preferred_segments),
                "PromotionAllocStart");
            ++result.alloc_started;
            RequireExpected(
                client.NotifyPromotionSuccess(client_id, task.key,
                                              task.tenant_id),
                "NotifyPromotionSuccess");
            ++result.completed;
        }
    }

    return result;
}

void WriteCsv(const std::vector<PhaseResult>& rows) {
    if (FLAGS_out_csv.empty()) return;
    std::ofstream out(FLAGS_out_csv);
    CHECK(out) << "cannot open output CSV: " << FLAGS_out_csv;
    out << "phase,injected,admitted_gets,heartbeat_returned,alloc_started,"
           "completed\n";
    for (const auto& row : rows) {
        out << row.phase << ',' << row.injected << ',' << row.admitted_gets
            << ',' << row.heartbeat_returned << ',' << row.alloc_started << ','
            << row.completed << '\n';
    }
}

}  // namespace
}  // namespace mooncake::test

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    using namespace mooncake;
    using namespace mooncake::test;

    const UUID client_id = generate_uuid();
    MasterClient client(client_id);
    CHECK(client.Connect(FLAGS_master_server) == ErrorCode::OK)
        << "failed to connect to master: " << FLAGS_master_server;

    Segment segment;
    segment.id = generate_uuid();
    segment.name = FLAGS_segment_name + "_" + std::to_string(::getpid());
    segment.base = 0x300000000ULL;
    segment.size = FLAGS_segment_size;
    segment.te_endpoint = segment.name;

    RequireExpected(client.MountSegment(segment), "MountSegment");
    RequireExpected(client.MountLocalDiskSegment(client_id, true),
                    "MountLocalDiskSegment");
    RequireExpected(client.ReportSsdCapacity(client_id, 1LL << 30),
                    "ReportSsdCapacity");

    std::vector<std::string> preferred_segments{segment.name};
    std::vector<PhaseResult> rows;
    rows.push_back(RunPhase(client, client_id, "within", FLAGS_within_tasks, 0,
                            true, preferred_segments));
    rows.push_back(RunPhase(client, client_id, "late", FLAGS_late_tasks,
                            FLAGS_late_wait_ms, false, preferred_segments));
    WriteCsv(rows);

    for (const auto& row : rows) {
        std::cout << row.phase << ": injected=" << row.injected
                  << " admitted_gets=" << row.admitted_gets
                  << " heartbeat_returned=" << row.heartbeat_returned
                  << " alloc_started=" << row.alloc_started
                  << " completed=" << row.completed << '\n';
    }

    return 0;
}
