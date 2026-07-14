#include <gflags/gflags.h>
#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store.h"
#include "ha/oplog/ordered_oplog_writer.h"

DEFINE_string(endpoints, "127.0.0.1:2379", "Etcd endpoints");
DEFINE_string(cluster_id, "", "Unique OpLog cluster ID");
DEFINE_string(mode, "writer", "Benchmark mode: backend or writer");
DEFINE_string(output_json, "", "Output JSON path");
DEFINE_uint64(max_entries, 64, "Maximum entries per batch");
DEFINE_uint64(entry_bytes, 128, "OpLog payload bytes per entry");
DEFINE_uint64(producer_threads, 1, "Writer producer threads");
DEFINE_uint64(warmup_sec, 10, "Warmup duration");
DEFINE_uint64(duration_sec, 30, "Measurement duration");

namespace mooncake {
namespace {

using Clock = std::chrono::steady_clock;

struct Counters {
    std::atomic<uint64_t> entries{0};
    std::atomic<uint64_t> batches{0};
    std::atomic<uint64_t> txn_us{0};
    std::atomic<uint64_t> callbacks{0};
    std::atomic<uint64_t> commit_to_callback_us{0};
    std::atomic<uint64_t> failures{0};
    std::atomic<uint64_t> last_batch_id{0};
};

struct Snapshot {
    uint64_t entries;
    uint64_t batches;
    uint64_t txn_us;
    uint64_t callbacks;
    uint64_t commit_to_callback_us;
    uint64_t failures;
    uint64_t last_batch_id;
};

Snapshot TakeSnapshot(const Counters& counters) {
    return {.entries = counters.entries.load(),
            .batches = counters.batches.load(),
            .txn_us = counters.txn_us.load(),
            .callbacks = counters.callbacks.load(),
            .commit_to_callback_us = counters.commit_to_callback_us.load(),
            .failures = counters.failures.load(),
            .last_batch_id = counters.last_batch_id.load()};
}

Snapshot Delta(const Snapshot& end, const Snapshot& begin) {
    return {.entries = end.entries - begin.entries,
            .batches = end.batches - begin.batches,
            .txn_us = end.txn_us - begin.txn_us,
            .callbacks = end.callbacks - begin.callbacks,
            .commit_to_callback_us =
                end.commit_to_callback_us - begin.commit_to_callback_us,
            .failures = end.failures - begin.failures,
            .last_batch_id = end.last_batch_id};
}

OpLogEntry MakeEntry(uint64_t id) {
    OpLogEntry entry;
    entry.timestamp_ms = 1;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "default";
    entry.object_key = "bench-key-" + std::to_string(id);
    entry.payload.assign(FLAGS_entry_bytes, 'x');
    entry.checksum = OpLogManager::ComputeChecksum(entry.payload);
    return entry;
}

ErrorCode RunBackend(OpLogBatchStorage& storage, Counters& counters,
                     Clock::time_point deadline) {
    DurablePrefix prefix;
    ErrorCode err = storage.ReadDurablePrefix(prefix);
    if (err != ErrorCode::OK) {
        return err;
    }
    while (Clock::now() < deadline) {
        OpLogBatchRecord batch;
        batch.batch_id = prefix.batch_id + 1;
        batch.first_seq = prefix.last_seq + 1;
        for (uint64_t i = 0; i < FLAGS_max_entries; ++i) {
            auto entry = MakeEntry(batch.first_seq + i);
            entry.sequence_id = batch.first_seq + i;
            batch.entries.push_back(std::move(entry));
        }
        batch.last_seq = batch.entries.back().sequence_id;
        const auto started = Clock::now();
        err = storage.WriteBatchAndAdvancePrefix(batch, prefix);
        counters.txn_us +=
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  started)
                .count();
        if (err != ErrorCode::OK) {
            ++counters.failures;
            return err;
        }
        prefix = {.batch_id = batch.batch_id, .last_seq = batch.last_seq};
        counters.last_batch_id = batch.batch_id;
        counters.entries += batch.entries.size();
        ++counters.batches;
    }
    return ErrorCode::OK;
}

ErrorCode VerifyHistory(OpLogBatchStorage& storage, uint64_t expected_entries) {
    DurablePrefix prefix;
    ErrorCode err = storage.ReadDurablePrefix(prefix);
    if (err != ErrorCode::OK || prefix.last_seq != expected_entries) {
        return ErrorCode::INTERNAL_ERROR;
    }
    std::vector<OpLogBatchRecord> batches;
    err = storage.ReadBatchesAfter(0, 0, batches);
    if (err != ErrorCode::OK || batches.size() != prefix.batch_id) {
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode MeasureEncodedBytes(OpLogBatchStorage& storage,
                              uint64_t after_batch_id, uint64_t last_batch_id,
                              uint64_t* encoded_bytes) {
    std::vector<OpLogBatchRecord> batches;
    const size_t count = static_cast<size_t>(last_batch_id - after_batch_id);
    ErrorCode err = storage.ReadBatchesAfter(after_batch_id, count, batches);
    if (err != ErrorCode::OK || batches.size() != count) {
        return ErrorCode::INTERNAL_ERROR;
    }
    *encoded_bytes = 0;
    for (const auto& batch : batches) {
        *encoded_bytes += EncodeOpLogBatchRecord(batch).size();
    }
    return ErrorCode::OK;
}

void WriteResult(const Snapshot& measured, double seconds,
                 double cpu_cores_used, uint64_t encoded_bytes,
                 const DurablePrefix& prefix) {
    Json::Value root;
    root["schema_version"] = 1;
    root["mode"] = FLAGS_mode;
    root["entries"] = Json::UInt64(measured.entries);
    root["batches"] = Json::UInt64(measured.batches);
    root["entries_per_sec"] = measured.entries / seconds;
    root["cpu_cores_used"] = cpu_cores_used;
    root["transactions_per_sec"] = measured.batches / seconds;
    root["batch_entries_mean"] =
        measured.batches
            ? static_cast<double>(measured.entries) / measured.batches
            : 0.0;
    root["encoded_bytes"] = Json::UInt64(encoded_bytes);
    root["failures"] = Json::UInt64(measured.failures);
    root["durable_batch_id"] = Json::UInt64(prefix.batch_id);
    root["durable_sequence"] = Json::UInt64(prefix.last_seq);
    const double txn_us =
        measured.batches
            ? static_cast<double>(measured.txn_us) / measured.batches
            : 0.0;
    const double commit_to_callback_us =
        measured.callbacks
            ? static_cast<double>(measured.commit_to_callback_us) /
                  measured.callbacks
            : 0.0;
    root["commit_to_callback_us"] = commit_to_callback_us;
    root["stage_latency_us"]["txn"] = txn_us;
    root["stage_latency_us"]["post_txn"] =
        std::max(0.0, commit_to_callback_us - txn_us);
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "  ";
    std::ofstream output(FLAGS_output_json);
    output << Json::writeString(builder, root) << '\n';
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_cluster_id.empty() || FLAGS_output_json.empty() ||
        (FLAGS_mode != "backend" && FLAGS_mode != "writer") ||
        FLAGS_max_entries == 0 || FLAGS_entry_bytes == 0 ||
        FLAGS_producer_threads == 0 || FLAGS_duration_sec == 0 ||
        (FLAGS_mode == "backend" && FLAGS_producer_threads != 1)) {
        LOG(ERROR) << "invalid benchmark arguments";
        return 1;
    }
    std::string cluster_id = FLAGS_cluster_id;
    if (!mooncake::NormalizeAndValidateClusterId(cluster_id) ||
        cluster_id.empty()) {
        LOG(ERROR) << "invalid cluster_id";
        return 1;
    }
    auto err = mooncake::EtcdHelper::ConnectToEtcdStoreClient(FLAGS_endpoints);
    if (err != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "failed to connect to etcd: " << mooncake::toString(err);
        return 1;
    }

    mooncake::EtcdHaKvBackend backend;
    mooncake::OpLogBatchStorage storage(cluster_id, backend);
    mooncake::DurablePrefix initial_prefix;
    err = storage.InitDurablePrefix(initial_prefix);
    if (err != mooncake::ErrorCode::OK || initial_prefix.batch_id != 0 ||
        initial_prefix.last_seq != 0) {
        LOG(ERROR) << "benchmark requires an empty cluster namespace";
        return 1;
    }

    mooncake::Counters counters;
    mooncake::Snapshot measured{};
    mooncake::Snapshot measure_begin{};
    mooncake::Snapshot measure_end{};
    std::clock_t cpu_begin = 0;
    std::clock_t cpu_end = 0;
    const auto warmup_deadline =
        mooncake::Clock::now() + std::chrono::seconds(FLAGS_warmup_sec);
    const auto measure_deadline =
        warmup_deadline + std::chrono::seconds(FLAGS_duration_sec);

    if (FLAGS_mode == "backend") {
        err = mooncake::RunBackend(storage, counters, warmup_deadline);
        measure_begin = mooncake::TakeSnapshot(counters);
        cpu_begin = std::clock();
        if (err == mooncake::ErrorCode::OK) {
            err = mooncake::RunBackend(storage, counters, measure_deadline);
        }
        measure_end = mooncake::TakeSnapshot(counters);
        cpu_end = std::clock();
        measured = mooncake::Delta(measure_end, measure_begin);
    } else {
        mooncake::OrderedOpLogWriter writer(
            {.max_entries_per_batch = static_cast<size_t>(FLAGS_max_entries)},
            [&](const mooncake::OpLogBatchRecord& batch,
                const mooncake::DurablePrefix& prefix) {
                const auto started = mooncake::Clock::now();
                const auto result =
                    storage.WriteBatchAndAdvancePrefix(batch, prefix);
                counters.txn_us +=
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        mooncake::Clock::now() - started)
                        .count();
                if (result == mooncake::ErrorCode::OK) {
                    counters.entries += batch.entries.size();
                    ++counters.batches;
                    counters.last_batch_id = batch.batch_id;
                } else {
                    ++counters.failures;
                }
                return result;
            });
        writer.Start();
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> next_id{1};
        std::vector<std::thread> producers;
        for (uint64_t i = 0; i < FLAGS_producer_threads; ++i) {
            producers.emplace_back([&] {
                while (!stop.load(std::memory_order_relaxed)) {
                    auto reservation = writer.Reserve();
                    if (!reservation.has_value()) {
                        std::this_thread::yield();
                        continue;
                    }
                    const auto committed_at = mooncake::Clock::now();
                    const auto committed = writer.Commit(
                        std::move(*reservation),
                        mooncake::MakeEntry(next_id.fetch_add(1)),
                        [&, committed_at](const mooncake::OpLogEntry&) {
                            counters.commit_to_callback_us +=
                                std::chrono::duration_cast<
                                    std::chrono::microseconds>(
                                    mooncake::Clock::now() - committed_at)
                                    .count();
                            ++counters.callbacks;
                        });
                    if (!committed.has_value()) {
                        ++counters.failures;
                    }
                }
            });
        }
        std::this_thread::sleep_until(warmup_deadline);
        measure_begin = mooncake::TakeSnapshot(counters);
        cpu_begin = std::clock();
        std::this_thread::sleep_until(measure_deadline);
        measure_end = mooncake::TakeSnapshot(counters);
        cpu_end = std::clock();
        measured = mooncake::Delta(measure_end, measure_begin);
        stop = true;
        for (auto& producer : producers) {
            producer.join();
        }
        writer.Stop();
        err = writer.LastError();
    }

    const auto final = mooncake::TakeSnapshot(counters);
    uint64_t encoded_bytes = 0;
    if (err == mooncake::ErrorCode::OK) {
        err = mooncake::VerifyHistory(storage, final.entries);
    }
    if (err == mooncake::ErrorCode::OK) {
        err = mooncake::MeasureEncodedBytes(
            storage, measure_begin.last_batch_id, measure_end.last_batch_id,
            &encoded_bytes);
    }
    mooncake::DurablePrefix final_prefix;
    if (err != mooncake::ErrorCode::OK ||
        storage.ReadDurablePrefix(final_prefix) != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "benchmark or history verification failed: "
                   << mooncake::toString(err);
        return 3;
    }
    const double cpu_cores_used = static_cast<double>(cpu_end - cpu_begin) /
                                  CLOCKS_PER_SEC / FLAGS_duration_sec;
    mooncake::WriteResult(measured, FLAGS_duration_sec, cpu_cores_used,
                          encoded_bytes, final_prefix);
    return measured.failures == 0 ? 0 : 3;
}
