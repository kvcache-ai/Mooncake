// Copyright 2026 Alibaba Cloud and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <latch>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"

static constexpr uint64_t KiB = 1024;
static constexpr uint64_t MiB = 1024 * KiB;
static constexpr uint64_t GiB = 1024 * MiB;
static constexpr uintptr_t kSegmentBase = 0x100000000ULL;

DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_string(label, "run", "Label written to text and CSV output");
DEFINE_string(csv_path, "", "Optional CSV output path");
DEFINE_string(key_prefix, "batchget_bench", "Object key prefix");
DEFINE_uint64(num_segments, 1, "Number of segments to mount");
DEFINE_uint64(segment_size, 48 * GiB, "Size of each mounted segment");
DEFINE_uint64(num_objects, 20 * 1000 * 1000,
              "Number of objects to prefill and sample from");
DEFINE_uint64(prefill_threads, 8, "Number of concurrent prefill threads");
DEFINE_uint64(prefill_batch_size, 30000, "Batch size used by prefill writes");
DEFINE_uint64(lookup_threads, 56,
              "Number of concurrent BatchGetReplicaList threads");
DEFINE_uint64(batch_size, 30000, "Keys per BatchGetReplicaList request");
DEFINE_uint64(value_size, 2048, "Object value size used for master allocation");
DEFINE_uint64(duration, 120, "Measured duration per cycle in seconds");
DEFINE_uint64(cycles, 2, "Number of measured lookup cycles");
DEFINE_uint64(requests_per_thread, 0,
              "Fixed BatchGetReplicaList requests per lookup thread per "
              "cycle. 0 uses duration-based cycles");
DEFINE_uint64(lookup_interval_ms, 500,
              "Sleep interval after each lookup request per thread");
DEFINE_uint64(refill_threads, 0,
              "Optional writer threads running during measured lookup");
DEFINE_uint64(refill_batch_size, 30000,
              "Batch size used by optional refill writer threads");
DEFINE_uint64(refill_interval_ms, 0,
              "Sleep interval after each refill write batch");
DEFINE_uint64(random_seed, 1, "Base RNG seed for lookup threads");
DEFINE_bool(skip_prefill, false,
            "Skip prefill and assume keys already exist on the master");
DEFINE_bool(log_progress, true, "Log prefill/refill progress");

static inline void unset_cpu_affinity() {
    cpu_set_t cpuset;
    memset(&cpuset, -1, sizeof(cpuset));
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
}

class SegmentClient {
   public:
    SegmentClient(const std::string& name, const std::string& master_server,
                  uintptr_t segment_base, uint64_t segment_size)
        : master_client_(mooncake::generate_uuid()) {
        auto ec = master_client_.Connect(master_server);
        if (ec != mooncake::ErrorCode::OK) {
            throw std::runtime_error("Cannot connect to master server at " +
                                     master_server + ", ec=" + toString(ec));
        }

        segment_.id = mooncake::generate_uuid();
        segment_.name = name;
        segment_.base = segment_base;
        segment_.size = segment_size;
        segment_.te_endpoint = name;
        auto mount_ec = master_client_.MountSegment(segment_);
        if (!mount_ec.has_value()) {
            throw std::runtime_error("Failed to mount segment " + name +
                                     ", ec=" + toString(mount_ec.error()));
        }
    }

    ~SegmentClient() {
        if (remount_future_.valid()) {
            remount_future_.wait();
        }

        auto unmount_result = master_client_.UnmountSegment(segment_.id);
        if (!unmount_result.has_value()) {
            LOG(ERROR) << "Failed to unmount segment " << segment_.name;
        }
    }

    void Ping() {
        if (remount_future_.valid() &&
            remount_future_.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_future_.get();
            remount_future_ = std::future<void>();
        }

        auto ping_result = master_client_.Ping();
        if (!ping_result.has_value()) {
            throw std::runtime_error("Failed to ping master server");
        }

        if (ping_result.value().client_status ==
                mooncake::ClientStatus::NEED_REMOUNT &&
            !remount_future_.valid()) {
            remount_future_ = std::async(std::launch::async, [&]() {
                auto remount_ec = master_client_.ReMountSegment({segment_});
                if (!remount_ec.has_value()) {
                    throw std::runtime_error("Failed to remount segment");
                }
            });
        }
    }

   private:
    mooncake::MasterClient master_client_;
    mooncake::Segment segment_;
    std::future<void> remount_future_;
};

struct WriteStats {
    uint64_t started = 0;
    uint64_t completed = 0;
    uint64_t start_failed = 0;
    uint64_t end_failed = 0;

    void Add(const WriteStats& other) {
        started += other.started;
        completed += other.completed;
        start_failed += other.start_failed;
        end_failed += other.end_failed;
    }
};

struct ThreadStats {
    std::vector<double> latency_ms;
    uint64_t requests = 0;
    uint64_t keys = 0;
    uint64_t success_keys = 0;
    uint64_t object_not_found = 0;
    uint64_t replica_not_ready = 0;
    uint64_t other_failures = 0;
    uint64_t bad_responses = 0;

    void Add(ThreadStats&& other) {
        requests += other.requests;
        keys += other.keys;
        success_keys += other.success_keys;
        object_not_found += other.object_not_found;
        replica_not_ready += other.replica_not_ready;
        other_failures += other.other_failures;
        bad_responses += other.bad_responses;
        latency_ms.insert(latency_ms.end(),
                          std::make_move_iterator(other.latency_ms.begin()),
                          std::make_move_iterator(other.latency_ms.end()));
    }
};

struct ResultRow {
    std::string label;
    std::string cycle;
    uint64_t requests = 0;
    uint64_t keys = 0;
    double duration_sec = 0;
    double batch_per_sec = 0;
    double key_per_sec = 0;
    double avg_ms = 0;
    double p50_ms = 0;
    double p90_ms = 0;
    double p99_ms = 0;
    double max_ms = 0;
    double object_not_found_rate = 0;
    uint64_t success_keys = 0;
    uint64_t object_not_found = 0;
    uint64_t replica_not_ready = 0;
    uint64_t other_failures = 0;
    uint64_t bad_responses = 0;
};

std::string MakeKey(uint64_t id) {
    std::ostringstream ss;
    ss << FLAGS_key_prefix << "_" << std::setw(16) << std::setfill('0') << id;
    return ss.str();
}

std::vector<std::string> MakeSequentialKeys(uint64_t start_id, uint64_t count) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        keys.push_back(MakeKey(start_id + i));
    }
    return keys;
}

std::vector<std::vector<uint64_t>> MakeSliceLengths(size_t count) {
    return std::vector<std::vector<uint64_t>>(
        count, std::vector<uint64_t>{FLAGS_value_size});
}

WriteStats PutCompletedBatch(mooncake::MasterClient& client,
                             const std::vector<std::string>& keys) {
    WriteStats stats;
    const mooncake::ReplicateConfig config;
    auto put_start_result =
        client.BatchPutStart(keys, MakeSliceLengths(keys.size()), config);

    std::vector<std::string> started_keys;
    started_keys.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i < put_start_result.size() && put_start_result[i].has_value()) {
            started_keys.push_back(keys[i]);
            ++stats.started;
        } else {
            ++stats.start_failed;
        }
    }

    if (started_keys.empty()) {
        return stats;
    }

    auto put_end_result =
        client.BatchPutEnd(started_keys, mooncake::ReplicaType::MEMORY);
    for (const auto& result : put_end_result) {
        if (result.has_value()) {
            ++stats.completed;
        } else {
            ++stats.end_failed;
        }
    }
    if (put_end_result.size() < started_keys.size()) {
        stats.end_failed += started_keys.size() - put_end_result.size();
    }
    return stats;
}

WriteStats PrefillObjects() {
    std::atomic<uint64_t> next_id{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> started{0};
    std::atomic<uint64_t> start_failed{0};
    std::atomic<uint64_t> end_failed{0};

    const uint64_t thread_count = std::max<uint64_t>(1, FLAGS_prefill_threads);
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    auto started_at = std::chrono::steady_clock::now();
    for (uint64_t thread_idx = 0; thread_idx < thread_count; ++thread_idx) {
        threads.emplace_back([&, thread_idx] {
            unset_cpu_affinity();
            mooncake::MasterClient client(mooncake::generate_uuid());
            auto ec = client.Connect(FLAGS_master_server);
            if (ec != mooncake::ErrorCode::OK) {
                throw std::runtime_error("Prefill client cannot connect, ec=" +
                                         toString(ec));
            }

            uint64_t next_log = 100000;
            while (true) {
                const uint64_t start_id =
                    next_id.fetch_add(FLAGS_prefill_batch_size);
                if (start_id >= FLAGS_num_objects) {
                    break;
                }
                const uint64_t count = std::min<uint64_t>(
                    FLAGS_prefill_batch_size, FLAGS_num_objects - start_id);
                auto stats = PutCompletedBatch(
                    client, MakeSequentialKeys(start_id, count));
                started.fetch_add(stats.started);
                completed.fetch_add(stats.completed);
                start_failed.fetch_add(stats.start_failed);
                end_failed.fetch_add(stats.end_failed);

                if (FLAGS_log_progress && thread_idx == 0 &&
                    completed.load() >= next_log) {
                    const auto elapsed =
                        std::chrono::duration<double>(
                            std::chrono::steady_clock::now() - started_at)
                            .count();
                    LOG(INFO)
                        << "Prefill progress: completed=" << completed.load()
                        << "/" << FLAGS_num_objects << ", elapsed=" << elapsed
                        << "s";
                    next_log += 100000;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    return {.started = started.load(),
            .completed = completed.load(),
            .start_failed = start_failed.load(),
            .end_failed = end_failed.load()};
}

void RunRefillWorker(std::atomic<bool>& running, std::atomic<uint64_t>& next_id,
                     WriteStats& stats) {
    unset_cpu_affinity();
    mooncake::MasterClient client(mooncake::generate_uuid());
    auto ec = client.Connect(FLAGS_master_server);
    if (ec != mooncake::ErrorCode::OK) {
        throw std::runtime_error("Refill client cannot connect, ec=" +
                                 toString(ec));
    }

    while (running.load(std::memory_order_relaxed)) {
        const uint64_t start_id = next_id.fetch_add(FLAGS_refill_batch_size);
        auto batch_stats = PutCompletedBatch(
            client, MakeSequentialKeys(start_id, FLAGS_refill_batch_size));
        stats.Add(batch_stats);
        if (FLAGS_refill_interval_ms > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(FLAGS_refill_interval_ms));
        }
    }
}

ThreadStats RunLookupWorker(uint64_t thread_idx,
                            std::chrono::steady_clock::time_point deadline,
                            std::shared_ptr<std::latch> barrier) {
    unset_cpu_affinity();
    ThreadStats stats;
    stats.latency_ms.reserve(
        FLAGS_lookup_interval_ms > 0
            ? std::max<uint64_t>(
                  1, FLAGS_duration * 1000 / FLAGS_lookup_interval_ms + 1)
            : 1024);

    mooncake::MasterClient client(mooncake::generate_uuid());
    auto ec = client.Connect(FLAGS_master_server);
    if (ec != mooncake::ErrorCode::OK) {
        throw std::runtime_error("Lookup client cannot connect, ec=" +
                                 toString(ec));
    }

    std::mt19937_64 rng(FLAGS_random_seed + thread_idx);
    std::uniform_int_distribution<uint64_t> key_dist(0, FLAGS_num_objects - 1);

    barrier->arrive_and_wait();
    uint64_t request_idx = 0;
    while (true) {
        const bool fixed_request_mode = FLAGS_requests_per_thread > 0;
        if (fixed_request_mode) {
            if (request_idx >= FLAGS_requests_per_thread) {
                break;
            }
        } else if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }

        std::vector<std::string> keys;
        keys.reserve(FLAGS_batch_size);
        for (uint64_t i = 0; i < FLAGS_batch_size; ++i) {
            keys.push_back(MakeKey(key_dist(rng)));
        }

        const auto started_at = std::chrono::steady_clock::now();
        auto results = client.BatchGetReplicaList(keys);
        const auto finished_at = std::chrono::steady_clock::now();
        const double latency_ms =
            std::chrono::duration<double, std::milli>(finished_at - started_at)
                .count();

        stats.latency_ms.push_back(latency_ms);
        ++stats.requests;
        ++request_idx;
        stats.keys += keys.size();

        if (results.size() != keys.size()) {
            ++stats.bad_responses;
            stats.other_failures += keys.size();
        } else {
            for (const auto& result : results) {
                if (result.has_value()) {
                    ++stats.success_keys;
                    continue;
                }
                switch (result.error()) {
                    case mooncake::ErrorCode::OBJECT_NOT_FOUND:
                        ++stats.object_not_found;
                        break;
                    case mooncake::ErrorCode::REPLICA_IS_NOT_READY:
                        ++stats.replica_not_ready;
                        break;
                    default:
                        ++stats.other_failures;
                        break;
                }
            }
        }

        if (FLAGS_lookup_interval_ms > 0) {
            const auto interval =
                std::chrono::milliseconds(FLAGS_lookup_interval_ms);
            if (fixed_request_mode) {
                std::this_thread::sleep_until(finished_at + interval);
            } else {
                std::this_thread::sleep_until(
                    std::min(finished_at + interval, deadline));
            }
        }
    }
    return stats;
}

double Percentile(const std::vector<double>& sorted_values, double percentile) {
    if (sorted_values.empty()) {
        return 0;
    }
    if (sorted_values.size() == 1) {
        return sorted_values.front();
    }
    const double rank = percentile * (sorted_values.size() - 1);
    const auto low = static_cast<size_t>(std::floor(rank));
    const auto high = static_cast<size_t>(std::ceil(rank));
    if (low == high) {
        return sorted_values[low];
    }
    return sorted_values[low] +
           (sorted_values[high] - sorted_values[low]) * (rank - low);
}

ResultRow BuildResultRow(const std::string& cycle, ThreadStats&& stats,
                         double duration_sec) {
    std::sort(stats.latency_ms.begin(), stats.latency_ms.end());
    const double sum_ms =
        std::accumulate(stats.latency_ms.begin(), stats.latency_ms.end(), 0.0);
    ResultRow row;
    row.label = FLAGS_label;
    row.cycle = cycle;
    row.requests = stats.requests;
    row.keys = stats.keys;
    row.duration_sec = duration_sec;
    row.batch_per_sec = duration_sec > 0
                            ? static_cast<double>(stats.requests) / duration_sec
                            : 0;
    row.key_per_sec =
        duration_sec > 0 ? static_cast<double>(stats.keys) / duration_sec : 0;
    row.avg_ms =
        stats.latency_ms.empty() ? 0 : sum_ms / stats.latency_ms.size();
    row.p50_ms = Percentile(stats.latency_ms, 0.50);
    row.p90_ms = Percentile(stats.latency_ms, 0.90);
    row.p99_ms = Percentile(stats.latency_ms, 0.99);
    row.max_ms = stats.latency_ms.empty() ? 0 : stats.latency_ms.back();
    row.object_not_found_rate =
        stats.keys > 0 ? static_cast<double>(stats.object_not_found) /
                             static_cast<double>(stats.keys)
                       : 0;
    row.success_keys = stats.success_keys;
    row.object_not_found = stats.object_not_found;
    row.replica_not_ready = stats.replica_not_ready;
    row.other_failures = stats.other_failures;
    row.bad_responses = stats.bad_responses;
    return row;
}

ResultRow RunLookupCycle(uint64_t cycle_idx) {
    std::atomic<bool> refill_running{true};
    std::atomic<uint64_t> next_refill_id{FLAGS_num_objects +
                                         cycle_idx * 1000000000ULL};
    std::vector<WriteStats> refill_stats(FLAGS_refill_threads);
    std::vector<std::thread> refill_threads;
    refill_threads.reserve(FLAGS_refill_threads);
    for (uint64_t i = 0; i < FLAGS_refill_threads; ++i) {
        refill_threads.emplace_back(RunRefillWorker, std::ref(refill_running),
                                    std::ref(next_refill_id),
                                    std::ref(refill_stats[i]));
    }

    const auto started_at = std::chrono::steady_clock::now();
    const auto deadline = started_at + std::chrono::seconds(FLAGS_duration);
    auto barrier = std::make_shared<std::latch>(FLAGS_lookup_threads + 1);
    std::vector<ThreadStats> per_thread_stats(FLAGS_lookup_threads);
    std::vector<std::thread> lookup_threads;
    lookup_threads.reserve(FLAGS_lookup_threads);

    for (uint64_t i = 0; i < FLAGS_lookup_threads; ++i) {
        lookup_threads.emplace_back([&, i] {
            per_thread_stats[i] = RunLookupWorker(i, deadline, barrier);
        });
    }

    barrier->arrive_and_wait();
    for (auto& thread : lookup_threads) {
        thread.join();
    }
    const auto finished_at = std::chrono::steady_clock::now();

    refill_running.store(false, std::memory_order_relaxed);
    for (auto& thread : refill_threads) {
        thread.join();
    }

    if (FLAGS_refill_threads > 0) {
        WriteStats total_refill;
        for (const auto& stats : refill_stats) {
            total_refill.Add(stats);
        }
        LOG(INFO) << "cycle=" << cycle_idx
                  << " refill completed=" << total_refill.completed
                  << ", start_failed=" << total_refill.start_failed
                  << ", end_failed=" << total_refill.end_failed;
    }

    ThreadStats merged;
    for (auto& stats : per_thread_stats) {
        merged.Add(std::move(stats));
    }
    const double duration_sec =
        std::chrono::duration<double>(finished_at - started_at).count();
    return BuildResultRow(std::to_string(cycle_idx), std::move(merged),
                          duration_sec);
}

ThreadStats RowsAsStats(const std::vector<ResultRow>& rows) {
    ThreadStats stats;
    for (const auto& row : rows) {
        stats.requests += row.requests;
        stats.keys += row.keys;
        stats.success_keys += row.success_keys;
        stats.object_not_found += row.object_not_found;
        stats.replica_not_ready += row.replica_not_ready;
        stats.other_failures += row.other_failures;
        stats.bad_responses += row.bad_responses;
    }
    return stats;
}

void PrintRows(const std::vector<ResultRow>& rows) {
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "\nBatchGetReplicaList latency summary\n";
    std::cout << "label=" << FLAGS_label << ", objects=" << FLAGS_num_objects
              << ", lookup_threads=" << FLAGS_lookup_threads
              << ", batch_size=" << FLAGS_batch_size
              << ", lookup_interval_ms=" << FLAGS_lookup_interval_ms
              << ", requests_per_thread=" << FLAGS_requests_per_thread
              << ", refill_threads=" << FLAGS_refill_threads << "\n";
    std::cout << std::left << std::setw(8) << "cycle" << std::right
              << std::setw(10) << "requests" << std::setw(14) << "keys/s"
              << std::setw(12) << "batch/s" << std::setw(12) << "avg(ms)"
              << std::setw(12) << "p50(ms)" << std::setw(12) << "p90(ms)"
              << std::setw(12) << "p99(ms)" << std::setw(12) << "max(ms)"
              << std::setw(12) << "miss(%)" << std::setw(14) << "not_found"
              << "\n";
    for (const auto& row : rows) {
        std::cout << std::left << std::setw(8) << row.cycle << std::right
                  << std::setw(10) << row.requests << std::setw(14)
                  << row.key_per_sec << std::setw(12) << row.batch_per_sec
                  << std::setw(12) << row.avg_ms << std::setw(12) << row.p50_ms
                  << std::setw(12) << row.p90_ms << std::setw(12) << row.p99_ms
                  << std::setw(12) << row.max_ms << std::setw(14)
                  << row.object_not_found_rate * 100.0 << std::setw(14)
                  << row.object_not_found << "\n";
    }
}

void WriteCsv(const std::vector<ResultRow>& rows) {
    if (FLAGS_csv_path.empty()) {
        return;
    }

    const bool append_header = !std::ifstream(FLAGS_csv_path).good() ||
                               std::ifstream(FLAGS_csv_path).peek() ==
                                   std::ifstream::traits_type::eof();
    std::ofstream out(FLAGS_csv_path, std::ios::app);
    if (!out) {
        throw std::runtime_error("Cannot open CSV output path: " +
                                 FLAGS_csv_path);
    }

    if (append_header) {
        out << "label,cycle,requests,total_keys,duration_sec,batch_per_sec,"
               "key_per_sec,avg_ms,p50_ms,p90_ms,p99_ms,max_ms,"
               "object_not_found_rate,success_keys,object_not_found,"
               "replica_not_ready,other_failures,bad_responses\n";
    }

    out << std::fixed << std::setprecision(6);
    for (const auto& row : rows) {
        out << row.label << "," << row.cycle << "," << row.requests << ","
            << row.keys << "," << row.duration_sec << "," << row.batch_per_sec
            << "," << row.key_per_sec << "," << row.avg_ms << "," << row.p50_ms
            << "," << row.p90_ms << "," << row.p99_ms << "," << row.max_ms
            << "," << row.object_not_found_rate << "," << row.success_keys
            << "," << row.object_not_found << "," << row.replica_not_ready
            << "," << row.other_failures << "," << row.bad_responses << "\n";
    }
}

void ValidateFlags() {
    if (FLAGS_num_objects == 0 || FLAGS_batch_size == 0 ||
        FLAGS_lookup_threads == 0 || FLAGS_cycles == 0 ||
        FLAGS_prefill_batch_size == 0 || FLAGS_refill_batch_size == 0) {
        throw std::invalid_argument(
            "num_objects, batch_size, lookup_threads, cycles, "
            "prefill_batch_size, and refill_batch_size must be positive");
    }
    if (FLAGS_requests_per_thread == 0 && FLAGS_duration == 0) {
        throw std::invalid_argument(
            "duration must be positive when requests_per_thread is 0");
    }
}

int main(int argc, char** argv) {
    google::InitGoogleLogging("BatchGetReplicaBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    try {
        ValidateFlags();

        std::vector<std::unique_ptr<SegmentClient>> segment_clients;
        std::mutex segment_clients_mutex;
        std::jthread ping_thread([&](std::stop_token stop_token) {
            static const auto OneSecond = std::chrono::seconds(1);
            unset_cpu_affinity();
            while (!stop_token.stop_requested()) {
                const auto started_at = std::chrono::steady_clock::now();
                {
                    std::lock_guard<std::mutex> guard(segment_clients_mutex);
                    for (auto& segment_client : segment_clients) {
                        segment_client->Ping();
                    }
                }
                const auto elapsed =
                    std::chrono::steady_clock::now() - started_at;
                if (elapsed < OneSecond) {
                    std::this_thread::sleep_for(OneSecond - elapsed);
                }
            }
        });

        LOG(INFO) << "Mounting " << FLAGS_num_segments << " segments";
        for (uint64_t i = 0; i < FLAGS_num_segments; ++i) {
            auto segment_client = std::make_unique<SegmentClient>(
                "batch_get_bench_segment_" + std::to_string(i),
                FLAGS_master_server, kSegmentBase + i * FLAGS_segment_size,
                FLAGS_segment_size);
            std::lock_guard<std::mutex> guard(segment_clients_mutex);
            segment_clients.push_back(std::move(segment_client));
        }

        if (!FLAGS_skip_prefill) {
            LOG(INFO) << "Prefilling objects: objects=" << FLAGS_num_objects
                      << ", threads=" << FLAGS_prefill_threads
                      << ", batch_size=" << FLAGS_prefill_batch_size
                      << ", value_size=" << FLAGS_value_size;
            const auto prefill_started_at = std::chrono::steady_clock::now();
            const auto prefill_stats = PrefillObjects();
            const auto prefill_sec =
                std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                              prefill_started_at)
                    .count();
            LOG(INFO) << "Prefill done: completed=" << prefill_stats.completed
                      << ", started=" << prefill_stats.started
                      << ", start_failed=" << prefill_stats.start_failed
                      << ", end_failed=" << prefill_stats.end_failed
                      << ", elapsed=" << prefill_sec << "s";
        }

        std::vector<ResultRow> rows;
        rows.reserve(FLAGS_cycles);
        for (uint64_t cycle = 1; cycle <= FLAGS_cycles; ++cycle) {
            LOG(INFO) << "Starting lookup cycle " << cycle << "/"
                      << FLAGS_cycles;
            rows.push_back(RunLookupCycle(cycle));
        }

        PrintRows(rows);
        WriteCsv(rows);

        if (ping_thread.joinable()) {
            ping_thread.request_stop();
            ping_thread.join();
        }
        segment_clients.clear();
        google::ShutdownGoogleLogging();
        return 0;
    } catch (const std::exception& ex) {
        LOG(ERROR) << "benchmark failed: " << ex.what();
        google::ShutdownGoogleLogging();
        return 1;
    }
}
