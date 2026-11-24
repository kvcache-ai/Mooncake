// Copyright 2025 Alibaba Cloud and its affiliates
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
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"
#include "random.h"

namespace {

constexpr uint64_t KiB = 1024;
constexpr uint64_t MiB = 1024 * KiB;
constexpr uint64_t GiB = 1024 * MiB;
constexpr uintptr_t kSegmentBase = 0x100000000ULL;

}  // namespace

DEFINE_string(master, "127.0.0.1:50051", "Master server address");
DEFINE_string(model, "qwen3-8b",
              "Model label recorded in the result; does not change workload");
DEFINE_uint64(keys, 100000000, "Number of objects in the memory segment");
DEFINE_uint64(object_bytes, 512, "Allocated bytes per object");
DEFINE_uint64(batch_size, 64, "Keys per batch RPC");
DEFINE_uint64(segments, 2, "Number of memory segments to mount");
DEFINE_uint64(segment_bytes, 64 * GiB, "Capacity of each memory segment");
DEFINE_uint64(fill_threads, 16, "Threads used to create objects");
DEFINE_uint64(read_threads, 16, "Threads used to read metadata");
DEFINE_uint64(read_duration, 10, "Read measurement duration in seconds");
DEFINE_uint64(read_rate, 0,
              "Target batch requests per second; 0 means saturate");
DEFINE_uint64(amplify, 1, "Multiplier applied to read_rate");
DEFINE_uint64(seed, 1, "Random seed");
DEFINE_string(result_file, "", "Write the result JSON to this path");

namespace {

struct LatencyStats {
    std::atomic<uint64_t> batches{0};
    std::atomic<uint64_t> keys{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<uint64_t> missing{0};
    std::mutex mutex;
    std::vector<double> latency_us;

    void Add(double us) {
        std::lock_guard<std::mutex> lock(mutex);
        latency_us.push_back(us);
    }

    void Merge(const LatencyStats& other) {
        batches.fetch_add(other.batches.load());
        keys.fetch_add(other.keys.load());
        errors.fetch_add(other.errors.load());
        missing.fetch_add(other.missing.load());
        std::lock_guard<std::mutex> lock(mutex);
        latency_us.insert(latency_us.end(), other.latency_us.begin(),
                          other.latency_us.end());
    }
};

double Percentile(std::vector<double> values, double percentile) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double index = percentile * static_cast<double>(values.size() - 1);
    const auto lower = static_cast<size_t>(std::floor(index));
    const auto upper = static_cast<size_t>(std::ceil(index));
    if (lower == upper) {
        return values[lower];
    }
    const double weight = index - static_cast<double>(lower);
    return values[lower] * (1.0 - weight) + values[upper] * weight;
}

std::string MakeKey(const std::string& prefix, uint64_t index) {
    std::ostringstream stream;
    stream << prefix << "/" << std::setw(20) << std::setfill('0') << index;
    return stream.str();
}

std::string JsonEscape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        if (c == '"' || c == '\\') {
            escaped.push_back('\\');
        }
        escaped.push_back(c);
    }
    return escaped;
}

class SegmentClient {
   public:
    SegmentClient(const std::string& name, const std::string& master,
                  uintptr_t base, uint64_t size)
        : client_(mooncake::generate_uuid()) {
        auto ec = client_.Connect(master);
        if (ec != mooncake::ErrorCode::OK) {
            throw std::runtime_error("Cannot connect to master at " + master +
                                     ", ec=" + toString(ec));
        }

        segment_.id = mooncake::generate_uuid();
        segment_.name = name;
        segment_.base = base;
        segment_.size = size;
        segment_.te_endpoint = name;
        auto mount = client_.MountSegment(segment_);
        if (!mount.has_value()) {
            throw std::runtime_error("Failed to mount segment " + name +
                                     ", ec=" + toString(mount.error()));
        }
    }

    ~SegmentClient() {
        if (remount_.valid()) {
            remount_.wait();
        }
        (void)client_.UnmountSegment(segment_.id);
    }

    void Ping() {
        if (remount_.valid() && remount_.wait_for(std::chrono::seconds(0)) ==
                                    std::future_status::ready) {
            remount_.get();
            remount_ = std::future<void>();
        }

        auto ping = client_.Ping();
        if (!ping.has_value()) {
            throw std::runtime_error("Ping failed");
        }
        if (ping->client_status == mooncake::ClientStatus::NEED_REMOUNT &&
            !remount_.valid()) {
            remount_ = std::async(std::launch::async, [this]() {
                auto result = client_.ReMountSegment({segment_});
                if (!result.has_value()) {
                    throw std::runtime_error("ReMountSegment failed");
                }
            });
        }
    }

   private:
    mooncake::MasterClient client_;
    mooncake::Segment segment_;
    std::future<void> remount_;
};

struct FillResult {
    uint64_t completed = 0;
    uint64_t errors = 0;
    double seconds = 0.0;
};

FillResult Fill(const std::string& master, uint64_t keys, uint64_t object_bytes,
                uint64_t batch_size, uint64_t threads) {
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> errors{0};

    const auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(threads);
    const uint64_t chunk = keys / threads;
    for (uint64_t thread_id = 0; thread_id < threads; ++thread_id) {
        const uint64_t begin = thread_id * chunk;
        const uint64_t end = thread_id + 1 == threads ? keys : begin + chunk;
        workers.emplace_back([&, begin, end]() {
            try {
                mooncake::MasterClient client(mooncake::generate_uuid());
                auto ec = client.Connect(master);
                if (ec != mooncake::ErrorCode::OK) {
                    errors.fetch_add(end - begin);
                    return;
                }

                mooncake::ReplicateConfig config;
                config.replica_num = 1;
                for (uint64_t key = begin; key < end; key += batch_size) {
                    const uint64_t count = std::min(batch_size, end - key);
                    std::vector<std::string> batch_keys;
                    std::vector<std::vector<uint64_t>> slice_lengths;
                    batch_keys.reserve(count);
                    slice_lengths.reserve(count);
                    for (uint64_t i = 0; i < count; ++i) {
                        batch_keys.push_back(MakeKey("mcbench", key + i));
                        slice_lengths.push_back({object_bytes});
                    }

                    auto starts =
                        client.BatchPutStart(batch_keys, slice_lengths, config);
                    std::vector<std::string> started_keys;
                    std::vector<mooncake::ObjectMeta> object_metas;
                    started_keys.reserve(count);
                    object_metas.reserve(count);
                    for (size_t i = 0; i < starts.size(); ++i) {
                        if (starts[i].has_value()) {
                            started_keys.push_back(batch_keys[i]);
                            object_metas.emplace_back(mooncake::ObjectMeta{
                                batch_keys[i], std::nullopt});
                        } else {
                            errors.fetch_add(1);
                        }
                    }

                    if (started_keys.empty()) {
                        continue;
                    }

                    auto ends = client.BatchPutEnd(
                        object_metas, mooncake::ReplicaType::MEMORY);
                    for (const auto& result : ends) {
                        if (result.has_value()) {
                            completed.fetch_add(1);
                        } else {
                            errors.fetch_add(1);
                        }
                    }
                }
            } catch (const std::exception& error) {
                LOG(ERROR) << "fill worker failed: " << error.what();
                errors.fetch_add(end - begin);
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    FillResult result;
    result.completed = completed.load();
    result.errors = errors.load();
    result.seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    return result;
}

void Read(const std::string& master, uint64_t keys, uint64_t batch_size,
          uint64_t threads, uint64_t duration_s, uint64_t rate,
          uint64_t amplify, uint64_t seed, LatencyStats& total) {
    std::atomic<uint64_t> next_batch{0};
    std::atomic<bool> stop{false};
    const uint64_t batches = std::max<uint64_t>(1, keys / batch_size);
    const uint64_t target_rate = rate * amplify;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(duration_s);

    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (uint64_t thread_id = 0; thread_id < threads; ++thread_id) {
        workers.emplace_back([&, thread_id]() {
            LatencyStats local;
            try {
                mooncake::MasterClient client(mooncake::generate_uuid());
                auto ec = client.Connect(master);
                if (ec != mooncake::ErrorCode::OK) {
                    return;
                }

                mooncake::RandomEngine engine(seed + thread_id);
                const auto interval =
                    target_rate == 0
                        ? std::chrono::nanoseconds(0)
                        : std::chrono::nanoseconds(static_cast<uint64_t>(
                              1e9 * threads / target_rate));

                while (!stop.load()) {
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= deadline) {
                        break;
                    }
                    if (interval.count() > 0) {
                        const auto remaining = std::chrono::duration_cast<
                            std::chrono::nanoseconds>(deadline - now);
                        const auto sleep = std::min(interval, remaining);
                        if (sleep.count() > 0) {
                            std::this_thread::sleep_for(sleep);
                        }
                    }
                    if (std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }

                    const uint64_t batch_id = next_batch.fetch_add(1) % batches;
                    const uint64_t begin =
                        mooncake::randomIndex(keys - batch_size + 1, engine);
                    std::vector<std::string> batch_keys;
                    batch_keys.reserve(batch_size);
                    for (uint64_t i = 0; i < batch_size; ++i) {
                        batch_keys.push_back(
                            MakeKey("mcbench", (begin + i) % keys));
                    }

                    const auto start = std::chrono::steady_clock::now();
                    if (batch_id % 2 == 0) {
                        auto results = client.BatchExistKey(batch_keys);
                        const double us =
                            std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - start)
                                .count();
                        local.Add(us);
                        for (const auto& result : results) {
                            if (result.has_value() && result.value()) {
                                local.keys.fetch_add(1);
                            } else if (result.has_value()) {
                                local.missing.fetch_add(1);
                            } else {
                                local.errors.fetch_add(1);
                            }
                        }
                    } else {
                        auto results = client.BatchGetReplicaList(batch_keys);
                        const double us =
                            std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - start)
                                .count();
                        local.Add(us);
                        for (const auto& result : results) {
                            if (result.has_value()) {
                                local.keys.fetch_add(1);
                            } else {
                                local.errors.fetch_add(1);
                            }
                        }
                    }
                    local.batches.fetch_add(1);
                }
            } catch (const std::exception& error) {
                LOG(ERROR) << "read worker failed: " << error.what();
                local.errors.fetch_add(1);
            }
            total.Merge(local);
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    if (FLAGS_keys == 0 || FLAGS_batch_size == 0 || FLAGS_segments == 0 ||
        FLAGS_segment_bytes == 0 || FLAGS_fill_threads == 0 ||
        FLAGS_read_threads == 0 || FLAGS_amplify == 0) {
        LOG(ERROR) << "keys, batch_size, segments, segment_bytes, "
                      "fill_threads, read_threads and amplify must be positive";
        return 2;
    }
    if (FLAGS_object_bytes == 0 || FLAGS_read_duration == 0) {
        LOG(ERROR) << "object_bytes and read_duration must be positive";
        return 2;
    }
    if (FLAGS_batch_size > FLAGS_keys) {
        LOG(ERROR) << "batch_size must not exceed keys";
        return 2;
    }

    std::vector<std::unique_ptr<SegmentClient>> mounted;
    try {
        for (uint64_t i = 0; i < FLAGS_segments; ++i) {
            mounted.push_back(std::make_unique<SegmentClient>(
                "mcbench-" + std::to_string(getpid()) + "-segment-" +
                    std::to_string(i),
                FLAGS_master, kSegmentBase + i * FLAGS_segment_bytes,
                FLAGS_segment_bytes));
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "cannot mount memory segments: " << error.what();
        return 1;
    }

    std::atomic<bool> stop_heartbeat{false};
    std::thread heartbeat([&]() {
        try {
            while (!stop_heartbeat.load()) {
                for (const auto& segment : mounted) {
                    segment->Ping();
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "heartbeat failed: " << error.what();
        }
    });

    FillResult fill = Fill(FLAGS_master, FLAGS_keys, FLAGS_object_bytes,
                           FLAGS_batch_size, FLAGS_fill_threads);
    LOG(INFO) << "fill: " << fill.completed << " completed, " << fill.errors
              << " errors, " << fill.seconds << " seconds";
    if (fill.errors != 0 || fill.completed != FLAGS_keys) {
        stop_heartbeat.store(true);
        heartbeat.join();
        return 1;
    }

    LatencyStats read;
    double read_seconds = 0.0;
    if (FLAGS_read_duration > 0) {
        const auto read_start = std::chrono::steady_clock::now();
        Read(FLAGS_master, FLAGS_keys, FLAGS_batch_size, FLAGS_read_threads,
             FLAGS_read_duration, FLAGS_read_rate, FLAGS_amplify, FLAGS_seed,
             read);
        read_seconds = std::chrono::duration<double>(
                           std::chrono::steady_clock::now() - read_start)
                           .count();
        LOG(INFO) << "read: " << read.batches.load() << " batches, "
                  << read.keys.load() << " keys, " << read.errors.load()
                  << " errors";
    }

    stop_heartbeat.store(true);
    heartbeat.join();

    std::vector<double> latency = read.latency_us;
    const double read_batches_per_second =
        read_seconds > 0 ? read.batches.load() / read_seconds : 0.0;
    const double read_keys_per_second =
        read_seconds > 0 ? read.keys.load() / read_seconds : 0.0;
    const double fill_keys_per_second =
        fill.seconds > 0 ? fill.completed / fill.seconds : 0.0;
    const uint64_t target_rate = FLAGS_read_rate * FLAGS_amplify;

    std::ostringstream json;
    json << std::fixed << std::setprecision(3);
    json << "{\n";
    json << "  \"benchmark\": \"master_memory_segment\",\n";
    json << "  \"master\": \"" << JsonEscape(FLAGS_master) << "\",\n";
    json << "  \"model\": \"" << JsonEscape(FLAGS_model) << "\",\n";
    json << "  \"keys\": " << FLAGS_keys << ",\n";
    json << "  \"object_bytes\": " << FLAGS_object_bytes << ",\n";
    json << "  \"batch_size\": " << FLAGS_batch_size << ",\n";
    json << "  \"fill_threads\": " << FLAGS_fill_threads << ",\n";
    json << "  \"seed\": " << FLAGS_seed << ",\n";
    json << "  \"segments\": " << FLAGS_segments << ",\n";
    json << "  \"segment_bytes\": " << FLAGS_segment_bytes << ",\n";
    json << "  \"fill\": {\"completed\": " << fill.completed
         << ", \"errors\": " << fill.errors << ", \"seconds\": " << fill.seconds
         << ", \"keys_per_second\": " << fill_keys_per_second << "},\n";
    json << "  \"read\": {\"amplify\": " << FLAGS_amplify
         << ", \"threads\": " << FLAGS_read_threads
         << ", \"duration_s\": " << FLAGS_read_duration
         << ", \"seconds\": " << read_seconds
         << ", \"rate_target\": " << target_rate
         << ", \"batches\": " << read.batches.load()
         << ", \"keys\": " << read.keys.load()
         << ", \"errors\": " << read.errors.load()
         << ", \"missing\": " << read.missing.load()
         << ", \"batches_per_second\": " << read_batches_per_second
         << ", \"keys_per_second\": " << read_keys_per_second
         << ", \"latency_us\": {\"p50\": " << Percentile(latency, 0.50)
         << ", \"p90\": " << Percentile(latency, 0.90)
         << ", \"p99\": " << Percentile(latency, 0.99)
         << ", \"p999\": " << Percentile(latency, 0.999) << "}}\n";
    json << "}\n";

    std::cout << json.str();
    if (!FLAGS_result_file.empty()) {
        std::ofstream output(FLAGS_result_file);
        if (!output) {
            LOG(ERROR) << "cannot open result file " << FLAGS_result_file;
            return 1;
        }
        output << json.str();
    }

    if (fill.errors != 0 || read.errors != 0 || read.missing.load() != 0) {
        return 1;
    }
    return 0;
}
