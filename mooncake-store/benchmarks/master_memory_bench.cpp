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

namespace {

constexpr uint64_t KiB = 1024;
constexpr uint64_t MiB = 1024 * KiB;
constexpr uint64_t GiB = 1024 * MiB;
constexpr uintptr_t kSegmentBase = 0x100000000ULL;

uint64_t MixSeed(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

uint64_t SampleStart(uint64_t seed, uint64_t request_id, uint64_t span) {
    return MixSeed(seed ^ (request_id * 0x9e3779b97f4a7c15ULL)) % span;
}

}  // namespace

DEFINE_string(master, "127.0.0.1:50051", "Master server address");
DEFINE_string(model, "qwen3-8b",
              "Model label written to the result; does not change workload");
DEFINE_uint64(keys, 100000000, "Number of objects in the memory segment");
DEFINE_uint64(key_offset, 0, "First object index owned by this client");
DEFINE_string(key_prefix, "mcbench", "Object key prefix");
DEFINE_string(segment_prefix, "mcbench",
              "Segment name prefix; use a unique value per host");
DEFINE_uint64(segment_base_offset, 0,
              "Address offset added to the fake segment base");
DEFINE_uint64(object_bytes, 512, "Allocated bytes per object");
DEFINE_uint64(batch_size, 64, "Keys per batch RPC");
DEFINE_uint64(segments, 2, "Number of memory segments to mount");
DEFINE_uint64(segment_bytes, 64 * GiB, "Capacity of each memory segment");
DEFINE_uint64(fill_threads, 16, "Threads used to create objects");
DEFINE_uint64(read_threads, 16, "Threads used to read metadata");
DEFINE_uint64(read_duration, 10, "Read duration in seconds, including warmup");
DEFINE_uint64(read_warmup, 0, "Read warmup seconds excluded from metrics");
DEFINE_uint64(read_rate, 0,
              "Target batch requests per second; 0 means saturate");
DEFINE_uint64(amplify, 1, "Multiplier applied to read_rate");
DEFINE_uint64(seed, 1, "Random seed");
DEFINE_string(result_file, "", "Write the result JSON to this path");

namespace {

struct LatencySamples {
    void Record(double us) {
        std::lock_guard<std::mutex> lock(mutex);
        values.push_back(us);
    }

    void Merge(LatencySamples& other) {
        std::vector<double> incoming;
        {
            std::lock_guard<std::mutex> lock(other.mutex);
            incoming = other.values;
        }
        std::lock_guard<std::mutex> lock(mutex);
        values.insert(values.end(), incoming.begin(), incoming.end());
    }

    std::vector<double> Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex);
        return values;
    }

    mutable std::mutex mutex;
    std::vector<double> values;
};

struct LatencyStats {
    std::atomic<uint64_t> batches{0};
    std::atomic<uint64_t> exist_batches{0};
    std::atomic<uint64_t> get_batches{0};
    std::atomic<uint64_t> keys{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<uint64_t> missing{0};
    std::atomic<uint64_t> missed_deadlines{0};
    LatencySamples latency;
    LatencySamples exist_latency;
    LatencySamples get_latency;

    void Merge(LatencyStats& other) {
        batches.fetch_add(other.batches.load());
        exist_batches.fetch_add(other.exist_batches.load());
        get_batches.fetch_add(other.get_batches.load());
        keys.fetch_add(other.keys.load());
        errors.fetch_add(other.errors.load());
        missing.fetch_add(other.missing.load());
        missed_deadlines.fetch_add(other.missed_deadlines.load());
        latency.Merge(other.latency);
        exist_latency.Merge(other.exist_latency);
        get_latency.Merge(other.get_latency);
    }
};

double PercentileSorted(const std::vector<double>& sorted, double percentile) {
    if (sorted.empty()) {
        return 0.0;
    }
    const double index = percentile * static_cast<double>(sorted.size() - 1);
    const auto lower = static_cast<size_t>(std::floor(index));
    const auto upper = static_cast<size_t>(std::ceil(index));
    if (lower == upper) {
        return sorted[lower];
    }
    const double weight = index - static_cast<double>(lower);
    return sorted[lower] * (1.0 - weight) + sorted[upper] * weight;
}

std::string PercentilesJson(const std::vector<double>& values) {
    std::vector<double> sorted = values;
    std::sort(sorted.begin(), sorted.end());
    std::ostringstream out;
    out << std::fixed << std::setprecision(3)
        << "{\"p50\": " << PercentileSorted(sorted, 0.50)
        << ", \"p90\": " << PercentileSorted(sorted, 0.90)
        << ", \"p99\": " << PercentileSorted(sorted, 0.99)
        << ", \"p999\": " << PercentileSorted(sorted, 0.999) << "}";
    return out.str();
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
    std::vector<double> put_start_us;
    std::vector<double> put_end_us;
};

FillResult Fill(const std::string& master, uint64_t keys, uint64_t object_bytes,
                uint64_t batch_size, uint64_t threads) {
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> errors{0};
    std::mutex samples_mutex;
    std::vector<double> put_start_us;
    std::vector<double> put_end_us;

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
                        batch_keys.push_back(MakeKey(
                            FLAGS_key_prefix, FLAGS_key_offset + key + i));
                        slice_lengths.push_back({object_bytes});
                    }

                    const auto start_time = std::chrono::steady_clock::now();
                    auto starts =
                        client.BatchPutStart(batch_keys, slice_lengths, config);
                    {
                        std::lock_guard<std::mutex> lock(samples_mutex);
                        put_start_us.push_back(
                            std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - start_time)
                                .count());
                    }
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

                    const auto end_time = std::chrono::steady_clock::now();
                    auto ends = client.BatchPutEnd(
                        object_metas, mooncake::ReplicaType::MEMORY);
                    {
                        std::lock_guard<std::mutex> lock(samples_mutex);
                        put_end_us.push_back(
                            std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - end_time)
                                .count());
                    }
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
    result.put_start_us = std::move(put_start_us);
    result.put_end_us = std::move(put_end_us);
    result.seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    return result;
}

void Read(const std::string& master, uint64_t keys, uint64_t batch_size,
          uint64_t threads, uint64_t duration_s, uint64_t warmup_s, uint64_t rate,
          uint64_t amplify, uint64_t seed, LatencyStats& total) {
    std::atomic<uint64_t> next_batch{0};
    const uint64_t target_rate = rate * amplify;
    const auto started = std::chrono::steady_clock::now();
    const auto measured_from = started + std::chrono::seconds(warmup_s);
    const auto deadline = started + std::chrono::seconds(duration_s);

    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (uint64_t thread_id = 0; thread_id < threads; ++thread_id) {
        workers.emplace_back([&]() {
            LatencyStats local;
            try {
                mooncake::MasterClient client(mooncake::generate_uuid());
                auto ec = client.Connect(master);
                if (ec != mooncake::ErrorCode::OK) {
                    throw std::runtime_error("read client connect failed");
                }

                const auto interval =
                    target_rate == 0
                        ? std::chrono::nanoseconds(0)
                        : std::chrono::nanoseconds(static_cast<uint64_t>(
                              1e9 * threads / target_rate));

                auto next_send = std::chrono::steady_clock::now();
                while (true) {
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= deadline) {
                        break;
                    }
                    if (interval.count() > 0 && next_send > now) {
                        const auto sleep = std::min(
                            std::chrono::duration_cast<
                                std::chrono::nanoseconds>(next_send - now),
                            std::chrono::duration_cast<
                                std::chrono::nanoseconds>(deadline - now));
                        if (sleep.count() > 0) {
                            std::this_thread::sleep_for(sleep);
                        }
                    }
                    if (std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }
                    if (interval.count() > 0 &&
                        std::chrono::steady_clock::now() >
                            next_send + interval) {
                        local.missed_deadlines.fetch_add(1);
                    }

                    const uint64_t request_id = next_batch.fetch_add(1);
                    const uint64_t begin =
                        SampleStart(seed, request_id, keys - batch_size + 1);
                    std::vector<std::string> batch_keys;
                    batch_keys.reserve(batch_size);
                    for (uint64_t i = 0; i < batch_size; ++i) {
                        batch_keys.push_back(
                            MakeKey(FLAGS_key_prefix,
                                    FLAGS_key_offset + (begin + i) % keys));
                    }

                    const auto request_start = std::chrono::steady_clock::now();
                    const bool measured = request_start >= measured_from;
                    if (request_id % 2 == 0) {
                        auto results = client.BatchExistKey(batch_keys);
                        const double us =
                            std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - request_start)
                                .count();
                        if (measured) {
                            local.latency.Record(us);
                            local.exist_latency.Record(us);
                            local.exist_batches.fetch_add(1);
                        }
                        if (measured) {
                            for (const auto& result : results) {
                                if (result.has_value() && result.value()) {
                                    local.keys.fetch_add(1);
                                } else if (result.has_value()) {
                                    local.missing.fetch_add(1);
                                } else {
                                    local.errors.fetch_add(1);
                                }
                            }
                        }
                    } else {
                        auto results = client.BatchGetReplicaList(batch_keys);
                        const double us =
                            std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - request_start)
                                .count();
                        if (measured) {
                            local.latency.Record(us);
                            local.get_latency.Record(us);
                            local.get_batches.fetch_add(1);
                        }
                        if (measured) {
                            for (const auto& result : results) {
                                if (result.has_value() &&
                                    !result->replicas.empty()) {
                                    local.keys.fetch_add(1);
                                } else if (result.has_value()) {
                                    local.missing.fetch_add(1);
                                } else {
                                    local.errors.fetch_add(1);
                                }
                            }
                        }
                    }
                    if (measured) {
                        local.batches.fetch_add(1);
                    }
                    if (interval.count() > 0) {
                        next_send += interval;
                    }
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
    if (FLAGS_object_bytes == 0 || FLAGS_read_duration == 0 ||
        FLAGS_read_warmup >= FLAGS_read_duration) {
        LOG(ERROR) << "object_bytes must be positive and read_warmup must be "
                      "smaller than read_duration";
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
                FLAGS_segment_prefix + "-" + std::to_string(getpid()) +
                    "-segment-" + std::to_string(i),
                FLAGS_master,
                kSegmentBase + FLAGS_segment_base_offset +
                    i * FLAGS_segment_bytes,
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
        Read(FLAGS_master, FLAGS_keys, FLAGS_batch_size, FLAGS_read_threads,
             FLAGS_read_duration, FLAGS_read_warmup, FLAGS_read_rate,
             FLAGS_amplify, FLAGS_seed,
             read);
        read_seconds = static_cast<double>(FLAGS_read_duration - FLAGS_read_warmup);
        LOG(INFO) << "read: " << read.batches.load() << " batches, "
                  << read.keys.load() << " keys, " << read.errors.load()
                  << " errors";
    }

    stop_heartbeat.store(true);
    heartbeat.join();

    std::vector<double> latency = read.latency.Snapshot();
    std::vector<double> exist_latency = read.exist_latency.Snapshot();
    std::vector<double> get_latency = read.get_latency.Snapshot();
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
    json << "  \"key_offset\": " << FLAGS_key_offset << ",\n";
    json << "  \"key_prefix\": \"" << JsonEscape(FLAGS_key_prefix) << "\",\n";
    json << "  \"segment_base_offset\": " << FLAGS_segment_base_offset << ",\n";
    json << "  \"object_bytes\": " << FLAGS_object_bytes << ",\n";
    json << "  \"batch_size\": " << FLAGS_batch_size << ",\n";
    json << "  \"fill_threads\": " << FLAGS_fill_threads << ",\n";
    json << "  \"seed\": " << FLAGS_seed << ",\n";
    json << "  \"segments\": " << FLAGS_segments << ",\n";
    json << "  \"segment_bytes\": " << FLAGS_segment_bytes << ",\n";
    json << "  \"fill\": {\"completed\": " << fill.completed
         << ", \"errors\": " << fill.errors << ", \"seconds\": " << fill.seconds
         << ", \"keys_per_second\": " << fill_keys_per_second
         << ", \"put_start_latency_us\": " << PercentilesJson(fill.put_start_us)
         << ", \"put_end_latency_us\": " << PercentilesJson(fill.put_end_us)
         << "},\n";
    json << "  \"read\": {\"amplify\": " << FLAGS_amplify
         << ", \"threads\": " << FLAGS_read_threads
         << ", \"duration_s\": " << FLAGS_read_duration
         << ", \"warmup_s\": " << FLAGS_read_warmup
         << ", \"seconds\": " << read_seconds
         << ", \"rate_target\": " << target_rate
         << ", \"batches\": " << read.batches.load()
         << ", \"exist_batches\": " << read.exist_batches.load()
         << ", \"get_batches\": " << read.get_batches.load()
         << ", \"keys\": " << read.keys.load()
         << ", \"errors\": " << read.errors.load()
         << ", \"missing\": " << read.missing.load()
         << ", \"missed_deadlines\": " << read.missed_deadlines.load()
         << ", \"batches_per_second\": " << read_batches_per_second
         << ", \"keys_per_second\": " << read_keys_per_second
         << ", \"latency_us\": " << PercentilesJson(latency)
         << ", \"exist_latency_us\": " << PercentilesJson(exist_latency)
         << ", \"get_latency_us\": " << PercentilesJson(get_latency) << "}\n";
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

    if (fill.errors != 0 || read.errors != 0 || read.missing.load() != 0 ||
        read.batches.load() == 0) {
        return 1;
    }
    return 0;
}
