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

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <latch>
#include <random>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"

// Size units for better readability
static constexpr size_t KiB = 1024;
static constexpr size_t MiB = 1024 * KiB;
static constexpr size_t GiB = 1024 * MiB;
static constexpr uintptr_t kSegmentBase = 0x100000000ULL;

DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_uint64(num_segments, 128, "Number of segments to mount");
DEFINE_uint64(segment_size, 64 * GiB, "Size of each segment");
DEFINE_uint64(num_clients, 4, "Number of clients to perform operations");
DEFINE_uint64(num_threads, 1,
              "Number of threads in each client to perform operations");
DEFINE_string(operation, "BatchPut", "Operation to perform");
DEFINE_uint64(num_keys, 10 * 1000,
              "Number of keys to prefill for Get operations on each thread");
DEFINE_uint64(batch_size, 128, "Batch size for batch operations");
DEFINE_uint64(value_size, 4096, "Size of object values");
DEFINE_uint64(duration, 60, "Test duration in seconds");
DEFINE_double(
    prefill_ratio, 0.0,
    "Ratio of segment capacity to prefill before test (0.0-1.0). "
    "E.g., 0.95 means fill segments to 95% before starting benchmark");

static inline void unset_cpu_affinity() {
    // Ensure that the worker threads are not bound to any CPU cores.
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
            throw std::invalid_argument("Cannot connect to master server at " +
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

static std::atomic<uint64_t> gCompletedOperations = 0;

enum class BenchOperation {
    PUT,
    GET,
    BATCH_PUT,
    BATCH_GET,
};

static inline BenchOperation ParseOperation(const std::string& operation_str) {
    if (operation_str == "Put") {
        return BenchOperation::PUT;
    } else if (operation_str == "Get") {
        return BenchOperation::GET;
    } else if (operation_str == "BatchPut") {
        return BenchOperation::BATCH_PUT;
    } else if (operation_str == "BatchGet") {
        return BenchOperation::BATCH_GET;
    } else {
        throw std::invalid_argument("Invalid operation");
    }
}

class BenchClient {
   public:
    BenchClient(const std::string& master_server)
        : master_client_(mooncake::generate_uuid()), running_(false) {
        auto ec = master_client_.Connect(master_server);
        if (ec != mooncake::ErrorCode::OK) {
            throw std::invalid_argument("Cannot connect to master server at " +
                                        master_server + ", ec=" + toString(ec));
        }
    }

    void StartBench(std::shared_ptr<std::latch> barrier,
                    BenchOperation operation, uint64_t num_threads,
                    uint64_t batch_size, uint64_t value_size,
                    uint64_t num_keys) {
        if (running_.load()) {
            return;
        }

        running_.store(true);
        for (size_t i = 0; i < num_threads; i++) {
            threads_.push_back(std::thread(
                std::bind(&BenchClient::BenchFn, this, barrier, operation,
                          batch_size, value_size, num_keys)));
        }
    }

    void StopBench() {
        if (!running_.load()) {
            return;
        }

        running_.store(false);
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }

        threads_.clear();
    }

   private:
    bool Put(const std::string& key, const std::vector<uint64_t>& slice_lengths,
             const mooncake::ReplicateConfig& config) {
        auto put_start_result =
            master_client_.PutStart(key, slice_lengths, config);
        if (!put_start_result.has_value()) {
            return false;
        }

        auto put_end_result =
            master_client_.PutEnd(key, mooncake::ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            return false;
        }

        return true;
    }

    bool Get(const std::string& key) {
        auto get_result = master_client_.GetReplicaList(key);
        return get_result.has_value() ||
               get_result.error() == mooncake::ErrorCode::OBJECT_NOT_FOUND;
    }

    uint64_t BatchPut(const std::vector<std::string>& keys,
                      const std::vector<std::vector<uint64_t>>& slice_lengths,
                      const mooncake::ReplicateConfig& config) {
        std::vector<std::string> started_keys;
        uint64_t success_cnt = 0;

        auto put_start_result =
            master_client_.BatchPutStart(keys, slice_lengths, config);
        for (size_t i = 0; i < keys.size(); i++) {
            if (put_start_result[i].has_value()) {
                started_keys.push_back(keys[i]);
            }
        }

        if (started_keys.empty()) {
            return 0;
        }

        auto put_end_result = master_client_.BatchPutEnd(started_keys);
        for (auto& result : put_end_result) {
            if (result.has_value()) {
                success_cnt++;
            }
        }

        return success_cnt;
    }

    uint64_t BatchGet(const std::vector<std::string>& keys) {
        uint64_t success_cnt = 0;
        auto get_results = master_client_.BatchGetReplicaList(keys);
        for (auto& get_result : get_results) {
            if (get_result.has_value() ||
                get_result.error() == mooncake::ErrorCode::OBJECT_NOT_FOUND) {
                success_cnt++;
            }
        }
        return success_cnt;
    }

    std::vector<std::string> GeneratePutKeys(uint64_t& key_id,
                                             uint64_t num_keys) {
        const auto self_id = std::this_thread::get_id();
        std::vector<std::string> keys;
        keys.reserve(num_keys);

        for (size_t i = 0; i < num_keys; i++, key_id++) {
            std::stringstream ss;
            ss << self_id << "_" << key_id;
            keys.push_back(ss.str());
        }

        return keys;
    }

    std::vector<std::string> GenerateGetKeys(uint64_t key_id,
                                             uint64_t num_keys) {
        static thread_local std::mt19937 generator(std::random_device{}());

        const auto self_id = std::this_thread::get_id();
        std::vector<std::string> keys;
        keys.reserve(num_keys);

        if (key_id == 0) {
            // No keys have been created yet; return empty vector.
            return keys;
        }

        std::uniform_int_distribution<uint64_t> distribution(0, key_id - 1);
        for (size_t i = 0; i < num_keys; i++) {
            std::stringstream ss;
            ss << self_id << "_" << distribution(generator);
            keys.push_back(ss.str());
        }

        return keys;
    }

    void PrefillKeys(uint64_t& key_id, uint64_t batch_size, uint64_t value_size,
                     uint64_t num_keys) {
        const mooncake::ReplicateConfig config;

        uint64_t num_to_prefill = std::min(batch_size, num_keys);
        while (num_to_prefill > 0) {
            auto keys = GeneratePutKeys(key_id, num_to_prefill);
            std::vector<std::vector<uint64_t>> slice_lengths(num_to_prefill,
                                                             {value_size});
            BatchPut(keys, slice_lengths, config);
            num_keys -= num_to_prefill;
            num_to_prefill = std::min(batch_size, num_keys);
        }
    }

    void BenchFn(std::shared_ptr<std::latch> barrier, BenchOperation operation,
                 uint64_t batch_size, uint64_t value_size, uint64_t num_keys) {
        unset_cpu_affinity();

        uint64_t key_id = 0;
        const mooncake::ReplicateConfig config;

        std::vector<std::string> keys;
        std::vector<std::vector<uint64_t>> slice_lengths;
        slice_lengths.reserve(batch_size);
        for (size_t i = 0; i < batch_size; i++) {
            slice_lengths.push_back({value_size});
        }

        if (operation == BenchOperation::GET ||
            operation == BenchOperation::BATCH_GET) {
            PrefillKeys(key_id, batch_size, value_size, num_keys);
        }

        barrier->arrive_and_wait();

        while (running_.load()) {
            switch (operation) {
                case BenchOperation::PUT:
                    keys = GeneratePutKeys(key_id, 1);
                    if (Put(keys[0], slice_lengths[0], config)) {
                        gCompletedOperations.fetch_add(1);
                    }
                    break;
                case BenchOperation::GET:
                    keys = GenerateGetKeys(key_id, 1);
                    if (Get(keys[0])) {
                        gCompletedOperations.fetch_add(1);
                    }
                    break;
                case BenchOperation::BATCH_PUT:
                    keys = GeneratePutKeys(key_id, batch_size);
                    gCompletedOperations.fetch_add(
                        BatchPut(keys, slice_lengths, config));
                    break;
                case BenchOperation::BATCH_GET:
                    keys = GenerateGetKeys(key_id, batch_size);
                    gCompletedOperations.fetch_add(BatchGet(keys));
                    break;
                default:
                    break;
            }
        }
    }

    mooncake::MasterClient master_client_;

    std::atomic<bool> running_;
    std::vector<std::thread> threads_;
};

int main(int argc, char** argv) {
    std::vector<std::unique_ptr<SegmentClient>> segment_clients;
    std::mutex segment_clients_mutex;
    std::jthread ping_thread;
    std::vector<std::unique_ptr<BenchClient>> bench_clients;

    google::InitGoogleLogging("MasterBench");
    FLAGS_logtostderr = true;

    gflags::ParseCommandLineFlags(&argc, &argv, false);

    ping_thread = std::jthread([&](std::stop_token stop_token) {
        static const auto OneSecond = std::chrono::seconds(1);

        unset_cpu_affinity();

        while (!stop_token.stop_requested()) {
            std::chrono::nanoseconds time_elapsed;
            auto start_time = std::chrono::steady_clock::now();
            {
                std::lock_guard<std::mutex> guard(segment_clients_mutex);
                for (auto& segment_client : segment_clients) {
                    segment_client->Ping();
                }
            }
            time_elapsed = std::chrono::steady_clock::now() - start_time;

            if (OneSecond > time_elapsed) {
                std::this_thread::sleep_for(OneSecond - time_elapsed);
            }
        }
    });

    LOG(INFO) << "Mounting " << FLAGS_num_segments << " segments...";
    for (size_t i = 0; i < FLAGS_num_segments; i++) {
        auto segment_client = std::make_unique<SegmentClient>(
            "segment_client_" + std::to_string(i), FLAGS_master_server,
            kSegmentBase + i * FLAGS_segment_size, FLAGS_segment_size);
        {
            std::lock_guard<std::mutex> guard(segment_clients_mutex);
            segment_clients.push_back(std::move(segment_client));
        }
    }
    LOG(INFO) << "Segments mounted";

    // Prefill segments if requested
    if (FLAGS_prefill_ratio > 0.0) {
        LOG(INFO) << "Prefilling segments to " << (FLAGS_prefill_ratio * 100)
                  << "% capacity...";

        // Calculate total capacity and target fill size
        uint64_t total_capacity = FLAGS_num_segments * FLAGS_segment_size;
        uint64_t target_bytes =
            static_cast<uint64_t>(total_capacity * FLAGS_prefill_ratio);
        uint64_t bytes_per_object = FLAGS_value_size;
        uint64_t target_objects = target_bytes / bytes_per_object;

        LOG(INFO) << "Target: " << target_objects << " objects ("
                  << (target_bytes / GiB) << " GiB)";

        // Create a temporary client for prefilling
        mooncake::MasterClient prefill_client(mooncake::generate_uuid());
        auto ec = prefill_client.Connect(FLAGS_master_server);
        if (ec != mooncake::ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect prefill client";
            return 1;
        }

        const mooncake::ReplicateConfig config;
        uint64_t filled_objects = 0;
        uint64_t key_id = 0;

        while (filled_objects < target_objects) {
            uint64_t batch =
                std::min(FLAGS_batch_size, target_objects - filled_objects);
            std::vector<std::string> keys;
            std::vector<std::vector<uint64_t>> slice_lengths;

            keys.reserve(batch);
            slice_lengths.reserve(batch);

            for (uint64_t i = 0; i < batch; i++) {
                keys.push_back("prefill_" + std::to_string(key_id++));
                slice_lengths.push_back({bytes_per_object});
            }

            auto put_start_result =
                prefill_client.BatchPutStart(keys, slice_lengths, config);
            std::vector<std::string> started_keys;
            for (size_t i = 0; i < keys.size(); i++) {
                if (put_start_result[i].has_value()) {
                    started_keys.push_back(keys[i]);
                }
            }

            if (!started_keys.empty()) {
                auto put_end_result = prefill_client.BatchPutEnd(started_keys);
                for (auto& result : put_end_result) {
                    if (result.has_value()) {
                        filled_objects++;
                    }
                }
            }

            if (filled_objects % 10000 == 0) {
                double progress =
                    (double)filled_objects / target_objects * 100.0;
                LOG(INFO) << "Prefill progress: " << filled_objects << "/"
                          << target_objects << " (" << std::fixed
                          << std::setprecision(1) << progress << "%)";
            }
        }

        LOG(INFO) << "Prefill completed: " << filled_objects << " objects";
    }

    LOG(INFO) << "Starting " << FLAGS_num_clients << " bench clients with "
              << FLAGS_num_threads << " threads for each...";
    for (size_t i = 0; i < FLAGS_num_clients; i++) {
        bench_clients.emplace_back(
            std::make_unique<BenchClient>(FLAGS_master_server));
    }

    const auto operation = ParseOperation(FLAGS_operation);
    auto num_threads = FLAGS_num_clients * FLAGS_num_threads;
    auto barrier = std::make_shared<std::latch>(num_threads +
                                                1);  // +1 for the main thread

    for (auto& bench_client : bench_clients) {
        bench_client->StartBench(barrier, operation, FLAGS_num_threads,
                                 FLAGS_batch_size, FLAGS_value_size,
                                 FLAGS_num_keys);
    }
    barrier->arrive_and_wait();
    LOG(INFO) << "Clients started";

    uint64_t last_completed = 0;
    for (size_t i = 0; i < FLAGS_duration; i++) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto curr_completed = gCompletedOperations.load();
        LOG(INFO) << "Completed operations: "
                  << curr_completed - last_completed;
        last_completed = curr_completed;
    }

    auto num_completed_operations = gCompletedOperations.load();

    LOG(INFO) << "Stopping bench clients...";
    for (auto& bench_client : bench_clients) {
        bench_client->StopBench();
    }
    LOG(INFO) << "Clients stopped";

    LOG(INFO) << "Stopping ping thread...";
    if (ping_thread.joinable()) {
        ping_thread.request_stop();
        ping_thread.join();
    }
    LOG(INFO) << "Ping thread stopped";

    LOG(INFO) << "Disconnecting from master...";
    bench_clients.clear();
    segment_clients.clear();
    LOG(INFO) << "Disconnected from master";

    std::cout << "Operations per second: " << std::fixed << std::setprecision(2)
              << num_completed_operations / (double)FLAGS_duration << "\n";

    google::ShutdownGoogleLogging();

    return 0;
}
