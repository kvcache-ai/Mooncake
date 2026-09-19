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
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"

namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kKiB = 1024;
constexpr uint64_t kGiB = 1024 * 1024 * 1024;
constexpr uintptr_t kSegmentBase = 0x100000000ULL;

// The defaults provide a representative three-segment KV-cache workload.
// Every topology, rate, batch, and key parameter can be overridden.
DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_uint64(num_segments, 3, "Number of synthetic workload segments");
DEFINE_uint64(segment_size, 256 * kGiB, "Size of each mounted segment");
DEFINE_uint64(workers_per_segment, 4,
              "Synchronous RPC workers assigned to each segment");
DEFINE_uint64(duration, 60, "Request generation duration in seconds");

DEFINE_double(exist_qps_per_segment, 6.3359,
              "BatchExistKey requests per second per segment");
DEFINE_double(put_qps_per_segment, 3.1431,
              "Complete BatchPut transactions per second per segment");
DEFINE_double(get_qps_per_segment, 2.0242,
              "BatchGetReplicaList requests per second per segment");
DEFINE_uint64(exist_batch_size, 86,
              "Keys in each synthetic BatchExistKey request");
DEFINE_uint64(put_batch_size, 45,
              "Keys in each synthetic BatchPut transaction");
DEFINE_uint64(get_batch_size, 128,
              "Keys in each synthetic BatchGetReplicaList request");
DEFINE_uint64(value_size, 448 * kKiB, "Value size for every generated key");
DEFINE_uint64(put_commit_delay_us, 1152,
              "Delay from BatchPutStart completion to BatchPutEnd");
DEFINE_string(arrival_model, "poisson",
              "Request interval model: poisson or fixed");

DEFINE_string(key_tag, "synthetic", "Tag encoded in every generated key");
DEFINE_uint64(key_size, 64,
              "Exact generated key size; 0 keeps the natural key length");
DEFINE_uint64(key_pool_size, 1000000,
              "Maximum committed key IDs retained per segment");
DEFINE_double(exist_hit_ratio, 0.5,
              "Requested fraction of Exist keys sampled from committed keys");
DEFINE_double(get_hit_ratio, 1.0,
              "Requested fraction of Get keys sampled from committed keys");
DEFINE_string(placement_mode, "preferred",
              "Put placement: preferred or global");
DEFINE_uint64(replica_num, 1, "Replica count requested by each PutStart");

DEFINE_uint64(ping_interval_ms, 1000,
              "Heartbeat interval for every mounted segment");
DEFINE_uint64(max_pending_events_per_segment, 100000,
              "Abort generation when a segment queue reaches this depth");
DEFINE_uint64(seed, 1, "Base random seed");
DEFINE_bool(cleanup_segments, true,
            "Unmount benchmark-created segments before exiting");

enum class SyntheticOperation {
    kExist,
    kPut,
    kGet,
};

uint64_t Mix64(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

std::string Hex64(uint64_t value) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result(16, '0');
    for (int index = 15; index >= 0; --index) {
        result[index] = kHex[value & 0xf];
        value >>= 4;
    }
    return result;
}

uint64_t LatenessNs(Clock::time_point due) {
    const auto now = Clock::now();
    if (now <= due) {
        return 0;
    }
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - due)
            .count());
}

template <typename T>
std::pair<uint64_t, uint64_t> CountExpectedResults(
    const std::vector<tl::expected<T, mooncake::ErrorCode>>& results) {
    uint64_t success = 0;
    uint64_t failed = 0;
    for (const auto& result : results) {
        if (result.has_value()) {
            ++success;
        } else {
            ++failed;
        }
    }
    return {success, failed};
}

struct SyntheticStats {
    std::atomic<uint64_t> scheduled_tasks{0};
    std::atomic<uint64_t> completed_tasks{0};
    std::atomic<uint64_t> rpc_failure_events{0};
    std::atomic<uint64_t> put_transactions{0};
    std::atomic<uint64_t> committed_keys{0};
    std::atomic<uint64_t> exist_true_items{0};
    std::atomic<uint64_t> exist_false_items{0};
    std::atomic<uint64_t> max_queue_depth{0};
    std::atomic<uint64_t> max_scheduler_lateness_ns{0};
    std::atomic<uint64_t> overload_events{0};
};

void AtomicMax(std::atomic<uint64_t>& target, uint64_t value) {
    uint64_t current = target.load(std::memory_order_relaxed);
    while (current < value && !target.compare_exchange_weak(
                                  current, value, std::memory_order_relaxed)) {
    }
}

class KeySpace {
   public:
    KeySpace(uint64_t segment_index, std::string tag, uint64_t key_size,
             uint64_t pool_capacity)
        : segment_index_(segment_index),
          tag_(std::move(tag)),
          key_size_(key_size),
          pool_capacity_(pool_capacity) {
        const std::string natural = BuildNaturalKey(false, 0);
        if (key_size_ != 0 && key_size_ < natural.size()) {
            throw std::invalid_argument(
                "key_size " + std::to_string(key_size_) +
                " is smaller than the generated key length " +
                std::to_string(natural.size()));
        }
        if (pool_capacity_ == 0) {
            throw std::invalid_argument("key_pool_size must be at least 1");
        }
        committed_ids_.reserve(
            static_cast<size_t>(std::min<uint64_t>(pool_capacity_, 1000000)));
    }

    std::vector<uint64_t> AllocatePutIds(uint64_t count) {
        const uint64_t first =
            next_key_id_.fetch_add(count, std::memory_order_relaxed);
        if (count != 0 &&
            first > std::numeric_limits<uint64_t>::max() - (count - 1)) {
            throw std::overflow_error("synthetic key sequence exhausted");
        }
        std::vector<uint64_t> ids;
        ids.reserve(static_cast<size_t>(count));
        for (uint64_t offset = 0; offset < count; ++offset) {
            ids.push_back(first + offset);
        }
        return ids;
    }

    std::vector<std::string> Materialize(
        const std::vector<uint64_t>& ids) const {
        std::vector<std::string> keys;
        keys.reserve(ids.size());
        for (const uint64_t id : ids) {
            keys.push_back(BuildKey(false, id));
        }
        return keys;
    }

    template <typename Generator>
    std::vector<std::string> MakeLookupKeys(uint64_t count, double hit_ratio,
                                            Generator& generator) {
        std::bernoulli_distribution choose_hit(hit_ratio);
        std::vector<std::string> keys;
        keys.reserve(static_cast<size_t>(count));
        for (uint64_t index = 0; index < count; ++index) {
            std::optional<uint64_t> existing;
            if (choose_hit(generator)) {
                existing = SampleCommitted(generator);
            }
            if (existing.has_value()) {
                keys.push_back(BuildKey(false, *existing));
            } else {
                const uint64_t miss_id =
                    next_miss_id_.fetch_add(1, std::memory_order_relaxed);
                keys.push_back(BuildKey(true, miss_id));
            }
        }
        return keys;
    }

    void Commit(const std::vector<uint64_t>& ids) {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        for (const uint64_t id : ids) {
            if (committed_ids_.size() < pool_capacity_) {
                committed_ids_.push_back(id);
            } else {
                committed_ids_[replace_cursor_] = id;
                replace_cursor_ = (replace_cursor_ + 1) % pool_capacity_;
            }
        }
    }

    uint64_t retained_keys() const {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        return committed_ids_.size();
    }

   private:
    std::string BuildNaturalKey(bool miss, uint64_t id) const {
        std::ostringstream output;
        output << tag_ << ':' << (miss ? 'm' : 'k') << ":s" << std::setw(8)
               << std::setfill('0') << segment_index_ << ':' << Hex64(id);
        return output.str();
    }

    std::string BuildKey(bool miss, uint64_t id) const {
        std::string key = BuildNaturalKey(miss, id);
        if (key_size_ != 0) {
            key.resize(static_cast<size_t>(key_size_), 'x');
        }
        return key;
    }

    template <typename Generator>
    std::optional<uint64_t> SampleCommitted(Generator& generator) const {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        if (committed_ids_.empty()) {
            return std::nullopt;
        }
        std::uniform_int_distribution<size_t> distribution(
            0, committed_ids_.size() - 1);
        return committed_ids_[distribution(generator)];
    }

    uint64_t segment_index_;
    std::string tag_;
    uint64_t key_size_;
    size_t pool_capacity_;
    std::atomic<uint64_t> next_key_id_{0};
    std::atomic<uint64_t> next_miss_id_{0};

    mutable std::mutex pool_mutex_;
    std::vector<uint64_t> committed_ids_;
    size_t replace_cursor_{0};
};

struct ScheduledTask {
    SyntheticOperation operation{SyntheticOperation::kExist};
    Clock::time_point due;
};

class TaskQueue {
   public:
    TaskQueue(uint64_t max_depth, SyntheticStats& stats)
        : max_depth_(max_depth), stats_(stats) {}

    bool Push(ScheduledTask task) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= max_depth_) {
            stats_.overload_events.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        queue_.push_back(std::move(task));
        AtomicMax(stats_.max_queue_depth, queue_.size());
        condition_.notify_one();
        return true;
    }

    bool Pop(ScheduledTask& task) {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [&] { return finished_ || !queue_.empty(); });
        if (queue_.empty()) {
            return false;
        }
        task = queue_.front();
        queue_.pop_front();
        return true;
    }

    void Finish() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finished_ = true;
        }
        condition_.notify_all();
    }

   private:
    size_t max_depth_;
    SyntheticStats& stats_;
    std::mutex mutex_;
    std::condition_variable condition_;
    std::deque<ScheduledTask> queue_;
    bool finished_{false};
};

class WorkloadWorker {
   public:
    WorkloadWorker(uint64_t segment_index, uint64_t worker_index,
                   std::string segment_name, KeySpace& key_space,
                   TaskQueue& queue, SyntheticStats& stats)
        : segment_index_(segment_index),
          worker_index_(worker_index),
          segment_name_(std::move(segment_name)),
          key_space_(key_space),
          queue_(queue),
          stats_(stats),
          generator_(Mix64(FLAGS_seed ^ (segment_index << 32) ^ worker_index)),
          master_client_(mooncake::generate_uuid()) {
        const auto error = master_client_.Connect(FLAGS_master_server);
        if (error != mooncake::ErrorCode::OK) {
            throw std::runtime_error(
                "cannot connect synthetic worker for segment " +
                std::to_string(segment_index_) + " to " + FLAGS_master_server +
                ", ec=" + toString(error));
        }
    }

    void Start() { thread_ = std::thread(&WorkloadWorker::Run, this); }

    void Join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

   private:
    void ExecuteExist() {
        const auto keys = key_space_.MakeLookupKeys(
            FLAGS_exist_batch_size, FLAGS_exist_hit_ratio, generator_);
        try {
            const auto results = master_client_.BatchExistKey(keys);
            uint64_t failed = 0;
            uint64_t found = 0;
            uint64_t missing = 0;
            for (const auto& result : results) {
                if (!result.has_value()) {
                    ++failed;
                } else {
                    if (*result) {
                        ++found;
                    } else {
                        ++missing;
                    }
                }
            }
            stats_.exist_true_items.fetch_add(found, std::memory_order_relaxed);
            stats_.exist_false_items.fetch_add(missing,
                                               std::memory_order_relaxed);
            if (failed != 0) {
                stats_.rpc_failure_events.fetch_add(1,
                                                    std::memory_order_relaxed);
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "Synthetic BatchExistKey threw on segment "
                       << segment_index_ << ", worker " << worker_index_ << ": "
                       << error.what();
            stats_.rpc_failure_events.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void ExecuteGet() {
        const auto keys = key_space_.MakeLookupKeys(
            FLAGS_get_batch_size, FLAGS_get_hit_ratio, generator_);
        try {
            const auto counts =
                CountExpectedResults(master_client_.BatchGetReplicaList(keys));
            if (counts.second != 0) {
                stats_.rpc_failure_events.fetch_add(1,
                                                    std::memory_order_relaxed);
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "Synthetic BatchGetReplicaList threw on segment "
                       << segment_index_ << ", worker " << worker_index_ << ": "
                       << error.what();
            stats_.rpc_failure_events.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void ExecutePut() {
        const auto ids = key_space_.AllocatePutIds(FLAGS_put_batch_size);
        const auto keys = key_space_.Materialize(ids);
        const std::vector<std::vector<uint64_t>> slice_lengths(
            keys.size(), {FLAGS_value_size});

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;
        if (FLAGS_placement_mode == "preferred") {
            config.preferred_segments = {segment_name_};
        }

        std::vector<uint64_t> started_ids;
        std::vector<std::string> started_keys;
        try {
            const auto results =
                master_client_.BatchPutStart(keys, slice_lengths, config);
            uint64_t failed = 0;
            const size_t result_count = std::min(results.size(), keys.size());
            started_ids.reserve(result_count);
            started_keys.reserve(result_count);
            for (size_t index = 0; index < result_count; ++index) {
                if (results[index].has_value()) {
                    started_ids.push_back(ids[index]);
                    started_keys.push_back(keys[index]);
                } else {
                    ++failed;
                }
            }
            failed += keys.size() - result_count;
            if (failed != 0) {
                stats_.rpc_failure_events.fetch_add(1,
                                                    std::memory_order_relaxed);
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "Synthetic BatchPutStart threw on segment "
                       << segment_index_ << ", worker " << worker_index_ << ": "
                       << error.what();
            stats_.rpc_failure_events.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        if (started_keys.empty()) {
            return;
        }

        const auto end_due =
            Clock::now() + std::chrono::microseconds(FLAGS_put_commit_delay_us);
        std::this_thread::sleep_until(end_due);
        try {
            const auto results = master_client_.BatchPutEnd(started_keys);
            std::vector<uint64_t> committed;
            committed.reserve(started_ids.size());
            uint64_t failed = 0;
            const size_t result_count =
                std::min(results.size(), started_ids.size());
            for (size_t index = 0; index < result_count; ++index) {
                if (results[index].has_value()) {
                    committed.push_back(started_ids[index]);
                } else {
                    ++failed;
                }
            }
            failed += started_ids.size() - result_count;
            key_space_.Commit(committed);
            stats_.committed_keys.fetch_add(committed.size(),
                                            std::memory_order_relaxed);
            if (failed != 0) {
                stats_.rpc_failure_events.fetch_add(1,
                                                    std::memory_order_relaxed);
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "Synthetic BatchPutEnd threw on segment "
                       << segment_index_ << ", worker " << worker_index_ << ": "
                       << error.what();
            stats_.rpc_failure_events.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void Run() {
        ScheduledTask task;
        while (queue_.Pop(task)) {
            switch (task.operation) {
                case SyntheticOperation::kExist:
                    ExecuteExist();
                    break;
                case SyntheticOperation::kPut:
                    ExecutePut();
                    stats_.put_transactions.fetch_add(
                        1, std::memory_order_relaxed);
                    break;
                case SyntheticOperation::kGet:
                    ExecuteGet();
                    break;
            }
            stats_.completed_tasks.fetch_add(1, std::memory_order_relaxed);
        }
    }

    uint64_t segment_index_;
    uint64_t worker_index_;
    std::string segment_name_;
    KeySpace& key_space_;
    TaskQueue& queue_;
    SyntheticStats& stats_;
    std::mt19937_64 generator_;
    mooncake::MasterClient master_client_;
    std::thread thread_;
};

class SegmentClient {
   public:
    SegmentClient(uint64_t segment_index, std::string segment_name,
                  uintptr_t base, uint64_t size)
        : segment_index_(segment_index),
          master_client_(mooncake::generate_uuid()) {
        const auto connect_error = master_client_.Connect(FLAGS_master_server);
        if (connect_error != mooncake::ErrorCode::OK) {
            throw std::runtime_error("cannot connect segment client " +
                                     std::to_string(segment_index_) + " to " +
                                     FLAGS_master_server +
                                     ", ec=" + toString(connect_error));
        }

        segment_.id = mooncake::generate_uuid();
        segment_.name = std::move(segment_name);
        segment_.base = base;
        segment_.size = size;
        segment_.te_endpoint = segment_.name;
        const auto mount_result = master_client_.MountSegment(segment_);
        if (!mount_result.has_value()) {
            throw std::runtime_error("failed to mount segment " +
                                     segment_.name +
                                     ", ec=" + toString(mount_result.error()));
        }
        mounted_ = true;
    }

    ~SegmentClient() {
        if (mounted_ && FLAGS_cleanup_segments) {
            const auto result = master_client_.UnmountSegment(segment_.id);
            if (!result.has_value()) {
                LOG(ERROR) << "Failed to unmount synthetic segment "
                           << segment_.name
                           << ", ec=" << toString(result.error());
            }
        }
    }

    const std::string& name() const { return segment_.name; }

    void Ping() {
        const auto result = master_client_.Ping();
        if (!result.has_value()) {
            LOG(WARNING) << "Ping failed for synthetic segment "
                         << segment_.name
                         << ", ec=" << toString(result.error());
            return;
        }
        if (result->client_status != mooncake::ClientStatus::NEED_REMOUNT) {
            return;
        }
        const auto remount_result = master_client_.ReMountSegment({segment_});
        if (!remount_result.has_value()) {
            LOG(WARNING) << "ReMountSegment failed for synthetic segment "
                         << segment_.name
                         << ", ec=" << toString(remount_result.error());
        }
    }

   private:
    uint64_t segment_index_;
    mooncake::MasterClient master_client_;
    mooncake::Segment segment_;
    bool mounted_{false};
};

struct ArrivalStream {
    SyntheticOperation operation;
    double qps;
    Clock::time_point next;
};

class SegmentRuntime {
   public:
    SegmentRuntime(uint64_t segment_index, std::string segment_name,
                   SyntheticStats& stats)
        : segment_index_(segment_index),
          segment_name_(std::move(segment_name)),
          stats_(stats),
          key_space_(segment_index_, FLAGS_key_tag, FLAGS_key_size,
                     FLAGS_key_pool_size),
          queue_(FLAGS_max_pending_events_per_segment, stats_),
          scheduler_generator_(
              Mix64(FLAGS_seed ^ segment_index_ ^ 0x5345474d454e5455ULL)) {
        workers_.reserve(static_cast<size_t>(FLAGS_workers_per_segment));
        for (uint64_t worker_index = 0;
             worker_index < FLAGS_workers_per_segment; ++worker_index) {
            workers_.push_back(std::make_unique<WorkloadWorker>(
                segment_index_, worker_index, segment_name_, key_space_, queue_,
                stats_));
        }
    }

    void StartWorkers() {
        for (auto& worker : workers_) {
            worker->Start();
        }
    }

    void StartScheduler(Clock::time_point start, Clock::time_point end) {
        scheduler_thread_ =
            std::thread(&SegmentRuntime::Schedule, this, start, end);
    }

    void JoinScheduler() {
        if (scheduler_thread_.joinable()) {
            scheduler_thread_.join();
        }
    }

    void FinishWorkers() { queue_.Finish(); }

    void JoinWorkers() {
        for (auto& worker : workers_) {
            worker->Join();
        }
    }

    uint64_t retained_keys() const { return key_space_.retained_keys(); }

   private:
    std::chrono::nanoseconds NextInterval(double qps) {
        if (qps <= 0.0) {
            return std::chrono::nanoseconds::max();
        }
        long double seconds = 0.0;
        if (FLAGS_arrival_model == "fixed") {
            seconds = 1.0L / qps;
        } else {
            std::exponential_distribution<long double> distribution(qps);
            seconds = distribution(scheduler_generator_);
        }
        const long double nanoseconds = seconds * 1'000'000'000.0L;
        return std::chrono::nanoseconds(
            std::max<int64_t>(1, static_cast<int64_t>(nanoseconds)));
    }

    void Schedule(Clock::time_point start, Clock::time_point end) {
        std::vector<ArrivalStream> streams{
            {SyntheticOperation::kExist, FLAGS_exist_qps_per_segment, start},
            {SyntheticOperation::kPut, FLAGS_put_qps_per_segment, start},
            {SyntheticOperation::kGet, FLAGS_get_qps_per_segment, start},
        };
        for (auto& stream : streams) {
            if (stream.qps > 0.0) {
                stream.next += NextInterval(stream.qps);
            } else {
                stream.next = Clock::time_point::max();
            }
        }

        while (true) {
            auto next = std::min_element(
                streams.begin(), streams.end(),
                [](const ArrivalStream& left, const ArrivalStream& right) {
                    return left.next < right.next;
                });
            if (next == streams.end() || next->next >= end) {
                // Preserve the configured generation window even when the
                // final Poisson sample lands well after it. Returning early
                // would shorten low-QPS runs and inflate reported averages.
                std::this_thread::sleep_until(end);
                return;
            }
            std::this_thread::sleep_until(next->next);
            AtomicMax(stats_.max_scheduler_lateness_ns, LatenessNs(next->next));

            if (!queue_.Push({next->operation, next->next})) {
                LOG(ERROR) << "Synthetic segment " << segment_index_
                           << " reached max_pending_events_per_segment="
                           << FLAGS_max_pending_events_per_segment
                           << "; stopping its open-loop scheduler";
                return;
            }
            stats_.scheduled_tasks.fetch_add(1, std::memory_order_relaxed);
            next->next += NextInterval(next->qps);
        }
    }

    uint64_t segment_index_;
    std::string segment_name_;
    SyntheticStats& stats_;
    KeySpace key_space_;
    TaskQueue queue_;
    std::mt19937_64 scheduler_generator_;
    std::vector<std::unique_ptr<WorkloadWorker>> workers_;
    std::thread scheduler_thread_;
};

void ValidateFlags() {
    if (FLAGS_num_segments == 0) {
        throw std::invalid_argument("num_segments must be at least 1");
    }
    if (FLAGS_segment_size == 0) {
        throw std::invalid_argument("segment_size must be at least 1");
    }
    if (FLAGS_workers_per_segment == 0) {
        throw std::invalid_argument("workers_per_segment must be at least 1");
    }
    if (FLAGS_duration == 0) {
        throw std::invalid_argument("duration must be at least 1 second");
    }
    if (FLAGS_exist_batch_size == 0 || FLAGS_put_batch_size == 0 ||
        FLAGS_get_batch_size == 0) {
        throw std::invalid_argument("all batch sizes must be at least 1");
    }
    if (FLAGS_value_size == 0) {
        throw std::invalid_argument("value_size must be at least 1");
    }
    if (!std::isfinite(FLAGS_exist_qps_per_segment) ||
        !std::isfinite(FLAGS_put_qps_per_segment) ||
        !std::isfinite(FLAGS_get_qps_per_segment) ||
        FLAGS_exist_qps_per_segment < 0.0 || FLAGS_put_qps_per_segment < 0.0 ||
        FLAGS_get_qps_per_segment < 0.0) {
        throw std::invalid_argument(
            "per-segment QPS must be finite and non-negative");
    }
    if (FLAGS_exist_qps_per_segment == 0.0 &&
        FLAGS_put_qps_per_segment == 0.0 && FLAGS_get_qps_per_segment == 0.0) {
        throw std::invalid_argument(
            "at least one per-segment QPS must be positive");
    }
    if (FLAGS_arrival_model != "poisson" && FLAGS_arrival_model != "fixed") {
        throw std::invalid_argument("arrival_model must be poisson or fixed");
    }
    if (!std::isfinite(FLAGS_exist_hit_ratio) ||
        !std::isfinite(FLAGS_get_hit_ratio) || FLAGS_exist_hit_ratio < 0.0 ||
        FLAGS_exist_hit_ratio > 1.0 || FLAGS_get_hit_ratio < 0.0 ||
        FLAGS_get_hit_ratio > 1.0) {
        throw std::invalid_argument("hit ratios must be finite and in [0, 1]");
    }
    if (FLAGS_placement_mode != "preferred" &&
        FLAGS_placement_mode != "global") {
        throw std::invalid_argument(
            "placement_mode must be preferred or global");
    }
    if (FLAGS_replica_num == 0) {
        throw std::invalid_argument("replica_num must be at least 1");
    }
    if (FLAGS_ping_interval_ms == 0) {
        throw std::invalid_argument("ping_interval_ms must be at least 1");
    }
    if (FLAGS_max_pending_events_per_segment == 0) {
        throw std::invalid_argument(
            "max_pending_events_per_segment must be at least 1");
    }
    if (FLAGS_num_segments > 1) {
        const uint64_t max_offset =
            std::numeric_limits<uintptr_t>::max() - kSegmentBase;
        if (FLAGS_segment_size > max_offset / (FLAGS_num_segments - 1)) {
            throw std::invalid_argument(
                "segment base addresses overflow uintptr_t");
        }
    }
}

int RunSyntheticBench() {
    ValidateFlags();

    LOG(INFO) << "Mounting " << FLAGS_num_segments << " synthetic segments of "
              << FLAGS_segment_size << " bytes";
    std::vector<std::unique_ptr<SegmentClient>> segment_clients;
    segment_clients.reserve(static_cast<size_t>(FLAGS_num_segments));
    std::vector<std::string> segment_names;
    segment_names.reserve(static_cast<size_t>(FLAGS_num_segments));
    for (uint64_t index = 0; index < FLAGS_num_segments; ++index) {
        const std::string name =
            FLAGS_key_tag + "_segment_" + std::to_string(index);
        segment_names.push_back(name);
        const uintptr_t base =
            kSegmentBase + static_cast<uintptr_t>(index * FLAGS_segment_size);
        segment_clients.push_back(std::make_unique<SegmentClient>(
            index, name, base, FLAGS_segment_size));
    }

    SyntheticStats stats;
    std::vector<std::unique_ptr<SegmentRuntime>> runtimes;
    runtimes.reserve(static_cast<size_t>(FLAGS_num_segments));
    for (uint64_t index = 0; index < FLAGS_num_segments; ++index) {
        runtimes.push_back(std::make_unique<SegmentRuntime>(
            index, segment_names[index], stats));
    }
    for (auto& runtime : runtimes) {
        runtime->StartWorkers();
    }

    const double business_rpc_qps =
        FLAGS_num_segments *
        (FLAGS_exist_qps_per_segment + 2.0 * FLAGS_put_qps_per_segment +
         FLAGS_get_qps_per_segment);
    const double item_qps =
        FLAGS_num_segments *
        (FLAGS_exist_qps_per_segment * FLAGS_exist_batch_size +
         2.0 * FLAGS_put_qps_per_segment * FLAGS_put_batch_size +
         FLAGS_get_qps_per_segment * FLAGS_get_batch_size);
    const long double reservation_bytes_per_second =
        static_cast<long double>(FLAGS_num_segments) *
        FLAGS_put_qps_per_segment * FLAGS_put_batch_size * FLAGS_value_size *
        FLAGS_replica_num;
    LOG(INFO) << "Starting segment synthetic benchmark: duration="
              << FLAGS_duration << "s, arrival_model=" << FLAGS_arrival_model
              << ", segments=" << FLAGS_num_segments
              << ", workers_per_segment=" << FLAGS_workers_per_segment
              << ", target_business_rpc_qps=" << business_rpc_qps
              << ", target_item_qps=" << item_qps
              << ", target_reservation_bytes_per_second="
              << static_cast<double>(reservation_bytes_per_second)
              << ", placement_mode=" << FLAGS_placement_mode;

    const auto run_start = Clock::now();
    const auto run_end = run_start + std::chrono::seconds(FLAGS_duration);
    std::atomic<bool> stop_ping{false};
    std::thread ping_thread([&] {
        auto next = run_start;
        while (!stop_ping.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_until(next);
            if (stop_ping.load(std::memory_order_relaxed)) {
                return;
            }
            for (auto& segment_client : segment_clients) {
                segment_client->Ping();
            }
            next += std::chrono::milliseconds(FLAGS_ping_interval_ms);
            if (next >= run_end) {
                return;
            }
        }
    });

    for (auto& runtime : runtimes) {
        runtime->StartScheduler(run_start, run_end);
    }
    for (auto& runtime : runtimes) {
        runtime->JoinScheduler();
    }
    stop_ping.store(true, std::memory_order_relaxed);
    if (ping_thread.joinable()) {
        ping_thread.join();
    }

    for (auto& runtime : runtimes) {
        runtime->FinishWorkers();
    }
    for (auto& runtime : runtimes) {
        runtime->JoinWorkers();
    }

    const auto completion_time = Clock::now();
    const double elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(
            completion_time - run_start)
            .count();
    const double drain_seconds =
        completion_time <= run_end
            ? 0.0
            : std::chrono::duration_cast<std::chrono::duration<double>>(
                  completion_time - run_end)
                  .count();
    uint64_t retained_keys = 0;
    for (const auto& runtime : runtimes) {
        retained_keys += runtime->retained_keys();
    }

    LOG(INFO) << "Synthetic benchmark complete: scheduled_tasks="
              << stats.scheduled_tasks.load()
              << ", completed_tasks=" << stats.completed_tasks.load()
              << ", rpc_failure_events=" << stats.rpc_failure_events.load()
              << ", put_transactions=" << stats.put_transactions.load()
              << ", committed_keys=" << stats.committed_keys.load()
              << ", retained_keys=" << retained_keys
              << ", exist_true_items=" << stats.exist_true_items.load()
              << ", exist_false_items=" << stats.exist_false_items.load()
              << ", max_queue_depth=" << stats.max_queue_depth.load()
              << ", max_scheduler_lateness_ns="
              << stats.max_scheduler_lateness_ns.load()
              << ", overload_events=" << stats.overload_events.load()
              << ", generation_seconds=" << FLAGS_duration
              << ", drain_seconds=" << drain_seconds
              << ", elapsed_seconds=" << elapsed_seconds;

    std::cout << "Synthetic benchmark: " << stats.completed_tasks.load()
              << " logical tasks, " << std::fixed << std::setprecision(2)
              << elapsed_seconds << " seconds, "
              << (elapsed_seconds == 0.0
                      ? 0.0
                      : stats.completed_tasks.load() / elapsed_seconds)
              << " logical tasks/s\n";

    // Destroy workload clients before segment clients. SegmentClient
    // destructors then perform optional benchmark-only cleanup.
    runtimes.clear();
    segment_clients.clear();
    return stats.overload_events.load() == 0 ? 0 : 2;
}

}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging("MasterSyntheticBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    int result = 1;
    try {
        result = RunSyntheticBench();
    } catch (const std::exception& error) {
        LOG(ERROR) << "Synthetic benchmark failed: " << error.what();
    }

    google::ShutdownGoogleLogging();
    return result;
}
