#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "spdk/spdk_wrapper.h"
#include "transfer_task.h"
#include "utils.h"

namespace {

constexpr uint64_t KiB = 1024;
constexpr uint64_t MiB = 1024 * KiB;
constexpr uint64_t GiB = 1024 * MiB;
constexpr uint64_t kDefaultIoSize = 128 * KiB;
constexpr uint64_t kDefaultIodepth = 64;
constexpr uint64_t kDefaultSubmitThreads = 1;
constexpr uint64_t kDefaultDurationSec = 30;
constexpr uint64_t kDefaultReportIntervalMs = 1000;
constexpr uint64_t kDefaultRangeBytes = 1 * GiB;
constexpr uint64_t kDefaultWarmupSec = 3;
constexpr char kDefaultOp[] = "read";

DEFINE_string(
    endpoints, "",
    "Comma-separated NoF transport endpoints. To use multiple "
    "SpdkNofWorkerPool workers with the current implementation, provide "
    "multiple distinct endpoints/namespaces/targets.");
DEFINE_string(op, "read", "Benchmark operation: read, write, or mixed.");
DEFINE_string(
    rw, "",
    "fio-style alias for --op/--random_lba. Supported values: read, write, "
    "randread, randwrite, mixed, randrw.");
DEFINE_uint64(io_size, 128 * KiB,
              "I/O size in bytes. Must align to device block size.");
DEFINE_uint64(bs, 0, "fio-style alias for --io_size.");
DEFINE_uint64(
    iodepth, 64,
    "Per-endpoint in-flight I/Os maintained by the benchmark. Total in-flight "
    "depth is endpoints * iodepth.");
DEFINE_uint64(
    submit_threads, 1,
    "Number of benchmark submit/completion threads. Endpoints are evenly "
    "partitioned across submit threads.");
DEFINE_uint64(numjobs, 0, "fio-style alias for --submit_threads.");
DEFINE_uint64(duration_sec, 30, "Measurement duration in seconds.");
DEFINE_uint64(runtime, 0, "fio-style alias for --duration_sec.");
DEFINE_uint64(report_interval_ms, 1000,
              "Periodic throughput report interval in milliseconds.");
DEFINE_uint64(status_interval, 0,
              "fio-style alias for --report_interval_ms, in seconds.");
DEFINE_uint64(start_lba, 0, "Starting LBA offset within each endpoint.");
DEFINE_uint64(range_bytes, 1 * GiB,
              "Per-endpoint logical range in bytes used by the benchmark. "
              "Must align to block size.");
DEFINE_uint64(size, 0, "fio-style alias for --range_bytes.");
DEFINE_bool(random_lba, false,
            "Whether to choose LBA randomly inside the per-endpoint range.");
DEFINE_uint64(seed, 1, "Random seed used when --random_lba or --op=mixed.");
DEFINE_uint64(warmup_sec, 3,
              "Warmup duration in seconds before measurement starts.");
DEFINE_uint64(ramp_time, 0, "fio-style alias for --warmup_sec.");
DEFINE_uint64(
    nof_workers, 0,
    "Optional override for MC_NOF_WORKERS. Equivalent to exporting "
    "MC_NOF_WORKERS before launch. 0 means keep environment/default.");
DEFINE_uint64(nof_submit_chunk_bytes, 0,
              "Optional override for MC_NOF_SUBMIT_CHUNK_BYTES. Equivalent to "
              "exporting MC_NOF_SUBMIT_CHUNK_BYTES before launch. 0 means keep "
              "environment/default.");
DEFINE_uint64(
    nof_inflight_bytes_limit, 0,
    "Optional override for MC_NOF_INFLIGHT_BYTES_LIMIT. Equivalent to "
    "exporting MC_NOF_INFLIGHT_BYTES_LIMIT before launch. 0 means keep "
    "environment/default.");
DEFINE_int32(socket_id, -1,
             "NUMA socket for benchmark buffers. -1 lets SPDK choose.");
DEFINE_bool(fill_on_write, true,
            "Fill buffers with a deterministic pattern for write/mixed ops.");

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

enum class BenchOp {
    READ = 0,
    WRITE = 1,
    MIXED = 2,
};

BenchOp ParseBenchOp(const std::string &value) {
    std::string normalized(value);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (normalized == "read") {
        return BenchOp::READ;
    }
    if (normalized == "write") {
        return BenchOp::WRITE;
    }
    if (normalized == "mixed") {
        return BenchOp::MIXED;
    }
    throw std::invalid_argument(
        "Invalid --op. Supported values: read, write, mixed");
}

struct ParsedRw {
    std::string op;
    bool random_lba{false};
};

ParsedRw ParseRwMode(const std::string &value) {
    std::string normalized(value);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (normalized == "read") {
        return {"read", false};
    }
    if (normalized == "write") {
        return {"write", false};
    }
    if (normalized == "mixed" || normalized == "rw") {
        return {"mixed", false};
    }
    if (normalized == "randread") {
        return {"read", true};
    }
    if (normalized == "randwrite") {
        return {"write", true};
    }
    if (normalized == "randrw") {
        return {"mixed", true};
    }
    throw std::invalid_argument(
        "Invalid --rw. Supported values: read, write, randread, randwrite, "
        "mixed, rw, randrw");
}

void ResolveUint64Alias(const char *legacy_name, uint64_t *legacy_value,
                        uint64_t legacy_default, const char *alias_name,
                        uint64_t alias_value) {
    if (alias_value == 0) {
        return;
    }
    if (*legacy_value != legacy_default && *legacy_value != alias_value) {
        throw std::invalid_argument(std::string("Conflicting --") +
                                    legacy_name + " and --" + alias_name +
                                    " values");
    }
    *legacy_value = alias_value;
}

void SetEnvU64IfRequested(const char *name, uint64_t value) {
    if (value == 0) {
        return;
    }
#ifdef _WIN32
    _putenv_s(name, std::to_string(value).c_str());
#else
    setenv(name, std::to_string(value).c_str(), 1);
#endif
}

std::string FormatBytesPerSecond(double bytes_per_sec) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (bytes_per_sec >= static_cast<double>(GiB)) {
        oss << (bytes_per_sec / GiB) << " GiB/s";
    } else if (bytes_per_sec >= static_cast<double>(MiB)) {
        oss << (bytes_per_sec / MiB) << " MiB/s";
    } else if (bytes_per_sec >= static_cast<double>(KiB)) {
        oss << (bytes_per_sec / KiB) << " KiB/s";
    } else {
        oss.unsetf(std::ios::fixed);
        oss << bytes_per_sec << " B/s";
    }
    return oss.str();
}

std::string FormatLatencyNs(uint64_t latency_ns) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (latency_ns >= 1000ULL * 1000ULL * 1000ULL) {
        oss << (static_cast<double>(latency_ns) / 1000.0 / 1000.0 / 1000.0)
            << " s";
    } else if (latency_ns >= 1000ULL * 1000ULL) {
        oss << (static_cast<double>(latency_ns) / 1000.0 / 1000.0) << " ms";
    } else if (latency_ns >= 1000ULL) {
        oss << (static_cast<double>(latency_ns) / 1000.0) << " us";
    } else {
        oss.unsetf(std::ios::fixed);
        oss << latency_ns << " ns";
    }
    return oss.str();
}

struct CounterSnapshot {
    uint64_t submitted_ops{0};
    uint64_t completed_ops{0};
    uint64_t failed_ops{0};
    uint64_t bytes{0};
};

struct LiveCounters {
    std::atomic<uint64_t> submitted_ops{0};
    std::atomic<uint64_t> completed_ops{0};
    std::atomic<uint64_t> failed_ops{0};
    std::atomic<uint64_t> bytes{0};

    CounterSnapshot Snapshot() const {
        return {
            submitted_ops.load(std::memory_order_relaxed),
            completed_ops.load(std::memory_order_relaxed),
            failed_ops.load(std::memory_order_relaxed),
            bytes.load(std::memory_order_relaxed),
        };
    }
};

class LatencyHistogram {
   public:
    static constexpr size_t kBuckets0 = 200;
    static constexpr size_t kBuckets1 = 180;
    static constexpr size_t kBuckets2 = 180;
    static constexpr size_t kBuckets3 = 180;
    static constexpr size_t kBucketCount =
        kBuckets0 + kBuckets1 + kBuckets2 + kBuckets3 + 1;

    void Record(uint64_t latency_ns);
    void MergeFrom(const LatencyHistogram &other);
    uint64_t Count() const { return count_; }
    uint64_t SumNs() const { return sum_ns_; }
    uint64_t PercentileNs(double percentile) const;

   private:
    static size_t BucketIndex(uint64_t latency_ns);
    static uint64_t BucketUpperBoundNs(size_t index);

    uint64_t count_{0};
    uint64_t sum_ns_{0};
    std::array<uint64_t, kBucketCount> buckets_{};
};

struct BenchStats {
    uint64_t submitted_ops{0};
    uint64_t completed_ops{0};
    uint64_t failed_ops{0};
    uint64_t bytes{0};
    LatencyHistogram latency_histogram;

    void RecordSubmit();
    void RecordSuccess(uint64_t io_bytes, uint64_t latency_ns);
    void RecordFailure();
    void MergeFrom(const BenchStats &other);
};

struct EndpointContext {
    std::string endpoint;
    mooncake::nof_seg_handle *seg_handle{nullptr};
    uint32_t block_size{0};
    uint64_t range_blocks{0};
    uint64_t io_blocks{0};
    uint64_t next_block{0};
    std::mt19937_64 rng;

    EndpointContext(std::string ep, uint64_t seed_value)
        : endpoint(std::move(ep)), rng(seed_value) {}
};

struct EndpointStats {
    BenchStats measured;
    LiveCounters live;
};

struct Slot {
    void *buffer{nullptr};
    size_t buffer_size{0};
    std::shared_ptr<mooncake::SpdkNofOperationState> state;
    std::unique_ptr<mooncake::TransferFuture> future;
    uint64_t io_bytes{0};
    size_t endpoint_index{0};
    bool active{false};
    bool measure{false};
    TimePoint submit_time{};
};

struct ThreadEndpointState {
    size_t endpoint_index{0};
    std::vector<Slot> slots;
};

struct BenchThreadContext {
    size_t thread_index{0};
    std::vector<ThreadEndpointState> endpoints;
};

struct ThroughputView {
    double bandwidth_bytes_per_sec{0.0};
    double iops{0.0};
    double fail_iops{0.0};
};

struct LatencyView {
    uint64_t avg_ns{0};
    uint64_t p50_ns{0};
    uint64_t p95_ns{0};
    uint64_t p99_ns{0};
};

ThroughputView ComputeThroughput(const CounterSnapshot &counters,
                                 double duration_sec);
LatencyView ComputeLatency(const BenchStats &stats);
CounterSnapshot DeltaCounters(const CounterSnapshot &current,
                              const CounterSnapshot &previous);
uint64_t NextLba(EndpointContext &endpoint, bool random_lba);
int PickTaskOp(BenchOp op_mode, std::mt19937_64 &rng);
void FillPattern(void *buffer, size_t size, uint64_t seq);
void PublishSubmit(LiveCounters &live);
void PublishSuccess(LiveCounters &live, uint64_t io_bytes);
void PublishFailure(LiveCounters &live);
void SubmitSlot(Slot &slot, EndpointContext &endpoint, size_t endpoint_index,
                mooncake::SpdkNofWorkerPool &pool, BenchOp op_mode,
                uint64_t submit_seq, bool measure_now,
                EndpointStats &endpoint_stats);
void WorkerLoop(BenchThreadContext *thread_context,
                std::vector<EndpointContext> *endpoints,
                std::vector<EndpointStats> *endpoint_stats,
                mooncake::SpdkNofWorkerPool *pool, BenchOp op_mode,
                TimePoint warmup_end, TimePoint end,
                std::atomic<size_t> *active_threads);
void FreeThreadSlots(mooncake::SpdkWrapper &wrapper,
                     std::vector<BenchThreadContext> &thread_contexts);

void LatencyHistogram::Record(uint64_t latency_ns) {
    ++count_;
    sum_ns_ += latency_ns;
    ++buckets_[BucketIndex(latency_ns)];
}

void LatencyHistogram::MergeFrom(const LatencyHistogram &other) {
    count_ += other.count_;
    sum_ns_ += other.sum_ns_;
    for (size_t i = 0; i < kBucketCount; ++i) {
        buckets_[i] += other.buckets_[i];
    }
}

uint64_t LatencyHistogram::PercentileNs(double percentile) const {
    if (count_ == 0) {
        return 0;
    }
    if (percentile <= 0.0) {
        return BucketUpperBoundNs(0);
    }
    if (percentile >= 1.0) {
        return BucketUpperBoundNs(kBucketCount - 1);
    }

    const uint64_t target =
        static_cast<uint64_t>(std::ceil(percentile * count_));
    uint64_t seen = 0;
    for (size_t i = 0; i < kBucketCount; ++i) {
        seen += buckets_[i];
        if (seen >= target) {
            return BucketUpperBoundNs(i);
        }
    }
    return BucketUpperBoundNs(kBucketCount - 1);
}

size_t LatencyHistogram::BucketIndex(uint64_t latency_ns) {
    if (latency_ns < 1'000'000ULL) {
        return static_cast<size_t>(latency_ns / 5'000ULL);
    }
    if (latency_ns < 10'000'000ULL) {
        return kBuckets0 +
               static_cast<size_t>((latency_ns - 1'000'000ULL) / 50'000ULL);
    }
    if (latency_ns < 100'000'000ULL) {
        return kBuckets0 + kBuckets1 +
               static_cast<size_t>((latency_ns - 10'000'000ULL) / 500'000ULL);
    }
    if (latency_ns < 1'000'000'000ULL) {
        return kBuckets0 + kBuckets1 + kBuckets2 +
               static_cast<size_t>((latency_ns - 100'000'000ULL) /
                                   5'000'000ULL);
    }
    return kBucketCount - 1;
}

uint64_t LatencyHistogram::BucketUpperBoundNs(size_t index) {
    if (index < kBuckets0) {
        return (index + 1) * 5'000ULL;
    }
    index -= kBuckets0;
    if (index < kBuckets1) {
        return 1'000'000ULL + (index + 1) * 50'000ULL;
    }
    index -= kBuckets1;
    if (index < kBuckets2) {
        return 10'000'000ULL + (index + 1) * 500'000ULL;
    }
    index -= kBuckets2;
    if (index < kBuckets3) {
        return 100'000'000ULL + (index + 1) * 5'000'000ULL;
    }
    return std::numeric_limits<uint64_t>::max();
}

void BenchStats::RecordSubmit() { ++submitted_ops; }

void BenchStats::RecordSuccess(uint64_t io_bytes, uint64_t latency_ns) {
    ++completed_ops;
    bytes += io_bytes;
    latency_histogram.Record(latency_ns);
}

void BenchStats::RecordFailure() { ++failed_ops; }

void BenchStats::MergeFrom(const BenchStats &other) {
    submitted_ops += other.submitted_ops;
    completed_ops += other.completed_ops;
    failed_ops += other.failed_ops;
    bytes += other.bytes;
    latency_histogram.MergeFrom(other.latency_histogram);
}

ThroughputView ComputeThroughput(const CounterSnapshot &counters,
                                 double duration_sec) {
    ThroughputView view;
    if (duration_sec <= 0.0) {
        return view;
    }
    view.bandwidth_bytes_per_sec = counters.bytes / duration_sec;
    view.iops = counters.completed_ops / duration_sec;
    view.fail_iops = counters.failed_ops / duration_sec;
    return view;
}

LatencyView ComputeLatency(const BenchStats &stats) {
    LatencyView view;
    const uint64_t count = stats.latency_histogram.Count();
    if (count == 0) {
        return view;
    }
    view.avg_ns = stats.latency_histogram.SumNs() / count;
    view.p50_ns = stats.latency_histogram.PercentileNs(0.50);
    view.p95_ns = stats.latency_histogram.PercentileNs(0.95);
    view.p99_ns = stats.latency_histogram.PercentileNs(0.99);
    return view;
}

CounterSnapshot DeltaCounters(const CounterSnapshot &current,
                              const CounterSnapshot &previous) {
    return {
        current.submitted_ops - previous.submitted_ops,
        current.completed_ops - previous.completed_ops,
        current.failed_ops - previous.failed_ops,
        current.bytes - previous.bytes,
    };
}

uint64_t NextLba(EndpointContext &endpoint, bool random_lba) {
    if (endpoint.range_blocks <= endpoint.io_blocks) {
        return FLAGS_start_lba;
    }

    if (random_lba) {
        std::uniform_int_distribution<uint64_t> dist(
            0, endpoint.range_blocks - endpoint.io_blocks);
        return FLAGS_start_lba + dist(endpoint.rng);
    }

    uint64_t lba = FLAGS_start_lba + endpoint.next_block;
    endpoint.next_block += endpoint.io_blocks;
    if (endpoint.next_block + endpoint.io_blocks > endpoint.range_blocks) {
        endpoint.next_block = 0;
    }
    return lba;
}

int PickTaskOp(BenchOp op_mode, std::mt19937_64 &rng) {
    switch (op_mode) {
        case BenchOp::READ:
            return 0;
        case BenchOp::WRITE:
            return 1;
        case BenchOp::MIXED: {
            std::uniform_int_distribution<int> dist(0, 1);
            return dist(rng);
        }
    }
    return 0;
}

void FillPattern(void *buffer, size_t size, uint64_t seq) {
    auto *ptr = reinterpret_cast<uint8_t *>(buffer);
    for (size_t i = 0; i < size; ++i) {
        ptr[i] = static_cast<uint8_t>((seq + i) & 0xff);
    }
}

void PublishSubmit(LiveCounters &live) {
    live.submitted_ops.fetch_add(1, std::memory_order_relaxed);
}

void PublishSuccess(LiveCounters &live, uint64_t io_bytes) {
    live.completed_ops.fetch_add(1, std::memory_order_relaxed);
    live.bytes.fetch_add(io_bytes, std::memory_order_relaxed);
}

void PublishFailure(LiveCounters &live) {
    live.failed_ops.fetch_add(1, std::memory_order_relaxed);
}

void SubmitSlot(Slot &slot, EndpointContext &endpoint, size_t endpoint_index,
                mooncake::SpdkNofWorkerPool &pool, BenchOp op_mode,
                uint64_t submit_seq, bool measure_now,
                EndpointStats &endpoint_stats) {
    int op = PickTaskOp(op_mode, endpoint.rng);
    if (op == 1 && FLAGS_fill_on_write) {
        FillPattern(slot.buffer, slot.buffer_size, submit_seq);
    }

    uint64_t lba = NextLba(endpoint, FLAGS_random_lba);
    slot.io_bytes = slot.buffer_size;
    slot.endpoint_index = endpoint_index;
    slot.state = std::make_shared<mooncake::SpdkNofOperationState>();
    slot.future = std::make_unique<mooncake::TransferFuture>(slot.state);
    slot.measure = measure_now;
    slot.submit_time = Clock::now();

    mooncake::SpdkNofTask task(endpoint.seg_handle, slot.buffer, lba,
                               static_cast<uint32_t>(endpoint.io_blocks), op,
                               slot.state);
    pool.submitTask(std::move(task));
    slot.active = true;

    if (measure_now) {
        endpoint_stats.measured.RecordSubmit();
        PublishSubmit(endpoint_stats.live);
    }
}

void WorkerLoop(BenchThreadContext *thread_context,
                std::vector<EndpointContext> *endpoints,
                std::vector<EndpointStats> *endpoint_stats,
                mooncake::SpdkNofWorkerPool *pool, BenchOp op_mode,
                TimePoint warmup_end, TimePoint end,
                std::atomic<size_t> *active_threads) {
    uint64_t submit_seq = static_cast<uint64_t>(thread_context->thread_index)
                          << 48;

    for (auto &thread_endpoint : thread_context->endpoints) {
        auto &endpoint = (*endpoints)[thread_endpoint.endpoint_index];
        auto &stats = (*endpoint_stats)[thread_endpoint.endpoint_index];
        for (auto &slot : thread_endpoint.slots) {
            SubmitSlot(slot, endpoint, thread_endpoint.endpoint_index, *pool,
                       op_mode, submit_seq++, false, stats);
        }
    }

    while (true) {
        const TimePoint now_before_scan = Clock::now();
        const bool before_end = now_before_scan < end;
        bool any_active = false;

        for (auto &thread_endpoint : thread_context->endpoints) {
            auto &endpoint = (*endpoints)[thread_endpoint.endpoint_index];
            auto &stats = (*endpoint_stats)[thread_endpoint.endpoint_index];
            for (auto &slot : thread_endpoint.slots) {
                if (!slot.active) {
                    continue;
                }
                any_active = true;
                if (!slot.future || !slot.future->isReady()) {
                    continue;
                }

                const TimePoint completion_time = Clock::now();
                mooncake::ErrorCode result = slot.future->get();
                slot.active = false;
                slot.future.reset();

                if (slot.measure) {
                    if (result == mooncake::ErrorCode::OK) {
                        const uint64_t latency_ns = static_cast<uint64_t>(
                            std::chrono::duration_cast<
                                std::chrono::nanoseconds>(completion_time -
                                                          slot.submit_time)
                                .count());
                        stats.measured.RecordSuccess(slot.io_bytes, latency_ns);
                        PublishSuccess(stats.live, slot.io_bytes);
                    } else {
                        stats.measured.RecordFailure();
                        PublishFailure(stats.live);
                    }
                }

                if (before_end) {
                    const bool measure_now = completion_time >= warmup_end;
                    SubmitSlot(slot, endpoint, thread_endpoint.endpoint_index,
                               *pool, op_mode, submit_seq++, measure_now,
                               stats);
                    any_active = true;
                }
            }
        }

        if (!before_end && !any_active) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }

    active_threads->fetch_sub(1, std::memory_order_relaxed);
}

void FreeThreadSlots(mooncake::SpdkWrapper &wrapper,
                     std::vector<BenchThreadContext> &thread_contexts) {
    for (auto &thread_context : thread_contexts) {
        for (auto &thread_endpoint : thread_context.endpoints) {
            for (auto &slot : thread_endpoint.slots) {
                if (slot.buffer) {
                    wrapper.Free(slot.buffer);
                    slot.buffer = nullptr;
                }
            }
        }
    }
}

}  // namespace

int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    gflags::SetUsageMessage(
        "NoF worker pool benchmark. Use --helpshort to list benchmark "
        "parameters.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    try {
        ResolveUint64Alias("io_size", &FLAGS_io_size, kDefaultIoSize, "bs",
                           FLAGS_bs);
        ResolveUint64Alias("submit_threads", &FLAGS_submit_threads,
                           kDefaultSubmitThreads, "numjobs", FLAGS_numjobs);
        ResolveUint64Alias("duration_sec", &FLAGS_duration_sec,
                           kDefaultDurationSec, "runtime", FLAGS_runtime);
        ResolveUint64Alias("range_bytes", &FLAGS_range_bytes,
                           kDefaultRangeBytes, "size", FLAGS_size);
        ResolveUint64Alias("warmup_sec", &FLAGS_warmup_sec, kDefaultWarmupSec,
                           "ramp_time", FLAGS_ramp_time);
        if (FLAGS_status_interval != 0) {
            ResolveUint64Alias("report_interval_ms", &FLAGS_report_interval_ms,
                               kDefaultReportIntervalMs, "status_interval",
                               FLAGS_status_interval * 1000);
        }
        if (!FLAGS_rw.empty()) {
            ParsedRw parsed_rw = ParseRwMode(FLAGS_rw);
            if (FLAGS_op != kDefaultOp && FLAGS_op != parsed_rw.op) {
                throw std::invalid_argument("Conflicting --op and --rw values");
            }
            if (parsed_rw.random_lba == false && FLAGS_random_lba) {
                throw std::invalid_argument(
                    "Conflicting --random_lba and --rw values");
            }
            FLAGS_op = parsed_rw.op;
            FLAGS_random_lba = parsed_rw.random_lba;
        }

        BenchOp op_mode = ParseBenchOp(FLAGS_op);
        if (FLAGS_endpoints.empty()) {
            LOG(ERROR) << "--endpoints is required";
            return 1;
        }
        if (FLAGS_iodepth == 0) {
            LOG(ERROR) << "--iodepth must be greater than 0";
            return 1;
        }
        if (FLAGS_submit_threads == 0) {
            LOG(ERROR) << "--submit_threads must be greater than 0";
            return 1;
        }
        if (FLAGS_duration_sec == 0) {
            LOG(ERROR) << "--duration_sec must be greater than 0";
            return 1;
        }

        SetEnvU64IfRequested("MC_NOF_WORKERS", FLAGS_nof_workers);
        SetEnvU64IfRequested("MC_NOF_SUBMIT_CHUNK_BYTES",
                             FLAGS_nof_submit_chunk_bytes);
        SetEnvU64IfRequested("MC_NOF_INFLIGHT_BYTES_LIMIT",
                             FLAGS_nof_inflight_bytes_limit);

        auto endpoint_strings = mooncake::splitString(FLAGS_endpoints);
        if (endpoint_strings.empty()) {
            LOG(ERROR) << "No valid endpoints parsed from --endpoints";
            return 1;
        }

        auto &wrapper = mooncake::SpdkWrapper::GetInstance();
        if (!wrapper.InitializeEnv()) {
            LOG(ERROR) << "Failed to initialize SPDK environment";
            return 1;
        }

        std::vector<EndpointContext> endpoints;
        endpoints.reserve(endpoint_strings.size());
        std::unordered_set<mooncake::nof_seg_handle *> unique_handles;
        for (size_t i = 0; i < endpoint_strings.size(); ++i) {
            EndpointContext endpoint(endpoint_strings[i], FLAGS_seed + i);
            endpoint.seg_handle = wrapper.OpenNofSegment(endpoint.endpoint);
            if (!endpoint.seg_handle) {
                LOG(ERROR) << "Failed to open NoF endpoint: "
                           << endpoint.endpoint;
                return 1;
            }
            endpoint.block_size = wrapper.GetBlockSize(endpoint.seg_handle);
            if (endpoint.block_size == INVALID_BLOCK_SIZE ||
                endpoint.block_size == 0) {
                LOG(ERROR) << "Invalid block size for endpoint: "
                           << endpoint.endpoint;
                return 1;
            }
            if (FLAGS_io_size % endpoint.block_size != 0) {
                LOG(ERROR) << "--io_size=" << FLAGS_io_size
                           << " is not aligned to block size "
                           << endpoint.block_size << " for endpoint "
                           << endpoint.endpoint;
                return 1;
            }
            if (FLAGS_range_bytes % endpoint.block_size != 0) {
                LOG(ERROR) << "--range_bytes=" << FLAGS_range_bytes
                           << " is not aligned to block size "
                           << endpoint.block_size;
                return 1;
            }
            endpoint.io_blocks = FLAGS_io_size / endpoint.block_size;
            endpoint.range_blocks = FLAGS_range_bytes / endpoint.block_size;
            if (endpoint.range_blocks < endpoint.io_blocks) {
                LOG(ERROR)
                    << "--range_bytes is smaller than one I/O for endpoint "
                    << endpoint.endpoint;
                return 1;
            }
            unique_handles.insert(endpoint.seg_handle);
            endpoints.push_back(std::move(endpoint));
        }

        const size_t effective_submit_threads =
            std::min<size_t>(FLAGS_submit_threads, endpoints.size());
        const uint64_t total_iodepth =
            static_cast<uint64_t>(endpoints.size()) * FLAGS_iodepth;

        LOG(INFO) << "Bench config: endpoints=" << endpoints.size()
                  << ", unique_handles=" << unique_handles.size()
                  << ", configured_nof_workers="
                  << (FLAGS_nof_workers == 0 ? mooncake::kDefaultSpdkNofWorkers
                                             : FLAGS_nof_workers)
                  << ", effective_worker_bindings<="
                  << std::min<uint64_t>(unique_handles.size(),
                                        FLAGS_nof_workers == 0
                                            ? mooncake::kDefaultSpdkNofWorkers
                                            : FLAGS_nof_workers)
                  << ", io_size=" << FLAGS_io_size
                  << ", iodepth_per_endpoint=" << FLAGS_iodepth
                  << ", total_iodepth=" << total_iodepth
                  << ", submit_threads=" << effective_submit_threads
                  << ", warmup_sec=" << FLAGS_warmup_sec
                  << ", duration_sec=" << FLAGS_duration_sec;
        if (unique_handles.size() < endpoints.size()) {
            LOG(WARNING)
                << "Some endpoints resolved to the same nof_seg_handle. In the "
                   "current SpdkNofWorkerPool implementation, the same handle "
                   "binds to a single worker thread, so duplicate endpoints "
                   "will "
                   "not increase worker parallelism.";
        }

        std::vector<BenchThreadContext> thread_contexts(
            effective_submit_threads);
        for (size_t thread_index = 0; thread_index < effective_submit_threads;
             ++thread_index) {
            thread_contexts[thread_index].thread_index = thread_index;
        }
        for (size_t endpoint_index = 0; endpoint_index < endpoints.size();
             ++endpoint_index) {
            size_t thread_index = endpoint_index % effective_submit_threads;
            ThreadEndpointState thread_endpoint;
            thread_endpoint.endpoint_index = endpoint_index;
            thread_endpoint.slots.resize(FLAGS_iodepth);
            thread_contexts[thread_index].endpoints.push_back(
                std::move(thread_endpoint));
        }

        for (auto &thread_context : thread_contexts) {
            for (auto &thread_endpoint : thread_context.endpoints) {
                for (auto &slot : thread_endpoint.slots) {
                    slot.buffer_size = FLAGS_io_size;
                    slot.buffer = wrapper.Alloc(slot.buffer_size, 0x1000,
                                                FLAGS_socket_id);
                    if (!slot.buffer) {
                        LOG(ERROR) << "Failed to allocate DMA buffer of size "
                                   << slot.buffer_size;
                        FreeThreadSlots(wrapper, thread_contexts);
                        return 1;
                    }
                }
            }
        }

        std::vector<EndpointStats> endpoint_stats(endpoints.size());
        mooncake::SpdkNofWorkerPool pool;

        TimePoint start = Clock::now();
        TimePoint warmup_end = start + std::chrono::seconds(FLAGS_warmup_sec);
        TimePoint end = warmup_end + std::chrono::seconds(FLAGS_duration_sec);

        std::vector<std::thread> workers;
        workers.reserve(thread_contexts.size());
        std::atomic<size_t> active_threads(thread_contexts.size());
        for (auto &thread_context : thread_contexts) {
            workers.emplace_back(WorkerLoop, &thread_context, &endpoints,
                                 &endpoint_stats, &pool, op_mode, warmup_end,
                                 end, &active_threads);
        }

        TimePoint next_report =
            Clock::now() + std::chrono::milliseconds(FLAGS_report_interval_ms);
        std::vector<CounterSnapshot> last_endpoint_snapshots(endpoints.size());

        while (active_threads.load(std::memory_order_relaxed) > 0) {
            TimePoint now = Clock::now();
            if (now < next_report) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            if (now >= warmup_end) {
                CounterSnapshot total_current;
                CounterSnapshot total_last;
                for (size_t endpoint_index = 0;
                     endpoint_index < endpoints.size(); ++endpoint_index) {
                    CounterSnapshot current =
                        endpoint_stats[endpoint_index].live.Snapshot();
                    total_current.submitted_ops += current.submitted_ops;
                    total_current.completed_ops += current.completed_ops;
                    total_current.failed_ops += current.failed_ops;
                    total_current.bytes += current.bytes;
                    total_last.submitted_ops +=
                        last_endpoint_snapshots[endpoint_index].submitted_ops;
                    total_last.completed_ops +=
                        last_endpoint_snapshots[endpoint_index].completed_ops;
                    total_last.failed_ops +=
                        last_endpoint_snapshots[endpoint_index].failed_ops;
                    total_last.bytes +=
                        last_endpoint_snapshots[endpoint_index].bytes;
                }

                double report_window_sec =
                    static_cast<double>(FLAGS_report_interval_ms) / 1000.0;
                CounterSnapshot delta_total =
                    DeltaCounters(total_current, total_last);
                ThroughputView total_view =
                    ComputeThroughput(delta_total, report_window_sec);
                LOG(INFO) << "interval bw="
                          << FormatBytesPerSecond(
                                 total_view.bandwidth_bytes_per_sec)
                          << ", iops=" << std::fixed << std::setprecision(2)
                          << total_view.iops
                          << ", fail_iops=" << total_view.fail_iops
                          << ", submitted=" << total_current.submitted_ops
                          << ", completed=" << total_current.completed_ops
                          << ", failed=" << total_current.failed_ops;

                for (size_t endpoint_index = 0;
                     endpoint_index < endpoints.size(); ++endpoint_index) {
                    CounterSnapshot current =
                        endpoint_stats[endpoint_index].live.Snapshot();
                    CounterSnapshot delta = DeltaCounters(
                        current, last_endpoint_snapshots[endpoint_index]);
                    ThroughputView endpoint_view =
                        ComputeThroughput(delta, report_window_sec);
                    LOG(INFO)
                        << "interval endpoint[" << endpoint_index << "] bw="
                        << FormatBytesPerSecond(
                               endpoint_view.bandwidth_bytes_per_sec)
                        << ", iops=" << std::fixed << std::setprecision(2)
                        << endpoint_view.iops
                        << ", fail_iops=" << endpoint_view.fail_iops
                        << ", endpoint=" << endpoints[endpoint_index].endpoint;
                    last_endpoint_snapshots[endpoint_index] = current;
                }
            }

            next_report =
                now + std::chrono::milliseconds(FLAGS_report_interval_ms);
        }

        for (auto &worker : workers) {
            worker.join();
        }

        FreeThreadSlots(wrapper, thread_contexts);

        BenchStats total_measured;
        for (const auto &stats : endpoint_stats) {
            total_measured.MergeFrom(stats.measured);
        }

        double duration = static_cast<double>(FLAGS_duration_sec);
        ThroughputView total_view = ComputeThroughput(
            {total_measured.submitted_ops, total_measured.completed_ops,
             total_measured.failed_ops, total_measured.bytes},
            duration);
        LatencyView total_latency = ComputeLatency(total_measured);

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "========== NoF Worker Pool Bench ==========\n";
        std::cout << "endpoints=" << endpoints.size() << "\n";
        std::cout << "unique_handles=" << unique_handles.size() << "\n";
        std::cout << "configured_nof_workers="
                  << (FLAGS_nof_workers == 0 ? mooncake::kDefaultSpdkNofWorkers
                                             : FLAGS_nof_workers)
                  << "\n";
        std::cout << "effective_worker_bindings<="
                  << std::min<uint64_t>(unique_handles.size(),
                                        FLAGS_nof_workers == 0
                                            ? mooncake::kDefaultSpdkNofWorkers
                                            : FLAGS_nof_workers)
                  << "\n";
        std::cout << "numjobs=" << effective_submit_threads << "\n";
        std::cout << "bs=" << FLAGS_io_size << "\n";
        std::cout << "iodepth=" << FLAGS_iodepth << "\n";
        std::cout << "total_iodepth=" << total_iodepth << "\n";
        std::cout << "runtime=" << FLAGS_duration_sec << "\n";
        std::cout << "completed_ops=" << total_measured.completed_ops << "\n";
        std::cout << "failed_ops=" << total_measured.failed_ops << "\n";
        std::cout << "bw="
                  << FormatBytesPerSecond(total_view.bandwidth_bytes_per_sec)
                  << "\n";
        std::cout << "iops=" << total_view.iops << "\n";
        std::cout << "fail_iops=" << total_view.fail_iops << "\n";
        std::cout << "clat_mean=" << FormatLatencyNs(total_latency.avg_ns)
                  << "\n";
        std::cout << "clat_p50=" << FormatLatencyNs(total_latency.p50_ns)
                  << "\n";
        std::cout << "clat_p95=" << FormatLatencyNs(total_latency.p95_ns)
                  << "\n";
        std::cout << "clat_p99=" << FormatLatencyNs(total_latency.p99_ns)
                  << "\n";

        for (size_t endpoint_index = 0; endpoint_index < endpoints.size();
             ++endpoint_index) {
            ThroughputView endpoint_view = ComputeThroughput(
                {endpoint_stats[endpoint_index].measured.submitted_ops,
                 endpoint_stats[endpoint_index].measured.completed_ops,
                 endpoint_stats[endpoint_index].measured.failed_ops,
                 endpoint_stats[endpoint_index].measured.bytes},
                duration);
            LatencyView endpoint_latency =
                ComputeLatency(endpoint_stats[endpoint_index].measured);
            std::cout << "endpoint[" << endpoint_index
                      << "]=" << endpoints[endpoint_index].endpoint << "\n";
            std::cout << "endpoint[" << endpoint_index << "].completed_ops="
                      << endpoint_stats[endpoint_index].measured.completed_ops
                      << "\n";
            std::cout << "endpoint[" << endpoint_index << "].failed_ops="
                      << endpoint_stats[endpoint_index].measured.failed_ops
                      << "\n";
            std::cout << "endpoint[" << endpoint_index << "].bw="
                      << FormatBytesPerSecond(
                             endpoint_view.bandwidth_bytes_per_sec)
                      << "\n";
            std::cout << "endpoint[" << endpoint_index
                      << "].iops=" << endpoint_view.iops << "\n";
            std::cout << "endpoint[" << endpoint_index
                      << "].fail_iops=" << endpoint_view.fail_iops << "\n";
            std::cout << "endpoint[" << endpoint_index << "].clat_mean="
                      << FormatLatencyNs(endpoint_latency.avg_ns) << "\n";
            std::cout << "endpoint[" << endpoint_index << "].clat_p50="
                      << FormatLatencyNs(endpoint_latency.p50_ns) << "\n";
            std::cout << "endpoint[" << endpoint_index << "].clat_p95="
                      << FormatLatencyNs(endpoint_latency.p95_ns) << "\n";
            std::cout << "endpoint[" << endpoint_index << "].clat_p99="
                      << FormatLatencyNs(endpoint_latency.p99_ns) << "\n";
        }
        std::cout << "==========================================\n";

        google::ShutdownGoogleLogging();
        return 0;
    } catch (const std::exception &e) {
        LOG(ERROR) << "Benchmark failed: " << e.what();
        google::ShutdownGoogleLogging();
        return 1;
    }
}
