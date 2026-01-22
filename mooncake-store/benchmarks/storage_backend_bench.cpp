// Copyright 2025 Mooncake Authors
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

/**
 * @file storage_backend_bench.cpp
 * @brief Comprehensive benchmark for storage backends (OffsetAllocator, Bucket,
 * FilePerKey)
 *
 * TESTS AVAILABLE:
 *   - init: Backend initialization time
 *   - offload: BatchOffload (write) performance
 *   - load: BatchLoad (read) performance - CRITICAL for TTFT
 *   - concurrent_load: Multi-threaded read performance
 *   - exist: IsExist metadata lookup (hit/miss separated)
 *   - mixed_rw: Concurrent readers + writers (realistic contention)
 *   - churn: Near-capacity steady-state with overwrites
 *   - restart: Recovery time (Init + ScanMeta)
 *
 * MEASUREMENT METHODOLOGY:
 *   - Thread-local stats (no mutex in hot path)
 *   - 4KB-aligned buffer pools (for potential future O_DIRECT support)
 *   - Reusable containers (no allocation in timed region)
 *   - Data integrity verification via deterministic patterns
 *
 * CACHE MODE STATUS:
 *   - buffered: default, uses OS page cache (may measure RAM, not SSD)
 *   - direct: NOT IMPLEMENTED (requires backend API changes for O_DIRECT)
 *   - fadvise_dontneed: NOT IMPLEMENTED (requires fd access)
 *   - drop_cache_external: prints instructions, user must run sync+drop
 * manually
 *
 * USAGE EXAMPLES:
 *
 *   # Basic with verification
 *   ./storage_backend_bench --backend=offset_allocator --verify=true
 *
 *   # Cold cache test (bypass page cache) - requires manual cache drop
 *   sync && echo 3 > /proc/sys/vm/drop_caches  # as root
 *   ./storage_backend_bench --backend=offset_allocator
 * --cache_mode=drop_cache_external
 *
 *   # Mixed read/write contention (8 readers, 2 writers)
 *   ./storage_backend_bench --test=mixed_rw --read_threads=8 --write_threads=2
 *
 *   # Churn test at 80% capacity
 *   ./storage_backend_bench --test=churn --fill_ratio=0.8
 *
 *   # Restart/recovery benchmark
 *   ./storage_backend_bench --test=restart
 *
 *   # Zipf access pattern (hot keys)
 *   ./storage_backend_bench --pattern=zipf --zipf_skew=1.2
 */

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#ifdef __linux__
#include <sys/utsname.h>
#endif

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "storage_backend.h"

namespace fs = std::filesystem;

// ============================================================================
// Command-line Flags
// ============================================================================

// === Core Parameters ===
DEFINE_string(backend, "offset_allocator",
              "Backend type: offset_allocator, bucket, file_per_key, or all");
DEFINE_uint64(value_size, 128 * 1024, "Value size in bytes (default: 128KB)");
DEFINE_uint64(batch_size, 32, "Batch size for operations (default: 32)");
DEFINE_uint64(num_operations, 1000,
              "Number of operations per test (default: 1000)");
DEFINE_uint64(warmup_operations, 100,
              "Number of warmup operations (default: 100)");
DEFINE_uint64(num_threads, 1,
              "Number of threads for concurrent tests (default: 1)");
DEFINE_string(storage_path, "/tmp/mooncake_bench_data",
              "Storage directory path");
DEFINE_uint64(capacity_gb, 20, "Storage capacity in GB (default: 20GB)");
DEFINE_bool(run_all, false,
            "Run full benchmark suite with all parameter combinations");
DEFINE_bool(skip_cleanup, false,
            "Skip cleanup after benchmark (for debugging)");
DEFINE_string(test, "all",
              "Test to run: init, offload, load, concurrent_load, exist, "
              "mixed_rw, churn, restart, all");

// === PR1: Verification & Correctness ===
DEFINE_bool(verify, true, "Enable data integrity verification (default: true)");
DEFINE_uint64(verify_rate, 1,
              "Verify 1 in N operations (default: 1 = verify all)");
DEFINE_bool(verify_warmup_all, true, "Always verify all warmup operations");
DEFINE_bool(fail_fast, true, "Exit immediately on first corruption detected");
DEFINE_double(fill_ratio, 0.1,
              "Pre-populate to this fraction of capacity (0.0-1.0)");

// === PR2: Cache Modes ===
DEFINE_string(cache_mode, "buffered",
              "Cache mode: buffered (page cache), direct (O_DIRECT), "
              "fadvise_dontneed (evict after use), drop_cache_external (print "
              "instructions)");
DEFINE_bool(flush_between_ops, false,
            "Call fdatasync after each write (NOT IMPLEMENTED - requires "
            "backend Flush API)");

// === PR3: Realistic Workloads ===
DEFINE_uint64(read_threads, 8, "Number of reader threads for mixed_rw test");
DEFINE_uint64(write_threads, 2, "Number of writer threads for mixed_rw test");
DEFINE_double(read_ratio, 0.9,
              "Fraction of read ops in mixed workload (0.0-1.0)");
DEFINE_double(hotset_ratio, 0.2, "Fraction of keys that are 'hot' (0.0-1.0)");
DEFINE_string(pattern, "uniform",
              "Key access pattern: sequential, uniform, zipf");
DEFINE_double(zipf_skew, 1.0,
              "Zipf distribution skew parameter (higher = more skewed)");
DEFINE_uint64(report_interval, 100,
              "Report stats every N operations in time-series mode");
DEFINE_bool(time_series, false, "Enable periodic stats reporting during test");

// ============================================================================
// Constants
// ============================================================================

namespace {
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;

// Alignment for potential future O_DIRECT support (not currently used)
constexpr size_t kAlignment = 4096;  // 4KB page alignment

// Realistic KV cache block sizes for different scenarios
const std::vector<size_t> kValueSizes = {
    32 * KB,   // Small model, short context
    128 * KB,  // Typical KV cache block
    512 * KB,  // Large model
    2 * MB     // MoE model, long context
};

// Batch sizes (matching HiCache transfer patterns)
const std::vector<size_t> kBatchSizes = {1, 8, 32, 128};

// Thread counts for concurrency testing
const std::vector<size_t> kThreadCounts = {1, 4, 8, 16};

// Global corruption counter for fail-fast
std::atomic<size_t> g_total_checksum_failures{0};

// Round up to alignment boundary
inline size_t AlignUp(size_t size, size_t alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
}

}  // namespace

// ============================================================================
// Cache Mode (PR2)
// ============================================================================

enum class CacheMode {
    BUFFERED,            // Default Linux page cache
    DIRECT,              // O_DIRECT (bypass page cache)
    FADVISE_DONTNEED,    // Buffered + evict after use
    DROP_CACHE_EXTERNAL  // Print instructions for external cache drop
};

CacheMode StringToCacheMode(const std::string& str) {
    if (str == "buffered") return CacheMode::BUFFERED;
    if (str == "direct") return CacheMode::DIRECT;
    if (str == "fadvise_dontneed") return CacheMode::FADVISE_DONTNEED;
    if (str == "drop_cache_external") return CacheMode::DROP_CACHE_EXTERNAL;
    LOG(WARNING) << "Unknown cache mode: " << str << ", using buffered";
    return CacheMode::BUFFERED;
}

std::string CacheModeToString(CacheMode mode) {
    switch (mode) {
        case CacheMode::BUFFERED:
            return "buffered (page cache)";
        case CacheMode::DIRECT:
            return "direct (O_DIRECT)";
        case CacheMode::FADVISE_DONTNEED:
            return "fadvise_dontneed";
        case CacheMode::DROP_CACHE_EXTERNAL:
            return "drop_cache_external";
    }
    return "unknown";
}

// ============================================================================
// Access Pattern (PR3)
// ============================================================================

enum class AccessPattern { SEQUENTIAL, UNIFORM, ZIPF };

AccessPattern StringToAccessPattern(const std::string& str) {
    if (str == "sequential") return AccessPattern::SEQUENTIAL;
    if (str == "uniform") return AccessPattern::UNIFORM;
    if (str == "zipf") return AccessPattern::ZIPF;
    LOG(WARNING) << "Unknown pattern: " << str << ", using uniform";
    return AccessPattern::UNIFORM;
}

// ============================================================================
// Backend Types
// ============================================================================

enum class BackendType { OFFSET_ALLOCATOR, BUCKET, FILE_PER_KEY };

std::string BackendTypeToString(BackendType type) {
    switch (type) {
        case BackendType::OFFSET_ALLOCATOR:
            return "offset_allocator";
        case BackendType::BUCKET:
            return "bucket";
        case BackendType::FILE_PER_KEY:
            return "file_per_key";
    }
    return "unknown";
}

BackendType StringToBackendType(const std::string& str) {
    if (str == "offset_allocator") return BackendType::OFFSET_ALLOCATOR;
    if (str == "bucket") return BackendType::BUCKET;
    if (str == "file_per_key") return BackendType::FILE_PER_KEY;
    LOG(FATAL) << "Unknown backend type: " << str;
    return BackendType::OFFSET_ALLOCATOR;
}

// ============================================================================
// Cache Control Utilities
// ============================================================================

// Drop Linux page cache globally (requires root)
static bool DropLinuxPageCacheGlobal() {
#ifdef __linux__
    // Flush dirty pages
    ::sync();

    if (::geteuid() != 0) {
        std::cerr << "[CACHE] Not root (euid=" << ::geteuid()
                  << "). Cannot write /proc/sys/vm/drop_caches.\n"
                  << "        Run: sudo -E ./storage_backend_bench ... "
                     "--cache_mode=drop_cache_external\n";
        return false;
    }

    std::ofstream drop("/proc/sys/vm/drop_caches");
    if (!drop.is_open()) {
        std::cerr << "[CACHE] Failed to open /proc/sys/vm/drop_caches: "
                  << std::strerror(errno) << "\n";
        return false;
    }

    // 3 = drop pagecache + dentries + inodes
    drop << "3\n";
    drop.flush();
    if (!drop) {
        std::cerr << "[CACHE] Failed to write to drop_caches: "
                  << std::strerror(errno) << "\n";
        return false;
    }

    // Another sync to reduce timing weirdness
    ::sync();
    std::cerr
        << "[CACHE] Dropped Linux global caches via /proc/sys/vm/drop_caches\n";
    return true;
#else
    return false;
#endif
}

// ============================================================================
// Thread-Local Statistics (NO MUTEX in hot path)
// ============================================================================

// Per-thread stats - no synchronization needed during collection
struct ThreadStats {
    std::vector<double> latencies;
    size_t bytes = 0;
    size_t operations = 0;
    size_t errors = 0;
    size_t checksum_failures = 0;

    // Reserve space upfront to avoid reallocs in hot path
    void Reserve(size_t expected_ops) { latencies.reserve(expected_ops); }

    void RecordLatency(double latency_ms) { latencies.push_back(latency_ms); }

    void RecordBytes(size_t b) { bytes += b; }
    void RecordOperation() { operations++; }
    void RecordError() { errors++; }
    void RecordChecksumFailure() { checksum_failures++; }
};

// Aggregated stats - merge happens AFTER benchmark completes
class BenchmarkStats {
   public:
    // Create thread-local stats for N threads
    void InitThreads(size_t num_threads, size_t expected_ops_per_thread) {
        thread_stats_.resize(num_threads);
        for (auto& ts : thread_stats_) {
            ts.Reserve(expected_ops_per_thread);
        }
    }

    // Get stats for a specific thread (no locking)
    ThreadStats& GetThreadStats(size_t thread_id) {
        return thread_stats_[thread_id];
    }

    // For single-threaded tests
    ThreadStats& GetStats() {
        if (thread_stats_.empty()) {
            thread_stats_.resize(1);
        }
        return thread_stats_[0];
    }

    void StartTimer() { start_time_ = std::chrono::steady_clock::now(); }

    void StopTimer() { end_time_ = std::chrono::steady_clock::now(); }

    double GetElapsedSeconds() const {
        return std::chrono::duration<double>(end_time_ - start_time_).count();
    }

    void Clear() { thread_stats_.clear(); }

    // Merge all thread stats and compute final results (call AFTER benchmark)
    void Finalize() {
        merged_latencies_.clear();
        total_bytes_ = 0;
        total_operations_ = 0;
        total_errors_ = 0;
        total_checksum_failures_ = 0;
        total_backend_time_ms_ = 0;

        for (const auto& ts : thread_stats_) {
            merged_latencies_.insert(merged_latencies_.end(),
                                     ts.latencies.begin(), ts.latencies.end());
            total_bytes_ += ts.bytes;
            total_operations_ += ts.operations;
            total_errors_ += ts.errors;
            total_checksum_failures_ += ts.checksum_failures;
            // Sum of per-op latencies = actual backend time
            for (double lat : ts.latencies) {
                total_backend_time_ms_ += lat;
            }
        }

        std::sort(merged_latencies_.begin(), merged_latencies_.end());
    }

    void PrintStatistics(const std::string& test_name) const {
        if (merged_latencies_.empty()) {
            std::cout << "  No data recorded.\n";
            return;
        }

        double wall_clock_sec = GetElapsedSeconds();
        double backend_time_sec = total_backend_time_ms_ / 1000.0;
        size_t n = merged_latencies_.size();

        double min_lat = merged_latencies_.front();
        double max_lat = merged_latencies_.back();
        double mean_lat = std::accumulate(merged_latencies_.begin(),
                                          merged_latencies_.end(), 0.0) /
                          n;
        double p50 = GetPercentile(50);
        double p90 = GetPercentile(90);
        double p99 = GetPercentile(99);
        double p999 = GetPercentile(99.9);

        // THROUGHPUT: computed from backend time only (sum of latencies)
        // This ensures throughput and latency measure the same thing
        double throughput_mbps =
            (backend_time_sec > 0)
                ? (static_cast<double>(total_bytes_) / MB) / backend_time_sec
                : 0;
        double ops_per_sec =
            (backend_time_sec > 0) ? total_operations_ / backend_time_sec : 0;

        // Harness overhead = wall clock - backend time (for single-threaded
        // tests)
        double overhead_pct =
            (wall_clock_sec > 0 && backend_time_sec > 0)
                ? ((wall_clock_sec - backend_time_sec) / wall_clock_sec) * 100.0
                : 0;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "\n  Wall clock:     " << wall_clock_sec << " s\n";
        std::cout << "  Backend time:   " << backend_time_sec
                  << " s (sum of latencies)\n";

        // Interpretation notes:
        // - Single-threaded: backend_time should be close to wall_clock
        // - Multi-threaded: backend_time = sum of all thread latencies (can
        // exceed wall_clock)
        if (thread_stats_.size() == 1) {
            if (overhead_pct > 5.0) {
                std::cout << "  ⚠️  Harness overhead: " << overhead_pct
                          << "% of wall clock\n";
            }
        } else {
            // Multi-threaded: show aggregate backend CPU time vs wall time
            double parallelism =
                (wall_clock_sec > 0) ? backend_time_sec / wall_clock_sec : 0;
            std::cout << "  Parallelism:    " << parallelism << "x ("
                      << thread_stats_.size() << " threads)\n";
        }
        std::cout << "  Operations:     " << total_operations_
                  << " (errors: " << total_errors_ << ")\n";
        if (total_checksum_failures_ > 0) {
            std::cout << "  *** CHECKSUM FAILURES: " << total_checksum_failures_
                      << " ***\n";
        }
        std::cout << "  Samples:        " << n << " latency measurements\n";
        std::cout << "  Total data:     " << (total_bytes_ / GB) << "."
                  << std::setw(2) << std::setfill('0')
                  << ((total_bytes_ % GB) * 100 / GB) << " GB\n";
        std::cout << "  Throughput:     " << throughput_mbps
                  << " MB/s (backend time)";
        if (throughput_mbps > 1024) {
            std::cout << " (" << (throughput_mbps / 1024) << " GB/s)";
        }
        std::cout << "\n";
        std::cout << "  Ops/sec:        " << ops_per_sec << " (backend time)\n";
        std::cout << "\n  Latency (ms):   [n=" << n << "]\n";
        std::cout << "    Min:    " << std::setw(8) << min_lat << "\n";
        std::cout << "    Mean:   " << std::setw(8) << mean_lat << "\n";
        std::cout << "    P50:    " << std::setw(8) << p50 << "\n";
        std::cout << "    P90:    " << std::setw(8) << p90 << "\n";
        std::cout << "    P99:    " << std::setw(8) << p99;
        if (n < 100) std::cout << "  (caution: n<100)";
        std::cout << "\n";
        std::cout << "    P999:   " << std::setw(8) << p999;
        if (n < 1000) std::cout << "  (caution: n<1000)";
        std::cout << "\n";
        std::cout << "    Max:    " << std::setw(8) << max_lat << "\n";
    }

    // Getters for summary table (also use backend time)
    double GetThroughputMBps() const {
        double backend_time_sec = total_backend_time_ms_ / 1000.0;
        return (backend_time_sec > 0)
                   ? (static_cast<double>(total_bytes_) / MB) / backend_time_sec
                   : 0;
    }

    double GetP99Latency() const {
        if (merged_latencies_.empty()) return 0;
        return GetPercentile(99);
    }

   private:
    double GetPercentile(double percentile) const {
        if (merged_latencies_.empty()) return 0;
        // Linear interpolation for better accuracy
        double rank = (percentile / 100.0) * (merged_latencies_.size() - 1);
        size_t lower = static_cast<size_t>(rank);
        size_t upper = std::min(lower + 1, merged_latencies_.size() - 1);
        double frac = rank - lower;
        return merged_latencies_[lower] * (1.0 - frac) +
               merged_latencies_[upper] * frac;
    }

    std::vector<ThreadStats> thread_stats_;
    std::vector<double> merged_latencies_;
    size_t total_bytes_ = 0;
    size_t total_operations_ = 0;
    size_t total_errors_ = 0;
    size_t total_checksum_failures_ = 0;
    double total_backend_time_ms_ =
        0;  // Sum of all latencies (backend-only time)
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point end_time_;
};

// ============================================================================
// Checksum Utilities (fast, not cryptographic)
// ============================================================================

// Simple but fast checksum - XOR all 64-bit words + length
inline uint64_t ComputeChecksum(const char* data, size_t size) {
    uint64_t checksum = size;  // Include size to catch truncation
    const uint64_t* ptr = reinterpret_cast<const uint64_t*>(data);
    size_t num_words = size / sizeof(uint64_t);

    for (size_t i = 0; i < num_words; ++i) {
        checksum ^= ptr[i];
        checksum = (checksum << 7) | (checksum >> 57);  // Rotate to spread bits
    }

    // Handle remaining bytes
    size_t remaining = size % sizeof(uint64_t);
    if (remaining > 0) {
        uint64_t last_word = 0;
        std::memcpy(&last_word, data + num_words * sizeof(uint64_t), remaining);
        checksum ^= last_word;
    }

    return checksum;
}

// ============================================================================
// Pre-allocated ALIGNED Buffer Pool
// NOTE: Buffers are 4KB-aligned but O_DIRECT is NOT currently used.
// This alignment is for potential future use and to avoid false sharing.
// ============================================================================

// Custom deleter for aligned memory
struct AlignedDeleter {
    void operator()(void* ptr) const {
        if (ptr) std::free(ptr);
    }
};

using AlignedBuffer = std::unique_ptr<char, AlignedDeleter>;

class BufferPool {
   public:
    BufferPool() = default;
    ~BufferPool() = default;

    // Pre-allocate N buffers of given size (aligned to 4KB)
    void Init(size_t num_buffers, size_t buffer_size) {
        // Round up to alignment (potential future O_DIRECT / avoid false
        // sharing)
        buffer_size_ = AlignUp(buffer_size, kAlignment);
        buffers_.resize(num_buffers);
        for (auto& buf : buffers_) {
            void* ptr = nullptr;
            int ret = posix_memalign(&ptr, kAlignment, buffer_size_);
            if (ret != 0 || ptr == nullptr) {
                LOG(FATAL) << "Failed to allocate aligned buffer of size "
                           << buffer_size_;
            }
            buf = AlignedBuffer(static_cast<char*>(ptr));
        }
    }

    // Get buffer by index (no bounds check for speed)
    char* Get(size_t index) { return buffers_[index].get(); }

    size_t Size() const { return buffers_.size(); }
    size_t BufferSize() const { return buffer_size_; }
    size_t AlignedBufferSize() const { return buffer_size_; }

   private:
    std::vector<AlignedBuffer> buffers_;
    size_t buffer_size_ = 0;
};

// ============================================================================
// Pre-built Keys (avoids string allocation in hot path)
// ============================================================================

class KeySet {
   public:
    void Init(size_t num_keys, const std::string& prefix = "kvcache_block_") {
        keys_.reserve(num_keys);
        for (size_t i = 0; i < num_keys; ++i) {
            keys_.push_back(prefix + std::to_string(i));
        }
    }

    const std::string& Get(size_t index) const {
        return keys_[index % keys_.size()];
    }

    size_t Size() const { return keys_.size(); }

   private:
    std::vector<std::string> keys_;
};

// ============================================================================
// Key Index Generator (supports different access patterns)
// ============================================================================

class KeyIndexGenerator {
   public:
    KeyIndexGenerator(size_t num_keys, AccessPattern pattern,
                      double zipf_skew = 1.0, size_t seed = 42)
        : num_keys_(num_keys), pattern_(pattern), rng_(seed) {
        if (pattern == AccessPattern::ZIPF) {
            // Pre-compute Zipf probabilities
            InitZipf(zipf_skew);
        }
    }

    // Get next key index based on pattern
    size_t Next() {
        switch (pattern_) {
            case AccessPattern::SEQUENTIAL:
                return sequential_idx_++ % num_keys_;

            case AccessPattern::UNIFORM: {
                std::uniform_int_distribution<size_t> dist(0, num_keys_ - 1);
                return dist(rng_);
            }

            case AccessPattern::ZIPF:
                return SampleZipf();
        }
        return 0;
    }

    // Get index for specific operation (deterministic for verification)
    size_t GetForOp(size_t op_index) const {
        switch (pattern_) {
            case AccessPattern::SEQUENTIAL:
                return op_index % num_keys_;
            case AccessPattern::UNIFORM:
            case AccessPattern::ZIPF:
                // Use op_index as seed for deterministic but "random-looking"
                // access
                return (op_index * 2654435761ULL) %
                       num_keys_;  // Knuth's multiplicative hash
        }
        return op_index % num_keys_;
    }

   private:
    void InitZipf(double skew) {
        // Compute cumulative distribution for inverse transform sampling
        zipf_cdf_.resize(num_keys_);
        double sum = 0.0;
        for (size_t i = 1; i <= num_keys_; ++i) {
            sum += 1.0 / std::pow(static_cast<double>(i), skew);
        }
        double cumulative = 0.0;
        for (size_t i = 0; i < num_keys_; ++i) {
            cumulative +=
                (1.0 / std::pow(static_cast<double>(i + 1), skew)) / sum;
            zipf_cdf_[i] = cumulative;
        }
    }

    size_t SampleZipf() {
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        double u = dist(rng_);
        // Binary search for the appropriate bucket
        auto it = std::lower_bound(zipf_cdf_.begin(), zipf_cdf_.end(), u);
        return std::distance(zipf_cdf_.begin(), it);
    }

    size_t num_keys_;
    AccessPattern pattern_;
    std::mt19937_64 rng_;
    size_t sequential_idx_ = 0;
    std::vector<double> zipf_cdf_;
};

// ============================================================================
// Reusable Batch Containers (avoid allocation in hot path)
// ============================================================================

struct BatchContainers {
    std::unordered_map<std::string, std::vector<mooncake::Slice>> offload_batch;
    std::unordered_map<std::string, mooncake::Slice> load_batch;
    std::vector<size_t> key_indices;

    void Reserve(size_t batch_size) {
        offload_batch.reserve(batch_size);
        load_batch.reserve(batch_size);
        key_indices.resize(batch_size);
    }

    void ClearOffload() { offload_batch.clear(); }

    void ClearLoad() { load_batch.clear(); }
};

// ============================================================================
// Data Generator with Checksum Support
// ============================================================================

class DataGenerator {
   public:
    explicit DataGenerator(size_t seed = 42) : rng_(seed) {}

    // Generate a unique key for the given operation index
    std::string GenerateKey(size_t index) {
        return "kvcache_block_" + std::to_string(index);
    }

    // Fill buffer with deterministic data based on key index
    // Returns checksum of generated data
    uint64_t FillBuffer(char* buffer, size_t size, size_t key_index) {
        // Use key_index as seed for reproducibility
        uint64_t val =
            key_index * 6364136223846793005ULL + 1442695040888963407ULL;

        uint64_t* ptr = reinterpret_cast<uint64_t*>(buffer);
        size_t num_words = size / sizeof(uint64_t);

        for (size_t i = 0; i < num_words; ++i) {
            ptr[i] = val;
            val = val * 6364136223846793005ULL + 1442695040888963407ULL;
        }

        // Fill remaining bytes
        size_t remaining = size % sizeof(uint64_t);
        if (remaining > 0) {
            std::memcpy(buffer + num_words * sizeof(uint64_t), &val, remaining);
        }

        return ComputeChecksum(buffer, size);
    }

    // Verify buffer matches expected content for key_index
    bool VerifyBuffer(const char* buffer, size_t size, size_t key_index) {
        // Regenerate expected data and compare checksums
        uint64_t val =
            key_index * 6364136223846793005ULL + 1442695040888963407ULL;

        const uint64_t* ptr = reinterpret_cast<const uint64_t*>(buffer);
        size_t num_words = size / sizeof(uint64_t);

        for (size_t i = 0; i < num_words; ++i) {
            if (ptr[i] != val) {
                return false;  // Mismatch
            }
            val = val * 6364136223846793005ULL + 1442695040888963407ULL;
        }

        // Check remaining bytes
        size_t remaining = size % sizeof(uint64_t);
        if (remaining > 0) {
            uint64_t expected = 0;
            std::memcpy(&expected, &val, remaining);
            uint64_t actual = 0;
            std::memcpy(&actual, buffer + num_words * sizeof(uint64_t),
                        remaining);
            if (actual != expected) {
                return false;
            }
        }

        return true;
    }

    // Legacy: Generate random data (still used for some setup)
    std::unique_ptr<char[]> GenerateData(size_t size) {
        auto data = std::make_unique<char[]>(size);
        uint64_t val = rng_();
        for (size_t i = 0; i < size; i += sizeof(uint64_t)) {
            size_t remaining = std::min(sizeof(uint64_t), size - i);
            std::memcpy(data.get() + i, &val, remaining);
            val = val * 6364136223846793005ULL + 1442695040888963407ULL;
        }
        return data;
    }

   private:
    std::mt19937_64 rng_;
};

// ============================================================================
// Backend Factory
// ============================================================================

std::shared_ptr<mooncake::StorageBackendInterface> CreateBackend(
    BackendType type, const std::string& storage_path, size_t capacity_bytes) {
    mooncake::FileStorageConfig config;
    config.storage_filepath = storage_path;
    config.total_size_limit = capacity_bytes;
    config.total_keys_limit = 10'000'000;

    switch (type) {
        case BackendType::OFFSET_ALLOCATOR: {
            config.storage_backend_type =
                mooncake::StorageBackendType::kOffsetAllocator;
            return std::make_shared<mooncake::OffsetAllocatorStorageBackend>(
                config);
        }
        case BackendType::BUCKET: {
            config.storage_backend_type = mooncake::StorageBackendType::kBucket;
            mooncake::BucketBackendConfig bucket_config;
            bucket_config.bucket_size_limit = 256 * MB;
            bucket_config.bucket_keys_limit = 500;
            return std::make_shared<mooncake::BucketStorageBackend>(
                config, bucket_config);
        }
        case BackendType::FILE_PER_KEY: {
            config.storage_backend_type =
                mooncake::StorageBackendType::kFilePerKey;
            mooncake::FilePerKeyConfig fpk_config;
            fpk_config.fsdir = "file_per_key_bench";
            fpk_config.enable_eviction = false;
            return std::make_shared<mooncake::StorageBackendAdaptor>(
                config, fpk_config);
        }
    }
    return nullptr;
}

// ============================================================================
// Utility Functions
// ============================================================================

void CleanupStoragePath(const std::string& path) {
    if (fs::exists(path)) {
        std::error_code ec;
        fs::remove_all(path, ec);
        if (ec) {
            LOG(WARNING) << "Failed to cleanup " << path << ": "
                         << ec.message();
        }
    }
    fs::create_directories(path);
}

// Print system info (best effort)
void PrintSystemInfo() {
#ifdef __linux__
    struct utsname uts;
    if (uname(&uts) == 0) {
        std::cout << "  System:       " << uts.sysname << " " << uts.release
                  << "\n";
        std::cout << "  Machine:      " << uts.machine << "\n";
    }
#else
    std::cout << "  System:       " << "macOS/BSD" << "\n";
#endif
}

// Print cache mode warning/status
void PrintCacheModeWarning(CacheMode mode) {
    switch (mode) {
        case CacheMode::BUFFERED:
            std::cout << "\n  ⚠️  WARNING: Running in BUFFERED mode (page cache "
                         "enabled)\n";
            std::cout << "     Results may reflect RAM bandwidth, not SSD "
                         "performance.\n";
            std::cout << "     Use --cache_mode=drop_cache_external for "
                         "cold-cache testing.\n\n";
            break;

        case CacheMode::DIRECT:
            std::cout << "\n  ❌  DIRECT (O_DIRECT) mode: NOT IMPLEMENTED\n";
            std::cout << "     Behaving like BUFFERED mode. O_DIRECT requires "
                         "backend changes.\n";
            std::cout << "     To bypass page cache, use "
                         "--cache_mode=drop_cache_external\n\n";
            break;

        case CacheMode::FADVISE_DONTNEED:
            std::cout << "\n  ❌  FADVISE_DONTNEED mode: NOT IMPLEMENTED\n";
            std::cout << "     Behaving like BUFFERED mode. posix_fadvise "
                         "requires fd access.\n";
            std::cout << "     To bypass page cache, use "
                         "--cache_mode=drop_cache_external\n\n";
            break;

        case CacheMode::DROP_CACHE_EXTERNAL:
            std::cout << "\n  ℹ️  DROP_CACHE_EXTERNAL mode selected.\n";
            std::cout
                << "     To drop page cache before benchmark, run as root:\n";
            std::cout << "       sync && echo 3 > /proc/sys/vm/drop_caches\n";
            std::cout << "     Then re-run the benchmark.\n\n";
            break;
    }
}

// Print full benchmark configuration
void PrintBenchmarkConfig(BackendType backend, CacheMode cache_mode) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "           STORAGE BACKEND BENCHMARK\n";
    std::cout << std::string(70, '=') << "\n";
    PrintSystemInfo();
    std::cout << "\n  --- Configuration ---\n";
    std::cout << "  Backend:      " << BackendTypeToString(backend) << "\n";
    std::cout << "  Storage:      " << FLAGS_storage_path << "\n";
    std::cout << "  Capacity:     " << FLAGS_capacity_gb << " GB\n";
    std::cout << "  Value size:   " << (FLAGS_value_size / KB)
              << " KB (aligned: "
              << (AlignUp(FLAGS_value_size, kAlignment) / KB) << " KB)\n";
    std::cout << "  Batch size:   " << FLAGS_batch_size << "\n";
    std::cout << "  Operations:   " << FLAGS_num_operations
              << " (warmup: " << FLAGS_warmup_operations << ")\n";
    std::cout << "  Threads:      " << FLAGS_num_threads << "\n";
    std::cout << "  Fill ratio:   " << (FLAGS_fill_ratio * 100) << "%\n";
    std::cout << "\n  --- Cache & Verification ---\n";
    std::cout << "  Cache mode:   " << CacheModeToString(cache_mode) << "\n";
    std::cout << "  Verify:       " << (FLAGS_verify ? "yes" : "no");
    if (FLAGS_verify && FLAGS_verify_rate > 1) {
        std::cout << " (1 in " << FLAGS_verify_rate << " ops)";
    }
    std::cout << "\n";
    std::cout << "  Fail fast:    " << (FLAGS_fail_fast ? "yes" : "no") << "\n";
    std::cout << "  Flush writes: "
              << (FLAGS_flush_between_ops
                      ? "yes (NOT IMPLEMENTED - requires backend API)"
                      : "no")
              << "\n";
    std::cout << "\n  --- Access Pattern ---\n";
    std::cout << "  Pattern:      " << FLAGS_pattern << "\n";
    if (FLAGS_pattern == "zipf") {
        std::cout << "  Zipf skew:    " << FLAGS_zipf_skew << "\n";
    }
    std::cout << std::string(70, '=') << "\n";

    PrintCacheModeWarning(cache_mode);
}

void PrintHeader(const std::string& test_name, BackendType backend_type,
                 size_t value_size, size_t batch_size, size_t num_threads = 1) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "[" << test_name << "]\n";
    std::cout << "  Backend:      " << BackendTypeToString(backend_type)
              << "\n";
    if (value_size > 0) {
        std::cout << "  Value size:   " << (value_size / KB) << " KB\n";
    }
    if (batch_size > 0) {
        std::cout << "  Batch size:   " << batch_size << "\n";
    }
    if (num_threads > 1) {
        std::cout << "  Threads:      " << num_threads << "\n";
    }
    std::cout << std::string(70, '-') << "\n";
}

// Check for corruption and optionally fail fast
bool CheckVerification(size_t checksum_failures, const std::string& context) {
    if (checksum_failures > 0) {
        g_total_checksum_failures.fetch_add(checksum_failures);
        LOG(ERROR) << "*** DATA CORRUPTION DETECTED: " << checksum_failures
                   << " checksum failures in " << context << " ***";
        if (FLAGS_fail_fast) {
            LOG(FATAL) << "Exiting due to --fail_fast=true";
        }
        return false;
    }
    return true;
}

// Determine if this operation should be verified
// NOTE: op_index should be the measured operation index (starting at 0 after
// warmup), not the total operation index including warmup.
inline bool ShouldVerify(size_t measured_op_index, bool is_warmup) {
    if (!FLAGS_verify) return false;
    if (is_warmup && FLAGS_verify_warmup_all) return true;
    // verify_rate means "verify 1 in N operations"
    return (measured_op_index % FLAGS_verify_rate) == 0;
}

// ============================================================================
// Benchmark: Init
// ============================================================================

double BenchInit(BackendType type, const std::string& storage_path,
                 size_t capacity) {
    PrintHeader("INIT", type, 0, 0);

    CleanupStoragePath(storage_path);

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend";
        return -1;
    }

    auto start = std::chrono::steady_clock::now();
    auto result = backend->Init();
    auto end = std::chrono::steady_clock::now();

    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();

    if (!result) {
        LOG(ERROR) << "Init failed with error: "
                   << static_cast<int>(result.error());
        return -1;
    }

    std::cout << "  Init time: " << std::fixed << std::setprecision(2)
              << elapsed_ms << " ms\n";
    return elapsed_ms;
}

// ============================================================================
// Benchmark: BatchOffload (Write)
// ============================================================================

void BenchBatchOffload(BackendType type, const std::string& storage_path,
                       size_t capacity, size_t value_size, size_t batch_size,
                       size_t num_operations, size_t warmup_operations,
                       BenchmarkStats& stats) {
    PrintHeader("BATCH_OFFLOAD", type, value_size, batch_size);

    CleanupStoragePath(storage_path);

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend";
        return;
    }

    auto init_result = backend->Init();
    if (!init_result) {
        LOG(ERROR) << "Init failed";
        return;
    }

    // For FilePerKey backend, we need to call ScanMeta first
    if (type == BackendType::FILE_PER_KEY) {
        backend->ScanMeta([](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
            return mooncake::ErrorCode::OK;
        });
    }

    size_t total_ops = warmup_operations + num_operations;
    size_t total_keys = total_ops * batch_size;
    size_t aligned_size = AlignUp(value_size, kAlignment);

    // === PRE-ALLOCATE EVERYTHING OUTSIDE TIMED REGION ===
    DataGenerator gen;
    KeySet keys;
    keys.Init(total_keys);

    BufferPool write_buffers;
    write_buffers.Init(batch_size, value_size);

    // Reusable batch container (avoid allocation in hot path)
    BatchContainers batch;
    batch.Reserve(batch_size);

    stats.Clear();
    stats.InitThreads(1, num_operations);
    auto& thread_stats = stats.GetStats();
    thread_stats.Reserve(num_operations);

    std::cout << "  Aligned buffer size: " << (aligned_size / KB) << " KB\n";
    std::cout << "  Warmup: " << warmup_operations << " operations\n";
    std::cout << "  Benchmark: " << num_operations << " operations\n";

    size_t key_index = 0;
    bool timer_started = false;

    for (size_t op = 0; op < total_ops; ++op) {
        bool is_warmup = (op < warmup_operations);

        // === SETUP OUTSIDE TIMED REGION ===
        // Fill buffers with verifiable data for this batch
        for (size_t i = 0; i < batch_size; ++i) {
            batch.key_indices[i] = key_index + i;
            gen.FillBuffer(write_buffers.Get(i), value_size,
                           batch.key_indices[i]);
        }

        // Build batch object (allocation here is fine - outside timed region)
        // Note: we clear and rebuild each iteration, but it's not in the timed
        // path
        batch.ClearOffload();
        for (size_t i = 0; i < batch_size; ++i) {
            batch.offload_batch.emplace(
                keys.Get(batch.key_indices[i]),
                std::vector<mooncake::Slice>{
                    {write_buffers.Get(i), value_size}});
        }

        // Start timer right before first measured op
        if (!is_warmup && !timer_started) {
            stats.StartTimer();
            timer_started = true;
        }

        // === TIMED REGION - ONLY THE ACTUAL OPERATION ===
        auto start = std::chrono::steady_clock::now();
        auto result = backend->BatchOffload(
            batch.offload_batch,
            [](const std::vector<std::string>&,
               std::vector<mooncake::StorageObjectMetadata>&) {
                return mooncake::ErrorCode::OK;
            });
        auto end = std::chrono::steady_clock::now();
        // === END TIMED REGION ===

        key_index += batch_size;

        if (!is_warmup) {
            double latency_ms =
                std::chrono::duration<double, std::milli>(end - start).count();
            thread_stats.RecordLatency(latency_ms);
            thread_stats.RecordOperation();

            if (result) {
                thread_stats.RecordBytes(batch_size * value_size);
            } else {
                thread_stats.RecordError();
            }
        }
    }

    stats.StopTimer();
    stats.Finalize();

    // === WRITE VERIFICATION (after timing, sample readback) ===
    if (FLAGS_verify) {
        std::cout << "  Verifying writes (readback check)...\n";

        // verify_rate means "verify 1 in N operations"
        // So if verify_rate=10 and num_operations=100, we verify 10 ops
        size_t verify_ops =
            std::max<size_t>(1, num_operations / FLAGS_verify_rate);
        size_t verify_step_ops =
            std::max<size_t>(1, num_operations / verify_ops);

        // Key range for measured operations (skip warmup)
        size_t measured_key_start = warmup_operations * batch_size;
        size_t measured_key_end =
            measured_key_start + num_operations * batch_size;

        BufferPool verify_buffers;
        verify_buffers.Init(batch_size, value_size);

        size_t verified_keys = 0;
        size_t verification_failures = 0;

        for (size_t op_idx = 0; op_idx < verify_ops; ++op_idx) {
            // Sample operation index within measured range
            size_t sampled_op = op_idx * verify_step_ops;
            size_t base_key = measured_key_start + sampled_op * batch_size;

            // Bounds check: ensure we don't read past the end
            if (base_key + batch_size > measured_key_end) {
                break;
            }

            // Build load batch
            std::unordered_map<std::string, mooncake::Slice> load_batch;
            load_batch.reserve(batch_size);
            for (size_t i = 0; i < batch_size; ++i) {
                load_batch.emplace(
                    keys.Get(base_key + i),
                    mooncake::Slice{verify_buffers.Get(i), value_size});
            }

            auto result = backend->BatchLoad(load_batch);
            if (result) {
                for (size_t i = 0; i < batch_size; ++i) {
                    if (!gen.VerifyBuffer(verify_buffers.Get(i), value_size,
                                          base_key + i)) {
                        verification_failures++;
                        if (FLAGS_fail_fast) {
                            LOG(FATAL)
                                << "Write verification failed for key "
                                << (base_key + i)
                                << " (data was written but readback differs)";
                        }
                    }
                }
                verified_keys += batch_size;
            } else {
                LOG(WARNING)
                    << "Readback failed for batch starting at key " << base_key;
            }
        }

        if (verification_failures > 0) {
            LOG(ERROR) << "Write verification: " << verification_failures
                       << " checksum failures out of " << verified_keys
                       << " verified";
        } else {
            std::cout << "  Write verification passed (" << verified_keys
                      << " keys, " << verify_ops << " ops sampled)\n";
        }
    }

    stats.PrintStatistics("BATCH_OFFLOAD");
}

// ============================================================================
// Benchmark: BatchLoad (Read) - THE CRITICAL BENCHMARK
// ============================================================================

void BenchBatchLoad(BackendType type, const std::string& storage_path,
                    size_t capacity, size_t value_size, size_t batch_size,
                    size_t num_operations, size_t warmup_operations,
                    BenchmarkStats& stats) {
    PrintHeader("BATCH_LOAD", type, value_size, batch_size);

    CleanupStoragePath(storage_path);

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend";
        return;
    }

    auto init_result = backend->Init();
    if (!init_result) {
        LOG(ERROR) << "Init failed";
        return;
    }

    // For FilePerKey backend, call ScanMeta first
    if (type == BackendType::FILE_PER_KEY) {
        backend->ScanMeta([](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
            return mooncake::ErrorCode::OK;
        });
    }

    size_t total_ops = warmup_operations + num_operations;
    size_t total_keys = total_ops * batch_size;
    size_t aligned_size = AlignUp(value_size, kAlignment);

    // === PRE-ALLOCATE EVERYTHING ===
    DataGenerator gen;
    KeySet keys;
    keys.Init(total_keys);

    BufferPool write_buffers, read_buffers;
    write_buffers.Init(batch_size, value_size);
    read_buffers.Init(batch_size, value_size);

    // Reusable containers
    BatchContainers batch;
    batch.Reserve(batch_size);

    // Phase 1: Write all data with deterministic content
    std::cout << "  Pre-populating " << total_keys << " keys...\n";

    for (size_t key_idx = 0; key_idx < total_keys; key_idx += batch_size) {
        // Fill write buffers with verifiable data
        for (size_t i = 0; i < batch_size && (key_idx + i) < total_keys; ++i) {
            gen.FillBuffer(write_buffers.Get(i), value_size, key_idx + i);
        }

        batch.ClearOffload();
        for (size_t i = 0; i < batch_size && (key_idx + i) < total_keys; ++i) {
            batch.offload_batch.emplace(
                keys.Get(key_idx + i), std::vector<mooncake::Slice>{
                                           {write_buffers.Get(i), value_size}});
        }

        auto result = backend->BatchOffload(
            batch.offload_batch,
            [](const std::vector<std::string>&,
               std::vector<mooncake::StorageObjectMetadata>&) {
                return mooncake::ErrorCode::OK;
            });

        if (!result) {
            LOG(ERROR) << "Pre-population failed at key " << key_idx;
            return;
        }
    }

    std::cout << "  Pre-population complete.\n";

    // === DROP CACHE AFTER PRE-POPULATION (before timed benchmark) ===
    // This ensures we measure cold cache performance, not warm cache
    CacheMode cache_mode = StringToCacheMode(FLAGS_cache_mode);
    if (cache_mode == CacheMode::DROP_CACHE_EXTERNAL) {
        std::cout << "  Dropping page cache before benchmark...\n";
        if (!DropLinuxPageCacheGlobal()) {
            std::cout << "  ⚠️  Cache drop failed (not root?). Results may "
                         "reflect warm cache.\n";
        }
    }

    std::cout << "  Aligned buffer size: " << (aligned_size / KB) << " KB\n";
    std::cout << "  Warmup: " << warmup_operations << " operations\n";
    std::cout << "  Benchmark: " << num_operations << " operations\n";
    std::cout << "  Verification: " << (FLAGS_verify ? "enabled" : "disabled");
    if (FLAGS_verify && FLAGS_verify_rate > 1) {
        std::cout << " (1 in " << FLAGS_verify_rate << ")";
    }
    std::cout << "\n";

    // Phase 2: Read data back with verification
    stats.Clear();
    stats.InitThreads(1, num_operations);
    auto& thread_stats = stats.GetStats();
    thread_stats.Reserve(num_operations);

    size_t key_index = 0;
    bool timer_started = false;

    for (size_t op = 0; op < total_ops; ++op) {
        bool is_warmup = (op < warmup_operations);

        // === SETUP OUTSIDE TIMED REGION ===
        // Record key indices for verification
        for (size_t i = 0; i < batch_size; ++i) {
            batch.key_indices[i] = key_index + i;
        }

        // Build load request using reusable container
        batch.ClearLoad();
        for (size_t i = 0; i < batch_size; ++i) {
            batch.load_batch.emplace(
                keys.Get(batch.key_indices[i]),
                mooncake::Slice{read_buffers.Get(i), value_size});
        }

        // Start timer right before first measured op
        if (!is_warmup && !timer_started) {
            stats.StartTimer();
            timer_started = true;
        }

        // === TIMED REGION - ONLY THE ACTUAL OPERATION ===
        auto start = std::chrono::steady_clock::now();
        auto result = backend->BatchLoad(batch.load_batch);
        auto end = std::chrono::steady_clock::now();
        // === END TIMED REGION ===

        key_index += batch_size;

        // === VERIFICATION OUTSIDE TIMED REGION ===
        if (!is_warmup) {
            double latency_ms =
                std::chrono::duration<double, std::milli>(end - start).count();
            thread_stats.RecordLatency(latency_ms);
            thread_stats.RecordOperation();

            if (result) {
                thread_stats.RecordBytes(batch_size * value_size);

                // Verify based on rate (use measured op index, not total op
                // index)
                size_t measured_op = op - warmup_operations;
                if (ShouldVerify(measured_op, is_warmup)) {
                    for (size_t i = 0; i < batch_size; ++i) {
                        if (!gen.VerifyBuffer(read_buffers.Get(i), value_size,
                                              batch.key_indices[i])) {
                            thread_stats.RecordChecksumFailure();
                            LOG(ERROR) << "Checksum mismatch for key "
                                       << batch.key_indices[i];
                            if (FLAGS_fail_fast) {
                                CheckVerification(1, "BatchLoad");
                            }
                        }
                    }
                }
            } else {
                thread_stats.RecordError();
            }
        } else if (FLAGS_verify_warmup_all && FLAGS_verify) {
            // Verify all warmup ops
            if (result) {
                for (size_t i = 0; i < batch_size; ++i) {
                    if (!gen.VerifyBuffer(read_buffers.Get(i), value_size,
                                          batch.key_indices[i])) {
                        LOG(ERROR) << "Warmup verification failed for key "
                                   << batch.key_indices[i];
                        if (FLAGS_fail_fast) {
                            CheckVerification(1, "BatchLoad warmup");
                        }
                    }
                }
            }
        }
    }

    stats.StopTimer();
    stats.Finalize();

    // Final verification check
    CheckVerification(thread_stats.checksum_failures, "BatchLoad");

    stats.PrintStatistics("BATCH_LOAD");
}

// ============================================================================
// Benchmark: Concurrent Load (with thread-local stats, buffer pools)
// ============================================================================

void BenchConcurrentLoad(BackendType type, const std::string& storage_path,
                         size_t capacity, size_t value_size, size_t batch_size,
                         size_t num_operations, size_t num_threads,
                         BenchmarkStats& stats) {
    PrintHeader("CONCURRENT_LOAD", type, value_size, batch_size, num_threads);

    CleanupStoragePath(storage_path);

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend";
        return;
    }

    auto init_result = backend->Init();
    if (!init_result) {
        LOG(ERROR) << "Init failed";
        return;
    }

    if (type == BackendType::FILE_PER_KEY) {
        backend->ScanMeta([](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
            return mooncake::ErrorCode::OK;
        });
    }

    size_t total_keys = num_operations * batch_size * num_threads;
    size_t ops_per_thread = num_operations;

    // === PRE-ALLOCATE SHARED RESOURCES ===
    DataGenerator gen;
    KeySet keys;
    keys.Init(total_keys);

    // Pre-populate data with verifiable content
    std::cout << "  Pre-populating " << total_keys << " keys...\n";

    BufferPool setup_buffers;
    setup_buffers.Init(batch_size, value_size);

    for (size_t key_idx = 0; key_idx < total_keys; key_idx += batch_size) {
        // Fill buffers with verifiable data
        for (size_t i = 0; i < batch_size && (key_idx + i) < total_keys; ++i) {
            gen.FillBuffer(setup_buffers.Get(i), value_size, key_idx + i);
        }

        std::unordered_map<std::string, std::vector<mooncake::Slice>>
            batch_object;
        batch_object.reserve(batch_size);
        for (size_t i = 0; i < batch_size && (key_idx + i) < total_keys; ++i) {
            std::vector<mooncake::Slice> slices;
            slices.emplace_back(
                mooncake::Slice{setup_buffers.Get(i), value_size});
            batch_object.emplace(keys.Get(key_idx + i), std::move(slices));
        }

        backend->BatchOffload(
            batch_object, [](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
                return mooncake::ErrorCode::OK;
            });
    }

    std::cout << "  Pre-population complete.\n";

    // === DROP CACHE AFTER PRE-POPULATION (before timed benchmark) ===
    CacheMode cache_mode = StringToCacheMode(FLAGS_cache_mode);
    if (cache_mode == CacheMode::DROP_CACHE_EXTERNAL) {
        std::cout << "  Dropping page cache before benchmark...\n";
        if (!DropLinuxPageCacheGlobal()) {
            std::cout << "  ⚠️  Cache drop failed (not root?). Results may "
                         "reflect warm cache.\n";
        }
    }

    std::cout << "  Threads: " << num_threads << "\n";
    std::cout << "  Ops per thread: " << ops_per_thread << "\n";

    // === SETUP THREAD-LOCAL STATS (NO MUTEX IN HOT PATH) ===
    stats.Clear();
    stats.InitThreads(num_threads, ops_per_thread);

    std::latch start_latch(num_threads + 1);
    std::latch end_latch(num_threads);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // === PER-THREAD RESOURCES (allocated before timing) ===
            DataGenerator local_gen(42 + t);
            BufferPool local_buffers;
            local_buffers.Init(batch_size, value_size);

            ThreadStats& my_stats = stats.GetThreadStats(t);
            my_stats.Reserve(ops_per_thread);

            size_t base_key_idx = t * ops_per_thread * batch_size;

            // === PRE-ALLOCATE REUSABLE CONTAINERS (avoid per-op allocation)
            // ===
            std::unordered_map<std::string, mooncake::Slice> load_slices;
            load_slices.reserve(batch_size);
            std::vector<size_t> batch_key_indices(batch_size);

            start_latch.arrive_and_wait();

            for (size_t op = 0; op < ops_per_thread; ++op) {
                // === SETUP OUTSIDE TIMED REGION ===
                // Clear but preserve capacity (no reallocation)
                load_slices.clear();

                for (size_t i = 0; i < batch_size; ++i) {
                    size_t key_idx = base_key_idx + op * batch_size + i;
                    batch_key_indices[i] = key_idx;
                    load_slices.emplace(
                        keys.Get(key_idx),
                        mooncake::Slice{local_buffers.Get(i), value_size});
                }

                // === TIMED REGION ===
                auto start = std::chrono::steady_clock::now();
                auto result = backend->BatchLoad(load_slices);
                auto end = std::chrono::steady_clock::now();
                // === END TIMED REGION ===

                double latency_ms =
                    std::chrono::duration<double, std::milli>(end - start)
                        .count();
                my_stats.RecordLatency(latency_ms);
                my_stats.RecordOperation();

                if (result) {
                    my_stats.RecordBytes(batch_size * value_size);

                    // Verify data integrity
                    for (size_t i = 0; i < batch_size; ++i) {
                        if (!local_gen.VerifyBuffer(local_buffers.Get(i),
                                                    value_size,
                                                    batch_key_indices[i])) {
                            my_stats.RecordChecksumFailure();
                        }
                    }
                } else {
                    my_stats.RecordError();
                }
            }

            end_latch.count_down();
        });
    }

    stats.StartTimer();
    start_latch.arrive_and_wait();
    end_latch.wait();
    stats.StopTimer();

    for (auto& t : threads) {
        t.join();
    }

    stats.Finalize();
    stats.PrintStatistics("CONCURRENT_LOAD");
}

// ============================================================================
// Benchmark: IsExist (Metadata Lookup) - with HIT/MISS separation
// ============================================================================

void BenchIsExist(BackendType type, const std::string& storage_path,
                  size_t capacity, size_t num_operations,
                  BenchmarkStats& hit_stats, BenchmarkStats& miss_stats) {
    PrintHeader("IS_EXIST", type, 0, 0);

    CleanupStoragePath(storage_path);

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend";
        return;
    }

    auto init_result = backend->Init();
    if (!init_result) {
        LOG(ERROR) << "Init failed";
        return;
    }

    if (type == BackendType::FILE_PER_KEY) {
        backend->ScanMeta([](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
            return mooncake::ErrorCode::OK;
        });
    }

    DataGenerator gen;
    size_t value_size = 1024;  // Small values for IsExist test

    // Pre-populate half the keys (will be "hits")
    size_t num_keys = num_operations / 2;
    std::cout << "  Pre-populating " << num_keys
              << " keys for hit testing...\n";

    KeySet keys;
    keys.Init(num_operations);

    BufferPool setup_buf;
    setup_buf.Init(1, value_size);

    for (size_t i = 0; i < num_keys; ++i) {
        gen.FillBuffer(setup_buf.Get(0), value_size, i);
        std::unordered_map<std::string, std::vector<mooncake::Slice>> batch;
        batch[keys.Get(i)] = {mooncake::Slice{setup_buf.Get(0), value_size}};
        backend->BatchOffload(
            batch, [](const std::vector<std::string>&,
                      std::vector<mooncake::StorageObjectMetadata>&) {
                return mooncake::ErrorCode::OK;
            });
    }

    std::cout << "  Testing " << num_operations << " lookups:\n";
    std::cout << "    - Keys 0.." << (num_keys - 1) << " should HIT\n";
    std::cout << "    - Keys " << num_keys << ".." << (num_operations - 1)
              << " should MISS\n";

    // Initialize stats
    hit_stats.Clear();
    hit_stats.InitThreads(1, num_keys);
    miss_stats.Clear();
    miss_stats.InitThreads(1, num_operations - num_keys);

    auto& hit_thread_stats = hit_stats.GetStats();
    auto& miss_thread_stats = miss_stats.GetStats();

    hit_thread_stats.Reserve(num_keys);
    miss_thread_stats.Reserve(num_operations - num_keys);

    // === PHASE 1: Time HIT lookups separately ===
    std::cout << "  Phase 1: HIT lookups...\n";
    hit_stats.StartTimer();

    for (size_t i = 0; i < num_keys; ++i) {
        const std::string& key = keys.Get(i);

        auto start = std::chrono::steady_clock::now();
        auto result = backend->IsExist(key);
        auto end = std::chrono::steady_clock::now();

        double latency_ms =
            std::chrono::duration<double, std::milli>(end - start).count();
        hit_thread_stats.RecordLatency(latency_ms);
        hit_thread_stats.RecordOperation();

        // Verify it actually existed
        if (!result || !result.value()) {
            hit_thread_stats.RecordError();
            LOG(WARNING) << "Expected key " << i << " to exist but it didn't";
        }
    }

    hit_stats.StopTimer();
    hit_stats.Finalize();

    // === PHASE 2: Time MISS lookups separately ===
    std::cout << "  Phase 2: MISS lookups...\n";
    miss_stats.StartTimer();

    for (size_t i = num_keys; i < num_operations; ++i) {
        const std::string& key = keys.Get(i);

        auto start = std::chrono::steady_clock::now();
        auto result = backend->IsExist(key);
        auto end = std::chrono::steady_clock::now();

        double latency_ms =
            std::chrono::duration<double, std::milli>(end - start).count();
        miss_thread_stats.RecordLatency(latency_ms);
        miss_thread_stats.RecordOperation();

        // Verify it actually didn't exist
        if (result && result.value()) {
            miss_thread_stats.RecordError();
            LOG(WARNING) << "Expected key " << i << " to NOT exist but it did";
        }
    }

    miss_stats.StopTimer();
    miss_stats.Finalize();

    std::cout << "\n  --- HIT Latency (key exists) ---";
    hit_stats.PrintStatistics("IS_EXIST_HIT");

    std::cout << "\n  --- MISS Latency (key not found) ---";
    miss_stats.PrintStatistics("IS_EXIST_MISS");
}

// Wrapper for backward compatibility
void BenchIsExist(BackendType type, const std::string& storage_path,
                  size_t capacity, size_t num_operations,
                  BenchmarkStats& stats) {
    BenchmarkStats miss_stats;
    BenchIsExist(type, storage_path, capacity, num_operations, stats,
                 miss_stats);
}

// ============================================================================
// PR3: Mixed Read/Write Workload
// ============================================================================

void BenchMixedRW(BackendType type, const std::string& storage_path,
                  size_t capacity, size_t value_size, size_t batch_size,
                  size_t num_operations, size_t read_threads,
                  size_t write_threads, BenchmarkStats& read_stats,
                  BenchmarkStats& write_stats) {
    size_t total_threads = read_threads + write_threads;
    PrintHeader("MIXED_RW", type, value_size, batch_size, total_threads);

    std::cout << "  Reader threads: " << read_threads << "\n";
    std::cout << "  Writer threads: " << write_threads << "\n";

    CleanupStoragePath(storage_path);

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend";
        return;
    }

    auto init_result = backend->Init();
    if (!init_result) {
        LOG(ERROR) << "Init failed";
        return;
    }

    if (type == BackendType::FILE_PER_KEY) {
        backend->ScanMeta([](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
            return mooncake::ErrorCode::OK;
        });
    }

    // Pre-populate keys for reading
    size_t total_keys = num_operations * batch_size * total_threads;
    size_t initial_keys = total_keys / 2;  // Pre-populate half

    DataGenerator gen;
    KeySet keys;
    keys.Init(total_keys);

    BufferPool setup_buffers;
    setup_buffers.Init(batch_size, value_size);

    std::cout << "  Pre-populating " << initial_keys << " keys...\n";

    for (size_t key_idx = 0; key_idx < initial_keys; key_idx += batch_size) {
        for (size_t i = 0; i < batch_size && (key_idx + i) < initial_keys;
             ++i) {
            gen.FillBuffer(setup_buffers.Get(i), value_size, key_idx + i);
        }

        std::unordered_map<std::string, std::vector<mooncake::Slice>>
            batch_object;
        for (size_t i = 0; i < batch_size && (key_idx + i) < initial_keys;
             ++i) {
            batch_object[keys.Get(key_idx + i)] = {
                mooncake::Slice{setup_buffers.Get(i), value_size}};
        }
        backend->BatchOffload(
            batch_object, [](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
                return mooncake::ErrorCode::OK;
            });
    }

    std::cout << "  Pre-population complete.\n";

    // === DROP CACHE AFTER PRE-POPULATION (before timed benchmark) ===
    CacheMode cache_mode = StringToCacheMode(FLAGS_cache_mode);
    if (cache_mode == CacheMode::DROP_CACHE_EXTERNAL) {
        std::cout << "  Dropping page cache before benchmark...\n";
        if (!DropLinuxPageCacheGlobal()) {
            std::cout << "  ⚠️  Cache drop failed (not root?). Results may "
                         "reflect warm cache.\n";
        }
    }

    // Setup stats
    read_stats.Clear();
    read_stats.InitThreads(read_threads, num_operations);
    write_stats.Clear();
    write_stats.InitThreads(write_threads, num_operations);

    std::atomic<size_t> next_write_key{initial_keys};
    std::latch start_latch(total_threads + 1);
    std::latch end_latch(total_threads);
    std::vector<std::thread> threads;

    // Create reader threads
    for (size_t t = 0; t < read_threads; ++t) {
        threads.emplace_back([&, t]() {
            DataGenerator local_gen(42 + t);
            BufferPool local_buffers;
            local_buffers.Init(batch_size, value_size);
            BatchContainers batch;
            batch.Reserve(batch_size);

            ThreadStats& my_stats = read_stats.GetThreadStats(t);
            my_stats.Reserve(num_operations);

            KeyIndexGenerator key_gen(initial_keys,
                                      StringToAccessPattern(FLAGS_pattern),
                                      FLAGS_zipf_skew, 42 + t);

            start_latch.arrive_and_wait();

            for (size_t op = 0; op < num_operations; ++op) {
                // Build load request
                batch.ClearLoad();
                for (size_t i = 0; i < batch_size; ++i) {
                    batch.key_indices[i] = key_gen.Next();
                    batch.load_batch.emplace(
                        keys.Get(batch.key_indices[i]),
                        mooncake::Slice{local_buffers.Get(i), value_size});
                }

                auto start = std::chrono::steady_clock::now();
                auto result = backend->BatchLoad(batch.load_batch);
                auto end = std::chrono::steady_clock::now();

                double latency_ms =
                    std::chrono::duration<double, std::milli>(end - start)
                        .count();
                my_stats.RecordLatency(latency_ms);
                my_stats.RecordOperation();

                if (result) {
                    my_stats.RecordBytes(batch_size * value_size);
                } else {
                    my_stats.RecordError();
                }
            }

            end_latch.count_down();
        });
    }

    // Create writer threads
    for (size_t t = 0; t < write_threads; ++t) {
        threads.emplace_back([&, t]() {
            DataGenerator local_gen(1000 + t);
            BufferPool local_buffers;
            local_buffers.Init(batch_size, value_size);
            BatchContainers batch;
            batch.Reserve(batch_size);

            ThreadStats& my_stats = write_stats.GetThreadStats(t);
            my_stats.Reserve(num_operations);

            start_latch.arrive_and_wait();

            for (size_t op = 0; op < num_operations; ++op) {
                // Get unique keys for writing
                size_t base_key = next_write_key.fetch_add(batch_size);

                // Fill buffers
                for (size_t i = 0; i < batch_size; ++i) {
                    batch.key_indices[i] = base_key + i;
                    local_gen.FillBuffer(local_buffers.Get(i), value_size,
                                         batch.key_indices[i]);
                }

                // Build batch (allocation OK - outside timed region)
                batch.ClearOffload();
                for (size_t i = 0; i < batch_size; ++i) {
                    batch.offload_batch.emplace(
                        keys.Get(batch.key_indices[i] % keys.Size()),
                        std::vector<mooncake::Slice>{
                            {local_buffers.Get(i), value_size}});
                }

                auto start = std::chrono::steady_clock::now();
                auto result = backend->BatchOffload(
                    batch.offload_batch,
                    [](const std::vector<std::string>&,
                       std::vector<mooncake::StorageObjectMetadata>&) {
                        return mooncake::ErrorCode::OK;
                    });
                auto end = std::chrono::steady_clock::now();

                double latency_ms =
                    std::chrono::duration<double, std::milli>(end - start)
                        .count();
                my_stats.RecordLatency(latency_ms);
                my_stats.RecordOperation();

                if (result) {
                    my_stats.RecordBytes(batch_size * value_size);
                } else {
                    my_stats.RecordError();
                }
            }

            end_latch.count_down();
        });
    }

    read_stats.StartTimer();
    write_stats.StartTimer();
    start_latch.arrive_and_wait();
    end_latch.wait();
    read_stats.StopTimer();
    write_stats.StopTimer();

    for (auto& t : threads) {
        t.join();
    }

    read_stats.Finalize();
    write_stats.Finalize();

    std::cout << "\n  --- READ Performance ---";
    read_stats.PrintStatistics("MIXED_RW_READ");

    std::cout << "\n  --- WRITE Performance ---";
    write_stats.PrintStatistics("MIXED_RW_WRITE");
}

// ============================================================================
// PR3: Churn Test (near-capacity steady-state)
// ============================================================================

void BenchChurn(BackendType type, const std::string& storage_path,
                size_t capacity, size_t value_size, size_t batch_size,
                size_t num_operations, double fill_ratio,
                BenchmarkStats& stats) {
    PrintHeader("CHURN", type, value_size, batch_size);
    std::cout << "  Fill ratio: " << (fill_ratio * 100) << "%\n";

    CleanupStoragePath(storage_path);

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend";
        return;
    }

    auto init_result = backend->Init();
    if (!init_result) {
        LOG(ERROR) << "Init failed";
        return;
    }

    if (type == BackendType::FILE_PER_KEY) {
        backend->ScanMeta([](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
            return mooncake::ErrorCode::OK;
        });
    }

    // Calculate how many keys to fill
    size_t target_bytes = static_cast<size_t>(capacity * fill_ratio);
    size_t num_fill_keys = target_bytes / value_size;

    DataGenerator gen;
    KeySet keys;
    keys.Init(num_fill_keys + num_operations * batch_size);

    BufferPool buffers;
    buffers.Init(batch_size, value_size);
    BatchContainers batch;
    batch.Reserve(batch_size);

    // Phase 1: Fill to target ratio
    std::cout << "  Filling to " << (num_fill_keys * value_size / MB) << " MB ("
              << num_fill_keys << " keys)...\n";

    for (size_t key_idx = 0; key_idx < num_fill_keys; key_idx += batch_size) {
        for (size_t i = 0; i < batch_size && (key_idx + i) < num_fill_keys;
             ++i) {
            gen.FillBuffer(buffers.Get(i), value_size, key_idx + i);
        }

        batch.ClearOffload();
        for (size_t i = 0; i < batch_size && (key_idx + i) < num_fill_keys;
             ++i) {
            batch.offload_batch.emplace(
                keys.Get(key_idx + i),
                std::vector<mooncake::Slice>{{buffers.Get(i), value_size}});
        }

        backend->BatchOffload(
            batch.offload_batch,
            [](const std::vector<std::string>&,
               std::vector<mooncake::StorageObjectMetadata>&) {
                return mooncake::ErrorCode::OK;
            });
    }

    std::cout << "  Initial fill complete.\n";

    // === DROP CACHE AFTER INITIAL FILL (before timed churn benchmark) ===
    CacheMode cache_mode = StringToCacheMode(FLAGS_cache_mode);
    if (cache_mode == CacheMode::DROP_CACHE_EXTERNAL) {
        std::cout << "  Dropping page cache before churn benchmark...\n";
        if (!DropLinuxPageCacheGlobal()) {
            std::cout << "  ⚠️  Cache drop failed (not root?). Results may "
                         "reflect warm cache.\n";
        }
    }

    std::cout << "  Running churn: " << num_operations
              << " mixed read/overwrite ops...\n";

    // Phase 2: Churn - mix of reads and overwrites
    stats.Clear();
    stats.InitThreads(1, num_operations);
    auto& thread_stats = stats.GetStats();
    thread_stats.Reserve(num_operations);

    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> op_dist(0.0, 1.0);
    std::uniform_int_distribution<size_t> key_dist(0, num_fill_keys - 1);

    auto churn_start = std::chrono::steady_clock::now();
    stats.StartTimer();

    for (size_t op = 0; op < num_operations; ++op) {
        bool is_read = (op_dist(rng) < FLAGS_read_ratio);

        if (is_read) {
            // Read existing keys
            batch.ClearLoad();
            for (size_t i = 0; i < batch_size; ++i) {
                batch.key_indices[i] = key_dist(rng);
                batch.load_batch.emplace(
                    keys.Get(batch.key_indices[i]),
                    mooncake::Slice{buffers.Get(i), value_size});
            }

            auto start = std::chrono::steady_clock::now();
            auto result = backend->BatchLoad(batch.load_batch);
            auto end = std::chrono::steady_clock::now();

            double latency_ms =
                std::chrono::duration<double, std::milli>(end - start).count();
            thread_stats.RecordLatency(latency_ms);
            thread_stats.RecordOperation();

            if (result) {
                thread_stats.RecordBytes(batch_size * value_size);
            } else {
                thread_stats.RecordError();
            }
        } else {
            // Overwrite existing keys (forces allocator reuse)
            for (size_t i = 0; i < batch_size; ++i) {
                batch.key_indices[i] = key_dist(rng);
                gen.FillBuffer(buffers.Get(i), value_size,
                               batch.key_indices[i] + op * 1000);
            }

            batch.ClearOffload();
            for (size_t i = 0; i < batch_size; ++i) {
                batch.offload_batch.emplace(
                    keys.Get(batch.key_indices[i]),
                    std::vector<mooncake::Slice>{{buffers.Get(i), value_size}});
            }

            auto start = std::chrono::steady_clock::now();
            auto result = backend->BatchOffload(
                batch.offload_batch,
                [](const std::vector<std::string>&,
                   std::vector<mooncake::StorageObjectMetadata>&) {
                    return mooncake::ErrorCode::OK;
                });
            auto end = std::chrono::steady_clock::now();

            double latency_ms =
                std::chrono::duration<double, std::milli>(end - start).count();
            thread_stats.RecordLatency(latency_ms);
            thread_stats.RecordOperation();

            if (result) {
                thread_stats.RecordBytes(batch_size * value_size);
            } else {
                thread_stats.RecordError();
            }
        }

        // Time-series reporting (wall-clock for progress; final stats use
        // backend time)
        if (FLAGS_time_series && (op + 1) % FLAGS_report_interval == 0) {
            double wall_elapsed =
                std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                              churn_start)
                    .count();
            // Sum of latencies for backend time
            double backend_time_sec =
                std::accumulate(thread_stats.latencies.begin(),
                                thread_stats.latencies.end(), 0.0) /
                1000.0;
            double backend_throughput =
                (backend_time_sec > 0)
                    ? static_cast<double>(thread_stats.bytes) / MB /
                          backend_time_sec
                    : 0;
            std::cout << "  [" << (op + 1) << "/" << num_operations << "] "
                      << "wall=" << std::fixed << std::setprecision(1)
                      << wall_elapsed << "s, "
                      << "ops=" << thread_stats.operations << ", "
                      << "throughput=" << std::setprecision(1)
                      << backend_throughput << " MB/s (backend)\n";
        }
    }

    stats.StopTimer();
    stats.Finalize();
    stats.PrintStatistics("CHURN");
}

// ============================================================================
// PR3: Restart Benchmark (recovery time)
// ============================================================================

void BenchRestart(BackendType type, const std::string& storage_path,
                  size_t capacity, size_t value_size, size_t num_keys) {
    PrintHeader("RESTART", type, value_size, 0);
    std::cout << "  Testing recovery with " << num_keys << " keys\n";

    CleanupStoragePath(storage_path);

    DataGenerator gen;
    KeySet keys;
    keys.Init(num_keys);

    BufferPool buffers;
    buffers.Init(1, value_size);

    // Phase 1: Create backend and populate data
    std::cout << "  Phase 1: Populating " << num_keys << " keys...\n";
    {
        auto backend = CreateBackend(type, storage_path, capacity);
        if (!backend) {
            LOG(ERROR) << "Failed to create backend";
            return;
        }
        backend->Init();

        if (type == BackendType::FILE_PER_KEY) {
            backend->ScanMeta(
                [](const std::vector<std::string>&,
                   std::vector<mooncake::StorageObjectMetadata>&) {
                    return mooncake::ErrorCode::OK;
                });
        }

        for (size_t i = 0; i < num_keys; ++i) {
            gen.FillBuffer(buffers.Get(0), value_size, i);
            std::unordered_map<std::string, std::vector<mooncake::Slice>> batch;
            batch[keys.Get(i)] = {mooncake::Slice{buffers.Get(0), value_size}};
            backend->BatchOffload(
                batch, [](const std::vector<std::string>&,
                          std::vector<mooncake::StorageObjectMetadata>&) {
                    return mooncake::ErrorCode::OK;
                });
        }

        std::cout << "  Population complete. Destroying backend...\n";
        // Backend destructor called here
    }

    // Phase 2: Measure restart time
    std::cout << "  Phase 2: Measuring restart time...\n";

    auto restart_start = std::chrono::steady_clock::now();

    auto backend = CreateBackend(type, storage_path, capacity);
    if (!backend) {
        LOG(ERROR) << "Failed to create backend on restart";
        return;
    }

    auto init_start = std::chrono::steady_clock::now();
    backend->Init();
    auto init_end = std::chrono::steady_clock::now();

    double init_ms =
        std::chrono::duration<double, std::milli>(init_end - init_start)
            .count();

    // For FilePerKey, ScanMeta is the expensive part
    double scanmeta_ms = 0;
    if (type == BackendType::FILE_PER_KEY) {
        auto scan_start = std::chrono::steady_clock::now();
        backend->ScanMeta([](const std::vector<std::string>&,
                             std::vector<mooncake::StorageObjectMetadata>&) {
            return mooncake::ErrorCode::OK;
        });
        auto scan_end = std::chrono::steady_clock::now();
        scanmeta_ms =
            std::chrono::duration<double, std::milli>(scan_end - scan_start)
                .count();
    }

    auto restart_end = std::chrono::steady_clock::now();
    double total_restart_ms =
        std::chrono::duration<double, std::milli>(restart_end - restart_start)
            .count();

    // Phase 3: Measure first-load latency
    std::cout << "  Phase 3: Measuring first-load latency...\n";

    std::vector<double> first_load_latencies;
    first_load_latencies.reserve(10);

    for (size_t i = 0; i < std::min(num_keys, size_t(10)); ++i) {
        std::unordered_map<std::string, mooncake::Slice> load_batch;
        load_batch[keys.Get(i)] = mooncake::Slice{buffers.Get(0), value_size};

        auto start = std::chrono::steady_clock::now();
        auto result = backend->BatchLoad(load_batch);
        auto end = std::chrono::steady_clock::now();

        double latency_ms =
            std::chrono::duration<double, std::milli>(end - start).count();
        first_load_latencies.push_back(latency_ms);

        // Verify data
        if (result && FLAGS_verify) {
            if (!gen.VerifyBuffer(buffers.Get(0), value_size, i)) {
                LOG(ERROR) << "Post-restart verification failed for key " << i;
            }
        }
    }

    // Report results
    std::cout << "\n  --- RESTART TIMING ---\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  Total restart:    " << total_restart_ms << " ms\n";
    std::cout << "  Init() time:      " << init_ms << " ms\n";
    if (type == BackendType::FILE_PER_KEY) {
        std::cout << "  ScanMeta() time:  " << scanmeta_ms << " ms\n";
    }

    double avg_first_load = std::accumulate(first_load_latencies.begin(),
                                            first_load_latencies.end(), 0.0) /
                            first_load_latencies.size();
    std::cout << "\n  --- FIRST-LOAD LATENCY (post-restart) ---\n";
    std::cout << "  Samples:          " << first_load_latencies.size() << "\n";
    std::cout << "  Average:          " << avg_first_load << " ms\n";
    std::cout << "  First:            " << first_load_latencies[0] << " ms\n";
}

// ============================================================================
// Run All Benchmarks
// ============================================================================

struct BenchmarkResult {
    BackendType backend;
    std::string test_name;
    size_t value_size;
    size_t batch_size;
    size_t threads;
    double throughput_mbps;
    double p99_latency_ms;
};

void RunAllBenchmarks(const std::string& storage_path, size_t capacity) {
    std::vector<BackendType> backends = {BackendType::OFFSET_ALLOCATOR,
                                         BackendType::BUCKET,
                                         BackendType::FILE_PER_KEY};

    std::vector<BenchmarkResult> results;

    std::cout << "\n" << std::string(60, '=') << "\n";
    std::cout << "          STORAGE BACKEND BENCHMARK SUITE\n";
    std::cout << std::string(60, '=') << "\n";
    std::cout << "Storage Path: " << storage_path << "\n";
    std::cout << "Capacity: " << (capacity / GB) << " GB\n";

    for (auto backend_type : backends) {
        std::cout << "\n" << std::string(60, '=') << "\n";
        std::cout << "BACKEND: " << BackendTypeToString(backend_type) << "\n";
        std::cout << std::string(60, '=') << "\n";

        // Init benchmark
        BenchInit(backend_type, storage_path, capacity);

        // BatchLoad and BatchOffload with different parameters
        for (size_t value_size : {128 * KB, 512 * KB}) {
            for (size_t batch_size : {8, 32, 128}) {
                BenchmarkStats write_stats, read_stats;

                BenchBatchOffload(backend_type, storage_path, capacity,
                                  value_size, batch_size, 500, 50, write_stats);

                results.push_back({backend_type, "BATCH_OFFLOAD", value_size,
                                   batch_size, 1,
                                   write_stats.GetThroughputMBps(),
                                   write_stats.GetP99Latency()});

                BenchBatchLoad(backend_type, storage_path, capacity, value_size,
                               batch_size, 500, 50, read_stats);

                results.push_back({backend_type, "BATCH_LOAD", value_size,
                                   batch_size, 1,
                                   read_stats.GetThroughputMBps(),
                                   read_stats.GetP99Latency()});
            }
        }

        // Concurrent load test
        for (size_t threads : {4, 8}) {
            BenchmarkStats concurrent_stats;
            BenchConcurrentLoad(backend_type, storage_path, capacity, 128 * KB,
                                32, 200, threads, concurrent_stats);

            results.push_back({backend_type, "CONCURRENT_LOAD", 128 * KB, 32,
                               threads, concurrent_stats.GetThroughputMBps(),
                               concurrent_stats.GetP99Latency()});
        }

        // IsExist benchmark
        BenchmarkStats exist_stats;
        BenchIsExist(backend_type, storage_path, capacity, 10000, exist_stats);
    }

    // Print summary table
    std::cout << "\n" << std::string(100, '=') << "\n";
    std::cout << "                                 SUMMARY TABLE\n";
    std::cout << std::string(100, '=') << "\n";
    std::cout << std::left << std::setw(18) << "Backend" << std::setw(18)
              << "Test" << std::setw(12) << "Value(KB)" << std::setw(10)
              << "Batch" << std::setw(10) << "Threads" << std::setw(15)
              << "MB/s" << std::setw(12) << "P99(ms)" << "\n";
    std::cout << std::string(100, '-') << "\n";

    for (const auto& r : results) {
        std::cout << std::left << std::setw(18)
                  << BackendTypeToString(r.backend) << std::setw(18)
                  << r.test_name << std::setw(12) << (r.value_size / KB)
                  << std::setw(10) << r.batch_size << std::setw(10) << r.threads
                  << std::fixed << std::setprecision(2) << std::setw(15)
                  << r.throughput_mbps << std::setw(12) << r.p99_latency_ms
                  << "\n";
    }
    std::cout << std::string(100, '=') << "\n";
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    google::InitGoogleLogging("StorageBackendBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::string storage_path = FLAGS_storage_path;
    size_t capacity = FLAGS_capacity_gb * GB;
    CacheMode cache_mode = StringToCacheMode(FLAGS_cache_mode);

    if (FLAGS_run_all) {
        RunAllBenchmarks(storage_path, capacity);
    } else if (FLAGS_backend == "all") {
        RunAllBenchmarks(storage_path, capacity);
    } else {
        BackendType backend_type = StringToBackendType(FLAGS_backend);
        BenchmarkStats stats;

        // Print full configuration (PR1 requirement)
        PrintBenchmarkConfig(backend_type, cache_mode);

        if (FLAGS_test == "init" || FLAGS_test == "all") {
            BenchInit(backend_type, storage_path, capacity);
        }

        if (FLAGS_test == "offload" || FLAGS_test == "all") {
            BenchBatchOffload(backend_type, storage_path, capacity,
                              FLAGS_value_size, FLAGS_batch_size,
                              FLAGS_num_operations, FLAGS_warmup_operations,
                              stats);
        }

        if (FLAGS_test == "load" || FLAGS_test == "all") {
            stats.Clear();
            BenchBatchLoad(backend_type, storage_path, capacity,
                           FLAGS_value_size, FLAGS_batch_size,
                           FLAGS_num_operations, FLAGS_warmup_operations,
                           stats);
        }

        if (FLAGS_test == "concurrent_load" || FLAGS_test == "all") {
            stats.Clear();
            BenchConcurrentLoad(backend_type, storage_path, capacity,
                                FLAGS_value_size, FLAGS_batch_size,
                                FLAGS_num_operations, FLAGS_num_threads, stats);
        }

        if (FLAGS_test == "exist" || FLAGS_test == "all") {
            stats.Clear();
            BenchIsExist(backend_type, storage_path, capacity,
                         FLAGS_num_operations, stats);
        }

        // PR3: New tests
        if (FLAGS_test == "mixed_rw") {
            BenchmarkStats read_stats, write_stats;
            BenchMixedRW(backend_type, storage_path, capacity, FLAGS_value_size,
                         FLAGS_batch_size, FLAGS_num_operations,
                         FLAGS_read_threads, FLAGS_write_threads, read_stats,
                         write_stats);
        }

        if (FLAGS_test == "churn") {
            stats.Clear();
            BenchChurn(backend_type, storage_path, capacity, FLAGS_value_size,
                       FLAGS_batch_size, FLAGS_num_operations, FLAGS_fill_ratio,
                       stats);
        }

        if (FLAGS_test == "restart") {
            BenchRestart(backend_type, storage_path, capacity, FLAGS_value_size,
                         FLAGS_num_operations);
        }
    }

    // Final corruption check
    size_t total_failures = g_total_checksum_failures.load();
    if (total_failures > 0) {
        std::cout << "\n" << std::string(70, '!') << "\n";
        std::cout << "  *** TOTAL CHECKSUM FAILURES: " << total_failures
                  << " ***\n";
        std::cout << std::string(70, '!') << "\n";
    }

    // Cleanup
    if (!FLAGS_skip_cleanup) {
        CleanupStoragePath(storage_path);
    }

    google::ShutdownGoogleLogging();

    // Return non-zero if corruption detected
    return total_failures > 0 ? 1 : 0;
}
