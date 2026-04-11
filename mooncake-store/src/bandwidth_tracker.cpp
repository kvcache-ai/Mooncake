#include "bandwidth_tracker.h"

#include <cinttypes>
#include <cstdio>
#include <cstdlib>

#include <glog/logging.h>

#include "utils.h"

namespace mooncake {

namespace {

std::string format_bw(double bytes_per_sec) {
    char buf[64];
    if (bytes_per_sec >= 1e9) {
        snprintf(buf, sizeof(buf), "%.2f GB/s", bytes_per_sec / 1e9);
    } else if (bytes_per_sec >= 1e6) {
        snprintf(buf, sizeof(buf), "%.2f MB/s", bytes_per_sec / 1e6);
    } else if (bytes_per_sec >= 1e3) {
        snprintf(buf, sizeof(buf), "%.2f KB/s", bytes_per_sec / 1e3);
    } else {
        snprintf(buf, sizeof(buf), "%.2f B/s", bytes_per_sec);
    }
    return buf;
}

std::string format_bytes(uint64_t bytes) {
    char buf[64];
    if (bytes >= (1ULL << 30)) {
        snprintf(buf, sizeof(buf), "%.2f GB",
                 bytes / static_cast<double>(1ULL << 30));
    } else if (bytes >= (1ULL << 20)) {
        snprintf(buf, sizeof(buf), "%.2f MB",
                 bytes / static_cast<double>(1ULL << 20));
    } else if (bytes >= (1ULL << 10)) {
        snprintf(buf, sizeof(buf), "%.2f KB",
                 bytes / static_cast<double>(1ULL << 10));
    } else {
        snprintf(buf, sizeof(buf), "%" PRIu64 " B", bytes);
    }
    return buf;
}

}  // namespace

BandwidthTracker::~BandwidthTracker() { stop(); }

void BandwidthTracker::start(const std::string& name) {
    // Check whether bandwidth metrics are enabled via environment variable.
    if (!GetEnvOr<bool>("MC_ENABLE_BANDWIDTH_METRICS", false)) {
        return;
    }

    // Prevent double-start.
    if (running_.exchange(true)) {
        return;
    }

    name_ = name;
    interval_s_ = GetEnvOr<int>("MC_BANDWIDTH_LOG_INTERVAL_S", 10);

    thread_ = std::thread([this] { thread_func(); });
}

void BandwidthTracker::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}

void BandwidthTracker::record_write(size_t bytes) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    total_write_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    write_ops_.fetch_add(1, std::memory_order_relaxed);
}

void BandwidthTracker::record_read(size_t bytes) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    total_read_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    read_ops_.fetch_add(1, std::memory_order_relaxed);
}

void BandwidthTracker::thread_func() {
    using namespace std::chrono;

    auto prev_time = steady_clock::now();
    uint64_t prev_write = total_write_bytes_.load(std::memory_order_relaxed);
    uint64_t prev_read = total_read_bytes_.load(std::memory_order_relaxed);
    uint64_t prev_write_ops = write_ops_.load(std::memory_order_relaxed);
    uint64_t prev_read_ops = read_ops_.load(std::memory_order_relaxed);

    // Sleep in 100 ms chunks so stop() is responsive.
    const int chunks = std::max(1, interval_s_ * 10);

    while (running_.load(std::memory_order_relaxed)) {
        for (int i = 0; i < chunks; ++i) {
            if (!running_.load(std::memory_order_relaxed)) {
                break;
            }
            std::this_thread::sleep_for(milliseconds(100));
        }

        if (!running_.load(std::memory_order_relaxed)) {
            break;
        }

        auto now = steady_clock::now();
        double elapsed =
            duration_cast<duration<double>>(now - prev_time).count();
        if (elapsed <= 0.0) {
            continue;
        }

        uint64_t cur_write = total_write_bytes_.load(std::memory_order_relaxed);
        uint64_t cur_read = total_read_bytes_.load(std::memory_order_relaxed);
        uint64_t cur_write_ops = write_ops_.load(std::memory_order_relaxed);
        uint64_t cur_read_ops = read_ops_.load(std::memory_order_relaxed);

        double write_bw = (cur_write - prev_write) / elapsed;
        double read_bw = (cur_read - prev_read) / elapsed;
        double write_ops_rate = (cur_write_ops - prev_write_ops) / elapsed;
        double read_ops_rate = (cur_read_ops - prev_read_ops) / elapsed;

        LOG(INFO) << "[BandwidthMetrics][" << name_ << "] "
                  << "Write: " << format_bw(write_bw) << " (" << write_ops_rate
                  << " ops/s), " << "Read: " << format_bw(read_bw) << " ("
                  << read_ops_rate << " ops/s) | "
                  << "Total write: " << format_bytes(cur_write)
                  << ", Total read: " << format_bytes(cur_read);

        prev_time = now;
        prev_write = cur_write;
        prev_read = cur_read;
        prev_write_ops = cur_write_ops;
        prev_read_ops = cur_read_ops;
    }
}

}  // namespace mooncake
