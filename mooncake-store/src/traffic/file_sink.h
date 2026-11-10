#pragma once

#include <sys/uio.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "traffic_format.h"

namespace mooncake::traffic {

class TrafficSink {
   public:
    virtual ~TrafficSink() = default;

    virtual bool OpenSession(const std::string& output_dir,
                             std::uint64_t start_ms) = 0;
    virtual bool WriteRecords(const struct iovec* iov, int iov_count,
                              std::uint64_t bytes, std::uint32_t records) = 0;
    virtual void Close() = 0;

    virtual std::string session_dir() const = 0;
    virtual std::uint64_t bytes_written() const = 0;
    virtual std::uint64_t segments() const = 0;
    virtual bool failed() const = 0;
    // True when the sink stopped accepting records because the session reached
    // its size limit. The session is still finalized normally, so this is not a
    // write failure.
    virtual bool quota_reached() const { return false; }
};

class FileSink final : public TrafficSink {
   public:
    // segment_bytes is a soft threshold: a block that would cross it rotates
    // first, and a block larger than the threshold still gets its own segment.
    // A zero segment_seconds or max_total_bytes disables that limit.
    FileSink(std::uint64_t segment_bytes, std::uint64_t segment_seconds,
             std::uint64_t max_total_bytes = 0,
             RedactionMode redaction = RedactionMode::kNone);
    ~FileSink() override;

    bool OpenSession(const std::string& output_dir,
                     std::uint64_t start_ms) override;
    bool WriteRecords(const struct iovec* iov, int iov_count,
                      std::uint64_t bytes, std::uint32_t records) override;
    void Close() override;

    std::string session_dir() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return session_dir_;
    }
    std::uint64_t bytes_written() const override {
        return bytes_written_.load(std::memory_order_relaxed);
    }
    std::uint64_t segments() const override {
        return segments_.load(std::memory_order_relaxed);
    }
    bool failed() const override {
        return failed_.load(std::memory_order_relaxed);
    }
    bool quota_reached() const override {
        return quota_reached_.load(std::memory_order_relaxed);
    }

   private:
    bool FitsQuota(std::uint64_t extra) const;
    // Marks the session as stopped by its size limit. The current segment is
    // still finalized, so this is not a write failure.
    void StopAtQuota();

    bool OpenNextSegment();
    // Returns whether this call finalized the segment. failed() also stays set
    // when an earlier write failed, so the two answer different questions.
    bool CloseSegment(bool sync);

    mutable std::mutex mu_;

    std::uint64_t segment_bytes_;
    std::uint64_t segment_seconds_;
    std::uint64_t max_total_bytes_;
    RedactionMode redaction_;
    std::string session_dir_;
    std::string segment_path_;
    int fd_ = -1;
    std::uint64_t segment_seq_ = 0;
    std::uint64_t segment_bytes_written_ = 0;
    std::uint64_t segment_start_ms_ = 0;
    // Rotation by age is measured on a monotonic clock, so a wall clock step
    // cannot stretch a segment or wrap the elapsed time below zero.
    std::chrono::steady_clock::time_point segment_started_at_{};
    std::array<std::uint8_t, 16> session_id_{};
    std::vector<struct iovec> iovec_scratch_;
    std::uint64_t block_count_ = 0;
    std::uint64_t record_count_ = 0;
    std::uint64_t record_bytes_ = 0;
    std::atomic<std::uint64_t> bytes_written_{0};
    std::atomic<std::uint64_t> segments_{0};
    std::atomic<bool> failed_{false};
    std::atomic<bool> quota_reached_{false};
};

}  // namespace mooncake::traffic
