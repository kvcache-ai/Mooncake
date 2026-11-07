#pragma once

#include <cstdint>
#include <cstdio>
#include <string>

#include "traffic/traffic_sink.h"

namespace mooncake::traffic {

class FileSink final : public TrafficSink {
   public:
    FileSink(std::uint64_t segment_bytes, std::uint64_t segment_seconds,
             std::uint64_t max_total_bytes = 0);
    ~FileSink() override;

    bool OpenSession(const std::string& output_dir,
                     std::uint64_t start_ms) override;
    bool WriteBlock(const void* data, std::uint32_t bytes,
                    std::uint32_t records, std::uint32_t block_type) override;
    bool WriteRecords(const struct iovec* iov, int iov_count,
                      std::uint64_t bytes, std::uint32_t records) override;
    bool Flush() override;
    void Close() override;

    const std::string& session_dir() const override { return session_dir_; }
    std::uint64_t bytes_written() const override { return bytes_written_; }
    std::uint64_t segments() const override { return segments_; }
    bool failed() const override { return failed_; }

   private:
    bool OpenNextSegment();
    bool CloseSegment(bool sync);

    std::uint64_t segment_bytes_;
    std::uint64_t segment_seconds_;
    std::uint64_t max_total_bytes_;
    std::string session_dir_;
    std::string segment_path_;
    std::FILE* file_ = nullptr;
    std::uint64_t segment_seq_ = 0;
    std::uint64_t segment_bytes_written_ = 0;
    std::uint64_t segment_start_ms_ = 0;
    std::uint64_t block_count_ = 0;
    std::uint64_t record_count_ = 0;
    std::uint64_t payload_bytes_ = 0;
    std::uint64_t bytes_written_ = 0;
    std::uint64_t segments_ = 0;
    bool failed_ = false;
};

}  // namespace mooncake::traffic
