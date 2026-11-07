#pragma once

#include <sys/uio.h>

#include <cstdint>
#include <string>

namespace mooncake::traffic {

class TrafficSink {
   public:
    virtual ~TrafficSink() = default;

    virtual bool OpenSession(const std::string& output_dir,
                             std::uint64_t start_ms) = 0;
    virtual bool WriteBlock(const void* data, std::uint32_t bytes,
                            std::uint32_t records,
                            std::uint32_t block_type) = 0;
    virtual bool WriteRecords(const struct iovec* iov, int iov_count,
                              std::uint64_t bytes, std::uint32_t records) = 0;
    virtual bool Flush() = 0;
    virtual void Close() = 0;

    virtual const std::string& session_dir() const = 0;
    virtual std::uint64_t bytes_written() const = 0;
    virtual std::uint64_t segments() const = 0;
    virtual bool failed() const = 0;
};

}  // namespace mooncake::traffic
