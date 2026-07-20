#pragma once

#include <cstdint>
#include <memory>
#include <sstream>

#include <glog/logging.h>

namespace mooncake::logging {

uint64_t NewTraceId();
uint64_t CurrentTraceId();
bool ShouldLog(google::LogSeverity severity);
bool ShouldVLog(int level);
void ApplyMooncakeLogEnableToGlog();

// High-frequency log sampling.  Parses MC_HIFREQ_LOG_SAMPLE_RATE once (default
// 0.1, clamped to [0,1]) and caches it process-wide.  ShouldSampleHiFreqLog()
// rolls a thread-local RNG: each call returns true with probability == rate.
// Used to gate per-request high-frequency logs (e.g. real_client breakdown).
double HiFreqLogSampleRate();
bool ShouldSampleHiFreqLog();

class ScopedTraceId {
   public:
    explicit ScopedTraceId(uint64_t trace_id);
    ~ScopedTraceId();

    ScopedTraceId(const ScopedTraceId&) = delete;
    ScopedTraceId& operator=(const ScopedTraceId&) = delete;

   private:
    uint64_t previous_trace_id_;
};

class AsyncLogMessage {
   public:
    AsyncLogMessage(const char* file, int line, google::LogSeverity severity,
                    bool enabled);
    ~AsyncLogMessage();

    AsyncLogMessage(const AsyncLogMessage&) = delete;
    AsyncLogMessage& operator=(const AsyncLogMessage&) = delete;

    std::ostream& stream();

   private:
    const char* file_;
    int line_;
    google::LogSeverity severity_;
    bool enabled_;
    uint64_t trace_id_;
    std::ostringstream stream_;
};

void FlushAsyncLogs();

}  // namespace mooncake::logging

#define MC_LOG(severity)                                \
    mooncake::logging::AsyncLogMessage(                 \
        __FILE__, __LINE__, google::severity,           \
        mooncake::logging::ShouldLog(google::severity)) \
        .stream()

#define MC_VLOG(level)                                                       \
    mooncake::logging::AsyncLogMessage(__FILE__, __LINE__, google::INFO,     \
                                       mooncake::logging::ShouldVLog(level)) \
        .stream()
