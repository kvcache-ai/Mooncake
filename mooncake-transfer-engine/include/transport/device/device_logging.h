#pragma once

#if !defined(MOONCAKE_EP_STANDALONE) || __has_include(<glog/logging.h>)
#include <glog/logging.h>
#else

#include <cstdlib>
#include <iostream>
#include <sstream>

namespace mooncake_ep {

enum class LogSeverity {
    INFO,
    WARNING,
    ERROR,
    FATAL,
};

class LogMessage {
   public:
    LogMessage(LogSeverity severity, const char* file, int line)
        : severity_(severity) {
        stream_ << file << ":" << line << ": " << name() << ": ";
    }

    ~LogMessage() {
        stream_ << '\n';
        std::cerr << stream_.str();
        if (severity_ == LogSeverity::FATAL) std::abort();
    }

    std::ostream& stream() { return stream_; }

   private:
    const char* name() const {
        switch (severity_) {
            case LogSeverity::INFO:
                return "INFO";
            case LogSeverity::WARNING:
                return "WARNING";
            case LogSeverity::ERROR:
                return "ERROR";
            case LogSeverity::FATAL:
                return "FATAL";
        }
        return "UNKNOWN";
    }

    LogSeverity severity_;
    std::ostringstream stream_;
};

}  // namespace mooncake_ep

#define LOG(severity)                                                   \
    ::mooncake_ep::LogMessage(::mooncake_ep::LogSeverity::severity,     \
                              __FILE__, __LINE__)                       \
        .stream()

#endif
