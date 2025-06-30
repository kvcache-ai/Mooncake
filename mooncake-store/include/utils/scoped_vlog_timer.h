#pragma once

#include <glog/logging.h>

#include <chrono>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include "types.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake {

/**
 * @brief RAII-style timer class for VLOG logging with request/response timing
 *
 * Usage example:
 *   ScopedVLogTimer timer(1, "GetReplicaList");
 *   timer.LogRequest("key=", key);
 *   // ... do work ...
 *   timer.LogResponse("replica_list=", replica_list);
 */
class ScopedVLogTimer {
   public:
    ScopedVLogTimer(int level, std::string_view function_name)
        : level_(level),
          function_name_(function_name),
          active_(VLOG_IS_ON(level_)) {
        if (active_) {
            start_time_ = std::chrono::steady_clock::now();
        }
    }

    // Call this *after* constructing the ScopedVLogTimer
    template <typename... Args>
    void LogRequest(Args&&... args) {
        if (active_) {
            std::ostringstream oss;
            (oss << ... << std::forward<Args>(args));
            VLOG(level_) << function_name_ << " request: " << oss.str();
        }
    }

    // Call this *before* the ScopedVLogTimer goes out of scope
    // For non-serializable types
    template <typename... Args>
    void LogResponse(Args&&... args) {
        if (active_) {
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time_);

            std::ostringstream oss;
            (oss << ... << std::forward<Args>(args));

            VLOG(level_) << function_name_ << " response: " << oss.str()
                         << ", latency=" << latency.count() << "us";
            logged_response_ = true;
        }
    }

    // Lazy evaluation version to avoid computing expensive arguments when
    // logging is disabled
    template <typename Func, typename... Args>
    void LogResponseLazy(Func&& func, Args&&... args) {
        if (active_) {
            auto result = func();
            LogResponse(std::forward<Args>(args)..., result);
        }
    }

    // Specialized method for logging tl::expected types efficiently
    template <typename T, typename... Args>
    void LogResponseExpected(const tl::expected<T, ErrorCode>& expected,
                             Args&&... args) {
        if (active_) {
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time_);

            std::ostringstream oss;
            (oss << ... << std::forward<Args>(args));

            if (expected.has_value()) {
                oss << "success, resp=";
                if constexpr (iguana::ylt_refletable_v<T>) {
                    struct_json::to_json(expected.value(), oss);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    oss << expected.value();
                } else if constexpr (std::is_same_v<T,
                                                    std::vector<std::string>>) {
                    oss << "[";
                    for (const auto& item : expected.value()) {
                        oss << item << ",";
                    }
                    oss << "]";
                } else if constexpr (std::is_same_v<T, void>) {
                    // For tl::expected<void, ErrorCode>
                    oss << "void";
                }
            } else {
                oss << "error=" << toString(expected.error());
            }

            VLOG(level_) << function_name_ << " response: " << oss.str()
                         << ", latency=" << latency.count() << "us";
            logged_response_ = true;
        }
    }

    // For serializable types
    template <typename T>
    void LogResponseJson(const T& obj) {
        if (active_) {
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time_);

            std::string json;
            struct_json::to_json(obj, json);

            VLOG(level_) << function_name_ << " response: " << json
                         << ", latency=" << latency.count() << "us";
            logged_response_ = true;
        }
    }

    // Destructor logs *only* latency if LogResponse wasn't called
    ~ScopedVLogTimer() {
        if (active_ && !logged_response_) {
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time_);
            VLOG(level_) << function_name_
                         << " finished, latency=" << latency.count() << "us";
        }
    }

   private:
    int level_;
    std::string_view function_name_;
    std::chrono::steady_clock::time_point start_time_;
    bool active_;
    bool logged_response_ = false;
};

}  // namespace mooncake
