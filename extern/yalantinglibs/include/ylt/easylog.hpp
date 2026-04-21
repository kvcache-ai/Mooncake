/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include <string_view>
#include <utility>
#include <vector>

#if __has_include(<format>)
#include <format>
#endif

#if __has_include(<fmt/format.h>)
#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY
#include <fmt/format.h>
#endif
#endif

#if __has_include(<stacktrace>) && __cplusplus >= 202302L
#include <stacktrace>
#endif

#include <ylt/util/b_stacktrace.h>

#include "easylog/appender.hpp"

namespace easylog {

template <size_t Id = 0>
class logger {
 public:
  static logger<Id> &instance() {
    static logger<Id> instance;
    return instance;
  }

  void operator+=(record_t &record) {
    if (has_destruct_) [[unlikely]] {
      return;
    }

    write(record);
  }

  void apply_extra_appenders(record_t &record) {
    if (!appenders_.empty()) [[unlikely]] {
      for (auto &app : appenders_) {
        app(record);
      }
    }
  }

  void write(record_t &record) {
    if (async_ && appender_) {
      apply_extra_appenders(record);
      append_record(std::move(record));
    }
    else {
      apply_extra_appenders(record);
      append_format(record);
    }

    if (record.get_severity() == Severity::CRITICAL) {
#if __cpp_lib_stacktrace && __cplusplus >= 202302L
      std::cerr << std::stacktrace::current() << std::endl;
#else
      auto str = ylt::util::b_stacktrace_get_string();
      std::cerr << str << std::endl;
      free(str);
#endif
      flush();
      std::exit(EXIT_FAILURE);
    }
  }

  void flush() {
    if (appender_) {
      appender_->flush();
    }
  }

  void init(Severity min_severity, bool async, bool enable_console,
            const std::string &filename, size_t max_file_size, size_t max_files,
            bool flush_every_time,
            std::chrono::milliseconds log_sample_interval = {},
            std::chrono::milliseconds log_sample_duartion = {}) {
    static appender appender(filename, async, enable_console, max_file_size,
                             max_files, flush_every_time);
    async_ = async;
    appender_ = &appender;
    min_severity_ = min_severity;
    enable_console_ = enable_console;
    log_sample_interval_ = log_sample_interval;
    log_sample_duration_ = log_sample_duartion;
  }

  bool check_severity(Severity severity) {
    return severity >= min_severity_.load(std::memory_order::relaxed);
  }

  bool check_tm(std::chrono::system_clock::time_point tm) {
    if (log_sample_interval_.load(std::memory_order::relaxed).count() <= 0) {
      return true;
    }
    auto duration = (tm - init_time_) / std::chrono::milliseconds{1};
    return (duration %
                log_sample_interval_.load(std::memory_order::relaxed).count() <
            log_sample_duration_.load(std::memory_order::relaxed).count());
  }

  void add_appender(std::function<void(record_t &)> fn) {
    appenders_.emplace_back(std::move(fn));
  }

  void stop_async_log() { appender_->stop(); }

  // set and get
  void set_min_severity(Severity severity) { min_severity_ = severity; }
  Severity get_min_severity() {
    return min_severity_.load(std::memory_order::relaxed);
  }

  void set_sample_duration(std::chrono::milliseconds ms) noexcept {
    log_sample_duration_.store(ms, std::memory_order_relaxed);
  }
  void set_sample_interval(std::chrono::milliseconds ms) noexcept {
    log_sample_interval_.store(ms, std::memory_order_relaxed);
  }

  void set_console(bool enable) {
    enable_console_ = enable;
    if (appender_) {
      appender_->enable_console(enable);
    }
  }
  bool get_console() { return enable_console_; }

  void set_async(bool enable) { async_ = enable; }
  bool get_async() { return async_; }
  ~logger() { has_destruct_ = true; }

 private:
  logger() {
    static appender appender{};
    appender.start_thread();
    appender.enable_console(true);
    async_ = true;
    appender_ = &appender;
  }

  logger(const logger &) = default;

  void append_record(record_t record) { appender_->write(std::move(record)); }

  void append_format(record_t &record) {
    if (appender_) {
      if (enable_console_) {
        appender_->write_record<true, true>(record);
      }
      else {
        appender_->write_record<true, false>(record);
      }
    }
  }

  std::atomic<Severity> min_severity_ =
#if NDEBUG
      Severity::WARN;
#else
      Severity::TRACE;
#endif
  bool async_ = false;
  bool enable_console_ = true;
  std::atomic<std::chrono::milliseconds> log_sample_interval_;
  std::atomic<std::chrono::milliseconds> log_sample_duration_;
  std::chrono::system_clock::time_point init_time_{};
  appender *appender_ = nullptr;
  std::vector<std::function<void(record_t &record)>> appenders_;
  inline static std::atomic<bool> has_destruct_ = false;
};

template <size_t Id = 0>
inline void init_log(Severity min_severity, const std::string &filename = "",
                     bool async = true, bool enable_console = true,
                     size_t max_file_size = 0, size_t max_files = 0,
                     bool flush_every_time = false) {
  logger<Id>::instance().init(min_severity, async, enable_console, filename,
                              max_file_size, max_files, flush_every_time);
}

template <size_t Id = 0>
inline void set_min_severity(Severity severity) {
  logger<Id>::instance().set_min_severity(severity);
}

template <size_t Id = 0>
inline Severity get_min_severity() {
  return logger<Id>::instance().get_min_severity();
}

template <size_t Id = 0>
inline void set_sample_duration(std::chrono::milliseconds ms) {
  logger<Id>::instance().set_sample_duration(ms);
}

template <size_t Id = 0>
inline Severity add_appenders(std::function<void(record_t &record)> app) {
  return logger<Id>::instance().add_appender(std::move(app));
}

template <size_t Id = 0>
inline void set_sample_interval(std::chrono::milliseconds ms) {
  logger<Id>::instance().set_sample_interval(ms);
}

template <size_t Id = 0>
inline void set_console(bool enable) {
  logger<Id>::instance().set_console(enable);
}

template <size_t Id = 0>
inline bool get_console() {
  return logger<Id>::instance().get_console();
}

template <size_t Id = 0>
inline void set_async(bool enable) {
  logger<Id>::instance().set_async(enable);
}

template <size_t Id = 0>
inline bool get_async() {
  return logger<Id>::instance().get_async();
}

template <size_t Id = 0>
inline void flush() {
  logger<Id>::instance().flush();
}

template <size_t Id = 0>
inline void stop_async_log() {
  logger<Id>::instance().stop_async_log();
}

template <size_t Id = 0>
inline void add_appender(std::function<void(record_t &)> fn) {
  logger<Id>::instance().add_appender(std::move(fn));
}
}  // namespace easylog

#define ELOG_IMPL(severity, Id, ...)                               \
  if (!easylog::logger<Id>::instance().check_severity(severity)) { \
    ;                                                              \
  }                                                                \
  else if (auto tm = std::chrono::system_clock::now();             \
           easylog::logger<Id>::instance().check_tm(tm))           \
  easylog::logger<Id>::instance() +=                               \
      easylog::record_t(tm, severity, GET_STRING(__FILE__, __LINE__)).ref()

#ifndef ELOG
#define ELOG(severity, ...) \
  ELOG_IMPL(easylog::Severity::severity, __VA_ARGS__, 0)
#endif

#define ELOGV_IMPL(severity, Id, fmt, ...)                              \
  if (!easylog::logger<Id>::instance().check_severity(severity)) {      \
    ;                                                                   \
  }                                                                     \
  else if (auto tm = std::chrono::system_clock::now();                  \
           easylog::logger<Id>::instance().check_tm(tm)) {              \
    easylog::logger<Id>::instance() +=                                  \
        easylog::record_t(tm, severity, GET_STRING(__FILE__, __LINE__)) \
            .sprintf(fmt, __VA_ARGS__);                                 \
    if constexpr (severity == easylog::Severity::CRITICAL) {            \
      easylog::flush<Id>();                                             \
      std::exit(EXIT_FAILURE);                                          \
    }                                                                   \
  }

#ifndef ELOGV
#define ELOGV(severity, ...) \
  ELOGV_IMPL(easylog::Severity::severity, 0, __VA_ARGS__, "\n")
#endif

#ifndef MELOGV
#define MELOGV(severity, Id, ...) \
  ELOGV_IMPL(easylog::Severity::severity, Id, __VA_ARGS__, "\n")
#endif

#if __has_include(<fmt/format.h>) || __has_include(<format>)

#define ELOGFMT_IMPL0(severity, Id, prefix, ...)                        \
  if (!easylog::logger<Id>::instance().check_severity(severity)) {      \
    ;                                                                   \
  }                                                                     \
  else if (auto tm = std::chrono::system_clock::now();                  \
           easylog::logger<Id>::instance().check_tm(tm)) {              \
    easylog::logger<Id>::instance() +=                                  \
        easylog::record_t(tm, severity, GET_STRING(__FILE__, __LINE__)) \
            .format(prefix::format(__VA_ARGS__));                       \
    if constexpr (severity == easylog::Severity::CRITICAL) {            \
      easylog::flush<Id>();                                             \
      std::exit(EXIT_FAILURE);                                          \
    }                                                                   \
  }

#if __has_include(<fmt/format.h>)
#define ELOGFMT_IMPL(severity, Id, ...) \
  ELOGFMT_IMPL0(severity, Id, fmt, __VA_ARGS__)
#else
#define ELOGFMT_IMPL(severity, Id, ...) \
  ELOGFMT_IMPL0(severity, Id, std, __VA_ARGS__)
#endif

#ifndef ELOGFMT
#define ELOGFMT(severity, ...) \
  ELOGFMT_IMPL(easylog::Severity::severity, 0, __VA_ARGS__)
#endif

#ifndef MELOGFMT
#define MELOGFMT(severity, Id, ...) \
  ELOGFMT_IMPL(easylog::Severity::severity, Id, __VA_ARGS__)
#endif
#endif

#ifndef ELOG_TRACE
#define ELOG_TRACE ELOG(TRACE)
#endif
#ifndef ELOG_DEBUG
#define ELOG_DEBUG ELOG(DEBUG)
#endif
#ifndef ELOG_INFO
#define ELOG_INFO ELOG(INFO)
#endif
#ifndef ELOG_WARN
#define ELOG_WARN ELOG(WARN)
#endif
#ifndef ELOG_ERROR
#define ELOG_ERROR ELOG(ERROR)
#endif
#ifndef ELOG_CRITICAL
#define ELOG_CRITICAL ELOG(CRITICAL)
#endif
#ifndef ELOG_FATAL
#define ELOG_FATAL ELOG(FATAL)
#endif

#ifndef MELOG_TRACE
#define MELOG_TRACE(id) ELOG(TRACE, id)
#endif
#ifndef MELOG_DEBUG
#define MELOG_DEBUG(id) ELOG(DEBUG, id)
#endif
#ifndef MELOG_INFO
#define MELOG_INFO(id) ELOG(INFO, id)
#endif
#ifndef MELOG_WARN
#define MELOG_WARN(id) ELOG(WARN, id)
#endif
#ifndef MELOG_ERROR
#define MELOG_ERROR(id) ELOG(ERROR, id)
#endif
#ifndef MELOG_FATAL
#define MELOG_FATAL(id) ELOG(FATAL, id)
#endif

#ifndef ELOGT
#define ELOGT ELOG_TRACE
#endif
#ifndef ELOGD
#define ELOGD ELOG_DEBUG
#endif
#ifndef ELOGI
#define ELOGI ELOG_INFO
#endif
#ifndef ELOGW
#define ELOGW ELOG_WARN
#endif
#ifndef ELOGE
#define ELOGE ELOG_ERROR
#endif
#ifndef ELOGC
#define ELOGC ELOG_CRITICAL
#endif
#ifndef ELOGF
#define ELOGF ELOG_FATAL
#endif
