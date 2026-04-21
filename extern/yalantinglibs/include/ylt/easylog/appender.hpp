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
#include <charconv>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>

#include "record.hpp"
#include "ylt/util/concurrentqueue.h"

namespace easylog {
struct empty_mutex {
  void lock() {}
  void unlock() {}
};
constexpr inline std::string_view BOM_STR = "\xEF\xBB\xBF";

constexpr char digits[10] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

template <size_t N, char c>
inline void to_int(int num, char *p, int &size) {
  for (int i = 0; i < N; i++) {
    p[--size] = digits[num % 10];
    num = num / 10;
  }

  if constexpr (N != 4)
    p[--size] = c;
}

inline std::tm localtime_safe(std::time_t timer) {
  std::tm bt{};
#if defined(__unix__)
  localtime_r(&timer, &bt);
#elif defined(_MSC_VER)
  localtime_s(&bt, &timer);
#else
  static std::mutex mtx;
  std::lock_guard<std::mutex> lock(mtx);
  bt = *std::localtime(&timer);
#endif
  return bt;
}

inline char *get_time_str(const auto &now) {
  static thread_local char buf[36];
  static thread_local std::chrono::seconds last_sec_{};

  std::chrono::system_clock::duration d = now.time_since_epoch();
  std::chrono::seconds s = std::chrono::duration_cast<std::chrono::seconds>(d);
  auto usec =
      std::chrono::duration_cast<std::chrono::microseconds>(d - s).count();
  int size = 26;
  if (last_sec_ == s) {
    to_int<6, '.'>(usec, buf, size);
    return buf;
  }

  last_sec_ = s;
  auto tm = std::chrono::system_clock::to_time_t(now);
  auto ltm = localtime_safe(tm);
  std::tm *gmt = &ltm;

  to_int<6, '.'>(usec, buf, size);
  to_int<2, ':'>(gmt->tm_sec, buf, size);
  to_int<2, ':'>(gmt->tm_min, buf, size);
  to_int<2, ' '>(gmt->tm_hour, buf, size);

  to_int<2, '-'>(gmt->tm_mday, buf, size);
  to_int<2, '-'>(gmt->tm_mon + 1, buf, size);
  to_int<4, ' '>(gmt->tm_year + 1900, buf, size);
  return buf;
}

class appender {
 public:
  appender() = default;
  appender(const std::string &filename, bool async, bool enable_console,
           size_t max_file_size, size_t max_files, bool flush_every_time)
      : has_init_(true),
        flush_every_time_(flush_every_time),
        enable_console_(enable_console),
        max_file_size_(max_file_size) {
    filename_ = filename;
    max_files_ = (std::min)(max_files, static_cast<size_t>(1000));
    open_log_file();
    if (async) {
      start_thread();
    }
  }

  void enable_console(bool b) { enable_console_ = b; }

  void start_thread() {
    write_thd_ = std::thread([this] {
      while (!stop_) {
        if (max_files_ > 0 && file_size_ > max_file_size_ &&
            static_cast<size_t>(-1) != file_size_) {
          roll_log_files();
        }

        record_t record;
        if (queue_.try_dequeue(record)) {
          enable_console_ ? write_record<false, true>(record)
                          : write_record<false, false>(record);
        }

        if (queue_.size_approx() == 0) {
          std::unique_lock lock(que_mtx_);
          cnd_.wait(lock, [&]() {
            return queue_.size_approx() > 0 || stop_;
          });
        }

        if (stop_) {
          if (queue_.size_approx() > 0) {
            while (queue_.try_dequeue(record)) {
              if (max_files_ > 0 && file_size_ > max_file_size_ &&
                  static_cast<size_t>(-1) != file_size_) {
                roll_log_files();
              }

              enable_console_ ? write_record<false, true>(record)
                              : write_record<false, false>(record);
            }
          }
        }
      }
    });
  }

  std::string_view get_tid_buf(unsigned int tid) {
    static thread_local char buf[24];
    static thread_local unsigned int last_tid;
    static thread_local size_t last_len;
    if (tid == last_tid) {
      return {buf, last_len};
    }

    buf[0] = '[';
    auto [ptr, ec] = std::to_chars(buf + 1, buf + 21, tid);
    buf[22] = ']';
    buf[23] = ' ';
    last_tid = tid;
    last_len = ptr - buf;
    buf[last_len++] = ']';
    buf[last_len++] = ' ';
    return {buf, last_len};
  }

  template <bool sync>
  auto &get_mutex() {
    if constexpr (sync) {
      return mtx_;
    }
    else {
      return empty_mtx_;
    }
  }

  template <bool sync = false, bool enable_console = false>
  void write_record(record_t &record) {
    std::unique_lock guard(get_mutex<sync>());
    if constexpr (sync == true) {
      if (max_files_ > 0 && file_size_ > max_file_size_ &&
          static_cast<size_t>(-1) != file_size_) {
        roll_log_files();
      }
    }

    auto buf = get_time_str(record.get_time_point());

    buf[26] = ' ';
    memcpy(buf + 27, severity_str(record.get_severity()).data(), 8);
    buf[35] = ' ';

    auto time_str = std::string_view(buf, 36);
    auto tid_str = get_tid_buf(record.get_tid());
    auto file_str = record.get_file_str();
    auto msg = record.get_message_inner();

    write_file(time_str);
    write_file(tid_str);
    write_file(file_str);
    write_file(msg);

    if constexpr (enable_console) {
      guard.unlock();
      std::unique_lock guard1(get_mutex<true>());
      add_color(record.get_severity());
      std::cout << time_str;
      clean_color(record.get_severity());
      std::cout << tid_str;
      std::cout << file_str;
      std::cout << msg;
      std::cout << std::flush;
    }
  }

#ifdef _WIN32
  enum class color_type : int {
    none = -1,
    black = 0,
    blue,
    green,
    cyan,
    red,
    magenta,
    yellow,
    white,
    black_bright,
    blue_bright,
    green_bright,
    cyan_bright,
    red_bright,
    magenta_bright,
    yellow_bright,
    white_bright
  };

  void windows_set_color(color_type fg, color_type bg) {
    auto handle = GetStdHandle(STD_OUTPUT_HANDLE);
    if (handle != nullptr) {
      CONSOLE_SCREEN_BUFFER_INFO info{};
      auto status = GetConsoleScreenBufferInfo(handle, &info);
      if (status) {
        WORD color = info.wAttributes;
        if (fg != color_type::none) {
          color = (color & 0xFFF0) | int(fg);
        }
        if (bg != color_type::none) {
          color = (color & 0xFF0F) | int(bg) << 4;
        }
        SetConsoleTextAttribute(handle, color);
      }
    }
  }
#endif

  void add_color(Severity severity) {
#if defined(_WIN32)
    if (severity == Severity::WARN)
      windows_set_color(color_type::black, color_type::yellow);
    if (severity == Severity::ERROR)
      windows_set_color(color_type::black, color_type::red);
    if (severity == Severity::CRITICAL)
      windows_set_color(color_type::white_bright, color_type::red);
#elif __APPLE__
#else
    if (severity == Severity::WARN)
      std::cout << "\x1B[93m";
    if (severity == Severity::ERROR)
      std::cout << "\x1B[91m";
    if (severity == Severity::CRITICAL)
      std::cout << "\x1B[97m\x1B[41m";
#endif
  }

  void clean_color(Severity severity) {
#if defined(_WIN32)
    if (severity >= Severity::WARN)
      windows_set_color(color_type::white, color_type::black);
#elif __APPLE__
#else
    if (severity >= Severity::WARN)
      std::cout << "\x1B[0m\x1B[0K";
#endif
  }

  void write(record_t &&r) {
    queue_.enqueue(std::move(r));
    cnd_.notify_one();
  }

  void flush() {
    std::lock_guard guard(mtx_);
    if (file_.is_open()) {
      file_.flush();
      file_.sync_with_stdio();
    }
  }

  void stop() {
    std::lock_guard guard(mtx_);
    if (!write_thd_.joinable()) {
      return;
    }

    if (stop_) {
      return;
    }
    stop_ = true;
    cnd_.notify_one();
  }

  ~appender() {
    stop();
    if (write_thd_.joinable())
      write_thd_.join();
  }

 private:
  void open_log_file() {
    file_size_ = 0;
    std::string filename = build_filename();

    if (std::filesystem::path(filename).has_parent_path()) {
      std::error_code ec;
      auto parant_path = std::filesystem::path(filename).parent_path();
      std::filesystem::create_directories(parant_path, ec);
      if (ec) {
        std::cout << "create directories error: " << ec.message() << std::flush;
        abort();
      }
    }

    file_.open(filename, std::ios::binary | std::ios::out | std::ios::app);
    if (file_) {
      std::error_code ec;
      size_t file_size = std::filesystem::file_size(filename, ec);
      if (ec) {
        std::cout << "get file size error" << std::flush;
        abort();
      }

      if (file_size == 0) {
        if (file_.write(BOM_STR.data(), BOM_STR.size())) {
          file_size_ += BOM_STR.size();
        }
      }
    }
  }

  std::string build_filename(int file_number = 0) {
    if (file_number == 0) {
      return filename_;
    }

    auto file_path = std::filesystem::path(filename_);
    std::string filename = file_path.stem().string();

    if (file_number > 0) {
      char buf[32];
      auto [ptr, ec] = std::to_chars(buf, buf + 32, file_number);
      filename.append(".").append(std::string_view(buf, ptr - buf));
    }

    if (file_path.has_extension()) {
      filename.append(file_path.extension().string());
    }

    if (std::filesystem::path(file_path).has_parent_path()) {
      return file_path.parent_path().append(filename).string();
    }

    return filename;
  }

  void roll_log_files() {
    file_.close();
    std::string last_filename = build_filename(max_files_ - 1);

    std::error_code ec;
    std::filesystem::remove(last_filename, ec);

    for (int file_number = max_files_ - 2; file_number >= 0; --file_number) {
      std::string current_fileName = build_filename(file_number);
      std::string next_fileName = build_filename(file_number + 1);

      std::filesystem::rename(current_fileName, next_fileName, ec);
    }

    open_log_file();
  }

  void write_file(std::string_view str) {
    if (has_init_) {
      if (file_.write(str.data(), str.size())) {
        if (flush_every_time_) {
          file_.flush();
        }

        file_size_ += str.size();
      }
    }
  }

  bool has_init_ = false;
  std::string filename_;

  std::atomic<bool> enable_console_ = false;
  bool flush_every_time_;
  size_t file_size_ = 0;
  size_t max_file_size_ = 0;
  size_t max_files_ = 0;

  std::shared_mutex mtx_;
  empty_mutex empty_mtx_;
  std::ofstream file_;

  std::mutex que_mtx_;

  ylt::detail::moodycamel::ConcurrentQueue<record_t> queue_;
  std::thread write_thd_;
  std::condition_variable cnd_;
  std::atomic<bool> stop_ = false;
};
}  // namespace easylog
