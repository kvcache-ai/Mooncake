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
#include <async_simple/Promise.h>
#include <async_simple/Traits.h>
#include <async_simple/coro/FutureAwaiter.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <span>

#include "async_simple/coro/SyncAwait.h"
#include "io_context_pool.hpp"
#if defined(ASIO_HAS_FILE)
#include <asio/random_access_file.hpp>
#include <asio/stream_file.hpp>
#endif
#include <async_simple/coro/Lazy.h>
#include <fcntl.h>

#include <asio/error.hpp>
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "coro_io.hpp"
#if defined(ASIO_WINDOWS)
#include <io.h>
#endif

namespace coro_io {

/*
              ┌─────────────┬───────────────────────────────┐
              │fopen() mode │ open() flags                  │
              ├─────────────┼───────────────────────────────┤
              │     r       │ O_RDONLY                      │
              ├─────────────┼───────────────────────────────┤
              │     w       │ O_WRONLY | O_CREAT | O_TRUNC  │
              ├─────────────┼───────────────────────────────┤
              │     a       │ O_WRONLY | O_CREAT | O_APPEND │
              ├─────────────┼───────────────────────────────┤
              │     r+      │ O_RDWR                        │
              ├─────────────┼───────────────────────────────┤
              │     w+      │ O_RDWR | O_CREAT | O_TRUNC    │
              ├─────────────┼───────────────────────────────┤
              │     a+      │ O_RDWR | O_CREAT | O_APPEND   │
              └─────────────┴───────────────────────────────┘
*/
enum flags {
#if defined(ASIO_WINDOWS)
  read_only = 1,
  write_only = 2,
  read_write = 4,
  append = 8,
  create = 16,
  exclusive = 32,
  truncate = 64,
  create_write = create | write_only,
  create_write_trunc = create | write_only | truncate,
  create_read_write_trunc = read_write | create | truncate,
  create_read_write_append = read_write | create | append,
  sync_all_on_write = 128
#else   // defined(ASIO_WINDOWS)
  read_only = O_RDONLY,
  write_only = O_WRONLY,
  read_write = O_RDWR,
  append = O_APPEND,
  create = O_CREAT,
  exclusive = O_EXCL,
  truncate = O_TRUNC,
  create_write = O_CREAT | O_WRONLY,
  create_write_trunc = O_WRONLY | O_CREAT | O_TRUNC,
  create_read_write_trunc = O_RDWR | O_CREAT | O_TRUNC,
  create_read_write_append = O_RDWR | O_CREAT | O_APPEND,
  sync_all_on_write = O_SYNC
#endif  // defined(ASIO_WINDOWS)
};

constexpr inline flags to_flags(std::ios::ios_base::openmode mode) {
  flags access = flags::read_write;

  if (mode == std::ios::in)
    access = flags::read_only;
  else if (mode == std::ios::out)
    access = flags::write_only;
  else if (mode == std::ios::app)
    access = flags::append;
  else if (mode == std::ios::trunc)
    access = flags::truncate;
  else if (mode == (std::ios::in | std::ios::out))
    access = flags::read_write;
  else if (mode == (std::ios::trunc | std::ios::out))
    access = flags::create_write_trunc;
  if (mode == (std::ios::in | std::ios::out | std::ios::trunc))
    access = create_read_write_trunc;
  else if (mode == (std::ios::in | std::ios::out | std::ios::app))
    access = create_read_write_append;

  return access;
}

#if defined(ASIO_HAS_FILE)
template <bool seq, typename File, typename Executor>
inline bool open_native_async_file(File &file, Executor &executor,
                                   std::string_view filepath,
                                   flags open_flags) {
  if (file && file->is_open()) {
    return true;
  }

  try {
    if constexpr (seq) {
      file = std::make_shared<asio::stream_file>(
          executor.get_asio_executor(), std::string(filepath),
          static_cast<asio::file_base::flags>(open_flags));
    }
    else {
      file = std::make_shared<asio::random_access_file>(
          executor.get_asio_executor(), std::string(filepath),
          static_cast<asio::file_base::flags>(open_flags));
    }
  } catch (std::exception &ex) {
    ELOG_INFO << "line " << __LINE__ << " coro_file open failed" << ex.what()
              << "\n";
    return false;
  }

  return true;
}
#endif

enum class execution_type { none, native_async, thread_pool };

template <execution_type execute_type = execution_type::native_async>
class basic_seq_coro_file {
 public:
  basic_seq_coro_file(coro_io::ExecutorWrapper<> *executor =
                          coro_io::get_global_block_executor())
      : basic_seq_coro_file(executor->get_asio_executor()) {}

  basic_seq_coro_file(asio::io_context::executor_type executor)
      : executor_wrapper_(executor) {}

  basic_seq_coro_file(std::string_view filepath,
                      std::ios::ios_base::openmode open_flags,
                      coro_io::ExecutorWrapper<> *executor =
                          coro_io::get_global_block_executor())
      : basic_seq_coro_file(filepath, open_flags,
                            executor->get_asio_executor()) {}

  basic_seq_coro_file(std::string_view filepath,
                      std::ios::ios_base::openmode open_flags,
                      asio::io_context::executor_type executor)
      : executor_wrapper_(executor) {
    open(filepath, open_flags);
  }

  bool open(std::string_view filepath,
            std::ios::ios_base::openmode open_flags) {
    file_path_ = std::string{filepath};
    if constexpr (execute_type == execution_type::thread_pool) {
      return open_stream_file_in_pool(open_flags);
    }
    else {
#if defined(ASIO_HAS_FILE)
      return open_native_async_file<true>(async_seq_file_, executor_wrapper_,
                                          filepath, to_flags(open_flags));
#else
      return open_stream_file_in_pool(open_flags);
#endif
    }
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read(
      char *buf, size_t size) {
    if constexpr (execute_type == execution_type::thread_pool) {
      co_return co_await async_read_write({buf, size});
    }
    else {
#if defined(ASIO_HAS_FILE)
      if (async_seq_file_ == nullptr) {
        co_return std::make_pair(
            std::make_error_code(std::errc::invalid_argument), 0);
      }
      auto [ec, read_size] = co_await coro_io::async_read(
          *async_seq_file_, asio::buffer(buf, size));
      if (ec == asio::error::eof) {
        eof_ = true;
        co_return std::make_pair(std::error_code{}, read_size);
      }

      co_return std::make_pair(ec, read_size);
#else
      co_return co_await async_read_write({buf, size});
#endif
    }
  }

  template <bool is_read = true>
  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read_write(
      std::span<char> buf) {
    auto result = co_await coro_io::post(
        [this, buf]() -> std::pair<std::error_code, size_t> {
          if constexpr (is_read) {
            if (frw_seq_file_.read(buf.data(), buf.size())) {
              return std::make_pair(std::error_code{}, frw_seq_file_.gcount());
            }
          }
          else {
            if (frw_seq_file_.write(buf.data(), buf.size())) {
              return std::make_pair(std::error_code{}, buf.size());
            }
          }

          if (frw_seq_file_.eof()) {
            eof_ = true;
            return std::make_pair(std::error_code{}, frw_seq_file_.gcount());
          }

          return std::make_pair(std::make_error_code(std::errc::io_error), 0);
        },
        &executor_wrapper_);

    co_return result.value();
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_write(
      std::string_view buf) {
    if constexpr (execute_type == execution_type::thread_pool) {
      co_return co_await async_read_write<false>(
          std::span(const_cast<char *>(buf.data()), buf.size()));
    }
    else {
#if defined(ASIO_HAS_FILE)
      if (async_seq_file_ == nullptr) {
        co_return std::make_pair(
            std::make_error_code(std::errc::invalid_argument), 0);
      }
      auto [ec, size] =
          co_await coro_io::async_write(*async_seq_file_, asio::buffer(buf));
      co_return std::make_pair(ec, size);
#else
      co_return co_await async_read_write<false>(
          std::span(const_cast<char *>(buf.data()), buf.size()));
#endif
    }
  }

#if defined(ASIO_HAS_FILE)
  std::shared_ptr<asio::stream_file> get_async_stream_file() {
    return async_seq_file_;
  }
#endif

  std::fstream &get_stream_file() { return frw_seq_file_; }

  bool is_open() {
#if defined(ASIO_HAS_FILE)
    if (async_seq_file_ && async_seq_file_->is_open()) {
      return true;
    }
#endif
    return frw_seq_file_.is_open();
  }

  bool eof() { return eof_; }

  void close() {
#if defined(ASIO_HAS_FILE)
    if (async_seq_file_ && async_seq_file_->is_open()) {
      std::error_code ec;
      async_seq_file_->close(ec);
    }
#endif
    if (frw_seq_file_.is_open()) {
      frw_seq_file_.close();
    }
  }

  bool seek(size_t offset, std::ios_base::seekdir dir) {
#if defined(ASIO_HAS_FILE)
    if (async_seq_file_ && async_seq_file_->is_open()) {
      int whence = SEEK_SET;
      if (dir == std::ios_base::cur)
        whence = SEEK_CUR;
      else if (dir == std::ios_base::end)
        whence = SEEK_END;

      std::error_code seek_ec;
      async_seq_file_->seek(
          offset, static_cast<asio::file_base::seek_basis>(whence), seek_ec);
      if (seek_ec) {
        return false;
      }
      return true;
    }
#endif
    if (frw_seq_file_.is_open()) {
      if (frw_seq_file_.seekg(offset, dir)) {
        return true;
      }
    }

    return false;
  }

  execution_type get_execution_type() {
#if defined(ASIO_HAS_FILE)
    if (async_seq_file_ && async_seq_file_->is_open()) {
      return execution_type::native_async;
    }
#endif
    if (frw_seq_file_.is_open()) {
      return execution_type::thread_pool;
    }

    return execution_type::none;
  }

  size_t file_size(std::error_code ec) const noexcept {
    return std::filesystem::file_size(file_path_, ec);
  }

  size_t file_size() const { return std::filesystem::file_size(file_path_); }

  std::string_view file_path() const { return file_path_; }

 private:
  bool open_stream_file_in_pool(std::ios::ios_base::openmode flags) {
    if (frw_seq_file_.is_open()) {
      return true;
    }
    auto coro_func = coro_io::post(
        [this, flags]() mutable {
          frw_seq_file_.open(file_path_, flags);
          if (!frw_seq_file_.is_open()) {
            ELOG_INFO << "line " << __LINE__ << " coro_file open failed "
                      << file_path_ << "\n";
            std::cerr << "Error: " << strerror(errno);
            return false;
          }
          return true;
        },
        &executor_wrapper_);
    auto result = async_simple::coro::syncAwait(coro_func);
    return result.value();
  }

  coro_io::ExecutorWrapper<> executor_wrapper_;
#if defined(ASIO_HAS_FILE)
  std::shared_ptr<asio::stream_file> async_seq_file_;  // seq
#endif
  std::fstream frw_seq_file_;  // fread/fwrite seq file
  std::string file_path_;
  bool eof_ = false;
};

using coro_file = basic_seq_coro_file<>;

template <execution_type execute_type = execution_type::native_async>
class basic_random_coro_file {
 public:
  basic_random_coro_file(coro_io::ExecutorWrapper<> *executor =
                             coro_io::get_global_block_executor())
      : basic_random_coro_file(executor->get_asio_executor()) {}

  basic_random_coro_file(asio::io_context::executor_type executor)
      : executor_wrapper_(executor) {}

  basic_random_coro_file(std::string_view filepath,
                         std::ios::ios_base::openmode open_flags,
                         coro_io::ExecutorWrapper<> *executor =
                             coro_io::get_global_block_executor())
      : basic_random_coro_file(filepath, open_flags,
                               executor->get_asio_executor()) {}

  basic_random_coro_file(std::string_view filepath,
                         std::ios::ios_base::openmode open_flags,
                         asio::io_context::executor_type executor)
      : executor_wrapper_(executor) {
    open(filepath, open_flags);
  }

  bool open(std::string_view filepath,
            std::ios::ios_base::openmode open_flags) {
    file_path_ = std::string{filepath};
    if constexpr (execute_type == execution_type::thread_pool) {
      return open_fd(filepath, to_flags(open_flags));
    }
    else {
#if defined(ASIO_HAS_FILE)
      return open_native_async_file<false>(async_random_file_,
                                           executor_wrapper_, filepath,
                                           to_flags(open_flags));
#else
      return open_fd(filepath, to_flags(open_flags));
#endif
    }
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read_at(
      uint64_t offset, char *buf, size_t size) {
    if constexpr (execute_type == execution_type::thread_pool) {
      co_return co_await async_pread(offset, buf, size);
    }
    else {
#if defined(ASIO_HAS_FILE)
      if (async_random_file_ == nullptr) {
        co_return std::make_pair(
            std::make_error_code(std::errc::invalid_argument), 0);
      }
      auto [ec, read_size] = co_await coro_io::async_read_at(
          offset, *async_random_file_, asio::buffer(buf, size));

      if (ec == asio::error::eof) {
        eof_ = true;
        co_return std::make_pair(std::error_code{}, read_size);
      }

      co_return std::make_pair(ec, read_size);
#else
      co_return co_await async_pread(offset, buf, size);
#endif
    }
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_write_at(
      uint64_t offset, std::string_view buf) {
    if constexpr (execute_type == execution_type::thread_pool) {
      co_return co_await async_pwrite(offset, buf.data(), buf.size());
    }
    else {
#if defined(ASIO_HAS_FILE)
      if (async_random_file_ == nullptr) {
        co_return std::make_pair(
            std::make_error_code(std::errc::invalid_argument), 0);
      }
      auto [ec, write_size] = co_await coro_io::async_write_at(
          offset, *async_random_file_, asio::buffer(buf));

      co_return std::make_pair(ec, write_size);
#else
      co_return co_await async_pwrite(offset, buf.data(), buf.size());
#endif
    }
  }

#if defined(ASIO_HAS_FILE)
  std::shared_ptr<asio::random_access_file> get_async_stream_file() {
    return async_random_file_;
  }
#endif

  std::shared_ptr<int> get_pread_file() { return prw_random_file_; }

  bool is_open() {
#if defined(ASIO_HAS_FILE)
    if (async_random_file_ && async_random_file_->is_open()) {
      return true;
    }
#endif
    return prw_random_file_ != nullptr;
  }

  bool eof() { return eof_; }

  execution_type get_execution_type() {
#if defined(ASIO_HAS_FILE)
    if (async_random_file_ && async_random_file_->is_open()) {
      return execution_type::native_async;
    }
#endif
    if (prw_random_file_ != nullptr) {
      return execution_type::thread_pool;
    }

    return execution_type::none;
  }

  void close() {
#if defined(ASIO_HAS_FILE)
    std::error_code ec;
    if (async_random_file_) {
      async_random_file_->close(ec);
    }
#endif
    prw_random_file_ = nullptr;
  }

  size_t file_size(std::error_code ec) const noexcept {
    return std::filesystem::file_size(file_path_, ec);
  }

  size_t file_size() const { return std::filesystem::file_size(file_path_); }

  std::string_view file_path() const { return file_path_; }

 private:
  bool open_fd(std::string_view filepath, int open_flags) {
    if (prw_random_file_) {
      return true;
    }

#if defined(ASIO_WINDOWS)
    int fd = _open(filepath.data(), adjust_flags(open_flags));
#else
    int fd = ::open(filepath.data(), open_flags);
#endif
    if (fd < 0) {
      return false;
    }

    prw_random_file_ = std::shared_ptr<int>(new int(fd), [](int *ptr) {
#if defined(ASIO_WINDOWS)
      _close(*ptr);
#else
      ::close(*ptr);
#endif
      delete ptr;
    });
    return true;
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_pread(
      size_t offset, char *data, size_t size) {
#if defined(ASIO_WINDOWS)
    auto pread = [](int fd, void *buf, uint64_t count,
                    uint64_t offset) -> int64_t {
      DWORD bytes_read = 0;
      OVERLAPPED overlapped;
      memset(&overlapped, 0, sizeof(OVERLAPPED));
      overlapped.Offset = offset & 0xFFFFFFFF;
      overlapped.OffsetHigh = (offset >> 32) & 0xFFFFFFFF;

      BOOL ok = ReadFile(reinterpret_cast<HANDLE>(_get_osfhandle(fd)), buf,
                         count, &bytes_read, &overlapped);
      if (!ok && (errno = GetLastError()) != ERROR_HANDLE_EOF) {
        return -1;
      }

      return bytes_read;
    };
#endif
    co_return co_await async_prw(pread, true, offset, data, size);
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_pwrite(
      size_t offset, const char *data, size_t size) {
#if defined(ASIO_WINDOWS)
    auto pwrite = [](int fd, const void *buf, uint64_t count,
                     uint64_t offset) -> int64_t {
      DWORD bytes_write = 0;
      OVERLAPPED overlapped;
      memset(&overlapped, 0, sizeof(OVERLAPPED));
      overlapped.Offset = offset & 0xFFFFFFFF;
      overlapped.OffsetHigh = (offset >> 32) & 0xFFFFFFFF;

      BOOL ok = WriteFile(reinterpret_cast<HANDLE>(_get_osfhandle(fd)), buf,
                          count, &bytes_write, &overlapped);
      if (!ok) {
        return -1;
      }

      return bytes_write;
    };
#endif
    co_return co_await async_prw(pwrite, false, offset, (char *)data, size);
  }

  async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_prw(
      auto io_func, bool is_read, size_t offset, char *buf, size_t size) {
    std::function<int()> func = [=, this] {
      int fd = *prw_random_file_;
      return io_func(fd, buf, size, offset);
    };

    std::error_code ec{};
    size_t op_size = 0;

    auto len_val = co_await coro_io::post(std::move(func), &executor_wrapper_);
    int len = len_val.value();
    if (len == 0) {
      if (is_read) {
        eof_ = true;
      }
    }
    else if (len > 0) {
      op_size = len;
    }
    else {
      ec = std::make_error_code(std::errc::io_error);
      op_size = len;
    }

    co_return std::make_pair(ec, op_size);
  }

#if defined(ASIO_WINDOWS)
  static int adjust_flags(int open_mode) {
    switch (open_mode) {
      case flags::read_only:
        return _O_RDONLY;
      case flags::write_only:
        return _O_WRONLY;
      case flags::read_write:
        return _O_RDWR;
      case flags::append:
        return _O_APPEND;
      case flags::create:
        return _O_CREAT;
      case flags::exclusive:
        return _O_EXCL;
      case flags::truncate:
        return _O_TRUNC;
      case flags::create_write:
        return _O_CREAT | _O_WRONLY;
      case flags::create_write_trunc:
        return _O_CREAT | _O_WRONLY | _O_TRUNC;
      case flags::create_read_write_trunc:
        return _O_RDWR | _O_CREAT | _O_TRUNC;
      case flags::create_read_write_append:
        return _O_RDWR | _O_CREAT | _O_APPEND;
      case flags::sync_all_on_write:
      default:
        return open_mode;
        break;
    }
    return open_mode;
  }
#endif

  coro_io::ExecutorWrapper<> executor_wrapper_;
#if defined(ASIO_HAS_FILE)
  std::shared_ptr<asio::random_access_file> async_random_file_;  // random file
#endif
  std::shared_ptr<int> prw_random_file_ = nullptr;  // pread/pwrite random file
  std::string file_path_;
  bool eof_ = false;
};

using random_coro_file = basic_random_coro_file<>;
}  // namespace coro_io
