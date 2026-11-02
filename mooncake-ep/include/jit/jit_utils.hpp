/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Ported from nccl-extensions: nccl_ep/device/jit/jit_utils.hpp.
 */

#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace mooncake {
namespace jit {

std::string env_value(const char* name);
bool env_flag_enabled(const char* name, bool default_value = false);
// MOONCAKE_EP_JIT_LOG, read once per process. Callers on hot paths must check
// this before building a log message so disabled logging costs nothing per
// launch.
bool jit_log_enabled();
void jit_log(std::string_view message);

// Returns true the first time each distinct `key` is seen. Thread-safe.
bool announce_once(const std::string& key);

class ScopedFileLock {
   public:
    ScopedFileLock() = default;
    ScopedFileLock(const ScopedFileLock&) = delete;
    ScopedFileLock& operator=(const ScopedFileLock&) = delete;
    ~ScopedFileLock();

    bool lock(const std::filesystem::path& path, std::string* error);

   private:
    int fd_ = -1;
};

std::vector<std::string> split_env_flags(const char* flags);
std::string json_escape(std::string_view text);
std::string read_file_or_empty(const std::filesystem::path& path);
bool write_file_atomic(const std::filesystem::path& path,
                       const std::string& data, bool binary);
std::string fnv1a_digest(const std::vector<std::string>& parts);

}  // namespace jit
}  // namespace mooncake
