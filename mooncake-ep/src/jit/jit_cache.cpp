/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Ported from nccl-extensions: nccl_ep/device/jit/jit_cache.cc.
 */

#include <jit/jit_cache.hpp>
#include <jit/jit_utils.hpp>

namespace mooncake {
namespace jit {

JitCache& JitCache::instance() {
    static JitCache cache;
    return cache;
}

std::filesystem::path JitCache::root_dir() const {
    std::string cache_dir = env_value("MOONCAKE_EP_JIT_CACHE_DIR");
    if (!cache_dir.empty()) return std::filesystem::path(cache_dir);

    return std::filesystem::path("/tmp") / "mooncake_ep" / "jit";
}

bool JitCache::has_failed(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return failed_keys_.find(key) != failed_keys_.end();
}

void JitCache::mark_failed(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    failed_keys_.insert(key);
}

void JitCache::warn_once(const std::string& key, const std::string& message) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!warned_keys_.insert(key).second) return;
    }
    jit_log(message);
}

}  // namespace jit
}  // namespace mooncake
