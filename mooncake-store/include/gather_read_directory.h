// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include "types.h"

namespace mooncake::store {
// Ephemeral service discovery only: never part of object/segment metadata or
// Master snapshots. Owners refresh through their existing heartbeat loop.
class GatherReadDirectory {
   public:
    explicit GatherReadDirectory(
        std::chrono::seconds ttl = std::chrono::seconds(60))
        : ttl_(ttl) {}
    tl::expected<void, ErrorCode> Publish(const std::string& owner,
                                          const std::string& endpoint,
                                          bool remove) {
        if (owner.empty() || endpoint.empty() || owner.size() > 1024 ||
            endpoint.size() > 1024)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        std::lock_guard lock(mutex_);
        auto now = std::chrono::steady_clock::now();
        if (remove) {
            auto it = entries_.find(owner);
            if (it != entries_.end() && it->second.endpoint == endpoint)
                entries_.erase(it);
            return {};
        }
        // Bound abandoned entries without another background thread.
        if (entries_.size() >= 65536) {
            std::erase_if(entries_, [now](const auto& e) {
                return e.second.expires <= now;
            });
            if (entries_.size() >= 65536 && !entries_.contains(owner))
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        entries_[owner] = {endpoint, now + ttl_};
        return {};
    }
    tl::expected<std::string, ErrorCode> Resolve(const std::string& owner) {
        std::lock_guard lock(mutex_);
        auto it = entries_.find(owner);
        if (it == entries_.end()) return std::string{};
        if (it->second.expires <= std::chrono::steady_clock::now()) {
            entries_.erase(it);
            return std::string{};
        }
        return it->second.endpoint;
    }

   private:
    struct Entry {
        std::string endpoint;
        std::chrono::steady_clock::time_point expires;
    };
    std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
    std::chrono::seconds ttl_;
};

}  // namespace mooncake::store
