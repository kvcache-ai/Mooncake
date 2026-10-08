// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>
#include <vector>
#include "transfer_engine.h"
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

struct GatherReadRange {
    uint64_t offset;  // Absolute address in a mounted CPU Store segment.
    uint64_t length;
};

// One owner and one contiguous destination run, possibly spanning keys.
struct GatherReadPlan {
    std::string endpoint;
    std::vector<size_t> transfers;
    std::vector<GatherReadRange> ranges;
    void* destination = nullptr;
    size_t bytes = 0;
};
std::vector<GatherReadPlan> PlanGatherReads(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers,
    const std::vector<std::string>& endpoints);

enum class GatherReadCompletion : uint8_t {
    Completed,
    Rejected,
    FailedDrained,
    Unknown,
    Pending,
    SessionExpired,
};

struct GatherReadResult {
    GatherReadCompletion completion = GatherReadCompletion::Rejected;
    std::string error;
    uint64_t bytes = 0;
    bool ok() const { return completion == GatherReadCompletion::Completed; }
    bool rejected() const {
        return completion == GatherReadCompletion::Rejected ||
               completion == GatherReadCompletion::SessionExpired;
    }
    bool drained() const {
        return ok() || rejected() ||
               completion == GatherReadCompletion::FailedDrained;
    }
};

struct GatherReadOptions {
    size_t workers = 8;
    size_t chunk_bytes = 512 * 1024;
    size_t pipeline_depth = 2;
    size_t max_ranges = 131072;
    size_t max_bytes = 512 * 1024 * 1024;
};

// Store-owned service for mounted CPU segments. removeSource waits for active
// packing/transfers before the caller can release the backing allocation.
// The shared TE must outlive the service and must not be reconfigured
// meanwhile. Uses a separate RPC endpoint; does not alter TE handshakes or
// scatter.
class GatherReadService {
   public:
    GatherReadService(std::shared_ptr<TransferEngine> engine,
                      GatherReadOptions options = {},
                      const std::string& owner_name = "");
    ~GatherReadService();
    GatherReadService(const GatherReadService&) = delete;
    GatherReadService& operator=(const GatherReadService&) = delete;
    // Port zero selects an available port. Publish this endpoint separately
    // from the ordinary TE endpoint. The service is for trusted cluster peers.
    void start(const std::string& listen_address, uint16_t port = 0);
    uint16_t port() const;
    void addSource(void* base, size_t bytes);
    void removeSource(void* base);

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    friend class GatherReadClient;
};

class GatherReadOperation {
   public:
    GatherReadResult wait() const;
    // Pending means no cancellation: the destination must remain live.
    GatherReadResult waitFor(std::chrono::milliseconds timeout) const;

   private:
    struct State;
    explicit GatherReadOperation(std::shared_ptr<State> state);
    std::shared_ptr<State> state_;
    friend class GatherReadClient;
};

// One persistent RPC connection and at most one in-flight operation per client.
// Independent clients may run concurrently, sharing the same TransferEngine.
// No automatic retry or fallback after submission. Unknown means the control
// connection failed. The implementation reconnects and fences the session
// before reporting a terminal result; it never retries a data write.
class GatherReadClient {
   public:
    GatherReadClient(
        std::shared_ptr<TransferEngine> engine,
        const std::string& service_endpoint,
        std::chrono::milliseconds rpc_timeout = std::chrono::seconds(60),
        const std::string& expected_owner = "");
    ~GatherReadClient();
    bool available() const;
    bool retired() const;
    // Concatenate ranges in their supplied order, including duplicates.
    // destination must be registered with remote_accessible=true in engine.
    // destination_lifetime must keep both allocation and registration alive;
    // it is held by the operation, including after Unknown completion.
    // GPU consumers must establish GPUDirect RDMA visibility before use.
    GatherReadOperation submitGatherRead(
        const std::vector<GatherReadRange>& ranges, void* destination,
        size_t destination_capacity,
        std::shared_ptr<void> destination_lifetime);

   private:
    class Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace mooncake::store
