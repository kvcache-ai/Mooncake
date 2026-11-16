#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "piece_put.h"

namespace mooncake {

struct PiecePutSpan {
    uint64_t offset{0};
    uint64_t length{0};
    UUID client_id{};
    uint64_t op_id{0};
    bool completed{false};
};

// Only active assembly progress lives here. Ordinary object metadata continues
// to own every Replica and allocation, and remains the authority for size,
// configuration and visibility.
struct PiecePutRound {
    uint64_t generation{0};
    std::string write_id;
    uint64_t completed_bytes{0};
    std::vector<ReplicaID> replica_ids;
    std::chrono::system_clock::time_point deadline{
        std::chrono::system_clock::time_point::max()};
    std::vector<PiecePutSpan> spans;
};

// Lock order: existing key write guard -> this short map mutex. Every method
// ending in Locked requires that guard for the same tenant/key. Returned Round
// and Span pointers must not escape its lifetime, or be retained across later
// mutations of the span vector. Only SnapshotExpired may inspect the map
// without key guards; it copies immutable deadline/identity, never progress.
class PiecePutManager {
   public:
    using TimePoint = std::chrono::system_clock::time_point;

    struct ExpiredRound {
        TenantId tenant_id;
        std::string key;
        uint64_t generation;
    };

    explicit PiecePutManager(size_t max_rounds = 1024, size_t max_spans = 128)
        : max_rounds_(max_rounds),
          max_spans_(max_spans),
          epoch_(generate_uuid()) {}

    const UUID& epoch() const noexcept { return epoch_; }
    size_t size() const noexcept {
        return active_.load(std::memory_order_acquire);
    }
    bool empty() const noexcept { return size() == 0; }

    PiecePutRound* FindLocked(const TenantId& tenant_id,
                              const std::string& key) {
        if (empty()) return nullptr;
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = rounds_.find(KeyView{tenant_id, key});
        return it == rounds_.end() ? nullptr : it->second.get();
    }

    // A generation-zero entry reserves a bounded slot before allocating the
    // ordinary object. Caller erases it on allocation failure, or initializes
    // identity before releasing the same key guard. No address has escaped yet.
    tl::expected<PiecePutRound*, ErrorCode> InsertLocked(
        const TenantId& tenant_id, const std::string& key,
        PiecePutRound round) {
        auto owned = std::make_unique<PiecePutRound>(std::move(round));
        std::lock_guard<std::mutex> lock(mutex_);
        Key map_key{tenant_id, key};
        if (rounds_.contains(map_key)) {
            return tl::unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        if (rounds_.size() >= max_rounds_) {
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        auto* result = owned.get();
        rounds_.emplace(std::move(map_key), std::move(owned));
        active_.store(rounds_.size(), std::memory_order_release);
        return result;
    }

    // Initialize these two GC-visible fields under the map mutex. All later
    // accesses are read-only, so progress updates need only the key guard.
    void InitializeLocked(PiecePutRound& round, uint64_t generation,
                          TimePoint deadline) {
        std::lock_guard<std::mutex> lock(mutex_);
        round.generation = generation;
        round.deadline = deadline;
    }

    bool EraseLocked(const TenantId& tenant_id, const std::string& key,
                     uint64_t generation) {
        if (empty()) return false;
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = rounds_.find(KeyView{tenant_id, key});
        if (it == rounds_.end() || it->second->generation != generation) {
            return false;
        }
        rounds_.erase(it);
        active_.store(rounds_.size(), std::memory_order_release);
        return true;
    }

    std::vector<ExpiredRound> SnapshotExpired(TimePoint now) const {
        std::vector<ExpiredRound> expired;
        if (empty()) return expired;
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [key, round] : rounds_) {
            if (round->generation != 0 && round->deadline <= now) {
                expired.push_back({key.tenant_id, key.key, round->generation});
            }
        }
        return expired;
    }

    // Checks arithmetic before forming ends. An original operation is replayed
    // only for identical geometry; a different operation cannot claim overlap.
    tl::expected<PiecePutSpan*, ErrorCode> ClaimLocked(
        PiecePutRound& round, const UUID& client_id, uint64_t op_id,
        uint64_t offset, uint64_t length, uint64_t total_size) const {
        if (op_id == 0 || length == 0 || offset >= total_size ||
            length > total_size - offset) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        for (auto& span : round.spans) {
            if (span.client_id == client_id && span.op_id == op_id) {
                if (span.offset != offset || span.length != length) {
                    return tl::unexpected(ErrorCode::INVALID_PARAMS);
                }
                return &span;
            }
            if (offset < span.offset + span.length &&
                span.offset < offset + length) {
                return tl::unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
        }
        if (round.spans.size() >= max_spans_) {
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        round.spans.push_back({offset, length, client_id, op_id, false});
        return &round.spans.back();
    }

    static PiecePutSpan* FindSpanLocked(PiecePutRound& round,
                                        const UUID& client_id, uint64_t op_id) {
        for (auto& span : round.spans) {
            if (span.client_id == client_id && span.op_id == op_id)
                return &span;
        }
        return nullptr;
    }

   private:
    struct Key {
        TenantId tenant_id;
        std::string key;
        bool operator==(const Key&) const = default;
    };
    struct KeyView {
        const TenantId& tenant_id;
        std::string_view key;
    };
    struct KeyHash {
        using is_transparent = void;
        size_t operator()(const Key& key) const noexcept {
            return (*this)(KeyView{key.tenant_id, key.key});
        }
        size_t operator()(const KeyView& key) const noexcept {
            const auto tenant_hash = TenantIdHash{}(key.tenant_id);
            const auto key_hash = std::hash<std::string_view>{}(key.key);
            return tenant_hash ^ (key_hash + 0x9e3779b9 + (tenant_hash << 6) +
                                  (tenant_hash >> 2));
        }
    };

    struct KeyEqual {
        using is_transparent = void;
        bool operator()(const Key& a, const Key& b) const noexcept {
            return a.tenant_id == b.tenant_id && a.key == b.key;
        }
        bool operator()(const Key& a, const KeyView& b) const noexcept {
            return a.tenant_id == b.tenant_id && a.key == b.key;
        }
        bool operator()(const KeyView& a, const Key& b) const noexcept {
            return (*this)(b, a);
        }
    };

    const size_t max_rounds_;
    const size_t max_spans_;
    const UUID epoch_;
    std::atomic<size_t> active_{0};
    mutable std::mutex mutex_;
    std::unordered_map<Key, std::unique_ptr<PiecePutRound>, KeyHash, KeyEqual>
        rounds_;
};

}  // namespace mooncake
