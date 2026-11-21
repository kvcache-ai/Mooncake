#include "p2p/client/data_manager/v2/block_index.h"

#include <algorithm>
#include <array>
#include <limits>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

// Reuse the sketch only in this implementation; the backend interface does not
// expose a dependency on the tiered-cache scheduler.
#include "p2p/client/tiered_cache/event_driven_scheduler/frequency_sketch.h"
#include "p2p/client/tiered_cache/event_driven_scheduler/multi_lru.h"
#include "utils.h"

namespace mooncake::v2 {
namespace {

constexpr size_t kDefaultSketchCapacity = 1u << 16;
constexpr size_t kMinSketchCapacity = 1u << 10;
constexpr size_t kMaxSketchCapacity = 1u << 20;

enum class Ordering { kLru, kMultiLru, kTinyLfuLru };

std::optional<Ordering> OrderingFromType(std::string_view type) {
    if (type == "lru") return Ordering::kLru;
    if (type == "multi_lru") return Ordering::kMultiLru;
    if (type == "tinylfu_lru") return Ordering::kTinyLfuLru;
    return std::nullopt;
}

BandThresholds ResolveThresholds(const BlockIndexConfig& config) {
    BandThresholds thresholds;
    if (config.band_warm_threshold > 0) {
        thresholds.warm = config.band_warm_threshold;
    }
    if (config.band_hot_threshold > 0) {
        thresholds.hot = config.band_hot_threshold;
    }
    if (config.band_veryhot_threshold > 0) {
        thresholds.very_hot = config.band_veryhot_threshold;
    }
    return thresholds;
}

size_t ResolveSketchCapacity(const BlockIndexConfig& config, size_t capacity) {
    if (config.sketch_capacity > 0) return config.sketch_capacity;
    if (capacity == 0) return kDefaultSketchCapacity;
    return std::clamp(capacity, kMinSketchCapacity, kMaxSketchCapacity);
}

/**
 * One container owns each immutable entry and its ordering links. unordered_map
 * keeps node addresses stable across rehashes, so intrusive chains need neither
 * a second map nor separately allocated list nodes.
 *
 * Every membership or ordering mutation takes ordering_mu_ before one shard's
 * mutex. Lookup, Stats and SnapshotShard take only a shard shared lock and
 * never inspect links. SelectVictims takes only ordering_mu_: all nodes and
 * their immutable entries remain stable until it releases that lock.
 */
class OrderedBlockIndex final : public BlockIndex {
   public:
    OrderedBlockIndex(const BlockIndexConfig& config, Ordering ordering,
                      const UUID& tiler_id, size_t capacity)
        : config_(config),
          ordering_(ordering),
          tiler_id_(tiler_id),
          thresholds_(ResolveThresholds(config)) {
        if (ordering_ == Ordering::kTinyLfuLru) {
            sketch_ = std::make_unique<FrequencySketch>(
                ResolveSketchCapacity(config, capacity));
        }
        shards_.reserve(config.shard_count);
        for (size_t i = 0; i < config.shard_count; ++i) {
            auto shard = std::make_unique<Shard>();
            shard->entries.max_load_factor(config.max_load_factor);
            shards_.push_back(std::move(shard));
        }
    }

    tl::expected<ImmutableBlock, ErrorCode> Insert(
        CompletedBlock&& block,
        const BlockRegistrationHandle& registration) override {
        // Consume before validation, including failures before entry creation.
        CompletedBlock owned = std::move(block);
        if (!owned) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        if (!registration)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

        try {
            // Keep an owner outside the lock scope even while emplace is
            // constructing a map node: a failed allocation must never perform
            // the physical free while holding an index lock.
            auto entry = MakeEntry(std::move(owned), registration, tiler_id_);
            const RegistrationId registration_id = registration.Id();
            const size_t shard_id = ShardFor(registration_id);
            auto& shard = *shards_[shard_id];
            {
                std::lock_guard<std::mutex> order_lock(ordering_mu_);
                std::unique_lock<std::shared_mutex> shard_lock(shard.mu);
                if (shard.entries.find(registration_id) !=
                    shard.entries.end()) {
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_ALREADY_EXISTS);
                }
                // Interleave per-shard sequences for unique tiler-local ids.
                const size_t shard_count = shards_.size();
                if (shard.next_block_sequence == 0 ||
                    shard.next_block_sequence >
                        (std::numeric_limits<uint64_t>::max() - shard_id) /
                            shard_count) {
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                entry->block.id.local_id =
                    shard.next_block_sequence * shard_count + shard_id;
                auto [it, inserted] =
                    shard.entries.try_emplace(registration_id, entry);
                // The duplicate was checked while holding the same lock.
                (void)inserted;
                Node& node = it->second;
                // No allocations follow emplace: publication and attachment
                // either both succeed or leave the shard entirely unchanged.
                LinkMru(node, BandFor(node, RecordFrequency(entry->block.key)));
                ++shard.next_block_sequence;
                shard.indexed_bytes += entry->block.size_bytes;
            }
            return ImmutableBlock(std::move(entry));
        } catch (const std::bad_alloc&) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        } catch (const std::length_error&) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    std::optional<ImmutableBlock> Lookup(
        const BlockRegistrationHandle& registration) const override {
        if (!registration) return std::nullopt;
        const RegistrationId id = registration.Id();
        const auto& shard = *shards_[ShardFor(id)];
        BlockEntryPtr entry;
        {
            std::shared_lock<std::shared_mutex> lock(shard.mu);
            const auto it = shard.entries.find(id);
            if (it == shard.entries.end()) return std::nullopt;
            entry = it->second.entry;
        }
        return ImmutableBlock(std::move(entry));
    }

    BlockEntryPtr Erase(const BlockRegistrationHandle& registration,
                        std::optional<BlockId> expected_id) override {
        if (!registration) return nullptr;
        return EraseById(registration.Id(), expected_id);
    }

    BlockEntryPtr EraseById(RegistrationId registration_id,
                            std::optional<BlockId> expected_id) override {
        auto& shard = *shards_[ShardFor(registration_id)];
        BlockEntryPtr detached;
        {
            std::lock_guard<std::mutex> order_lock(ordering_mu_);
            std::unique_lock<std::shared_mutex> shard_lock(shard.mu);
            auto it = shard.entries.find(registration_id);
            if (it == shard.entries.end()) return nullptr;
            Node& node = it->second;
            if (expected_id && !(node.entry->block.id == *expected_id)) {
                return nullptr;
            }
            Unlink(node);
            detached = std::move(node.entry);
            shard.indexed_bytes -= detached->block.size_bytes;
            shard.entries.erase(it);
        }
        return detached;
    }

    void OnAccess(const BlockToken& token) override {
        if (!(token.tiler_id == tiler_id_)) return;
        auto& shard = *shards_[ShardFor(token.registration_id)];
        std::lock_guard<std::mutex> order_lock(ordering_mu_);
        std::unique_lock<std::shared_mutex> shard_lock(shard.mu);
        const auto it = shard.entries.find(token.registration_id);
        if (it == shard.entries.end()) return;
        Node& node = it->second;
        if (!(node.entry->block.id == token.block_id)) return;
        // Validate before recording frequency: a delayed access must not heat
        // the newer block that replaced the one its token names.
        if (node.access_count < std::numeric_limits<uint64_t>::max()) {
            ++node.access_count;
        }
        const size_t band =
            BandFor(node, RecordFrequency(node.entry->block.key));
        Unlink(node);
        LinkMru(node, band);
    }

    std::vector<BlockToken> SelectVictims(size_t target_bytes) override {
        std::vector<BlockToken> victims;
        if (target_bytes == 0) return victims;
        size_t collected = 0;
        std::lock_guard<std::mutex> lock(ordering_mu_);
        for (size_t band = 0; band < tails_.size(); ++band) {
            for (const Node* node = tails_[band]; node; node = node->previous) {
                const Block& block = node->entry->block;
                BlockToken token;
                token.key = block.key;
                token.registration_id = block.registration.Id();
                token.registration = block.registration.Downgrade();
                token.tiler_id = tiler_id_;
                token.block_id = block.id;
                token.size_bytes = block.size_bytes;
                victims.push_back(std::move(token));
                // Subtract from the remaining target to avoid overflowing the
                // byte sum when selecting very large blocks.
                if (block.size_bytes >= target_bytes - collected ||
                    victims.size() >= config_.max_victim_candidates) {
                    return victims;
                }
                collected += block.size_bytes;
            }
        }
        return victims;
    }

    std::vector<BlockEntryPtr> SnapshotShard(size_t shard_id) const override {
        std::vector<BlockEntryPtr> out;
        if (shard_id >= shards_.size()) return out;
        const auto& shard = *shards_[shard_id];
        std::shared_lock<std::shared_mutex> lock(shard.mu);
        out.reserve(shard.entries.size());
        for (const auto& [id, node] : shard.entries) {
            out.push_back(node.entry);
        }
        return out;
    }

    std::vector<BlockEntryPtr> Drain() override {
        std::vector<BlockEntryPtr> out;
        for (auto& shard : shards_) {
            EntryMap taken;
            taken.max_load_factor(config_.max_load_factor);
            {
                std::lock_guard<std::mutex> order_lock(ordering_mu_);
                std::unique_lock<std::shared_mutex> shard_lock(shard->mu);
                for (auto& [id, node] : shard->entries) Unlink(node);
                taken.swap(shard->entries);
                shard->indexed_bytes = 0;
            }
            // Reserve and destruction both happen outside the locks. Concurrent
            // inserts into a drained shard belong to the next drain operation.
            out.reserve(out.size() + taken.size());
            for (auto& [id, node] : taken) {
                out.push_back(std::move(node.entry));
            }
        }
        return out;
    }

    size_t ShardCount() const override { return shards_.size(); }

    BlockIndexStats Stats() const override {
        BlockIndexStats stats;
        for (const auto& shard : shards_) {
            std::shared_lock<std::shared_mutex> lock(shard->mu);
            stats.entry_count += shard->entries.size();
            stats.indexed_bytes += shard->indexed_bytes;
        }
        return stats;
    }

    UUID TilerId() const override { return tiler_id_; }

    size_t ShardFor(RegistrationId id) const override {
        return RegistrationIdHash{}(id) % shards_.size();
    }

   private:
    struct Node {
        explicit Node(BlockEntryPtr value) : entry(std::move(value)) {}
        BlockEntryPtr entry;
        uint64_t access_count = 1;
        size_t band = 0;
        Node* previous = nullptr;  // toward the band's MRU
        Node* next = nullptr;      // toward the band's LRU
    };

    using EntryMap =
        std::unordered_map<RegistrationId, Node, RegistrationIdHash>;
    struct Shard {
        mutable std::shared_mutex mu;
        EntryMap entries;
        uint64_t next_block_sequence = 1;
        size_t indexed_bytes = 0;
    };

    uint64_t RecordFrequency(std::string_view key) {
        if (!sketch_) return 0;
        return sketch_->IncrementAndEstimate(
            static_cast<uint64_t>(StringHash{}(key)));
    }

    size_t BandFor(const Node& node, uint64_t sketch_frequency) const {
        if (ordering_ == Ordering::kLru) return 0;
        const uint64_t frequency = ordering_ == Ordering::kMultiLru
                                       ? node.access_count
                                       : sketch_frequency;
        return static_cast<size_t>(BandOf(frequency, thresholds_));
    }

    void LinkMru(Node& node, size_t band) noexcept {
        node.band = band;
        node.previous = nullptr;
        node.next = heads_[band];
        if (node.next) node.next->previous = &node;
        if (!tails_[band]) tails_[band] = &node;
        heads_[band] = &node;
    }

    void Unlink(Node& node) noexcept {
        if (node.previous) {
            node.previous->next = node.next;
        } else {
            heads_[node.band] = node.next;
        }
        if (node.next) {
            node.next->previous = node.previous;
        } else {
            tails_[node.band] = node.previous;
        }
        node.previous = nullptr;
        node.next = nullptr;
    }

    const BlockIndexConfig config_;
    const Ordering ordering_;
    const UUID tiler_id_;
    const BandThresholds thresholds_;
    std::unique_ptr<FrequencySketch> sketch_;
    mutable std::mutex ordering_mu_;
    std::array<Node*, kNumHeatBands> heads_{};
    std::array<Node*, kNumHeatBands> tails_{};
    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace

std::shared_ptr<BlockEntry> BlockIndex::MakeEntry(
    CompletedBlock&& block, const BlockRegistrationHandle& registration,
    const UUID& tiler_id) {
    Block value;
    value.id.tiler_id = tiler_id;
    value.id.generation = block.PhysicalId().generation;
    value.size_bytes = block.Size();
    value.key = std::move(block.key_);
    value.allocation = std::move(block.allocation_);
    value.registration = registration;
    block.armed_ = false;
    return std::make_shared<BlockEntry>(std::move(value));
}

tl::expected<void, ErrorCode> ValidateBlockIndexConfig(
    const BlockIndexConfig& config) {
    if (config.shard_count == 0) {
        LOG(ERROR) << "block_index.shard_count must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.max_load_factor > 0.0F) || config.max_load_factor > 1.0F) {
        LOG(ERROR) << "block_index.max_load_factor must be in (0, 1], got "
                   << config.max_load_factor;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!OrderingFromType(config.type)) {
        LOG(ERROR) << "Unknown block index type '" << config.type
                   << "'; expected lru, multi_lru or tinylfu_lru";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.max_victim_candidates == 0) {
        LOG(ERROR) << "block_index.max_victim_candidates must be greater "
                      "than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const BandThresholds thresholds = ResolveThresholds(config);
    if (thresholds.hot <= thresholds.warm ||
        thresholds.very_hot <= thresholds.hot) {
        LOG(ERROR) << "block_index band thresholds must be strictly "
                      "increasing, got warm="
                   << thresholds.warm << " hot=" << thresholds.hot
                   << " veryhot=" << thresholds.very_hot;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<std::unique_ptr<BlockIndex>, ErrorCode> CreateBlockIndex(
    const BlockIndexConfig& config, const UUID& tiler_id, size_t capacity) {
    const auto valid = ValidateBlockIndexConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    if (IsZeroUUID(tiler_id)) {
        LOG(ERROR) << "CreateBlockIndex needs a real tiler id";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    try {
        std::unique_ptr<BlockIndex> index = std::make_unique<OrderedBlockIndex>(
            config, *OrderingFromType(config.type), tiler_id, capacity);
        return index;
    } catch (const std::bad_alloc&) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    } catch (const std::length_error&) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

}  // namespace mooncake::v2
