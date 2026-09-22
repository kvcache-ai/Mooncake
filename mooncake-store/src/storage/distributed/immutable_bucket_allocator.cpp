#include "storage/distributed/immutable_bucket_allocator.h"

#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>
#include <utility>

#include <glog/logging.h>

#include "config/distributed_storage_config.h"
#include "storage/distributed/posix_fs_adapter.h"
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {
namespace {

bool IsBucketArtifact(std::string_view name) {
    return name.starts_with("bucket_") &&
           (name.ends_with(".data") || name.ends_with(".meta"));
}

}  // namespace

ImmutableBucketAllocator::PendingEviction::~PendingEviction() {
    if (owner_) owner_->AbortEvictionLocked(*this, false);
}

ImmutableBucketAllocator::PendingEviction::PendingEviction(
    PendingEviction&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      bucket_id_(std::exchange(other.bucket_id_, -1)),
      identity_(std::move(other.identity_)),
      candidates_(std::move(other.candidates_)) {}

ImmutableBucketAllocator::PendingEviction&
ImmutableBucketAllocator::PendingEviction::operator=(
    PendingEviction&& other) noexcept {
    if (this == &other) return *this;
    if (owner_) owner_->AbortEvictionLocked(*this, false);
    owner_ = std::exchange(other.owner_, nullptr);
    bucket_id_ = std::exchange(other.bucket_id_, -1);
    identity_ = std::move(other.identity_);
    candidates_ = std::move(other.candidates_);
    return *this;
}

ImmutableBucketAllocator::EvictedBucket::EvictedBucket(
    EvictedBucket&& other) noexcept
    : bucket_id_(std::exchange(other.bucket_id_, -1)),
      identity_(std::move(other.identity_)) {}

ImmutableBucketAllocator::EvictedBucket&
ImmutableBucketAllocator::EvictedBucket::operator=(
    EvictedBucket&& other) noexcept {
    if (this == &other) return *this;
    bucket_id_ = std::exchange(other.bucket_id_, -1);
    identity_ = std::move(other.identity_);
    return *this;
}

ImmutableBucketAllocator::~ImmutableBucketAllocator() {
    initialized_.store(false, std::memory_order_release);
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> ImmutableBucketAllocator::Init(
    const DistributedStorageConfig& config) {
    if (initialized_.load(std::memory_order_acquire)) return {};
    if (!config.ValidateForAllocator() ||
        config.bucket_capacity >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        config.bucket_capacity >
            std::numeric_limits<uint64_t>::max() /
                static_cast<uint64_t>(config.max_bucket_count)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::unique_ptr<FileSystemAdapter> adapter;
    if (config.fs_adapter_type == "posix") {
        adapter = std::make_unique<PosixFsAdapter>();
    } else if (config.fs_adapter_type == "hf3fs") {
#ifdef USE_3FS
        adapter = std::make_unique<Hf3fsAdapter>();
#else
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
    } else {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto init = adapter->Init(config.fsdir);
    if (!init) return tl::make_unexpected(init.error());
    auto files = adapter->ListFiles(config.fsdir);
    if (!files) {
        adapter->Shutdown();
        return tl::make_unexpected(files.error());
    }
    for (const auto& name : *files) {
        if (IsBucketArtifact(name)) {
            LOG(ERROR) << "Immutable DFS bucket recovery is unsupported; "
                          "refusing non-empty root containing "
                       << name << ". Configure a new empty DFS root.";
            adapter->Shutdown();
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    std::lock_guard lock(mutex_);
    fsdir_ = config.fsdir;
    bucket_capacity_ = config.bucket_capacity;
    alignment_ = config.alignment;
    max_bucket_count_ = config.max_bucket_count;
    eviction_enabled_ = config.eviction_enabled;
    eviction_high_watermark_ = config.eviction_high_watermark;
    eviction_low_watermark_ = config.eviction_low_watermark;
    eviction_check_interval_ = config.eviction_check_interval;
    fs_adapter_ = std::move(adapter);
    initialized_.store(true, std::memory_order_release);
    return {};
}

std::string ImmutableBucketAllocator::FormatBucketId(int64_t bucket_id) {
    std::ostringstream stream;
    stream << std::setw(6) << std::setfill('0') << bucket_id;
    return stream.str();
}

std::string ImmutableBucketAllocator::BucketDataPath(int64_t bucket_id) const {
    return fsdir_ + "/bucket_" + FormatBucketId(bucket_id) + ".data";
}

tl::expected<ImmutableBucketAllocator::BucketPtr, ErrorCode>
ImmutableBucketAllocator::EnsureActiveBucketLocked(uint64_t required) {
    if (required == 0 || required > bucket_capacity_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (active_bucket_id_ >= 0) {
        const auto current = buckets_.find(active_bucket_id_);
        if (current != buckets_.end() &&
            current->second->lifecycle == BucketLifecycle::ACTIVE &&
            current->second->append_offset <= current->second->capacity &&
            required <=
                current->second->capacity - current->second->append_offset) {
            return current->second;
        }
        if (current != buckets_.end() &&
            current->second->lifecycle == BucketLifecycle::ACTIVE) {
            current->second->lifecycle = BucketLifecycle::SEALED;
            TouchLruLocked(current->first);
        }
        active_bucket_id_ = -1;
    }

    if (next_bucket_id_ > kMaxBucketId) {
        LOG(ERROR) << "Immutable DFS bucket id space exhausted, next_id="
                   << next_bucket_id_;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (buckets_.size() >= static_cast<size_t>(max_bucket_count_)) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    const int64_t bucket_id = next_bucket_id_++;
    const std::string path = BucketDataPath(bucket_id);
    auto preallocated = fs_adapter_->PreallocateFile(path, bucket_capacity_);
    if (!preallocated) {
        auto cleaned = fs_adapter_->DeleteFile(path);
        if (!cleaned && cleaned.error() != ErrorCode::FILE_NOT_FOUND) {
            LOG(WARNING) << "Failed to clean up unpublished DFS bucket " << path
                         << ": " << cleaned.error();
        }
        return tl::make_unexpected(preallocated.error());
    }

    auto bucket = std::make_shared<BucketState>();
    bucket->id = bucket_id;
    bucket->capacity = bucket_capacity_;
    buckets_.emplace(bucket_id, bucket);
    active_bucket_id_ = bucket_id;
    return bucket;
}

tl::expected<DistributedFSDescriptor, ErrorCode>
ImmutableBucketAllocator::ReserveInBucketLocked(BucketState& bucket,
                                                const std::string& key,
                                                uint64_t size) {
    if (key.empty() || key_index_.contains(key)) {
        return tl::make_unexpected(key.empty()
                                       ? ErrorCode::INVALID_PARAMS
                                       : ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    const auto layout =
        ComputeBucketEntryLayout(bucket.append_offset, size, alignment_);
    if (!layout || layout->end() > bucket.capacity ||
        layout->offset >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        LOG(ERROR) << "Immutable DFS bucket layout invariant violated, bucket="
                   << bucket.id << ", append_offset=" << bucket.append_offset
                   << ", size=" << size;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    BucketEntry entry;
    entry.layout = *layout;
    // A key may be allocated again after its previous reservation became a
    // tombstone. Replace only the runtime index record; append_offset is never
    // rewound, so the stale descriptor's physical range is never reused.
    bucket.entries.insert_or_assign(key, entry);
    key_index_[key] = bucket.id;
    bucket.append_offset = layout->end();
    ++bucket.pending_entries;
    return MakeBucketDescriptor(BucketDataPath(bucket.id), *layout, bucket.id);
}

tl::expected<DistributedFSDescriptor, ErrorCode>
ImmutableBucketAllocator::Allocate(const std::string& key, uint64_t size) {
    if (!IsInitialized()) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    const auto layout = ComputeBucketEntryLayout(0, size, alignment_);
    if (key.empty() || !layout || layout->aligned_size > bucket_capacity_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard lock(mutex_);
    if (key_index_.contains(key)) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    auto bucket_result = EnsureActiveBucketLocked(layout->aligned_size);
    if (!bucket_result) {
        return tl::make_unexpected(bucket_result.error());
    }
    BucketPtr bucket = *bucket_result;
    return ReserveInBucketLocked(*bucket, key, size);
}

std::vector<BatchAllocateResult> ImmutableBucketAllocator::BatchAllocate(
    const std::vector<BatchAllocateRequest>& requests) {
    std::vector<BatchAllocateResult> results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        results.push_back({request.key, {}, false, ErrorCode::OK});
    }
    if (!IsInitialized()) {
        for (auto& result : results) {
            result.error = ErrorCode::DFS_SERVICE_UNAVAILABLE;
        }
        return results;
    }

    std::lock_guard lock(mutex_);
    ErrorCode failure = ErrorCode::OK;
    size_t completed = 0;
    for (; completed < requests.size(); ++completed) {
        const auto& request = requests[completed];
        const auto layout =
            ComputeBucketEntryLayout(0, request.size, alignment_);
        if (request.key.empty() || !layout ||
            layout->aligned_size > bucket_capacity_) {
            failure = ErrorCode::INVALID_PARAMS;
            break;
        }
        if (key_index_.contains(request.key)) {
            failure = ErrorCode::OBJECT_ALREADY_EXISTS;
            break;
        }
        auto bucket_result = EnsureActiveBucketLocked(layout->aligned_size);
        if (!bucket_result) {
            failure = bucket_result.error();
            break;
        }
        BucketPtr bucket = *bucket_result;
        auto descriptor =
            ReserveInBucketLocked(*bucket, request.key, request.size);
        if (!descriptor) {
            failure = descriptor.error();
            break;
        }
        results[completed].descriptor = std::move(*descriptor);
        results[completed].success = true;
    }

    if (completed == requests.size()) return results;
    for (size_t i = completed; i > 0; --i) {
        auto& result = results[i - 1];
        BucketPtr bucket;
        if (auto* entry = FindMatchingEntryLocked(result.key, result.descriptor,
                                                  &bucket)) {
            TombstoneLocked(result.key, *bucket, *entry);
        }
    }
    for (auto& result : results) {
        result.descriptor = {};
        result.success = false;
        result.error = failure;
    }
    return results;
}

ImmutableBucketAllocator::BucketEntry*
ImmutableBucketAllocator::FindMatchingEntryLocked(
    const std::string& key, const DistributedFSDescriptor& descriptor,
    BucketPtr* out_bucket) {
    const auto key_it = key_index_.find(key);
    if (key_it == key_index_.end() || key_it->second != descriptor.shard_idx) {
        return nullptr;
    }
    const auto bucket_it = buckets_.find(key_it->second);
    if (bucket_it == buckets_.end()) return nullptr;
    const auto entry_it = bucket_it->second->entries.find(key);
    if (entry_it == bucket_it->second->entries.end()) return nullptr;
    auto& entry = entry_it->second;
    if (entry.state == BucketEntryState::TOMBSTONE ||
        descriptor.file_path != BucketDataPath(bucket_it->first) ||
        descriptor.offset != entry.layout.offset ||
        descriptor.object_size != entry.layout.object_size ||
        descriptor.aligned_size != entry.layout.aligned_size) {
        return nullptr;
    }
    if (out_bucket) *out_bucket = bucket_it->second;
    return &entry;
}

void ImmutableBucketAllocator::TombstoneLocked(const std::string& key,
                                               BucketState& bucket,
                                               BucketEntry& entry) {
    if (entry.state == BucketEntryState::TOMBSTONE) return;
    if (entry.state == BucketEntryState::PENDING &&
        bucket.pending_entries > 0) {
        --bucket.pending_entries;
    }
    entry.state = BucketEntryState::TOMBSTONE;
    const auto key_it = key_index_.find(key);
    if (key_it != key_index_.end() && key_it->second == bucket.id) {
        key_index_.erase(key_it);
    }
}

bool ImmutableBucketAllocator::MarkCommitted(
    const std::string& key, const DistributedFSDescriptor& descriptor) {
    if (!IsInitialized()) return false;
    std::lock_guard lock(mutex_);
    BucketPtr bucket;
    auto* entry = FindMatchingEntryLocked(key, descriptor, &bucket);
    if (!entry) return false;
    if (entry->state == BucketEntryState::COMMITTED) return true;
    if (entry->state != BucketEntryState::PENDING) return false;
    entry->state = BucketEntryState::COMMITTED;
    if (bucket->pending_entries > 0) --bucket->pending_entries;
    return true;
}

void ImmutableBucketAllocator::Free(const std::string& key,
                                    const DistributedFSDescriptor& descriptor) {
    if (!IsInitialized()) return;
    std::lock_guard lock(mutex_);
    BucketPtr bucket;
    auto* entry = FindMatchingEntryLocked(key, descriptor, &bucket);
    if (entry) TombstoneLocked(key, *bucket, *entry);
}

void ImmutableBucketAllocator::UpdateAccess(
    const std::string& key, const DistributedFSDescriptor& descriptor) {
    if (!IsInitialized()) return;
    std::lock_guard lock(mutex_);
    BucketPtr bucket;
    if (!FindMatchingEntryLocked(key, descriptor, &bucket)) return;
    if (bucket->lifecycle == BucketLifecycle::SEALED) {
        TouchLruLocked(bucket->id);
    }
}

void ImmutableBucketAllocator::TouchLruLocked(int64_t bucket_id) {
    const auto existing = lru_index_.find(bucket_id);
    if (existing != lru_index_.end()) {
        lru_.erase(existing->second);
    }
    lru_.push_front(bucket_id);
    lru_index_[bucket_id] = lru_.begin();
}

uint64_t ImmutableBucketAllocator::UsedBytesLocked() const {
    // Append-only ranges remain consumed until their whole bucket is deleted.
    uint64_t used = 0;
    for (const auto& [id, bucket] : buckets_) {
        (void)id;
        const uint64_t contribution =
            bucket->lifecycle == BucketLifecycle::EVICTING
                ? bucket->capacity
                : bucket->append_offset;
        if (contribution > std::numeric_limits<uint64_t>::max() - used) {
            return std::numeric_limits<uint64_t>::max();
        }
        used += contribution;
    }
    return used;
}

uint64_t ImmutableBucketAllocator::GetUsedBytes() const {
    std::lock_guard lock(mutex_);
    return UsedBytesLocked();
}

uint64_t ImmutableBucketAllocator::GetTotalCapacity() const {
    return bucket_capacity_ * static_cast<uint64_t>(max_bucket_count_);
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEvictionLocked(bool force_one) {
    PendingEviction pending;
    if (!eviction_enabled_) return pending;
    const uint64_t capacity = GetTotalCapacity();
    const uint64_t used = UsedBytesLocked();
    const double usage = capacity == 0 ? 0.0
                                       : static_cast<double>(used) /
                                             static_cast<double>(capacity);
    if (!force_one) {
        if (!eviction_active_ && usage < eviction_high_watermark_) {
            return pending;
        }
        eviction_active_ = usage > eviction_low_watermark_;
        if (!eviction_active_) return pending;
    }

    for (auto it = lru_.rbegin(); it != lru_.rend(); ++it) {
        const auto bucket_it = buckets_.find(*it);
        if (bucket_it == buckets_.end()) continue;
        auto& bucket = *bucket_it->second;
        if (bucket.id == active_bucket_id_ ||
            bucket.lifecycle != BucketLifecycle::SEALED ||
            bucket.pending_entries != 0) {
            continue;
        }
        bucket.lifecycle = BucketLifecycle::FROZEN;
        const auto lru_it = lru_index_.find(bucket.id);
        if (lru_it != lru_index_.end()) {
            lru_.erase(lru_it->second);
            lru_index_.erase(lru_it);
        }
        pending.owner_ = this;
        pending.bucket_id_ = bucket.id;
        pending.identity_ = bucket_it->second;
        for (const auto& [key, entry] : bucket.entries) {
            if (entry.state != BucketEntryState::COMMITTED) continue;
            auto layout = RebuildBucketEntryLayout(
                entry.layout.offset, entry.layout.object_size, alignment_);
            if (!layout || layout->aligned_size != entry.layout.aligned_size ||
                layout->end() > bucket.capacity) {
                bucket.lifecycle = BucketLifecycle::SEALED;
                TouchLruLocked(bucket.id);
                pending.owner_ = nullptr;
                pending.bucket_id_ = -1;
                pending.identity_.reset();
                pending.candidates_.clear();
                return pending;
            }
            auto descriptor = MakeBucketDescriptor(BucketDataPath(bucket.id),
                                                   *layout, bucket.id);
            pending.candidates_.push_back({key, static_cast<int>(bucket.id),
                                           entry.layout.offset,
                                           std::move(descriptor)});
        }
        return pending;
    }
    return pending;
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEviction() {
    std::lock_guard lock(mutex_);
    return PrepareEvictionLocked(false);
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEvictionForAllocationFailure() {
    std::lock_guard lock(mutex_);
    return PrepareEvictionLocked(true);
}

void ImmutableBucketAllocator::AbortEvictionLocked(PendingEviction& pending,
                                                   bool warm) {
    std::lock_guard lock(mutex_);
    if (pending.owner_ != this) return;
    const auto bucket_it = buckets_.find(pending.bucket_id_);
    if (bucket_it != buckets_.end() &&
        bucket_it->second.get() == pending.identity_.get() &&
        bucket_it->second->lifecycle == BucketLifecycle::FROZEN) {
        bucket_it->second->lifecycle = BucketLifecycle::SEALED;
        if (warm) {
            TouchLruLocked(bucket_it->first);
        } else {
            lru_.push_back(bucket_it->first);
            lru_index_[bucket_it->first] = std::prev(lru_.end());
        }
    }
    pending.owner_ = nullptr;
    pending.bucket_id_ = -1;
    pending.identity_.reset();
    pending.candidates_.clear();
}

void ImmutableBucketAllocator::AbortEviction(PendingEviction&& pending) {
    AbortEvictionLocked(pending, true);
}

tl::expected<ImmutableBucketAllocator::EvictedBucket, ErrorCode>
ImmutableBucketAllocator::CommitEvictionLogical(PendingEviction&& pending) {
    std::lock_guard lock(mutex_);
    if (pending.owner_ != this) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto it = buckets_.find(pending.bucket_id_);
    if (it == buckets_.end() || it->second.get() != pending.identity_.get() ||
        it->second->lifecycle != BucketLifecycle::FROZEN ||
        it->second->pending_entries != 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const BucketPtr bucket = it->second;
    for (const auto& [key, entry] : bucket->entries) {
        (void)entry;
        const auto indexed = key_index_.find(key);
        if (indexed != key_index_.end() && indexed->second == bucket->id) {
            key_index_.erase(indexed);
        }
    }
    bucket->lifecycle = BucketLifecycle::EVICTING;
    EvictedBucket evicted;
    evicted.bucket_id_ = pending.bucket_id_;
    evicted.identity_ = pending.identity_;
    pending.owner_ = nullptr;
    return evicted;
}

tl::expected<void, ErrorCode> ImmutableBucketAllocator::DeleteEvictedBucket(
    EvictedBucket&& bucket) {
    if (bucket.bucket_id_ < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto deleted = fs_adapter_->DeleteFile(BucketDataPath(bucket.bucket_id_));
    if (!deleted && deleted.error() != ErrorCode::FILE_NOT_FOUND) {
        std::lock_guard lock(mutex_);
        failed_deletions_.insert(bucket.bucket_id_);
        return tl::make_unexpected(deleted.error());
    }

    std::lock_guard lock(mutex_);
    const auto current = buckets_.find(bucket.bucket_id_);
    if (current != buckets_.end() && current->second == bucket.identity_) {
        current->second->lifecycle = BucketLifecycle::RETIRED;
        buckets_.erase(current);
    }
    failed_deletions_.erase(bucket.bucket_id_);
    return {};
}

size_t ImmutableBucketAllocator::RetryFailedEvictions() {
    std::vector<std::pair<int64_t, BucketPtr>> retries;
    {
        std::lock_guard lock(mutex_);
        retries.reserve(failed_deletions_.size());
        for (const int64_t id : failed_deletions_) {
            const auto it = buckets_.find(id);
            if (it != buckets_.end() &&
                it->second->lifecycle == BucketLifecycle::EVICTING) {
                retries.emplace_back(id, it->second);
            }
        }
    }

    size_t completed = 0;
    for (const auto& [id, bucket] : retries) {
        auto deleted = fs_adapter_->DeleteFile(BucketDataPath(id));
        if (!deleted && deleted.error() != ErrorCode::FILE_NOT_FOUND) continue;
        std::lock_guard lock(mutex_);
        const auto current = buckets_.find(id);
        if (current == buckets_.end() || current->second != bucket ||
            bucket->lifecycle != BucketLifecycle::EVICTING) {
            continue;
        }
        bucket->lifecycle = BucketLifecycle::RETIRED;
        buckets_.erase(current);
        failed_deletions_.erase(id);
        ++completed;
    }
    return completed;
}

size_t ImmutableBucketAllocator::GetBucketCount() const {
    std::lock_guard lock(mutex_);
    return buckets_.size();
}

std::optional<int64_t> ImmutableBucketAllocator::GetBucketIdForKey(
    const std::string& key) const {
    std::lock_guard lock(mutex_);
    const auto it = key_index_.find(key);
    if (it == key_index_.end()) return std::nullopt;
    return it->second;
}

}  // namespace mooncake
