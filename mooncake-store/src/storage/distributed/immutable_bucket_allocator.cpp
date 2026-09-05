#include "storage/distributed/immutable_bucket_allocator.h"

#include <glog/logging.h>

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <unordered_set>
#include <utility>

#include <ylt/struct_pb.hpp>

#include "crc32c.h"
#include "config/distributed_storage_config.h"
#include "storage/distributed/posix_fs_adapter.h"
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {

namespace {

constexpr const char* kBucketFilePrefix = "bucket_";
constexpr const char* kBucketDataSuffix = ".data";
constexpr const char* kBucketMetaSuffix = ".meta";

int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool IsLive(BucketEntryState state) {
    return state == BucketEntryState::PENDING ||
           state == BucketEntryState::COMMITTED;
}

uint32_t ComputeMetadataChecksum(PersistedBucketMetadata snapshot) {
    // The checksum covers the serialized form with the checksum field zeroed,
    // so verification can recompute it the same way after loading.
    snapshot.checksum = 0;
    std::string payload;
    struct_pb::to_pb(snapshot, payload);
    return Crc32cValue(payload.data(), payload.size());
}

/**
 * @brief Extract the bucket id from a `bucket_<id><suffix>` file name.
 */
std::optional<int64_t> ParseBucketFileName(const std::string& name,
                                           const char* suffix) {
    const std::string prefix(kBucketFilePrefix);
    const std::string tail(suffix);
    if (name.size() <= prefix.size() + tail.size()) return std::nullopt;
    if (name.compare(0, prefix.size(), prefix) != 0) return std::nullopt;
    if (name.compare(name.size() - tail.size(), tail.size(), tail) != 0) {
        return std::nullopt;
    }

    const std::string digits =
        name.substr(prefix.size(), name.size() - prefix.size() - tail.size());
    if (digits.empty() ||
        !std::all_of(digits.begin(), digits.end(),
                     [](unsigned char c) { return std::isdigit(c) != 0; })) {
        return std::nullopt;
    }
    try {
        const long long value = std::stoll(digits);
        if (value < 0 || value > kMaxBucketId) return std::nullopt;
        return static_cast<int64_t>(value);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

}  // namespace

ImmutableBucketAllocator::PendingEviction::~PendingEviction() {
    // An unresolved transaction must not leave the bucket frozen forever.
    // Note: `owner_` must still be set when AbortEviction is entered, because
    // that is how it recognises the transaction as its own; clearing it here
    // first would make the abort a silent no-op. AbortEviction clears it.
    if (owner_ != nullptr) {
        owner_->AbortEviction(std::move(*this), /*demote=*/false);
    }
}

ImmutableBucketAllocator::PendingEviction::PendingEviction(
    PendingEviction&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      bucket_id_(std::exchange(other.bucket_id_, -1)),
      bucket_generation_(std::exchange(other.bucket_generation_, 0)),
      candidates_(std::move(other.candidates_)) {
    other.candidates_.clear();
}

ImmutableBucketAllocator::PendingEviction&
ImmutableBucketAllocator::PendingEviction::operator=(
    PendingEviction&& other) noexcept {
    if (this != &other) {
        if (owner_ != nullptr) {
            PendingEviction discarded;
            discarded.owner_ = std::exchange(owner_, nullptr);
            discarded.bucket_id_ = bucket_id_;
            discarded.bucket_generation_ = bucket_generation_;
            discarded.candidates_ = std::move(candidates_);
            discarded.owner_->AbortEviction(std::move(discarded),
                                            /*demote=*/false);
        }
        owner_ = std::exchange(other.owner_, nullptr);
        bucket_id_ = std::exchange(other.bucket_id_, -1);
        bucket_generation_ = std::exchange(other.bucket_generation_, 0);
        candidates_ = std::move(other.candidates_);
        other.candidates_.clear();
    }
    return *this;
}

ImmutableBucketAllocator::~ImmutableBucketAllocator() {
    if (initialized_.load(std::memory_order_acquire)) {
        // A clean shutdown must not leave a sealed bucket's metadata unwritten.
        // The still-active bucket is deliberately left unpersisted: its data is
        // discarded on the next start, which is the accepted trade-off for
        // keeping the hot path free of metadata I/O.
        FlushDirtyMetadata();
    }
    if (fs_adapter_) fs_adapter_->Shutdown();
}

std::string ImmutableBucketAllocator::FormatBucketId(int64_t bucket_id) {
    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << bucket_id;
    return oss.str();
}

std::string ImmutableBucketAllocator::BucketDataPath(int64_t bucket_id) const {
    return fsdir_ + "/" + kBucketFilePrefix + FormatBucketId(bucket_id) +
           kBucketDataSuffix;
}

std::string ImmutableBucketAllocator::BucketMetaPath(int64_t bucket_id) const {
    return fsdir_ + "/" + kBucketFilePrefix + FormatBucketId(bucket_id) +
           kBucketMetaSuffix;
}

tl::expected<void, ErrorCode> ImmutableBucketAllocator::Init(
    const DistributedStorageConfig& config) {
    if (initialized_.load(std::memory_order_acquire)) return {};

    if (!config.ValidateForAllocator()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!config.ValidateForBucketAllocator()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    fsdir_ = config.fsdir;
    fs_adapter_type_ = config.fs_adapter_type;
    bucket_capacity_ = config.bucket_capacity;
    alignment_ = config.alignment;
    max_bucket_count_ = config.max_bucket_count;
    eviction_enabled_ = config.eviction_enabled;
    eviction_high_watermark_ = config.eviction_high_watermark;
    eviction_low_watermark_ = config.eviction_low_watermark;
    eviction_check_interval_ = config.eviction_check_interval;

    std::error_code ec;
    std::filesystem::create_directories(fsdir_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS bucket directory " << fsdir_ << ": "
                   << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (config.fs_adapter_type == "posix") {
        fs_adapter_ = std::make_unique<PosixFsAdapter>();
    } else if (config.fs_adapter_type == "hf3fs") {
#ifdef USE_3FS
        fs_adapter_ = std::make_unique<Hf3fsAdapter>();
#else
        LOG(ERROR) << "The hf3fs DFS adapter requires Mooncake to be built "
                      "with the USE_3FS compile-time option (-DUSE_3FS=ON)";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
    }
    if (!fs_adapter_) {
        LOG(ERROR) << "Unsupported DFS fs adapter type "
                   << config.fs_adapter_type;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto adapter_init = fs_adapter_->Init(fsdir_);
    if (!adapter_init) {
        LOG(ERROR) << "Failed to initialize DFS fs adapter "
                   << config.fs_adapter_type << " for fsdir=" << fsdir_
                   << ", error=" << adapter_init.error();
        fs_adapter_.reset();
        return tl::make_unexpected(adapter_init.error());
    }

    auto recovered = RecoverFromDisk();
    if (!recovered) {
        fs_adapter_->Shutdown();
        fs_adapter_.reset();
        return recovered;
    }

    initialized_.store(true, std::memory_order_release);

    // Recovery may have rewritten in-memory state (for example, entries whose
    // data no longer fits the file). Persist that immediately so the next start
    // reads exactly what this process believes.
    const size_t rewritten = FlushDirtyMetadata();

    LOG(INFO) << "DFS bucket allocator initialized, fsdir=" << fsdir_
              << ", bucket_capacity=" << bucket_capacity_
              << ", alignment=" << alignment_
              << ", max_bucket_count=" << max_bucket_count_
              << ", recovered_buckets=" << buckets_.size()
              << ", recovered_replicas=" << recovered_replicas_.size()
              << ", rewritten_buckets=" << rewritten;
    return {};
}

PersistedBucketMetadata ImmutableBucketAllocator::SnapshotLocked(
    BucketState& bucket, bool evicting) {
    PersistedBucketMetadata snapshot;
    snapshot.version = kBucketMetadataVersion;
    snapshot.bucket_id = bucket.bucket_id;
    snapshot.bucket_generation = bucket.generation;
    snapshot.capacity = bucket.capacity;
    snapshot.alignment = alignment_;
    snapshot.append_offset = bucket.append_offset;
    snapshot.evicting = evicting;
    snapshot.entries.reserve(bucket.entries.size());
    for (const auto& [key, entry] : bucket.entries) {
        if (entry.state != BucketEntryState::COMMITTED) continue;
        PersistedBucketEntry persisted;
        persisted.key = key;
        persisted.entry_offset = entry.entry_offset;
        persisted.key_size = entry.key_size;
        persisted.value_size = entry.value_size;
        persisted.reserved_size = entry.reserved_size;
        persisted.generation = entry.generation;
        snapshot.entries.push_back(std::move(persisted));
    }
    // Deterministic order keeps the serialized bytes (and hence the checksum)
    // stable for identical logical state, which makes tests reproducible.
    std::sort(
        snapshot.entries.begin(), snapshot.entries.end(),
        [](const PersistedBucketEntry& lhs, const PersistedBucketEntry& rhs) {
            if (lhs.entry_offset != rhs.entry_offset) {
                return lhs.entry_offset < rhs.entry_offset;
            }
            return lhs.key < rhs.key;
        });
    snapshot.checksum = ComputeMetadataChecksum(snapshot);
    return snapshot;
}

void ImmutableBucketAllocator::MarkMetaDirtyLocked(BucketState& bucket) {
    // An active bucket has no `.meta` file yet and is not supposed to get one,
    // so there is nothing to keep in sync until it is sealed. Sealing sets the
    // flag itself, which is what makes the first write happen.
    if (bucket.sealed) bucket.meta_dirty = true;
}

void ImmutableBucketAllocator::SealActiveBucketLocked() {
    if (active_bucket_id_ < 0) return;
    auto it = buckets_.find(active_bucket_id_);
    active_bucket_id_ = -1;
    if (it == buckets_.end()) return;
    BucketState& bucket = *it->second;
    if (bucket.sealed) return;
    bucket.sealed = true;
    // The bucket accepts no further allocations, so this is the moment its
    // metadata becomes worth persisting. The write itself happens outside the
    // lock, in FlushDirtyMetadata().
    bucket.meta_dirty = true;
}

tl::expected<void, ErrorCode> ImmutableBucketAllocator::PersistMetadata(
    const PersistedBucketMetadata& snapshot) {
    if (!fs_adapter_) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    std::string payload;
    try {
        struct_pb::to_pb(snapshot, payload);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize bucket metadata, bucket_id="
                   << snapshot.bucket_id << ", error=" << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const std::string meta_path = BucketMetaPath(snapshot.bucket_id);
    bool meta_existed = false;
    auto meta_exists_result = fs_adapter_->FileExists(meta_path);
    if (meta_exists_result) meta_existed = *meta_exists_result;

    // Overwrite the bucket's single `.meta` file in place: one file per bucket
    // is the point, so no temporary file and no rename are involved. A torn
    // rewrite loses this bucket's metadata entirely, and recovery then removes
    // its data file - the accepted failure mode.
    auto write_result = fs_adapter_->WriteFile(
        meta_path, std::span<const char>(payload.data(), payload.size()));
    if (!write_result) {
        LOG(ERROR) << "Failed to write bucket metadata " << meta_path
                   << ", error=" << write_result.error();
        return tl::make_unexpected(write_result.error());
    }
    if (*write_result != payload.size()) {
        LOG(ERROR) << "Short write of bucket metadata " << meta_path
                   << ", expected=" << payload.size()
                   << ", actual=" << *write_result;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    auto sync_file = fs_adapter_->SyncFile(meta_path);
    if (!sync_file) {
        LOG(ERROR) << "Failed to fsync bucket metadata " << meta_path
                   << ", error=" << sync_file.error();
        return tl::make_unexpected(sync_file.error());
    }

    // Creating a directory entry needs one directory sync. Rewriting an existing
    // file changes no namespace metadata and deliberately avoids that DFS lock.
    if (!meta_existed) {
        auto sync_dir = fs_adapter_->SyncDirectory(fsdir_);
        if (!sync_dir) {
            LOG(ERROR) << "Failed to fsync DFS bucket directory " << fsdir_
                       << ", error=" << sync_dir.error();
            return tl::make_unexpected(sync_dir.error());
        }
    }
    return {};
}

void ImmutableBucketAllocator::DeleteBucketFiles(int64_t bucket_id) {
    if (!fs_adapter_) return;
    auto meta_result = fs_adapter_->DeleteFile(BucketMetaPath(bucket_id));
    if (!meta_result && meta_result.error() != ErrorCode::FILE_NOT_FOUND) {
        LOG(ERROR) << "Failed to delete bucket metadata file for bucket_id="
                   << bucket_id << ", error=" << meta_result.error();
    }
    auto data_result = fs_adapter_->DeleteFile(BucketDataPath(bucket_id));
    if (!data_result && data_result.error() != ErrorCode::FILE_NOT_FOUND) {
        LOG(ERROR) << "Failed to delete bucket data file for bucket_id="
                   << bucket_id << ", error=" << data_result.error();
    }
}

// === bucket lifecycle ===

tl::expected<ImmutableBucketAllocator::BucketPtr, ErrorCode>
ImmutableBucketAllocator::CreateBucketUnlocked(std::unique_lock<std::mutex>& lock) {
    if (max_bucket_count_ > 0 &&
        static_cast<int64_t>(buckets_.size()) >= max_bucket_count_) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    if (next_bucket_id_ > kMaxBucketId) {
        LOG(ERROR) << "DFS bucket id space exhausted, next_bucket_id="
                   << next_bucket_id_;
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Only one thread may be creating a bucket at a time. Without this, N
    // threads that all find the active bucket full each reserve a *different*
    // id, then race in the unlocked I/O section below: the last one to relock
    // wins `active_bucket_id_`, and the losers' buckets are published but
    // orphaned. Worse, each loser's rollback path can delete files belonging to
    // an id another thread has already published, which is what made concurrent
    // Allocate() fail with FILE_NOT_FOUND.
    //
    // EnsureActiveBucket guarantees `bucket_creation_in_flight_` is false here:
    // it waits out an in-flight creation and re-checks the active bucket first,
    // so a waiter reuses the new bucket instead of creating a redundant one.
    bucket_creation_in_flight_ = true;
    // Clears the flag and wakes the next waiter on every exit path.
    struct CreationGuard {
        ImmutableBucketAllocator* self;
        ~CreationGuard() {
            self->bucket_creation_in_flight_ = false;
            self->bucket_creation_cv_.notify_all();
        }
    } creation_guard{this};

    const int64_t bucket_id = next_bucket_id_++;
    const uint64_t generation = next_generation_++;

    auto bucket = std::make_shared<BucketState>();
    bucket->bucket_id = bucket_id;
    bucket->generation = generation;
    bucket->capacity = bucket_capacity_;
    bucket->append_offset = 0;
    bucket->live_bytes = 0;
    bucket->last_access_ns = NowNs();

    // Only the data file is created here. The bucket's metadata stays in memory
    // until the bucket is sealed, which is what keeps it down to a single
    // `.meta` file written once instead of a snapshot plus a growing log.
    lock.unlock();
    auto prealloc = fs_adapter_->PreallocateFile(BucketDataPath(bucket_id),
                                                 bucket_capacity_);
    lock.lock();

    if (!prealloc) {
        const ErrorCode error = prealloc.error();
        LOG(ERROR) << "Failed to create DFS bucket " << bucket_id
                   << ", error=" << error;
        // Roll back the id reservation when nothing else claimed it meanwhile,
        // and remove any partially created files.
        if (next_bucket_id_ == bucket_id + 1 && !buckets_.count(bucket_id)) {
            next_bucket_id_ = bucket_id;
        }
        lock.unlock();
        DeleteBucketFiles(bucket_id);
        lock.lock();
        return tl::make_unexpected(error);
    }

    // A concurrent creator may have published this id while we were unlocked.
    auto existing = buckets_.find(bucket_id);
    if (existing != buckets_.end()) {
        LOG(WARNING) << "DFS bucket " << bucket_id
                     << " was published concurrently; reusing existing state";
        return existing->second;
    }

    buckets_.emplace(bucket_id, bucket);
    // The bucket being replaced will never be appended to again, so this is
    // where its metadata becomes final and worth writing out.
    SealActiveBucketLocked();
    active_bucket_id_ = bucket_id;
    TouchLruLocked(bucket_id, bucket->last_access_ns);

    // Write the just-sealed bucket's `.meta` before returning. Deferring it to
    // the maintenance tick would put several sealed buckets at risk of a crash;
    // doing it here bounds the loss to the single bucket that is still active.
    // The cost is one metadata write per bucket rollover, off the per-object
    // path.
    lock.unlock();
    FlushDirtyMetadata();
    lock.lock();
    return bucket;
}

tl::expected<ImmutableBucketAllocator::BucketPtr, ErrorCode>
ImmutableBucketAllocator::EnsureActiveBucket(std::unique_lock<std::mutex>& lock,
                                          uint64_t required) {
    if (required > bucket_capacity_) {
        // Refuse rather than spill across buckets: the caller asked for one
        // contiguous region and no bucket can ever satisfy it.
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Each iteration re-checks the active bucket, so a thread that waited for
    // someone else's creation reuses that bucket instead of adding another. The
    // bound keeps a pathological interleaving from looping forever; in practice
    // one wait plus one creation is the worst case.
    constexpr int kMaxAttempts = 8;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        if (active_bucket_id_ >= 0) {
            auto it = buckets_.find(active_bucket_id_);
            if (it != buckets_.end() && !it->second->frozen) {
                auto& bucket = *it->second;
                auto entry_start =
                    CheckedAlignUp(bucket.append_offset, alignment_);
                if (entry_start && *entry_start <= bucket.capacity &&
                    required <= bucket.capacity - *entry_start) {
                    return it->second;
                }
            }
        }
        if (bucket_creation_in_flight_) {
            // Someone else is already creating one. Wait for it and loop, so we
            // consume their bucket rather than creating a competing one.
            bucket_creation_cv_.wait(lock);
            continue;
        }
        auto created = CreateBucketUnlocked(lock);
        if (!created) return tl::make_unexpected(created.error());
        // Loop once more so the freshly created bucket goes through the same
        // capacity check instead of being trusted blindly.
    }
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
}

void ImmutableBucketAllocator::TouchLruLocked(int64_t bucket_id, int64_t now_ns) {
    auto index_it = lru_index_.find(bucket_id);
    if (index_it != lru_index_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_, index_it->second);
    } else {
        lru_list_.push_front(bucket_id);
        lru_index_[bucket_id] = lru_list_.begin();
    }
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it != buckets_.end()) {
        bucket_it->second->last_access_ns = now_ns;
    }
}

void ImmutableBucketAllocator::RemoveFromLruLocked(int64_t bucket_id) {
    auto index_it = lru_index_.find(bucket_id);
    if (index_it == lru_index_.end()) return;
    lru_list_.erase(index_it->second);
    lru_index_.erase(index_it);
}

tl::expected<DistributedFSDescriptor, ErrorCode>
ImmutableBucketAllocator::ReserveInBucketLocked(BucketState& bucket,
                                             const std::string& key,
                                             uint64_t size) {
    auto layout = ComputeBucketEntryLayout(bucket.append_offset, key.size(),
                                           size, alignment_);
    if (!layout) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (layout->entry_end() > bucket.capacity) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    BucketEntry entry;
    entry.entry_offset = layout->entry_start;
    entry.key_size = key.size();
    entry.value_size = size;
    entry.reserved_size = layout->reserved_size;
    entry.generation = next_generation_++;
    entry.state = BucketEntryState::PENDING;

    bucket.entries[key] = entry;
    bucket.append_offset = layout->entry_end();
    bucket.live_bytes += layout->reserved_size;
    key_index_[key] = bucket.bucket_id;

    // Reservations in the active bucket touch no file at all. A reservation in
    // an already sealed bucket cannot happen (it accepts no allocations), but
    // marking it keeps the invariant local instead of implicit.
    MarkMetaDirtyLocked(bucket);

    return MakeBucketDescriptor(BucketDataPath(bucket.bucket_id), *layout, size,
                                bucket.bucket_id);
}

void ImmutableBucketAllocator::UnreserveInBucketLocked(
    BucketState& bucket, const std::string& key,
    const DistributedFSDescriptor& descriptor) {
    auto entry_it = bucket.entries.find(key);
    if (entry_it == bucket.entries.end()) return;
    const auto& entry = entry_it->second;
    if (entry.value_size != descriptor.object_size ||
        entry.reserved_size != descriptor.aligned_size) {
        return;
    }

    const uint64_t entry_end = entry.entry_offset + entry.reserved_size;
    if (bucket.append_offset == entry_end) {
        // Only the most recent reservation can give its space back, which is
        // exactly how BatchAllocate unwinds (reverse order).
        bucket.append_offset = entry.entry_offset;
    }
    if (bucket.live_bytes >= entry.reserved_size) {
        bucket.live_bytes -= entry.reserved_size;
    } else {
        bucket.live_bytes = 0;
    }

    // The reservation existed only in memory, so undoing it is just erasing it.
    // If the bucket happens to be sealed already, its `.meta` file has to be
    // rewritten without this entry.
    MarkMetaDirtyLocked(bucket);

    bucket.entries.erase(entry_it);

    auto index_it = key_index_.find(key);
    if (index_it != key_index_.end() && index_it->second == bucket.bucket_id) {
        key_index_.erase(index_it);
    }
}

ImmutableBucketAllocator::BucketEntry*
ImmutableBucketAllocator::FindMatchingEntryLocked(
    const std::string& key, const DistributedFSDescriptor& desc,
    BucketPtr* out_bucket) {
    auto fail = [&key, &desc](const char* reason) -> BucketEntry* {
        LOG(ERROR) << "DFS entry match failed: reason=" << reason
                   << ", key=" << key << ", bucket_id=" << desc.shard_idx
                   << ", offset=" << desc.offset;
        return nullptr;
    };

    if (desc.shard_idx < 0) return fail("invalid_bucket_id");
    const int64_t bucket_id = static_cast<int64_t>(desc.shard_idx);

    auto index_it = key_index_.find(key);
    if (index_it == key_index_.end()) return fail("key_not_indexed");
    if (index_it->second != bucket_id)
        return fail("key_index_bucket_mismatch");
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it == buckets_.end()) return fail("bucket_not_found");

    auto entry_it = bucket_it->second->entries.find(key);
    if (entry_it == bucket_it->second->entries.end())
        return fail("bucket_entry_not_found");

    // Match on every layout-defining field so a descriptor from a superseded
    // allocation cannot address the current one.
    auto& entry = entry_it->second;
    if (entry.value_size != desc.object_size)
        return fail("object_size_mismatch");
    if (entry.reserved_size != desc.aligned_size)
        return fail("aligned_size_mismatch");
    if (entry.key_size != key.size()) return fail("key_size_mismatch");
    auto layout = RebuildBucketEntryLayout(entry.entry_offset, entry.key_size,
                                           entry.value_size, alignment_);
    if (!layout) return fail("invalid_entry_layout");
    if (layout->value_offset != desc.offset)
        return fail("value_offset_mismatch");

    if (out_bucket) *out_bucket = bucket_it->second;
    return &entry;
}

// === allocation ===

tl::expected<DistributedFSDescriptor, ErrorCode>
ImmutableBucketAllocator::Allocate(const std::string& key, uint64_t size) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (key.empty() || size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    BatchAllocateRequest request{key, size};
    auto results = BatchAllocate({request});
    if (results.size() != 1) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (!results[0].success) {
        return tl::make_unexpected(results[0].error);
    }
    return results[0].descriptor;
}

std::vector<BatchAllocateResult> ImmutableBucketAllocator::BatchAllocate(
    const std::vector<BatchAllocateRequest>& requests) {
    std::vector<BatchAllocateResult> results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        results.push_back(
            BatchAllocateResult{request.key, {}, false, ErrorCode::OK});
    }
    if (requests.empty()) return results;

    auto fail_all = [&results](ErrorCode error) {
        for (auto& result : results) {
            result.success = false;
            result.error = error;
            result.descriptor = DistributedFSDescriptor{};
        }
    };
    if (!initialized_.load(std::memory_order_acquire)) {
        fail_all(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        return results;
    }

    // Validate request shape before changing allocator state. A batch may span
    // buckets, but one object must always fit in one bucket.
    std::vector<size_t> allocatable;
    allocatable.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.key.empty() || request.size == 0) {
            fail_all(ErrorCode::INVALID_PARAMS);
            return results;
        }
        for (size_t j = 0; j < i; ++j) {
            if (requests[j].key == request.key) {
                LOG(ERROR) << "Duplicate key " << request.key
                           << " in DFS batch allocate request";
                fail_all(ErrorCode::INVALID_PARAMS);
                return results;
            }
        }
        auto layout = ComputeBucketEntryLayout(0, request.key.size(),
                                               request.size, alignment_);
        if (!layout || layout->reserved_size > bucket_capacity_) {
            LOG(ERROR) << "DFS object for key " << request.key
                       << " exceeds bucket capacity, object_size="
                       << request.size << ", reserved_size="
                       << (layout ? layout->reserved_size : 0)
                       << ", bucket_capacity=" << bucket_capacity_;
            fail_all(ErrorCode::INVALID_PARAMS);
            return results;
        }
        allocatable.push_back(i);
    }

    std::unique_lock<std::mutex> lock(mutex_);
    // Existing live keys retain their per-key OBJECT_ALREADY_EXISTS outcome;
    // the remaining requests are packed in their original order.
    allocatable.clear();
    for (size_t i = 0; i < requests.size(); ++i) {
        if (key_index_.count(requests[i].key) != 0) {
            LOG(WARNING) << "DFS batch allocate skipped key " << requests[i].key
                         << ": it already has a live allocation";
            results[i].error = ErrorCode::OBJECT_ALREADY_EXISTS;
            continue;
        }
        allocatable.push_back(i);
    }
    if (allocatable.empty()) return results;

    struct Reservation {
        size_t request_index = 0;
        int64_t bucket_id = -1;
        uint64_t generation = 0;
    };
    std::vector<Reservation> reserved;
    reserved.reserve(allocatable.size());

    auto fail_allocatable = [&]() {
        for (const size_t index : allocatable) {
            results[index].success = false;
            results[index].descriptor = DistributedFSDescriptor{};
            if (results[index].error == ErrorCode::OK) {
                results[index].error = ErrorCode::NO_AVAILABLE_HANDLE;
            }
        }
    };

    // Append-pack in request order. EnsureActiveBucket receives only the
    // current object's reserved size, so a partially filled active bucket is
    // used before a new bucket is created. A bucket boundary can therefore
    // occur only between two objects, never inside one object.
    for (const size_t index : allocatable) {
        const auto& request = requests[index];
        auto object_layout = ComputeBucketEntryLayout(
            0, request.key.size(), request.size, alignment_);
        if (!object_layout) {
            fail_allocatable();
            break;
        }
        auto bucket_result = EnsureActiveBucket(lock,
                                                object_layout->reserved_size);
        if (!bucket_result) {
            fail_allocatable();
            results[index].error = bucket_result.error();
            break;
        }
        auto bucket = bucket_result.value();
        auto descriptor =
            ReserveInBucketLocked(*bucket, request.key, request.size);
        if (!descriptor) {
            fail_allocatable();
            results[index].error = descriptor.error();
            break;
        }
        results[index].descriptor = std::move(descriptor.value());
        results[index].success = true;
        results[index].error = ErrorCode::OK;
        reserved.push_back({index, bucket->bucket_id, bucket->generation});
        TouchLruLocked(bucket->bucket_id, NowNs());
    }

    if (reserved.size() != allocatable.size()) {
        // Roll back in reverse reservation order, across every bucket touched
        // by this batch. Nothing was written to disk, so undoing the in-memory
        // state is all it takes.
        for (auto it = reserved.rbegin(); it != reserved.rend(); ++it) {
            auto bucket_it = buckets_.find(it->bucket_id);
            if (bucket_it != buckets_.end() &&
                bucket_it->second->generation == it->generation) {
                UnreserveInBucketLocked(*bucket_it->second,
                                        requests[it->request_index].key,
                                        results[it->request_index].descriptor);
            }
        }
        lock.unlock();
        fail_allocatable();
        return results;
    }

    // Reservations live in memory only: no metadata I/O happens on this path,
    // which is the whole point of deferring persistence to the moment a bucket
    // is sealed.
    return results;
}

bool ImmutableBucketAllocator::MarkCommitted(
    const std::string& key, const DistributedFSDescriptor& descriptor) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DFS commit rejected: reason=allocator_not_initialized"
                   << ", key=" << key
                   << ", bucket_id=" << descriptor.shard_idx;
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    BucketPtr bucket;
    auto* entry = FindMatchingEntryLocked(key, descriptor, &bucket);
    if (entry == nullptr) return false;
    if (entry->state == BucketEntryState::COMMITTED) {
        // Idempotent: a duplicate PutEnd for the same generation succeeds.
        return true;
    }
    if (entry->state != BucketEntryState::PENDING) {
        LOG(ERROR) << "DFS commit rejected: reason=invalid_entry_state"
                   << ", key=" << key
                   << ", bucket_id=" << descriptor.shard_idx
                   << ", state=" << static_cast<int32_t>(entry->state);
        return false;
    }
    entry->state = BucketEntryState::COMMITTED;
    // PutEnd never touches a file. For the active bucket the transition is
    // simply part of the state that gets written when the bucket is sealed; for
    // an already sealed bucket (a reservation that committed after the switch)
    // the flag schedules a rewrite on the maintenance path.
    MarkMetaDirtyLocked(*bucket);
    return true;
}

void ImmutableBucketAllocator::Free(const std::string& key,
                                 const DistributedFSDescriptor& descriptor) {
    if (!initialized_.load(std::memory_order_acquire)) return;

    std::lock_guard<std::mutex> lock(mutex_);
    BucketPtr bucket;
    auto* entry = FindMatchingEntryLocked(key, descriptor, &bucket);
    if (entry == nullptr) {
        // Stale Free from a superseded generation: ignore it so it cannot
        // drop the allocation that replaced it.
        return;
    }
    if (!IsLive(entry->state)) return;

    // Buckets are append-only, so freeing leaves a tombstone rather than
    // reclaiming the middle of the file. Space comes back only when the
    // whole bucket is evicted.
    entry->state = BucketEntryState::TOMBSTONE;
    ++bucket->tombstones;
    if (bucket->live_bytes >= entry->reserved_size) {
        bucket->live_bytes -= entry->reserved_size;
    } else {
        bucket->live_bytes = 0;
    }
    auto index_it = key_index_.find(key);
    if (index_it != key_index_.end() && index_it->second == bucket->bucket_id) {
        key_index_.erase(index_it);
    }
    // The committed entry must be removed from the persisted snapshot so the
    // key cannot come back, but the master calls Free() while holding a metadata
    // shard lock, so no I/O may happen here. Mark the bucket instead and let
    // FlushDirtyMetadata() rewrite its `.meta` file. For the still-active bucket
    // there is nothing to rewrite yet; tombstones are omitted when it is sealed.
    //
    // Losing a tombstone in a crash before the flush is safe: recovery only
    // revives COMMITTED entries, and the master's own metadata (which no longer
    // references the key) is the authority on visibility.
    MarkMetaDirtyLocked(*bucket);
}

size_t ImmutableBucketAllocator::FlushDirtyMetadata() {
    if (!initialized_.load(std::memory_order_acquire)) return 0;

    // Snapshot every dirty bucket under the lock, then do the writes without it.
    struct PendingWrite {
        int64_t bucket_id = -1;
        uint64_t generation = 0;
        PersistedBucketMetadata snapshot;
    };
    std::vector<PendingWrite> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [bucket_id, bucket] : buckets_) {
            // An unsealed bucket is deliberately never written: it is still
            // being appended to, and its data is discarded on the next start.
            if (!bucket->sealed || !bucket->meta_dirty) continue;
            pending.push_back({bucket_id, bucket->generation,
                               SnapshotLocked(*bucket, /*evicting=*/false)});
            // Clear the flag together with taking the snapshot. A change that
            // lands while we are unlocked sets it again and is picked up by the
            // next flush, instead of being swallowed by a late clear. It also
            // keeps a concurrent flush from writing the same file.
            bucket->meta_dirty = false;
        }
    }
    if (pending.empty()) return 0;

    // Deterministic order keeps concurrent flushes from interleaving writes to
    // the same bucket in an unpredictable sequence.
    std::sort(pending.begin(), pending.end(),
              [](const PendingWrite& lhs, const PendingWrite& rhs) {
                  return lhs.bucket_id < rhs.bucket_id;
              });

    size_t flushed = 0;
    for (const auto& item : pending) {
        auto persisted = PersistMetadata(item.snapshot);
        if (persisted) {
            ++flushed;
            continue;
        }
        LOG(WARNING) << "Failed to write DFS bucket metadata, bucket_id="
                     << item.bucket_id << ", error=" << persisted.error();
        // Put the flag back so the next maintenance tick retries, unless the
        // bucket changed identity meanwhile - then it is not ours to mark.
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = buckets_.find(item.bucket_id);
        if (it != buckets_.end() && it->second->generation == item.generation) {
            it->second->meta_dirty = true;
        }
    }
    return flushed;
}

void ImmutableBucketAllocator::UpdateAccess(
    const std::string& key, const DistributedFSDescriptor& descriptor) {
    if (!initialized_.load(std::memory_order_acquire)) return;

    std::lock_guard<std::mutex> lock(mutex_);
    BucketPtr bucket;
    auto* entry = FindMatchingEntryLocked(key, descriptor, &bucket);
    if (entry == nullptr || !IsLive(entry->state)) return;
    // A frozen bucket is mid-eviction; refreshing it would fight the
    // transaction the master is currently resolving.
    if (bucket->frozen) return;
    TouchLruLocked(bucket->bucket_id, NowNs());
}

uint64_t ImmutableBucketAllocator::GetTotalCapacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    // A fixed denominator: watermarks must not move as buckets come and go,
    // or deleting a bucket would shrink capacity in lockstep with usage and
    // eviction would never converge.
    if (max_bucket_count_ > 0) {
        return static_cast<uint64_t>(max_bucket_count_) * bucket_capacity_;
    }
    return static_cast<uint64_t>(buckets_.size()) * bucket_capacity_;
}

uint64_t ImmutableBucketAllocator::UsedBytesLocked() const {
    // Physical reservation, not live bytes: an append-only bucket keeps its
    // whole reserved prefix until the bucket itself is evicted.
    uint64_t used = 0;
    for (const auto& [bucket_id, bucket] : buckets_) {
        (void)bucket_id;
        used += bucket->append_offset;
    }
    return used;
}

uint64_t ImmutableBucketAllocator::GetUsedBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return UsedBytesLocked();
}

tl::expected<int64_t, ErrorCode>
ImmutableBucketAllocator::SetMaxBucketCount(
    int64_t new_max_bucket_count) {
    if (new_max_bucket_count <= 0 || new_max_bucket_count > kMaxBucketId) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const int64_t old = max_bucket_count_;
    max_bucket_count_ = new_max_bucket_count;
    LOG(INFO) << "Dynamic max_bucket_count changed from "
              << old << " to " << new_max_bucket_count;
    return old;
}

size_t ImmutableBucketAllocator::GetBucketCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return buckets_.size();
}

std::optional<int64_t> ImmutableBucketAllocator::GetBucketIdForKey(
    const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = key_index_.find(key);
    if (it == key_index_.end()) return std::nullopt;
    return it->second;
}

std::vector<ImmutableBucketAllocator::RecoveredReplica>
ImmutableBucketAllocator::TakeRecoveredReplicas() {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::move(recovered_replicas_);
}

// === eviction ===

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEviction() {
    return PrepareEvictionInternal(/*force_one=*/false);
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEvictionForAllocationFailure() {
    return PrepareEvictionInternal(/*force_one=*/true);
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEvictionInternal(bool force_one) {
    PendingEviction pending;
    if (!initialized_.load(std::memory_order_acquire)) return pending;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        const uint64_t capacity =
            max_bucket_count_ > 0
                ? static_cast<uint64_t>(max_bucket_count_) * bucket_capacity_
                : static_cast<uint64_t>(buckets_.size()) * bucket_capacity_;
        if (capacity == 0) return pending;

        const double usage = static_cast<double>(UsedBytesLocked()) /
                             static_cast<double>(capacity);
        if (!force_one) {
            if (usage >= eviction_high_watermark_) {
                eviction_active_ = true;
            }
            if (!eviction_active_) return pending;
            if (usage < eviction_low_watermark_) {
                eviction_active_ = false;
                return pending;
            }
        }

        // Walk the LRU from the cold end and take the first bucket that is
        // neither active nor already frozen.
        BucketPtr victim;
        for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
            const int64_t bucket_id = *it;
            if (bucket_id == active_bucket_id_) continue;
            auto bucket_it = buckets_.find(bucket_id);
            if (bucket_it == buckets_.end()) continue;
            if (bucket_it->second->frozen) continue;
            victim = bucket_it->second;
            break;
        }
        if (!victim) return pending;

        std::vector<GlobalAllocatorInterface::EvictionCandidate> candidates;
        for (const auto& [key, entry] : victim->entries) {
            // Tombstoned entries are already gone from the master's view; only
            // live entries need validating.
            if (!IsLive(entry.state)) continue;
            auto layout = RebuildBucketEntryLayout(
                entry.entry_offset, entry.key_size, entry.value_size,
                alignment_);
            if (!layout) {
                LOG(ERROR) << "Skipping DFS eviction of bucket "
                           << victim->bucket_id << ": entry for key " << key
                           << " has an inconsistent layout";
                return pending;
            }
            GlobalAllocatorInterface::EvictionCandidate candidate;
            candidate.key = key;
            candidate.shard_idx = static_cast<int>(victim->bucket_id);
            candidate.offset = layout->value_offset;
            // Byte-identical to what Allocate handed out, so the master can
            // match replica metadata field by field.
            candidate.descriptor = MakeBucketDescriptor(
                BucketDataPath(victim->bucket_id), *layout, entry.value_size,
                victim->bucket_id);
            candidates.push_back(std::move(candidate));
        }

        victim->frozen = true;
        RemoveFromLruLocked(victim->bucket_id);

        // A frozen bucket takes no further appends, so this is the point where
        // its in-memory state becomes final. Sealing it here means the `.meta`
        // file exists before the master starts validating candidates, which is
        // what lets the commit path publish an eviction marker over it.
        if (!victim->sealed) {
            victim->sealed = true;
            victim->meta_dirty = true;
        }

        pending.owner_ = this;
        pending.bucket_id_ = victim->bucket_id;
        pending.bucket_generation_ = victim->generation;
        pending.candidates_ = std::move(candidates);
    }

    // Write the sealed metadata out before returning, without the lock held.
    FlushDirtyMetadata();
    return pending;
}

void ImmutableBucketAllocator::CommitEviction(PendingEviction&& pending) {
    // Detach first so the destructor of `pending` cannot abort what we commit.
    auto* owner = std::exchange(pending.owner_, nullptr);
    if (owner != this) return;

    const int64_t bucket_id = pending.bucket_id_;
    const uint64_t generation = pending.bucket_generation_;
    pending.candidates_.clear();
    if (bucket_id < 0) return;

    PersistedBucketMetadata marker;
    bool have_marker = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = buckets_.find(bucket_id);
        if (it == buckets_.end() || it->second->generation != generation) {
            return;
        }
        marker = SnapshotLocked(*it->second, /*evicting=*/true);
        have_marker = true;
    }

    // Publish a durable "this bucket is being evicted" marker before deleting
    // anything. If we crash between the marker and the deletes, recovery sees
    // the marker and treats the bucket as gone instead of resurrecting entries
    // whose data file may already be missing.
    if (have_marker) {
        auto persisted = PersistMetadata(marker);
        if (!persisted) {
            LOG(ERROR) << "Failed to persist DFS eviction marker for bucket "
                       << bucket_id << ", error=" << persisted.error()
                       << "; the bucket is already invisible to readers and "
                          "will be reclaimed on a later attempt";
        }
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = buckets_.find(bucket_id);
        if (it == buckets_.end() || it->second->generation != generation) {
            return;
        }
        for (const auto& [key, entry] : it->second->entries) {
            if (!IsLive(entry.state)) continue;
            auto index_it = key_index_.find(key);
            if (index_it != key_index_.end() && index_it->second == bucket_id) {
                key_index_.erase(index_it);
            }
        }
        buckets_.erase(it);
        RemoveFromLruLocked(bucket_id);
        if (active_bucket_id_ == bucket_id) active_bucket_id_ = -1;
    }

    // File deletion happens last and outside the lock. A failure here leaks
    // space but can no longer produce a dangling read, because the master has
    // already dropped every replica in this bucket.
    DeleteBucketFiles(bucket_id);
    LOG(INFO) << "Evicted DFS bucket " << bucket_id;
}

void ImmutableBucketAllocator::AbortEviction(PendingEviction&& pending) {
    AbortEviction(std::move(pending), /*demote=*/true);
}

void ImmutableBucketAllocator::AbortEviction(PendingEviction&& pending,
                                          bool demote) {
    auto* owner = std::exchange(pending.owner_, nullptr);
    if (owner != this) return;

    const int64_t bucket_id = pending.bucket_id_;
    const uint64_t generation = pending.bucket_generation_;
    pending.candidates_.clear();
    if (bucket_id < 0) return;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = buckets_.find(bucket_id);
    if (it == buckets_.end() || it->second->generation != generation) return;
    it->second->frozen = false;
    if (lru_index_.find(bucket_id) != lru_index_.end()) return;

    if (demote) {
        // The master actively rejected this bucket, so put it back at the warm
        // end: PrepareEviction scans from the cold end, and reinserting there
        // would hand back the same rejected bucket on every subsequent round,
        // never letting the scan reach another candidate.
        lru_list_.push_front(bucket_id);
        lru_index_[bucket_id] = lru_list_.begin();
    } else {
        // The transaction was dropped without a verdict (destructor or move
        // assignment). Nothing judged the bucket, so restore its original cold
        // position and let the next round reconsider it.
        lru_list_.push_back(bucket_id);
        lru_index_[bucket_id] = std::prev(lru_list_.end());
    }
}

// === recovery ===

tl::expected<void, ErrorCode> ImmutableBucketAllocator::RecoverFromDisk() {
    auto files = fs_adapter_->ListFiles(fsdir_);
    if (!files) {
        if (files.error() == ErrorCode::FILE_NOT_FOUND) return {};
        LOG(ERROR) << "Failed to list DFS bucket directory " << fsdir_
                   << ", error=" << files.error();
        return tl::make_unexpected(files.error());
    }

    std::vector<int64_t> meta_ids;
    std::vector<int64_t> data_ids;
    for (const auto& name : *files) {
        if (auto id = ParseBucketFileName(name, kBucketMetaSuffix)) {
            meta_ids.push_back(*id);
        } else if (auto data_id =
                       ParseBucketFileName(name, kBucketDataSuffix)) {
            data_ids.push_back(*data_id);
        }
        // Anything else - including `.meta.0`/`.meta.1` snapshot slots,
        // `.meta.log` and `.meta.tmp.*` leftovers from older layouts - is
        // deliberately ignored.
    }
    std::sort(meta_ids.begin(), meta_ids.end());

    int64_t max_seen_id = -1;
    uint64_t max_generation = 0;
    // Buckets already reclaimed by this pass: their metadata could not be
    // trusted, so both files were deleted. A cache entry that cannot be proven
    // correct is worse than a miss - the reader would recompute it anyway, while
    // serving unverifiable bytes would silently corrupt the result. Tracked only
    // so the orphan sweep below does not report them a second time.
    std::unordered_set<int64_t> discarded_ids;
    // key -> (generation, bucket_id): resolves the same key appearing in more
    // than one bucket by keeping the newest committed generation.
    std::unordered_map<std::string, std::pair<uint64_t, int64_t>> winners;

    for (const int64_t bucket_id : meta_ids) {
        max_seen_id = std::max(max_seen_id, bucket_id);

        const std::string meta_path = BucketMetaPath(bucket_id);
        PersistedBucketMetadata snapshot;
        bool valid = false;
        auto file_size = fs_adapter_->GetFileSize(meta_path);
        if (file_size) {
            std::string payload(*file_size, '\0');
            bool read_ok = true;
            if (*file_size > 0) {
                auto read = fs_adapter_->ReadFile(meta_path, payload.data(),
                                                  payload.size());
                read_ok = read && *read == payload.size();
            }
            if (read_ok) {
                try {
                    struct_pb::from_pb(snapshot, payload);
                    valid = snapshot.version == kBucketMetadataVersion &&
                            ComputeMetadataChecksum(snapshot) ==
                                snapshot.checksum;
                } catch (...) {
                    valid = false;
                }
            }
        }
        if (!valid || snapshot.bucket_id != bucket_id) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": no valid metadata snapshot, so none of its data can"
                          " be proven correct";
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }

        // Account for the generation before any of the checks below can reject
        // the bucket: stale descriptors handed out by a previous run must never
        // be confused with a generation this run allocates.
        max_generation =
            std::max(max_generation, snapshot.bucket_generation + 1);

        if (snapshot.alignment != alignment_) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": metadata alignment " << snapshot.alignment
                       << " does not match configured " << alignment_
                       << ", so entry offsets cannot be recomputed";
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }
        if (snapshot.capacity == 0 || snapshot.capacity > bucket_capacity_) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": metadata capacity " << snapshot.capacity
                       << " is incompatible with configured "
                       << bucket_capacity_;
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }
        if (snapshot.append_offset > snapshot.capacity) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": append_offset " << snapshot.append_offset
                       << " exceeds capacity " << snapshot.capacity;
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }
        if (snapshot.evicting) {
            // The marker says the data file was being deleted; finish the job
            // rather than exposing entries whose data may already be gone.
            LOG(INFO) << "Completing interrupted eviction of DFS bucket "
                      << bucket_id;
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }

        auto data_size = fs_adapter_->GetFileSize(BucketDataPath(bucket_id));
        if (!data_size) {
            // Either the data file is gone or it cannot even be sized. Both
            // leave the metadata describing bytes nobody can verify, so the
            // whole bucket goes away instead of being reported every restart.
            LOG(WARNING) << "Discarding DFS bucket " << bucket_id
                         << ": data file is missing or unreadable, error="
                         << data_size.error();
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }

        auto bucket = std::make_shared<BucketState>();
        bucket->bucket_id = bucket_id;
        bucket->generation = snapshot.bucket_generation;
        bucket->capacity = snapshot.capacity;
        bucket->append_offset = 0;
        bucket->live_bytes = 0;
        bucket->last_access_ns = NowNs();
        // Everything on disk was written at seal time, so a recovered bucket is
        // sealed by definition and its `.meta` already matches this state.
        bucket->sealed = true;
        bucket->meta_dirty = false;

        bool bucket_ok = true;
        for (const auto& persisted : snapshot.entries) {
            max_generation = std::max(max_generation, persisted.generation + 1);
            if (persisted.key.empty() ||
                persisted.key_size != persisted.key.size()) {
                LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                           << ": entry key size mismatch";
                bucket_ok = false;
                break;
            }
            auto layout = RebuildBucketEntryLayout(persisted.entry_offset,
                                                   persisted.key_size,
                                                   persisted.value_size,
                                                   alignment_);
            if (!layout || layout->reserved_size != persisted.reserved_size ||
                layout->entry_end() > snapshot.capacity) {
                LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                           << ": entry for key " << persisted.key
                           << " has an out-of-range or inconsistent layout";
                bucket_ok = false;
                break;
            }

            BucketEntry entry;
            entry.entry_offset = persisted.entry_offset;
            entry.key_size = persisted.key_size;
            entry.value_size = persisted.value_size;
            entry.reserved_size = persisted.reserved_size;
            entry.generation = persisted.generation;
            // v5 snapshots contain committed entries only.
            entry.state = BucketEntryState::COMMITTED;

            // Reconstruct the occupied extent; append_offset below also keeps
            // space used by omitted pending and tombstoned entries reserved.
            bucket->append_offset =
                std::max(bucket->append_offset, layout->entry_end());
            bucket->entries[persisted.key] = entry;
        }
        if (!bucket_ok) {
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }

        // The persisted append_offset is authoritative when it is at least as
        // large as what the entries imply (it also covers rolled-back space).
        bucket->append_offset =
            std::max(bucket->append_offset, snapshot.append_offset);

        if (bucket->append_offset > bucket->capacity) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": persisted append_offset " << bucket->append_offset
                       << " exceeds capacity " << bucket->capacity;
            DeleteBucketFiles(bucket_id);
            discarded_ids.insert(bucket_id);
            continue;
        }

        // Validate committed entries restored from the snapshot against the
        // data file. Pending reservations and tombstones were not serialized.
        for (auto& [key, entry] : bucket->entries) {
            auto layout = RebuildBucketEntryLayout(entry.entry_offset,
                                                   entry.key_size,
                                                   entry.value_size,
                                                   alignment_);
            if (!layout ||
                layout->entry_end() > static_cast<uint64_t>(*data_size)) {
                LOG(ERROR) << "Dropping committed DFS entry for key " << key
                           << " in bucket " << bucket_id
                           << ": it extends past the data file end";
                entry.state = BucketEntryState::TOMBSTONE;
                ++bucket->tombstones;
                continue;
            }
            bucket->live_bytes += entry.reserved_size;
        }

        for (const auto& [key, entry] : bucket->entries) {
            if (entry.state != BucketEntryState::COMMITTED) continue;
            auto winner_it = winners.find(key);
            if (winner_it == winners.end()) {
                winners[key] = {entry.generation, bucket_id};
            } else if (entry.generation > winner_it->second.first) {
                LOG(WARNING) << "DFS key " << key << " found in buckets "
                             << winner_it->second.second << " and " << bucket_id
                             << "; keeping the newer generation";
                winner_it->second = {entry.generation, bucket_id};
            } else {
                LOG(WARNING) << "DFS key " << key << " in bucket " << bucket_id
                             << " is superseded by bucket "
                             << winner_it->second.second;
            }
        }

        buckets_.emplace(bucket_id, std::move(bucket));
    }

    // Drop entries that lost the duplicate-key race, then index the winners.
    for (auto& [bucket_id, bucket] : buckets_) {
        for (auto& [key, entry] : bucket->entries) {
            if (entry.state != BucketEntryState::COMMITTED) continue;
            auto winner_it = winners.find(key);
            if (winner_it == winners.end() ||
                winner_it->second.second != bucket_id) {
                entry.state = BucketEntryState::TOMBSTONE;
                ++bucket->tombstones;
                if (bucket->live_bytes >= entry.reserved_size) {
                    bucket->live_bytes -= entry.reserved_size;
                } else {
                    bucket->live_bytes = 0;
                }
                continue;
            }
            key_index_[key] = bucket_id;

            auto layout = RebuildBucketEntryLayout(
                entry.entry_offset, entry.key_size, entry.value_size,
                alignment_);
            if (!layout) continue;
            recovered_replicas_.push_back(RecoveredReplica{
                key, MakeBucketDescriptor(BucketDataPath(bucket_id), *layout,
                                          entry.value_size, bucket_id)});
        }
    }

    // A data file with no `.meta` at all is the signature of a crash while the
    // bucket was still active: its metadata only lived in memory, so nothing can
    // ever address the data again. Reclaim it. Buckets rejected above were
    // already deleted, so they are skipped here to avoid a duplicate log line.
    for (const int64_t data_id : data_ids) {
        max_seen_id = std::max(max_seen_id, data_id);
        if (buckets_.count(data_id) > 0) continue;
        if (discarded_ids.count(data_id) > 0) continue;
        LOG(WARNING) << "Removing orphaned DFS bucket data file for bucket_id="
                     << data_id << " (no metadata, lost on an unclean restart)";
        DeleteBucketFiles(data_id);
    }

    next_bucket_id_ = max_seen_id + 1;
    next_generation_ = std::max<uint64_t>(1, max_generation);

    // Every recovered bucket is sealed, so none of them is resumed: the next
    // allocation opens a fresh bucket. This is what keeps a sealed bucket's
    // `.meta` final, written once when the bucket stopped growing. The cost is
    // the unused tail of the bucket that was active before the restart.
    active_bucket_id_ = -1;

    // Seed the LRU newest-first so recovered buckets have a defined order.
    std::vector<int64_t> ordered;
    ordered.reserve(buckets_.size());
    for (const auto& [bucket_id, bucket] : buckets_) {
        (void)bucket;
        ordered.push_back(bucket_id);
    }
    std::sort(ordered.begin(), ordered.end());
    for (const int64_t bucket_id : ordered) {
        lru_list_.push_front(bucket_id);
        lru_index_[bucket_id] = lru_list_.begin();
    }

    return {};
}

}  // namespace mooncake
