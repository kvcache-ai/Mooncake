#include "storage/distributed/dfs_global_allocator.h"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <ylt/struct_pb.hpp>

#include "crc32c.h"
#include "serializer.h"
#include "config/distributed_storage_config.h"
#include "storage/distributed/posix_fs_adapter.h"
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {
namespace {

constexpr uint32_t kCheckpointVersion = 1;
constexpr uint32_t kWalVersion = 1;
constexpr uint16_t kWalAllocate = 1;
constexpr uint16_t kWalRelease = 2;
constexpr size_t kMaxMetadataFileSize = 512ULL * 1024 * 1024;
constexpr size_t kMaxWalFrameSize = 16ULL * 1024 * 1024;
constexpr char kCheckpointMagic[8] = {'M', 'C', 'D', 'F', 'S', 'M', '0', '2'};
constexpr char kWalMagic[8] = {'M', 'C', 'D', 'F', 'S', 'W', '0', '1'};
constexpr char kWalFrameMagic[4] = {'D', 'F', 'W', '1'};

struct CheckpointHeader {
    char magic[8];
    uint32_t version = 0;
    uint32_t header_size = 0;
    uint64_t payload_size = 0;
    uint32_t payload_crc32c = 0;
    uint32_t reserved = 0;
};
static_assert(sizeof(CheckpointHeader) == 32);

struct WalFileHeader {
    char magic[8];
    uint32_t version = 0;
    uint32_t header_size = 0;
    uint64_t generation = 0;
    uint64_t base_sequence = 0;
    uint32_t header_crc32c = 0;
    uint32_t reserved = 0;
};
static_assert(sizeof(WalFileHeader) == 40);

struct WalFrameHeader {
    char magic[4];
    uint16_t version = 0;
    uint16_t type = 0;
    uint64_t sequence = 0;
    uint64_t payload_size = 0;
    uint32_t payload_crc32c = 0;
    uint32_t header_crc32c = 0;
};
static_assert(sizeof(WalFrameHeader) == 32);

struct PersistedAllocation {
    std::string key;
    uint64_t raw_offset = 0;
    uint64_t offset = 0;
    uint64_t object_size = 0;
    uint64_t aligned_size = 0;
    uint64_t requested_size = 0;
    uint64_t reserved_bytes = 0;
};
YLT_REFL(PersistedAllocation, key, raw_offset, offset, object_size,
         aligned_size, requested_size, reserved_bytes);

struct PersistedPendingFree {
    uint64_t raw_offset = 0;
    uint64_t requested_size = 0;
    uint64_t reserved_bytes = 0;
};
YLT_REFL(PersistedPendingFree, raw_offset, requested_size, reserved_bytes);

struct PersistedShard {
    uint32_t version = kCheckpointVersion;
    std::string configuration_fingerprint;
    uint64_t generation = 0;
    uint64_t base_sequence = 0;
    int32_t shard_idx = 0;
    int32_t shard_count = 0;
    uint64_t capacity = 0;
    uint64_t alignment = 0;
    std::string data_path;
    std::string allocator_state;
    std::vector<PersistedAllocation> allocations;
    std::vector<PersistedPendingFree> pending_frees;
};
YLT_REFL(PersistedShard, version, configuration_fingerprint, generation,
         base_sequence, shard_idx, shard_count, capacity, alignment, data_path,
         allocator_state, allocations, pending_frees);

struct WalAllocatePayload {
    PersistedAllocation allocation;
};
YLT_REFL(WalAllocatePayload, allocation);

struct WalReleasePayload {
    std::vector<uint64_t> raw_offsets;
};
YLT_REFL(WalReleasePayload, raw_offsets);

struct UsedNode {
    uint64_t size = 0;
    uint32_t index = 0;
};

uint32_t WalFileHeaderCrc(WalFileHeader header) {
    header.header_crc32c = 0;
    return Crc32cValue(&header, sizeof(header));
}

uint32_t WalFrameHeaderCrc(WalFrameHeader header) {
    header.header_crc32c = 0;
    return Crc32cValue(&header, sizeof(header));
}

std::vector<char> BuildWalFileHeader(uint64_t generation,
                                     uint64_t base_sequence) {
    WalFileHeader header{};
    std::memcpy(header.magic, kWalMagic, sizeof(header.magic));
    header.version = kWalVersion;
    header.header_size = sizeof(header);
    header.generation = generation;
    header.base_sequence = base_sequence;
    header.header_crc32c = WalFileHeaderCrc(header);
    std::vector<char> bytes(sizeof(header));
    std::memcpy(bytes.data(), &header, sizeof(header));
    return bytes;
}

tl::expected<std::vector<char>, ErrorCode> ReadWholeFile(
    FileSystemAdapter& adapter, const std::string& path, size_t max_size) {
    auto size = adapter.GetFileSize(path);
    if (!size) return tl::make_unexpected(size.error());
    if (*size == 0 || *size > max_size) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    std::vector<char> result(*size);
    auto read = adapter.ReadFile(path, result.data(), result.size());
    if (!read || *read != result.size()) {
        return tl::make_unexpected(read ? ErrorCode::FILE_READ_FAIL
                                        : read.error());
    }
    return result;
}

template <typename T>
tl::expected<std::string, ErrorCode> EncodePb(const T& value) {
    std::string payload;
    try {
        struct_pb::to_pb(value, payload);
    } catch (...) {
        return tl::make_unexpected(ErrorCode::SERIALIZE_FAIL);
    }
    if (payload.empty() || payload.size() > kMaxWalFrameSize) {
        return tl::make_unexpected(ErrorCode::SERIALIZE_FAIL);
    }
    return payload;
}

template <typename T>
tl::expected<T, ErrorCode> DecodePb(std::string_view payload) {
    T value;
    try {
        struct_pb::from_pb(value, payload);
    } catch (...) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    return value;
}

tl::expected<std::vector<char>, ErrorCode> BuildWalFrame(
    uint16_t type, uint64_t sequence, const std::string& payload) {
    if (payload.empty() || payload.size() > kMaxWalFrameSize) {
        return tl::make_unexpected(ErrorCode::SERIALIZE_FAIL);
    }
    WalFrameHeader header{};
    std::memcpy(header.magic, kWalFrameMagic, sizeof(header.magic));
    header.version = kWalVersion;
    header.type = type;
    header.sequence = sequence;
    header.payload_size = payload.size();
    header.payload_crc32c = Crc32cValue(payload.data(), payload.size());
    header.header_crc32c = WalFrameHeaderCrc(header);

    std::vector<char> frame(sizeof(header) + payload.size());
    std::memcpy(frame.data(), &header, sizeof(header));
    std::memcpy(frame.data() + sizeof(header), payload.data(), payload.size());
    return frame;
}

}  // namespace

DfsGlobalAllocator::PendingEviction::~PendingEviction() {
    if (owner_ != nullptr) {
        owner_->RestorePreparedEviction(std::move(*this));
    }
}

DfsGlobalAllocator::PendingEviction::PendingEviction(
    PendingEviction&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      candidates_(std::move(other.candidates_)),
      prepared_(std::move(other.prepared_)) {}

DfsGlobalAllocator::~DfsGlobalAllocator() {
    initialized_.store(false, std::memory_order_release);
    recovery_ready_.store(false, std::memory_order_release);
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::Init(
    const DistributedStorageConfig& config, bool defer_recovery) {
    std::lock_guard expansion_lock(expansion_mutex_);
    if (initialized_.load(std::memory_order_acquire)) return {};
    if (!config.ValidateForAllocator() ||
        config.shard_capacity >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        config.shard_capacity > std::numeric_limits<uint64_t>::max() /
                                    static_cast<uint64_t>(config.shard_count)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    mount_path_ =
        std::filesystem::path(config.fsdir).lexically_normal().string();
    fs_adapter_type_ = config.fs_adapter_type;
    metadata_namespace_ = config.metadata_namespace;
    metadata_checkpoint_interval_ = config.metadata_checkpoint_interval;
    metadata_wal_compaction_threshold_bytes_ =
        config.metadata_wal_compaction_threshold_bytes;
    shard_capacity_ = config.shard_capacity;
    alignment_ = config.alignment;
    eviction_enabled_ = config.eviction_enabled;
    eviction_high_watermark_ = config.eviction_high_watermark;
    eviction_low_watermark_ = config.eviction_low_watermark;
    deferred_free_duration_ = config.deferred_free_duration;
    eviction_check_interval_ = config.eviction_check_interval;

    std::error_code ec;
    std::filesystem::create_directories(mount_path_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS mount path " << mount_path_ << ": "
                   << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (fs_adapter_) fs_adapter_->Shutdown();
    if (config.fs_adapter_type == "posix") {
        fs_adapter_ = std::make_unique<PosixFsAdapter>();
    } else if (config.fs_adapter_type == "hf3fs") {
#ifdef USE_3FS
        fs_adapter_ = std::make_unique<Hf3fsAdapter>();
#else
        LOG(ERROR) << "The hf3fs DFS adapter requires Mooncake to be built "
                      "with the USE_3FS compile-time option "
                      "(-DUSE_3FS=ON)";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
    }

    auto adapter_init = fs_adapter_->Init(mount_path_);
    if (!adapter_init) {
        LOG(ERROR) << "Failed to initialize DFS fs adapter "
                   << config.fs_adapter_type
                   << " for mount_path=" << mount_path_
                   << ", error=" << adapter_init.error();
        return tl::make_unexpected(adapter_init.error());
    }

    // Discover the capacity layout before recovering allocator sidecars.
    // Keep the exact paths of older layouts whose padding depended on count.
    auto files = fs_adapter_->ListFiles(mount_path_);
    if (!files) return tl::make_unexpected(files.error());
    std::map<int, std::string> existing_paths;
    constexpr std::string_view prefix = "dfs_shard_";
    constexpr std::string_view suffix = ".data";
    for (const auto& name : *files) {
        std::string_view view(name);
        if (!view.starts_with(prefix) || !view.ends_with(suffix)) continue;
        const auto digits = view.substr(
            prefix.size(), view.size() - prefix.size() - suffix.size());
        int index = -1;
        const auto [end, error] = std::from_chars(
            digits.data(), digits.data() + digits.size(), index);
        if (digits.size() < 2 || digits.front() < '0' || digits.front() > '9' ||
            error != std::errc{} || end != digits.data() + digits.size() ||
            index < 0 || index == std::numeric_limits<int>::max() ||
            !existing_paths.emplace(index, mount_path_ + "/" + name).second) {
            LOG(ERROR) << "Invalid or ambiguous DFS shard filename " << name;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    int discovered_count = 0;
    for (const auto& [index, path] : existing_paths) {
        if (index != discovered_count++) {
            LOG(ERROR) << "DFS shard layout is not contiguous at " << path;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    const int target_count = std::max(config.shard_count, discovered_count);
    if (shard_capacity_ > std::numeric_limits<uint64_t>::max() /
                              static_cast<uint64_t>(target_count)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<std::string> created_files;
    try {
        auto ready = std::make_shared<ShardList>();
        for (int i = 0; i < target_count; ++i) {
            const auto existing = existing_paths.find(i);
            const std::string path = existing == existing_paths.end()
                                         ? mount_path_ + "/dfs_shard_" +
                                               FormatShardIdx(i, 0) + ".data"
                                         : existing->second;
            auto shard = CreateShard(i, path, created_files);
            if (!shard) {
                CleanupCreatedFiles(created_files);
                return tl::make_unexpected(shard.error());
            }
            ready->push_back(std::move(*shard));
        }
        std::atomic_store_explicit(
            &shards_, std::shared_ptr<const ShardList>(std::move(ready)),
            std::memory_order_release);
    } catch (const std::exception& error) {
        CleanupCreatedFiles(created_files);
        LOG(ERROR) << "Failed to initialize DFS shards: " << error.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    recovery_ready_.store(!defer_recovery, std::memory_order_release);
    initialized_.store(true, std::memory_order_release);
    return {};
}

int DfsGlobalAllocator::GetShardCount() const {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    return shards ? static_cast<int>(shards->size()) : 0;
}

tl::expected<int, ErrorCode> DfsGlobalAllocator::ExpandShards(
    int target_count) {
    std::lock_guard expansion_lock(expansion_mutex_);
    if (!initialized_.load(std::memory_order_acquire) ||
        !recovery_ready_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    const auto current =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (target_count <= 0 || target_count < static_cast<int>(current->size()) ||
        shard_capacity_ > std::numeric_limits<uint64_t>::max() /
                              static_cast<uint64_t>(target_count)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (target_count == static_cast<int>(current->size())) return target_count;

    std::vector<std::string> created_files;
    try {
        auto ready = std::make_shared<ShardList>(*current);
        for (int i = static_cast<int>(current->size()); i < target_count; ++i) {
            const std::string path =
                mount_path_ + "/dfs_shard_" + FormatShardIdx(i, 0) + ".data";
            auto shard = CreateShard(i, path, created_files);
            if (!shard) {
                // Files from before this operation are never truncated or
                // removed. Failed cleanup can leave a ready file for retry,
                // but no staged shard is allocatable before publication.
                CleanupCreatedFiles(created_files);
                return tl::make_unexpected(shard.error());
            }
            ready->push_back(std::move(*shard));
        }
        std::atomic_store_explicit(
            &shards_, std::shared_ptr<const ShardList>(std::move(ready)),
            std::memory_order_release);
    } catch (const std::exception& error) {
        CleanupCreatedFiles(created_files);
        LOG(ERROR) << "Failed to expand DFS shards: " << error.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return target_count;
}

tl::expected<std::shared_ptr<DfsGlobalAllocator::ShardState>, ErrorCode>
DfsGlobalAllocator::CreateShard(int shard_idx, const std::string& path,
                                std::vector<std::string>& created_files) {
    auto owned = std::make_shared<ShardState>();
    auto& shard = *owned;
    shard.path = path;
    shard.capacity = shard_capacity_;
    const uint32_t init_cap = static_cast<uint32_t>(std::max<uint64_t>(
        1, std::min<uint64_t>(shard.capacity / 4096, 64ULL * 1024)));
    const uint32_t max_cap = static_cast<uint32_t>(std::max<uint64_t>(
        init_cap,
        std::min<uint64_t>(shard.capacity / 1024, 64ULL * 1024 * 1024)));
    shard.allocator =
        OffsetAllocator::create(0, shard.capacity, init_cap, max_cap);
    if (!shard.allocator) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto data_exists = fs_adapter_->FileExists(path);
    auto meta_exists = fs_adapter_->FileExists(ShardMetadataPath(shard));
    auto wal_exists = fs_adapter_->FileExists(ShardWalPath(shard));
    if (!data_exists || !meta_exists || !wal_exists)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    if (*data_exists) {
        std::error_code ec;
        if (!std::filesystem::is_regular_file(path, ec))
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        auto size = fs_adapter_->GetFileSize(path);
        if (!size) return tl::make_unexpected(size.error());
        if (*size != shard.capacity)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const int exists_count = static_cast<int>(*data_exists) +
                             static_cast<int>(*meta_exists) +
                             static_cast<int>(*wal_exists);
    tl::expected<void, ErrorCode> result;
    if (exists_count == 0) {
        created_files.push_back(path);
        created_files.push_back(ShardMetadataPath(shard));
        created_files.push_back(ShardWalPath(shard));
        result = CreateFreshShard(shard, shard_idx);
    } else if (exists_count == 3) {
        result = LoadShard(shard, shard_idx);
    } else {
        LOG(ERROR) << "Incomplete DFS allocator sidecar for shard=" << shard_idx
                   << "; Phase-1 data directories are not auto-migrated";
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    if (!result) return tl::make_unexpected(result.error());
    return owned;
}

void DfsGlobalAllocator::CleanupCreatedFiles(
    const std::vector<std::string>& created_files) {
    for (const auto& path : created_files) {
        auto result = fs_adapter_->DeleteFile(path);
        if (!result && result.error() != ErrorCode::FILE_NOT_FOUND) {
            LOG(WARNING) << "Failed to clean up unpublished DFS shard " << path
                         << ": " << result.error();
        }
    }
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::CreateFreshShard(
    ShardState& shard, int shard_idx) {
    auto preallocate =
        fs_adapter_->PreallocateFile(ShardDataPath(shard), shard.capacity);
    if (!preallocate) return tl::make_unexpected(preallocate.error());

    auto checkpoint = SaveCheckpointLocked(shard, shard_idx, 1, 0);
    if (!checkpoint) return checkpoint;
    auto wal_header = BuildWalFileHeader(1, 0);
    auto wal_write = fs_adapter_->AtomicWriteFile(
        ShardWalPath(shard),
        std::span<const char>(wal_header.data(), wal_header.size()));
    if (!wal_write) return wal_write;
    shard.generation = 1;
    shard.base_sequence = 0;
    shard.next_sequence = 1;
    shard.wal_bytes = wal_header.size();
    shard.last_checkpoint = std::chrono::steady_clock::now();
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::LoadShard(ShardState& shard,
                                                            int shard_idx) {
    auto data_size = fs_adapter_->GetFileSize(ShardDataPath(shard));
    if (!data_size || *data_size != shard.capacity) {
        LOG(ERROR) << "DFS data file size mismatch for shard=" << shard_idx;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    auto checkpoint = LoadCheckpointLocked(shard, shard_idx);
    if (!checkpoint) return checkpoint;
    auto wal = LoadWalLocked(shard, shard_idx);
    if (!wal) return wal;
    shard.last_checkpoint = std::chrono::steady_clock::now();
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::SaveCheckpointLocked(
    ShardState& shard, int shard_idx, uint64_t generation,
    uint64_t base_sequence) {
    std::vector<SerializedByte> allocator_bytes;
    const auto serialized = serialize_to(*shard.allocator, allocator_bytes);
    if (serialized != ErrorCode::OK || allocator_bytes.empty()) {
        return tl::make_unexpected(serialized == ErrorCode::OK
                                       ? ErrorCode::SERIALIZE_FAIL
                                       : serialized);
    }

    PersistedShard persisted;
    persisted.configuration_fingerprint = ConfigurationFingerprint(shard);
    persisted.generation = generation;
    persisted.base_sequence = base_sequence;
    persisted.shard_idx = shard_idx;
    persisted.capacity = shard.capacity;
    persisted.alignment = alignment_;
    persisted.data_path = ShardDataPath(shard);
    persisted.allocator_state.assign(
        reinterpret_cast<const char*>(allocator_bytes.data()),
        allocator_bytes.size());
    persisted.allocations.reserve(shard.offset_to_handle.size());
    for (const auto& [offset, record] : shard.offset_to_handle) {
        persisted.allocations.push_back(
            {record.key, record.raw_offset, offset, record.object_size,
             record.aligned_size, record.requested_size, record.bytes});
    }
    std::sort(persisted.allocations.begin(), persisted.allocations.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.raw_offset < rhs.raw_offset;
              });
    persisted.pending_frees.reserve(shard.pending_free.size());
    for (const auto& pending : shard.pending_free) {
        persisted.pending_frees.push_back(
            {pending.raw_offset, pending.requested_size, pending.bytes});
    }
    std::sort(persisted.pending_frees.begin(), persisted.pending_frees.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.raw_offset < rhs.raw_offset;
              });

    std::string payload;
    try {
        struct_pb::to_pb(persisted, payload);
    } catch (...) {
        return tl::make_unexpected(ErrorCode::SERIALIZE_FAIL);
    }
    if (payload.empty() || payload.size() > kMaxMetadataFileSize) {
        return tl::make_unexpected(ErrorCode::SERIALIZE_FAIL);
    }
    CheckpointHeader header{};
    std::memcpy(header.magic, kCheckpointMagic, sizeof(header.magic));
    header.version = kCheckpointVersion;
    header.header_size = sizeof(header);
    header.payload_size = payload.size();
    header.payload_crc32c = Crc32cValue(payload.data(), payload.size());
    std::vector<char> contents(sizeof(header) + payload.size());
    std::memcpy(contents.data(), &header, sizeof(header));
    std::memcpy(contents.data() + sizeof(header), payload.data(),
                payload.size());
    return fs_adapter_->AtomicWriteFile(
        ShardMetadataPath(shard),
        std::span<const char>(contents.data(), contents.size()));
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::LoadCheckpointLocked(
    ShardState& shard, int shard_idx) {
    auto contents = ReadWholeFile(*fs_adapter_, ShardMetadataPath(shard),
                                  kMaxMetadataFileSize);
    if (!contents || contents->size() < sizeof(CheckpointHeader)) {
        return tl::make_unexpected(contents ? ErrorCode::FILE_READ_FAIL
                                            : contents.error());
    }
    CheckpointHeader header{};
    std::memcpy(&header, contents->data(), sizeof(header));
    if (std::memcmp(header.magic, kCheckpointMagic, sizeof(header.magic)) !=
            0 ||
        header.version != kCheckpointVersion ||
        header.header_size != sizeof(header) ||
        header.payload_size != contents->size() - sizeof(header)) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    const char* payload = contents->data() + sizeof(header);
    if (Crc32cValue(payload, header.payload_size) != header.payload_crc32c) {
        return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }
    auto decoded = DecodePb<PersistedShard>(
        std::string_view(payload, header.payload_size));
    if (!decoded) return tl::make_unexpected(decoded.error());
    const auto& persisted = *decoded;
    if (persisted.version != kCheckpointVersion ||
        persisted.configuration_fingerprint !=
            ConfigurationFingerprint(shard) ||
        persisted.shard_idx != shard_idx ||
        persisted.capacity != shard.capacity ||
        persisted.alignment != alignment_ ||
        persisted.data_path != ShardDataPath(shard) ||
        persisted.generation == 0 || persisted.allocator_state.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
    }

    std::vector<SerializedByte> allocator_bytes(
        persisted.allocator_state.size());
    std::memcpy(allocator_bytes.data(), persisted.allocator_state.data(),
                persisted.allocator_state.size());
    auto allocator = deserialize_from<OffsetAllocator>(allocator_bytes);
    if (!allocator || allocator->get_metrics().capacity != shard.capacity) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }

    std::unordered_map<uint64_t, UsedNode> nodes;
    bool invalid_node = false;
    allocator->visit_used_nodes(
        [&](uint64_t offset, uint64_t size, uint32_t index) {
            if (size == 0 || offset > shard.capacity ||
                size > shard.capacity - offset ||
                !nodes.emplace(offset, UsedNode{size, index}).second) {
                invalid_node = true;
            }
        });
    if (invalid_node) return tl::make_unexpected(ErrorCode::INVALID_REPLICA);

    shard.allocator = std::move(allocator);
    shard.offset_to_handle.clear();
    shard.pending_free.clear();
    shard.pending_free_bytes = 0;
    std::unordered_set<uint64_t> consumed;
    const uint64_t padding = alignment_ - 1;
    for (const auto& record : persisted.allocations) {
        auto node = nodes.find(record.raw_offset);
        if (record.key.empty() || record.object_size == 0 ||
            record.object_size >
                std::numeric_limits<uint64_t>::max() - padding ||
            record.aligned_size != AlignSize(record.object_size) ||
            record.requested_size != record.aligned_size + padding ||
            record.offset != AlignSize(record.raw_offset) ||
            record.offset > shard.capacity ||
            record.aligned_size > shard.capacity - record.offset ||
            node == nodes.end() || !consumed.insert(record.raw_offset).second ||
            node->second.size != record.reserved_bytes ||
            shard.allocator->normalizedAllocationSize(record.requested_size) !=
                record.reserved_bytes) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        auto handle = shard.allocator->createHandleAtNode(
            node->second.index, record.raw_offset, record.requested_size);
        if (!handle) return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        auto shared =
            std::make_shared<OffsetAllocationHandle>(std::move(*handle));
        if (!shard.offset_to_handle
                 .emplace(
                     record.offset,
                     ShardState::AllocationRecord{
                         record.key, shared, record.raw_offset,
                         record.object_size, record.aligned_size,
                         record.requested_size, record.reserved_bytes, false})
                 .second) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
    }
    const auto free_at =
        std::chrono::steady_clock::now() + deferred_free_duration_;
    for (const auto& pending : persisted.pending_frees) {
        auto node = nodes.find(pending.raw_offset);
        if (pending.requested_size == 0 || node == nodes.end() ||
            !consumed.insert(pending.raw_offset).second ||
            node->second.size != pending.reserved_bytes ||
            shard.allocator->normalizedAllocationSize(pending.requested_size) !=
                pending.reserved_bytes) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        auto handle = shard.allocator->createHandleAtNode(
            node->second.index, pending.raw_offset, pending.requested_size);
        if (!handle) return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        QueuePendingFreeLocked(
            shard, std::make_shared<OffsetAllocationHandle>(std::move(*handle)),
            pending.raw_offset, pending.requested_size, pending.reserved_bytes,
            free_at);
    }
    if (consumed.size() != nodes.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
    }
    shard.generation = persisted.generation;
    shard.base_sequence = persisted.base_sequence;
    shard.next_sequence = persisted.base_sequence + 1;
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::LoadWalLocked(
    ShardState& shard, int shard_idx) {
    const size_t max_wal_size = std::max<size_t>(
        kMaxMetadataFileSize, metadata_wal_compaction_threshold_bytes_ +
                                  kMaxWalFrameSize + sizeof(WalFrameHeader));
    auto contents =
        ReadWholeFile(*fs_adapter_, ShardWalPath(shard), max_wal_size);
    if (!contents || contents->size() < sizeof(WalFileHeader)) {
        return tl::make_unexpected(contents ? ErrorCode::FILE_READ_FAIL
                                            : contents.error());
    }
    WalFileHeader file_header{};
    std::memcpy(&file_header, contents->data(), sizeof(file_header));
    if (std::memcmp(file_header.magic, kWalMagic, sizeof(file_header.magic)) !=
            0 ||
        file_header.version != kWalVersion ||
        file_header.header_size != sizeof(file_header) ||
        file_header.header_crc32c != WalFileHeaderCrc(file_header)) {
        return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }
    if (file_header.generation < shard.generation) {
        auto wal_header =
            BuildWalFileHeader(shard.generation, shard.base_sequence);
        auto reset = fs_adapter_->AtomicWriteFile(
            ShardWalPath(shard),
            std::span<const char>(wal_header.data(), wal_header.size()));
        if (!reset) return reset;
        shard.wal_bytes = wal_header.size();
        return {};
    }
    if (file_header.generation != shard.generation ||
        file_header.base_sequence != shard.base_sequence) {
        return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
    }

    size_t cursor = sizeof(file_header);
    uint64_t expected_sequence = shard.base_sequence + 1;
    bool torn_tail = false;
    while (cursor < contents->size()) {
        if (contents->size() - cursor < sizeof(WalFrameHeader)) {
            torn_tail = true;
            break;
        }
        WalFrameHeader frame{};
        std::memcpy(&frame, contents->data() + cursor, sizeof(frame));
        if (std::memcmp(frame.magic, kWalFrameMagic, sizeof(frame.magic)) !=
                0 ||
            frame.version != kWalVersion ||
            (frame.type != kWalAllocate && frame.type != kWalRelease) ||
            frame.sequence != expected_sequence || frame.payload_size == 0 ||
            frame.payload_size > kMaxWalFrameSize ||
            frame.header_crc32c != WalFrameHeaderCrc(frame)) {
            return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
        }
        if (frame.payload_size >
            contents->size() - cursor - sizeof(WalFrameHeader)) {
            torn_tail = true;
            break;
        }
        const char* payload =
            contents->data() + cursor + sizeof(WalFrameHeader);
        if (Crc32cValue(payload, frame.payload_size) != frame.payload_crc32c) {
            return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
        }
        std::string_view payload_view(payload, frame.payload_size);
        if (frame.type == kWalAllocate) {
            auto decoded = DecodePb<WalAllocatePayload>(payload_view);
            if (!decoded) return tl::make_unexpected(decoded.error());
            const auto& record = decoded->allocation;
            auto handle = shard.allocator->allocate(record.requested_size);
            if (!handle || handle->address() != record.raw_offset ||
                AlignSize(record.raw_offset) != record.offset ||
                shard.allocator->normalizedAllocationSize(
                    record.requested_size) != record.reserved_bytes ||
                record.aligned_size != AlignSize(record.object_size) ||
                record.requested_size != record.aligned_size + alignment_ - 1) {
                return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
            }
            auto shared =
                std::make_shared<OffsetAllocationHandle>(std::move(*handle));
            if (!shard.offset_to_handle
                     .emplace(record.offset,
                              ShardState::AllocationRecord{
                                  record.key, shared, record.raw_offset,
                                  record.object_size, record.aligned_size,
                                  record.requested_size, record.reserved_bytes,
                                  false})
                     .second) {
                return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
            }
        } else {
            auto decoded = DecodePb<WalReleasePayload>(payload_view);
            if (!decoded || decoded->raw_offsets.empty()) {
                return tl::make_unexpected(decoded ? ErrorCode::INVALID_REPLICA
                                                   : decoded.error());
            }
            for (uint64_t raw_offset : decoded->raw_offsets) {
                bool released = false;
                for (auto it = shard.offset_to_handle.begin();
                     it != shard.offset_to_handle.end(); ++it) {
                    if (it->second.raw_offset == raw_offset) {
                        shard.offset_to_handle.erase(it);
                        released = true;
                        break;
                    }
                }
                if (!released) {
                    auto pending = std::find_if(
                        shard.pending_free.begin(), shard.pending_free.end(),
                        [raw_offset](const auto& item) {
                            return item.raw_offset == raw_offset;
                        });
                    if (pending != shard.pending_free.end()) {
                        shard.pending_free_bytes -= pending->bytes;
                        shard.pending_free.erase(pending);
                        released = true;
                    }
                }
                if (!released) {
                    return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
                }
            }
        }
        ++expected_sequence;
        cursor += sizeof(WalFrameHeader) + frame.payload_size;
    }
    shard.next_sequence = expected_sequence;
    shard.wal_bytes = contents->size();
    if (torn_tail) {
        LOG(WARNING) << "Ignoring incomplete terminal DFS WAL frame, shard="
                     << shard_idx;
        return CompactShardLocked(shard, shard_idx);
    }
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::AppendAllocationLocked(
    ShardState& shard, int shard_idx,
    const ShardState::AllocationRecord& record, uint64_t exposed_offset) {
    WalAllocatePayload payload{{record.key, record.raw_offset, exposed_offset,
                                record.object_size, record.aligned_size,
                                record.requested_size, record.bytes}};
    auto encoded = EncodePb(payload);
    if (!encoded) return tl::make_unexpected(encoded.error());
    auto frame = BuildWalFrame(kWalAllocate, shard.next_sequence, *encoded);
    if (!frame) return tl::make_unexpected(frame.error());
    auto append = fs_adapter_->AppendAndSyncFile(
        ShardWalPath(shard),
        std::span<const char>(frame->data(), frame->size()));
    if (!append) {
        shard.poisoned = true;
        return append;
    }
    ++shard.next_sequence;
    shard.wal_bytes += frame->size();
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::AppendReleaseLocked(
    ShardState& shard, int shard_idx,
    const std::vector<uint64_t>& raw_offsets) {
    auto encoded = EncodePb(WalReleasePayload{raw_offsets});
    if (!encoded) return tl::make_unexpected(encoded.error());
    auto frame = BuildWalFrame(kWalRelease, shard.next_sequence, *encoded);
    if (!frame) return tl::make_unexpected(frame.error());
    auto append = fs_adapter_->AppendAndSyncFile(
        ShardWalPath(shard),
        std::span<const char>(frame->data(), frame->size()));
    if (!append) {
        shard.poisoned = true;
        return append;
    }
    ++shard.next_sequence;
    shard.wal_bytes += frame->size();
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::CompactShardLocked(
    ShardState& shard, int shard_idx) {
    if (shard.poisoned) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    const uint64_t generation = shard.generation + 1;
    const uint64_t base_sequence = shard.next_sequence - 1;
    auto checkpoint =
        SaveCheckpointLocked(shard, shard_idx, generation, base_sequence);
    if (!checkpoint) return checkpoint;
    auto wal_header = BuildWalFileHeader(generation, base_sequence);
    auto wal = fs_adapter_->AtomicWriteFile(
        ShardWalPath(shard),
        std::span<const char>(wal_header.data(), wal_header.size()));
    if (!wal) {
        shard.poisoned = true;
        return wal;
    }
    shard.generation = generation;
    shard.base_sequence = base_sequence;
    shard.wal_bytes = wal_header.size();
    shard.last_checkpoint = std::chrono::steady_clock::now();
    return {};
}

tl::expected<DistributedFSDescriptor, ErrorCode> DfsGlobalAllocator::Allocate(
    const std::string& key, uint64_t size) {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (!initialized_.load(std::memory_order_acquire) ||
        !recovery_ready_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (key.empty() || size == 0 ||
        size > std::numeric_limits<uint64_t>::max() - (alignment_ - 1)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto key_lock = LockKey(key);
    const uint64_t aligned_size = AlignSize(size);
    if (aligned_size > std::numeric_limits<uint64_t>::max() - (alignment_ - 1))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const uint64_t requested_size = aligned_size + alignment_ - 1;
    if (requested_size > shard_capacity_)
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    const size_t preferred = std::hash<std::string>{}(key) % shards->size();
    for (size_t attempt = 0; attempt < shards->size(); ++attempt) {
        const int shard_idx =
            static_cast<int>((preferred + attempt) % shards->size());
        auto& shard = *(*shards)[shard_idx];
        std::lock_guard lock(shard.mutex);
        if (shard.poisoned) {
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        auto cleanup = CleanupExpiredPendingFreesLocked(
            shard, shard_idx, std::chrono::steady_clock::now());
        if (!cleanup) return tl::make_unexpected(cleanup.error());

        const uint64_t reserved =
            shard.allocator->normalizedAllocationSize(requested_size);
        auto handle = shard.allocator->allocate(requested_size);
        if (!handle) continue;
        const uint64_t raw_offset = handle->address();
        const uint64_t offset = AlignSize(raw_offset);
        auto shared =
            std::make_shared<OffsetAllocationHandle>(std::move(*handle));
        auto [it, inserted] = shard.offset_to_handle.emplace(
            offset, ShardState::AllocationRecord{key, shared, raw_offset, size,
                                                 aligned_size, requested_size,
                                                 reserved, false});
        if (!inserted) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        auto durable =
            AppendAllocationLocked(shard, shard_idx, it->second, offset);
        if (!durable) {
            LOG(ERROR) << "DFS ALLOC WAL append failed; shard poisoned, shard="
                       << shard_idx << ", key=" << key;
            return tl::make_unexpected(durable.error());
        }
        return DistributedFSDescriptor{ShardDataPath(shard), offset, size,
                                       aligned_size, shard_idx};
    }
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
}

void DfsGlobalAllocator::Free(uint64_t offset, uint64_t aligned_size,
                              int shard_idx, const std::string& key) {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (!initialized_.load(std::memory_order_acquire) || shard_idx < 0 ||
        shard_idx >= static_cast<int>(shards->size())) {
        return;
    }
    auto& shard = *(*shards)[shard_idx];
    std::lock_guard lock(shard.mutex);
    auto it = shard.offset_to_handle.find(offset);
    if (it == shard.offset_to_handle.end() || it->second.key != key ||
        it->second.aligned_size != aligned_size) {
        return;
    }
    auto lru = shard.lru_index.find(key);
    if (lru != shard.lru_index.end() && lru->second->second == offset) {
        shard.lru_list.erase(lru->second);
        shard.lru_index.erase(lru);
    }
    QueuePendingFreeLocked(
        shard, it->second.handle, it->second.raw_offset,
        it->second.requested_size, it->second.bytes,
        std::chrono::steady_clock::now() + deferred_free_duration_);
    shard.offset_to_handle.erase(it);
}

void DfsGlobalAllocator::UpdateAccess(const std::string& key, int shard_idx,
                                      uint64_t offset) {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (!recovery_ready_.load(std::memory_order_acquire) || shard_idx < 0 ||
        shard_idx >= static_cast<int>(shards->size())) {
        return;
    }
    auto& shard = *(*shards)[shard_idx];
    std::lock_guard lock(shard.mutex);
    auto record = shard.offset_to_handle.find(offset);
    if (record == shard.offset_to_handle.end() || record->second.key != key ||
        record->second.eviction_prepared) {
        return;
    }
    auto lru = shard.lru_index.find(key);
    if (lru != shard.lru_index.end()) {
        lru->second->second = offset;
        shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list,
                              lru->second);
    } else {
        shard.lru_list.push_front({key, offset});
        shard.lru_index[key] = shard.lru_list.begin();
    }
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::ValidateAllocation(
    const std::string& key, const DistributedFSDescriptor& descriptor) const {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (!initialized_.load(std::memory_order_acquire) || key.empty() ||
        descriptor.shard_idx < 0 ||
        descriptor.shard_idx >= static_cast<int>(shards->size()) ||
        descriptor.object_size == 0 ||
        descriptor.object_size >
            std::numeric_limits<uint64_t>::max() - (alignment_ - 1) ||
        descriptor.file_path !=
            ShardDataPath(*(*shards)[descriptor.shard_idx]) ||
        descriptor.offset % alignment_ != 0 ||
        descriptor.aligned_size != AlignSize(descriptor.object_size)) {
        return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
    }
    const auto& shard = *(*shards)[descriptor.shard_idx];
    if (descriptor.offset > shard.capacity ||
        descriptor.aligned_size > shard.capacity - descriptor.offset) {
        return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
    }
    std::lock_guard lock(shard.mutex);
    auto it = shard.offset_to_handle.find(descriptor.offset);
    if (it == shard.offset_to_handle.end() || it->second.key != key ||
        it->second.object_size != descriptor.object_size ||
        it->second.aligned_size != descriptor.aligned_size) {
        return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
    }
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::CompleteRecovery(
    const std::vector<RecoveryReference>& references) {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    std::set<std::pair<int, uint64_t>> claimed;
    for (const auto& reference : references) {
        auto valid = ValidateAllocation(reference.key, reference.descriptor);
        if (!valid || !claimed
                           .emplace(reference.descriptor.shard_idx,
                                    reference.descriptor.offset)
                           .second) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
    }

    const auto free_at =
        std::chrono::steady_clock::now() + deferred_free_duration_;
    for (int shard_idx = 0; shard_idx < static_cast<int>(shards->size());
         ++shard_idx) {
        auto& shard = *(*shards)[shard_idx];
        std::lock_guard lock(shard.mutex);
        for (auto& pending : shard.pending_free) {
            pending.when = free_at;
        }
        shard.lru_list.clear();
        shard.lru_index.clear();
        std::vector<std::pair<std::string, uint64_t>> survivors;
        for (auto it = shard.offset_to_handle.begin();
             it != shard.offset_to_handle.end();) {
            if (!claimed.contains({shard_idx, it->first})) {
                QueuePendingFreeLocked(
                    shard, it->second.handle, it->second.raw_offset,
                    it->second.requested_size, it->second.bytes, free_at);
                it = shard.offset_to_handle.erase(it);
                continue;
            }
            it->second.eviction_prepared = false;
            survivors.emplace_back(it->second.key, it->first);
            ++it;
        }
        std::sort(survivors.begin(), survivors.end());
        for (const auto& item : survivors) {
            shard.lru_list.push_front(item);
            shard.lru_index[item.first] = shard.lru_list.begin();
        }
    }
    recovery_ready_.store(true, std::memory_order_release);
    return {};
}

void DfsGlobalAllocator::QueuePendingFreeLocked(
    ShardState& shard, const std::shared_ptr<OffsetAllocationHandle>& handle,
    uint64_t raw_offset, uint64_t requested_size, uint64_t bytes,
    std::chrono::steady_clock::time_point when) {
    if (!handle) return;
    if (bytes == 0) bytes = handle->size();
    shard.pending_free.push_back(
        {handle, raw_offset, requested_size, bytes, when});
    shard.pending_free_bytes += bytes;
}

tl::expected<void, ErrorCode>
DfsGlobalAllocator::CleanupExpiredPendingFreesLocked(
    ShardState& shard, int shard_idx,
    std::chrono::steady_clock::time_point now) {
    size_t count = 0;
    std::vector<uint64_t> raw_offsets;
    constexpr size_t kMaxReleaseBatch = 4096;
    for (const auto& pending : shard.pending_free) {
        if (pending.when > now || raw_offsets.size() >= kMaxReleaseBatch) break;
        raw_offsets.push_back(pending.raw_offset);
        ++count;
    }
    if (raw_offsets.empty()) return {};
    auto durable = AppendReleaseLocked(shard, shard_idx, raw_offsets);
    if (!durable) return durable;
    while (count-- > 0) {
        const uint64_t bytes = shard.pending_free.front().bytes;
        shard.pending_free.pop_front();
        shard.pending_free_bytes -= bytes;
    }
    return {};
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::RunMaintenance() {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (!initialized_.load(std::memory_order_acquire) ||
        !recovery_ready_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    tl::expected<void, ErrorCode> result{};
    const auto now = std::chrono::steady_clock::now();
    for (int i = 0; i < static_cast<int>(shards->size()); ++i) {
        auto& shard = *(*shards)[i];
        std::lock_guard lock(shard.mutex);
        if (shard.poisoned) {
            result = tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
            continue;
        }
        auto cleanup = CleanupExpiredPendingFreesLocked(shard, i, now);
        if (!cleanup) {
            result = cleanup;
            continue;
        }
        if (shard.wal_bytes >= metadata_wal_compaction_threshold_bytes_ ||
            now - shard.last_checkpoint >= metadata_checkpoint_interval_) {
            auto compact = CompactShardLocked(shard, i);
            if (!compact) result = compact;
        }
    }
    return result;
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::Checkpoint() {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    for (int i = 0; i < static_cast<int>(shards->size()); ++i) {
        auto& shard = *(*shards)[i];
        std::lock_guard lock(shard.mutex);
        auto result = CompactShardLocked(shard, i);
        if (!result) return result;
    }
    return {};
}

double DfsGlobalAllocator::EffectiveUsageLocked(const ShardState& shard) const {
    const uint64_t physical_free =
        shard.allocator->storageReport().totalFreeSpace;
    const uint64_t capped_free =
        std::min<uint64_t>(physical_free, shard.capacity);
    const uint64_t used = shard.capacity - capped_free;
    const uint64_t effective_pending =
        std::min<uint64_t>(shard.pending_free_bytes, used);
    if (shard.capacity == 0) return 0.0;
    return static_cast<double>(used - effective_pending) /
           static_cast<double>(shard.capacity);
}

DfsGlobalAllocator::PendingEviction DfsGlobalAllocator::PrepareEviction() {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    PendingEviction pending(this);
    if (!recovery_ready_.load(std::memory_order_acquire)) return pending;
    for (int i = 0; i < static_cast<int>(shards->size()); ++i) {
        auto& shard = *(*shards)[i];
        PrepareEvictionFromShard(shard, i, pending);
    }
    return pending;
}

void DfsGlobalAllocator::PrepareEvictionFromShard(ShardState& shard,
                                                  int shard_idx,
                                                  PendingEviction& pending) {
    std::lock_guard lock(shard.mutex);
    if (shard.poisoned) return;
    const double usage = EffectiveUsageLocked(shard);
    if (usage >= eviction_high_watermark_) shard.eviction_active = true;
    if (!shard.eviction_active) return;
    if (usage < eviction_low_watermark_) {
        shard.eviction_active = false;
        return;
    }
    uint64_t prepared_bytes = 0;
    while (!shard.lru_list.empty()) {
        auto lru = std::prev(shard.lru_list.end());
        auto record = shard.offset_to_handle.find(lru->second);
        if (record == shard.offset_to_handle.end() ||
            record->second.key != lru->first ||
            record->second.eviction_prepared) {
            shard.lru_index.erase(lru->first);
            shard.lru_list.erase(lru);
            continue;
        }
        EvictionCandidate candidate{lru->first, shard_idx, lru->second};
        pending.prepared_.push_back(
            {candidate, record->second.handle, record->second.bytes});
        pending.candidates_.push_back(candidate);
        record->second.eviction_prepared = true;
        prepared_bytes += record->second.bytes;
        shard.lru_index.erase(lru->first);
        shard.lru_list.erase(lru);
        const double projected =
            usage - static_cast<double>(prepared_bytes) / shard.capacity;
        if (projected < eviction_low_watermark_) break;
    }
}

void DfsGlobalAllocator::CommitPreparedEviction(PendingEviction&& pending) {
    std::vector<bool> accepted(pending.prepared_.size(), true);
    ResolvePreparedEviction(std::move(pending), accepted);
}

void DfsGlobalAllocator::RestorePreparedEviction(PendingEviction&& pending) {
    std::vector<bool> accepted(pending.prepared_.size(), false);
    ResolvePreparedEviction(std::move(pending), accepted);
}

void DfsGlobalAllocator::ResolvePreparedEviction(
    PendingEviction&& pending, const std::vector<bool>& accepted) {
    const auto shards =
        std::atomic_load_explicit(&shards_, std::memory_order_acquire);
    if (pending.owner_ != this) return;
    if (accepted.size() != pending.prepared_.size()) {
        RestorePreparedEviction(std::move(pending));
        return;
    }
    const auto free_at =
        std::chrono::steady_clock::now() + deferred_free_duration_;
    for (size_t i = 0; i < pending.prepared_.size(); ++i) {
        if (!accepted[i]) continue;
        const auto& prepared = pending.prepared_[i];
        auto& shard = *(*shards)[prepared.candidate.shard_idx];
        std::lock_guard lock(shard.mutex);
        auto record = shard.offset_to_handle.find(prepared.candidate.offset);
        if (record == shard.offset_to_handle.end() ||
            record->second.key != prepared.candidate.key ||
            record->second.handle != prepared.handle) {
            continue;
        }
        QueuePendingFreeLocked(
            shard, record->second.handle, record->second.raw_offset,
            record->second.requested_size, record->second.bytes, free_at);
        shard.offset_to_handle.erase(record);
    }
    // Candidates are prepared oldest-first. Reinsert rejected candidates in
    // reverse so their relative LRU order is unchanged.
    for (size_t i = pending.prepared_.size(); i > 0; --i) {
        if (accepted[i - 1]) continue;
        const auto& prepared = pending.prepared_[i - 1];
        auto& shard = *(*shards)[prepared.candidate.shard_idx];
        std::lock_guard lock(shard.mutex);
        auto record = shard.offset_to_handle.find(prepared.candidate.offset);
        if (record == shard.offset_to_handle.end() ||
            record->second.key != prepared.candidate.key ||
            record->second.handle != prepared.handle) {
            continue;
        }
        record->second.eviction_prepared = false;
        if (shard.lru_index.contains(prepared.candidate.key)) continue;
        shard.lru_list.push_back(
            {prepared.candidate.key, prepared.candidate.offset});
        shard.lru_index[prepared.candidate.key] =
            std::prev(shard.lru_list.end());
    }
    pending.prepared_.clear();
    pending.candidates_.clear();
    pending.owner_ = nullptr;
}

std::string DfsGlobalAllocator::ShardDataPath(const ShardState& shard) const {
    return shard.path;
}

std::string DfsGlobalAllocator::ShardMetadataPath(
    const ShardState& shard) const {
    return std::filesystem::path(shard.path)
        .replace_extension(".meta")
        .string();
}

std::string DfsGlobalAllocator::ShardWalPath(const ShardState& shard) const {
    return std::filesystem::path(shard.path).replace_extension(".wal").string();
}

std::string DfsGlobalAllocator::ConfigurationFingerprint(
    const ShardState& shard) const {
    return "v1|" + fs_adapter_type_ + "|" + mount_path_ + "|" +
           metadata_namespace_ + "|" + shard.path + "|" +
           std::to_string(shard.capacity) + "|" + std::to_string(alignment_);
}

uint64_t DfsGlobalAllocator::AlignSize(uint64_t size) const {
    return (size + alignment_ - 1) & ~(alignment_ - 1);
}

std::string DfsGlobalAllocator::FormatShardIdx(int idx, int shard_count) {
    const int width = static_cast<int>(std::max<size_t>(
        2, std::to_string(std::max(0, shard_count - 1)).size()));
    std::ostringstream oss;
    oss << std::setw(width) << std::setfill('0') << idx;
    return oss.str();
}

}  // namespace mooncake
