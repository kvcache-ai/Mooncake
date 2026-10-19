#pragma once

#include <cstdint>
#include <ostream>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

enum class BucketEvictionPolicy {
    NONE,  // No eviction (default)
    FIFO,  // Evict oldest bucket first (by creation order)
    LRU,   // Evict least recently read bucket first
};

inline std::ostream& operator<<(std::ostream& os,
                                const BucketEvictionPolicy& policy) {
    switch (policy) {
        case BucketEvictionPolicy::NONE:
            return os << "none";
        case BucketEvictionPolicy::FIFO:
            return os << "fifo";
        case BucketEvictionPolicy::LRU:
            return os << "lru";
        default:
            return os << "unknown";
    }
}

struct BucketBackendConfig {
    int64_t bucket_size_limit =
        256LL * 1024 * 1024;  // Max total size of a single bucket (256 MB)

    int64_t bucket_keys_limit = 500;  // Max number of keys allowed in a single
                                      // bucket, required by bucket backend only

    BucketEvictionPolicy eviction_policy =
        BucketEvictionPolicy::NONE;  // Eviction strategy

    int64_t max_total_size = 0;  // 0 = unlimited; evict when total_size_
                                 // exceeds this threshold (bytes)

    // Hard cap on *actual on-disk* bytes of the offload directory, measured by
    // scanning real file allocation (stat st_blocks), NOT the in-memory
    // total_size_. 0 = disabled.
    //
    // Why a separate, disk-measured cap: total_size_ is decremented per object
    // on eviction/delete, but a 256 MB bucket file is only removed once *all*
    // its objects are gone — so total_size_ (and any sum over the in-memory
    // bucket map) reads well below real disk usage under per-object eviction
    // (cf. unreliable bucket GC, kvcache-ai/Mooncake#3220/#3221). The other
    // physical guard, std::filesystem::space(), reports the *host* filesystem
    // under a cgroup / K8s emptyDir sizeLimit rather than the enforced quota,
    // so it never fires either. This cap scans the actual directory (the same
    // st_blocks basis kubelet's emptyDir accounting uses), so it matches what
    // actually triggers pod eviction. Set it to the real quota minus headroom.
    int64_t max_physical_bytes = 0;

    // How long ActualDiskBytesUsedLocked() caches its directory-scan result
    // (milliseconds). Bounds scan cost under a high offload rate; larger =
    // cheaper but a staler physical bound (disk may momentarily exceed the cap
    // by roughly one interval's worth of writes), smaller = tighter but scans
    // more often. <=0 disables caching (scan every check). Only relevant when
    // max_physical_bytes > 0.
    int64_t disk_scan_cache_ms = 500;

    // Per-disk quotas, positionally aligned with the disks parsed out of
    // FileStorageConfig::storage_filepath. Empty => broadcast the scalar
    // max_total_size to every disk. Otherwise it must have exactly one entry
    // per disk; BucketStorageBackend::Init() rejects any other length.
    std::vector<int64_t> max_total_size_per_disk;

    bool Validate() const;

    /**
     * @brief Build the configuration from MOONCAKE_OFFLOAD_BUCKET_* variables.
     *
     * Scalar variables that fail to parse fall back to their defaults with a
     * warning. The per-disk quota list is stricter because it is positionally
     * aligned with the disks: an empty or unparsable entry is an error.
     *
     * @return The configuration; INVALID_PARAMS if the per-disk quota list
     *         has an empty or unparsable entry.
     */
    static tl::expected<BucketBackendConfig, ErrorCode> FromEnvironment();
};

}  // namespace mooncake
