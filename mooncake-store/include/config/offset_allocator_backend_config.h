#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace mooncake {

enum class OffsetEvictionPolicy {
    NONE,  // No eviction
    FIFO,  // Evict oldest key first (by insertion order)
    LRU,   // Approximate LRU via cross-shard sampling (phase 2)
};

enum class OffsetPersistMode {
    kDisabled,  // No persistence (default)
    kRelaxed,   // Periodic checkpoint
    kStrict,    // Every BatchOffload is durable
};

struct OffsetAllocatorBackendConfig {
    OffsetEvictionPolicy eviction_policy = OffsetEvictionPolicy::NONE;

    // Watermark thresholds: eviction triggers when total_size_ exceeds high,
    // drives down to low. 0 = auto-resolved in Init() from ratios.
    int64_t high_watermark_bytes = 0;
    int64_t low_watermark_bytes = 0;
    double high_ratio = 0.90;
    double low_ratio = 0.80;

    // Key-count watermarks (symmetric with byte watermarks).
    // high triggers eviction, drives down to low.
    int64_t high_watermark_keys = 0;
    int64_t low_watermark_keys = 0;
    double keys_high_ratio = 0.95;
    double keys_low_ratio = 0.90;

    // Eviction caps
    size_t max_evict_per_offload = 4096;
    size_t fallback_evict_batch = 16;

    // Allocator node capacity override.
    // 0 = auto-derived from capacity_ / kMinObjectSize (capped at RAM budget).
    // Must be <= UINT32_MAX (OffsetAllocator::create takes uint32
    // max_capacity).
    int64_t max_capacity_nodes = 0;

    bool Validate() const;

    static OffsetAllocatorBackendConfig FromEnvironment();

    // ---- Persistence settings ----
    OffsetPersistMode persist_mode = OffsetPersistMode::kDisabled;
    int64_t persist_interval_seconds = 60;

    // ---- Record integrity ----
    // When true (default), every written record carries a CRC-32C over
    // header-prefix + key + value (RecordHeader::kFlagHasCrc), verified
    // once on recovery.  Disable only when torn writes are otherwise
    // impossible (kStrict mode on storage that honors fsync ordering,
    // e.g. power-loss-protected NVMe) or when values never pass through
    // the CPU (future DMA/GDS writers): unchecksummed records are then
    // validated by checkpoint ordering (seq guard) alone.
    bool enable_record_crc = true;

    // ---- Device-DAX / byte-addressable arena ----
    // When non-empty, the data arena is an mmap(MAP_SHARED) of this path (a
    // device-DAX chardev such as /dev/dax0.0, an fsdax file, or any regular
    // file) instead of {storage_filepath}/kv_cache.data. The metadata
    // checkpoint still lives under storage_filepath. See DaxFile.
    std::string dax_device_path;

    // Capacity is rounded down to a multiple of this when dax_device_path is
    // set. Device-DAX rejects mmap lengths that are not a multiple of the
    // device alignment (2 MiB, or 1 GiB for some namespaces).
    int64_t dax_alignment_bytes = 2 * 1024 * 1024;
};

}  // namespace mooncake
