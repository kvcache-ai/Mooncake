#pragma once

// Types shared by the DataManager interface and its implementations. This
// header exists so that neither the interface nor a caller has to include
// `tiered_cache/**`: everything needed at the interface boundary lives here,
// and the tiered-cache tree includes this header instead of owning these
// definitions.
//
// Invariant: this header must compile standalone and must never include
// `tiered_backend.h` or anything under `tiered_cache/`.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "client_buffer.hpp"
#include "client_config_builder.h"
#include "p2p/client/task_handle.h"
#include "types.h"

namespace mooncake {

// ---------------------------------------------------------------------------
// Local IO result types
// ---------------------------------------------------------------------------

/**
 * @struct ReadTaskHandle
 * @brief Handle for a read operation.
 */
struct ReadTaskHandle {
    std::unique_ptr<TaskHandle<void>> task_handle;
    int64_t data_size;

    // if user use zero-copy get(), the var is useless;
    // if user provides allocator, the var is the buffer allocated by allocator;
    std::shared_ptr<BufferHandle> read_buf;

    // Whether this handle was served from local storage.
    bool is_local = false;
};

// ---------------------------------------------------------------------------
// Configuration shared by both implementations
// ---------------------------------------------------------------------------

/**
 * @struct LocalTransferConfig
 * @brief Configuration for local data transfer operations.
 */
struct LocalTransferConfig {
    LocalTransferMode mode = LocalTransferMode::TE;

    // When mode == TE, the following parameters are used:
    std::string te_endpoint;

    // When mode == MEMCPY, the following parameters are used:
    // 0 means forbid async memcpy (fall back to synchronous).
    size_t local_memcpy_async_worker_num = 32;

    // Dedicated coro_io pool for TE wait coroutines (poll getTransferStatus,
    // then co_await sleep_for). Independent of `mode`. 0 keeps synchronous TE
    // wait on the caller thread.
    size_t te_async_poll_worker_num = 32;
};

/**
 * @struct KeyLeaseConfig
 * @brief PreWrite / PinKey key lease timing (independent of local transfer).
 */
struct KeyLeaseConfig {
    // Max lifetime (ms) of intermediate lease state on a key. 0 = built-in
    // default.
    uint32_t duration_ms = 0;
    // Background scan interval (ms) for expired leases. 0 = built-in default.
    uint32_t scan_interval_ms = 0;
};

// ---------------------------------------------------------------------------
// Topology reporting
// ---------------------------------------------------------------------------

/**
 * @brief Segment name reported to Master for a logical tier ("tier_<uuid>").
 *        Also used as the per-tier metric label.
 */
std::string MakeTierSegmentName(const UUID& id);

/**
 * @struct TierView
 * @brief A snapshot of a tier's status, used for reporting topology to the
 * Master.
 */
struct TierView {
    UUID id;
    MemoryType type;
    size_t capacity;
    size_t usage;
    size_t free_space;
    int priority;
    std::vector<std::string> tags;

    // Segment name reported to Master ("tier_<uuid>"), also used as the
    // per-tier metric label.
    std::string GetName() const;
};

// ---------------------------------------------------------------------------
// Metadata synchronization callbacks
// ---------------------------------------------------------------------------

/**
 * @brief Callback for metadata synchronization when a replica is added.
 * Invoked after data copy is complete.
 */
using AddReplicaCallback = std::function<tl::expected<void, ErrorCode>(
    std::string_view key, const UUID& tier_id, size_t size)>;

/**
 * @brief Callback for metadata synchronization when a replica is removed.
 * Returns OK on success.
 */
using RemoveReplicaCallback = std::function<tl::expected<void, ErrorCode>(
    std::string_view key, const UUID& tier_id)>;

/**
 * @brief Callback for segment lifecycle synchronization.
 * Invoked when a tier is created (mount=true) or destroyed (mount=false).
 * The callback should register/unregister the segment with Master.
 */
using SegmentSyncCallback = std::function<tl::expected<void, ErrorCode>(
    const Segment& segment, bool mount)>;

/**
 * @brief Callback used to remove a stale replica route from Master when a read
 *        misses locally.
 */
using RectifyRouteCallback =
    std::function<void(std::string_view, std::optional<UUID>)>;

// ---------------------------------------------------------------------------
// Access statistics
// ---------------------------------------------------------------------------

/**
 * @enum AccessStatMetric
 * @brief Semantic meaning carried by a stats snapshot.
 */
enum class AccessStatMetric {
    kRecentHeat,
    kRecencyRank,
    kFrequency,
};

/**
 * @struct AccessStatEntry
 * @brief Per-key access metadata emitted by a stats collector.
 */
struct AccessStatEntry {
    std::string key;
    double recent_heat_score = 0.0;
    size_t recency_rank = 0;
};

/**
 * @struct AccessStats
 * @brief Snapshot of access statistics.
 */
struct AccessStats {
    AccessStatMetric metric = AccessStatMetric::kRecentHeat;
    std::vector<AccessStatEntry> hot_keys;
};

}  // namespace mooncake
