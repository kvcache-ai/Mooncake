#pragma once

// Construction of a DataManager. Callers pick an implementation by
// configuration only; nothing outside this header and its .cpp names a
// concrete DataManager type.

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>

#include <json/value.h>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager.h"
#include "p2p/client/data_manager_types.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {

struct TierMetric;          // p2p/client/p2p_client_metric.h
struct KeyRetentionMetric;  // p2p/client/p2p_client_metric.h

/**
 * @enum DataManagerVersion
 * @brief Which implementation the factory builds. Defaults to kV1 until V2
 *        clears the acceptance criteria (section 11).
 */
enum class DataManagerVersion { kV1, kV2 };

/** Parses "v1"/"v2" (case-insensitive). Unknown values yield nullopt. */
std::optional<DataManagerVersion> ParseDataManagerVersion(
    std::string_view text);

const char* ToString(DataManagerVersion version);

/**
 * @struct DataManagerMetrics
 * @brief Metric sinks injected from outside, so both implementations emit the
 *        same metric names (p2p_client_http_endpoints_test asserts on them).
 *        Null members simply disable that sink.
 */
struct DataManagerMetrics {
    std::shared_ptr<TierMetric> tier_metric;
    std::shared_ptr<KeyRetentionMetric> key_retention;
};

/**
 * @struct DataManagerConfig
 * @brief Everything the factory needs. Fields not applicable to the selected
 *        version are ignored.
 */
struct DataManagerConfig {
    DataManagerVersion version = DataManagerVersion::kV1;

    // Raw tier configuration ("tiers": [...]). V1 hands it to TieredBackend;
    // V2 parses it with its own parser into logical tilers.
    Json::Value tier_config;

    // Lease-table shard count for V1's pending-write / pinned-key tables.
    size_t v1_lock_shard_count = 1024;

    LocalTransferConfig local_transfer;
    KeyLeaseConfig key_lease;

    // Upper bound on how long Stop() waits for in-flight work before it gives
    // up and cancels. Exceeding it is not an error: a caller may hold a
    // TaskHandle it never waits on, so an unbounded wait would hang shutdown.
    //
    // Unset means "whatever the tier configuration says", which is the normal
    // case. It used to be a plain value with a default, and the factory
    // assigned it unconditionally -- so `v2.stop_drain_timeout_ms` was parsed,
    // validated and then thrown away, and no deployment could ever change it.
    // A caller that sets it here is deliberately overriding the file, which is
    // what the tests do to keep shutdown quick.
    std::optional<std::chrono::milliseconds> stop_drain_timeout;

    // Register tier memory with the TransferEngine so peers can address it.
    // Production must leave this true; without registration no remote read or
    // write can reach the tier. Set false only when the caller knowingly hands
    // in an un-initialized TransferEngine (local-only tests), because
    // registering against one dereferences an absent transport.
    bool register_tiers_with_transfer_engine = true;

    // TODO(phase-2): std::optional<DataManagerV2Config> v2;
};

/**
 * @brief Build a DataManager.
 * @param transfer_engine Shared with the client; must not be null.
 * @param callbacks Metadata hooks. `rectify_route` is installed via
 *        SetRectifyCallback, the other three are wired into the data plane.
 */
tl::expected<std::unique_ptr<DataManager>, ErrorCode> CreateDataManager(
    const DataManagerConfig& config,
    std::shared_ptr<TransferEngine> transfer_engine,
    MetadataCallbacks callbacks, DataManagerMetrics metrics = {});

}  // namespace mooncake
