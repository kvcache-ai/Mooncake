#include "p2p/client/data_manager_factory.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "p2p/client/tiered_cache/tiered_backend.h"
#include "p2p/client/v1/data_manager_v1.h"
#include "p2p/client/v2/data_manager_v2.h"

namespace mooncake {

std::optional<DataManagerVersion> ParseDataManagerVersion(
    std::string_view text) {
    std::string lowered(text);
    std::transform(
        lowered.begin(), lowered.end(), lowered.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (lowered == "v1" || lowered == "1") return DataManagerVersion::kV1;
    if (lowered == "v2" || lowered == "2") return DataManagerVersion::kV2;
    return std::nullopt;
}

const char* ToString(DataManagerVersion version) {
    switch (version) {
        case DataManagerVersion::kV1:
            return "v1";
        case DataManagerVersion::kV2:
            return "v2";
    }
    return "unknown";
}

namespace {

tl::expected<std::unique_ptr<DataManager>, ErrorCode> CreateV1(
    const DataManagerConfig& config,
    std::shared_ptr<TransferEngine> transfer_engine,
    MetadataCallbacks callbacks, const DataManagerMetrics& metrics) {
    auto tiered_backend =
        std::make_unique<TieredBackend>(config.v1_lock_shard_count);
    TransferEngine* tier_engine = config.register_tiers_with_transfer_engine
                                      ? transfer_engine.get()
                                      : nullptr;
    auto init_result = tiered_backend->Init(
        config.tier_config, tier_engine, std::move(callbacks.add_replica),
        std::move(callbacks.remove_replica), std::move(callbacks.segment_sync),
        metrics.tier_metric, metrics.key_retention);
    if (!init_result) {
        LOG(ERROR) << "Failed to init TieredBackend: " << init_result.error();
        return tl::make_unexpected(init_result.error());
    }

    auto data_manager = std::make_unique<DataManagerV1>(
        std::move(tiered_backend), std::move(transfer_engine),
        config.v1_lock_shard_count, config.local_transfer, config.key_lease);
    if (callbacks.rectify_route) {
        data_manager->SetRectifyCallback(std::move(callbacks.rectify_route));
    }
    return data_manager;
}

tl::expected<std::unique_ptr<DataManager>, ErrorCode> CreateV2(
    const DataManagerConfig& config,
    std::shared_ptr<TransferEngine> transfer_engine,
    MetadataCallbacks callbacks, const DataManagerMetrics& metrics) {
    auto v2_config = v2::ParseDataManagerV2Config(
        config.tier_config, config.local_transfer, config.key_lease);
    if (!v2_config) {
        return tl::make_unexpected(v2_config.error());
    }
    v2_config->register_tiers_with_transfer_engine =
        config.register_tiers_with_transfer_engine;
    // Only when the caller asked for it. ParseDataManagerV2Config has already
    // read and validated the tier configuration's own value; overwriting it
    // unconditionally is what made that knob dead.
    if (config.stop_drain_timeout.has_value()) {
        v2_config->stop_drain_timeout = *config.stop_drain_timeout;
    }

    auto data_manager = std::make_unique<v2::DataManagerV2>(
        *v2_config, std::move(transfer_engine), std::move(callbacks),
        metrics.tier_metric, metrics.key_retention);
    auto initialized = data_manager->Init();
    if (!initialized) {
        return tl::make_unexpected(initialized.error());
    }
    return data_manager;
}

}  // namespace

tl::expected<std::unique_ptr<DataManager>, ErrorCode> CreateDataManager(
    const DataManagerConfig& config,
    std::shared_ptr<TransferEngine> transfer_engine,
    MetadataCallbacks callbacks, DataManagerMetrics metrics) {
    if (!transfer_engine) {
        LOG(ERROR) << "CreateDataManager: transfer_engine must not be null";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    LOG(INFO) << "Creating DataManager, version=" << ToString(config.version);
    switch (config.version) {
        case DataManagerVersion::kV1:
            return CreateV1(config, std::move(transfer_engine),
                            std::move(callbacks), metrics);
        case DataManagerVersion::kV2:
            return CreateV2(config, std::move(transfer_engine),
                            std::move(callbacks), metrics);
    }
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake
