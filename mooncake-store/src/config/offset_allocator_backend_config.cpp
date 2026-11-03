#include "config/offset_allocator_backend_config.h"

#include <glog/logging.h>

#include <string>

#include "ascii_string.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

bool OffsetAllocatorBackendConfig::Validate() const {
    if (persist_mode == OffsetPersistMode::kRelaxed) {
        if (persist_interval_seconds < 5) {
            LOG(ERROR) << "OffsetAllocatorBackendConfig: "
                          "persist_interval_seconds must be >= 5 for "
                          "kRelaxed mode";
            return false;
        }
    }
    if (high_ratio <= 0.0 || high_ratio > 1.0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: high_ratio must be in (0,1]";
        return false;
    }
    if (low_ratio <= 0.0 || low_ratio >= high_ratio) {
        LOG(ERROR) << "OffsetAllocatorBackendConfig: low_ratio must be in (0, "
                      "high_ratio)";
        return false;
    }
    if (keys_high_ratio <= 0.0 || keys_high_ratio > 1.0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: keys_high_ratio must be in (0,1]";
        return false;
    }
    if (keys_low_ratio <= 0.0 || keys_low_ratio >= keys_high_ratio) {
        LOG(ERROR) << "OffsetAllocatorBackendConfig: keys_low_ratio must be in "
                      "(0, keys_high_ratio)";
        return false;
    }
    if (max_evict_per_offload == 0) {
        LOG(ERROR) << "OffsetAllocatorBackendConfig: max_evict_per_offload "
                      "must be > 0";
        return false;
    }
    if (fallback_evict_batch == 0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: fallback_evict_batch must be > 0";
        return false;
    }
    if (max_capacity_nodes < 0) {
        LOG(ERROR)
            << "OffsetAllocatorBackendConfig: max_capacity_nodes must be >= 0";
        return false;
    }
    return true;
}

OffsetAllocatorBackendConfig OffsetAllocatorBackendConfig::FromEnvironment() {
    OffsetAllocatorBackendConfig cfg;
    using Variables = OffsetAllocatorBackendEnvironmentVariables;

    const auto policy =
        Environ::Read(Variables::MOONCAKE_OFFSET_EVICTION_POLICY);
    if (policy.has_value()) {
        if (AsciiCaseInsensitiveEquals(*policy, "fifo")) {
            cfg.eviction_policy = OffsetEvictionPolicy::FIFO;
        }
        // NONE is default; LRU reserved for phase 2
    }

    constexpr EnvironmentDoubleParseOptions kLegacyRatioParsing{
        .allow_trailing_characters = true,
        .allow_non_finite = true,
    };
    if (const auto value =
            Environ::Read(Variables::MOONCAKE_OFFSET_HIGH_RATIO)) {
        cfg.high_ratio = TryParseEnvironmentDouble(*value, kLegacyRatioParsing)
                             .value_or(cfg.high_ratio);
    }
    if (const auto value =
            Environ::Read(Variables::MOONCAKE_OFFSET_LOW_RATIO)) {
        cfg.low_ratio = TryParseEnvironmentDouble(*value, kLegacyRatioParsing)
                            .value_or(cfg.low_ratio);
    }
    // Both byte and key watermarks derive from the same ratio pair.
    cfg.keys_high_ratio = cfg.high_ratio;
    cfg.keys_low_ratio = cfg.low_ratio;

    cfg.max_capacity_nodes = Environ::ReadOr(
        Variables::MOONCAKE_OFFSET_MAX_CAPACITY_NODES, cfg.max_capacity_nodes);

    // Read eviction cap as int64_t to guard against negative env values
    // which would wrap to SIZE_MAX with an unsigned parser.
    auto max_evict_raw =
        Environ::ReadOr(Variables::MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD,
                        static_cast<int64_t>(cfg.max_evict_per_offload));
    if (max_evict_raw > 0) {
        cfg.max_evict_per_offload = static_cast<size_t>(max_evict_raw);
    } else if (max_evict_raw <= 0) {
        LOG(WARNING) << "MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD="
                     << max_evict_raw << " is non-positive; using default "
                     << cfg.max_evict_per_offload;
    }

    // Persistence mode
    const auto persist = Environ::Read(Variables::MOONCAKE_OFFSET_PERSIST_MODE);
    if (persist.has_value()) {
        const std::string& s = *persist;
        if (AsciiCaseInsensitiveEquals(s, "disabled")) {
            cfg.persist_mode = OffsetPersistMode::kDisabled;
        } else if (AsciiCaseInsensitiveEquals(s, "relaxed")) {
            cfg.persist_mode = OffsetPersistMode::kRelaxed;
        } else if (AsciiCaseInsensitiveEquals(s, "strict")) {
            cfg.persist_mode = OffsetPersistMode::kStrict;
        } else {
            LOG(WARNING) << "Unknown MOONCAKE_OFFSET_PERSIST_MODE=" << s
                         << "; using default (disabled)";
        }
    }

    cfg.persist_interval_seconds =
        Environ::ReadOr(Variables::MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS,
                        cfg.persist_interval_seconds);

    // Record CRC-32C: "0"/"false"/"off" disables per-record checksums.
    if (const auto record_crc =
            Environ::Read(Variables::MOONCAKE_OFFSET_RECORD_CRC);
        record_crc.has_value() && !*record_crc) {
        cfg.enable_record_crc = false;
    }

    return cfg;
}

}  // namespace mooncake
