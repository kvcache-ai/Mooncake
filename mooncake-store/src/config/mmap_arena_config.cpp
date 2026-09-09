#include "mmap_arena_config.h"

#include <optional>
#include <string>

#include <glog/logging.h>

#include "bool_parser.h"
#include "common/byte_size.h"
#include "common/client_buffer_allocation.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

MmapArenaConfig MmapArenaConfig::FromEnvironment(bool enabled_by_flag,
                                                 uint64_t flag_pool_size) {
    MmapArenaConfig config{.enabled = enabled_by_flag,
                           .pool_size = flag_pool_size};
    using Variables = MmapArenaEnvironmentVariables;

    const std::string env_pool_size =
        Environ::ReadOr(Variables::MC_MMAP_ARENA_POOL_SIZE, std::string{});
    // An explicit pool-size env var is treated as an opt-in because pybind11
    // users cannot easily pass gflags.
    const std::string env_disable =
        Environ::ReadOr(Variables::MC_DISABLE_MMAP_ARENA, std::string{});
    const std::optional<bool> disable_override = TryParseBool(env_disable);
    if (!env_disable.empty() && !disable_override.has_value()) {
        LOG(WARNING) << "Ignoring invalid MC_DISABLE_MMAP_ARENA='"
                     << env_disable
                     << "'; accepted values: 1/0, true/false, yes/no, on/off";
    }

    const bool arena_requested = enabled_by_flag || !env_pool_size.empty();
    const bool arena_disabled = disable_override.value_or(false);
    if (!arena_requested || arena_disabled) {
        config.enabled = false;
        LOG(INFO) << "=== ARENA ALLOCATOR DISABLED ===";
        if (arena_disabled) {
            LOG(INFO) << "MC_DISABLE_MMAP_ARENA=" << env_disable
                      << " forces direct mmap()";
        } else {
            LOG(INFO) << "Arena is opt-in; set --use_mmap_arena_allocator or "
                         "MC_MMAP_ARENA_POOL_SIZE to enable it";
        }
        return config;
    }

    config.enabled = true;
    // Keep arena init consistent with the direct-mmap path:
    //   MC_STORE_USE_HUGEPAGE=1  -> strict: hard fail if hugepage mmap fails
    //   unset                    -> permissive: try hugepages, retry on
    //                               regular pages if HugeTLB is unavailable
    // This preserves both pre-existing contracts and avoids surprising
    // operators with a silent hugepage downgrade.
    config.hugepages_explicitly_requested = get_hugepage_size_from_env() > 0;

    // Supports human-readable sizes via string_to_byte_size(): "20gb", "16GB",
    // etc.
    if (!env_pool_size.empty()) {
        const uint64_t parsed = string_to_byte_size(env_pool_size);
        if (parsed > 0) {
            config.pool_size = parsed;
            LOG(INFO) << "MC_MMAP_ARENA_POOL_SIZE override: " << env_pool_size
                      << " (" << byte_size_to_string(config.pool_size) << ")";
        } else {
            LOG(WARNING) << "Invalid MC_MMAP_ARENA_POOL_SIZE='" << env_pool_size
                         << "', using default "
                         << byte_size_to_string(flag_pool_size);
        }
    }

    return config;
}

}  // namespace mooncake
