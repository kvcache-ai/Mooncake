#pragma once

#include <cstdint>

namespace mooncake {

struct MmapArenaConfig {
    bool enabled = false;
    uint64_t pool_size = 0;
    bool hugepages_explicitly_requested = false;

    static MmapArenaConfig FromEnvironment(bool enabled_by_flag,
                                           uint64_t flag_pool_size);
};

}  // namespace mooncake
