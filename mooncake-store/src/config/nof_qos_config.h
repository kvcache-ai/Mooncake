#pragma once

namespace mooncake {

struct NoFQosConfig {
    int submit_chunk_bytes = 1 << 17;
    int inflight_bytes_limit = 1 << 25;

    static NoFQosConfig FromEnvironment();
    static const NoFQosConfig& AtFirstUse();
};

}  // namespace mooncake
