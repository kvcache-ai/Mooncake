#pragma once

namespace mooncake {

struct NoFWorkerPoolConfig {
    int worker_count = 4;

    static NoFWorkerPoolConfig FromEnvironment();
    static const NoFWorkerPoolConfig& AtFirstUse();
};

}  // namespace mooncake
