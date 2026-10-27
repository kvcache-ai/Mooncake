#pragma once

namespace mooncake {

struct OffloadParallelWorkerPoolConfig {
    int worker_count = 8;

    static OffloadParallelWorkerPoolConfig FromEnvironment();
};

}  // namespace mooncake
