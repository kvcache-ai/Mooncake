#pragma once

namespace mooncake {

class Environ;

struct NoFWorkerPoolConfig {
    int worker_count = 4;

    static NoFWorkerPoolConfig FromEnvironment(const Environ& env);
    static const NoFWorkerPoolConfig& AtFirstUse();
};

}  // namespace mooncake
