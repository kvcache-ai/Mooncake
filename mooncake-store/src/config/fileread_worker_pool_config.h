#pragma once

namespace mooncake {

class Environ;

struct FilereadWorkerPoolConfig {
    int worker_count = 10;

    static FilereadWorkerPoolConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
