#pragma once

namespace mooncake {

struct FilereadWorkerPoolConfig {
    int worker_count = 10;

    static FilereadWorkerPoolConfig FromEnvironment();
};

}  // namespace mooncake
