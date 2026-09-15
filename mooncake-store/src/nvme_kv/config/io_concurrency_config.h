#pragma once

#include <cstddef>

namespace mooncake {

struct NvmeKvIoConcurrencyConfig {
    std::size_t max_io_concurrency = 256;
    std::size_t io_concurrency = 1;
    std::size_t batch_submit_concurrency = 1;
    std::size_t root_submit_concurrency = 1;
    std::size_t prepare_concurrency = 1;

    static NvmeKvIoConcurrencyConfig FromEnvironment(
        std::size_t device_queue_depth);
};

}  // namespace mooncake
