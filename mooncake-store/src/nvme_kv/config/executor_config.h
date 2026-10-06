#pragma once

#include <cstddef>
#include <cstdint>

namespace mooncake {

struct NvmeKvExecutorConfig {
    uint32_t transfer_alignment_bytes = 4096;
    uint32_t value_block_unit_bytes = 512;
    uint32_t protocol_max_value_size = 512 * 1024;
    std::size_t read_plan_batch_size = 8;

    // Field-specific readers preserve the existing late-read boundaries
    // without parsing unrelated executor settings on each call.
    static uint32_t ReadTransferAlignmentBytesFromEnvironment();
    static uint32_t ReadValueBlockUnitBytesFromEnvironment();
    static uint32_t ReadProtocolMaxValueSizeFromEnvironment();
    static std::size_t ReadPlanBatchSizeFromEnvironment();

    static NvmeKvExecutorConfig FromEnvironment();
};

}  // namespace mooncake
