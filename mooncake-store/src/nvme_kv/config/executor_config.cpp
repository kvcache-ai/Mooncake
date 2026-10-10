#include "executor_config.h"

#include <algorithm>

#include "environ.h"
#include "environment_variables.h"
#include "u32_parser.h"

namespace mooncake {
namespace {

uint32_t ReadNvmeKvU32Or(const Environ& env,
                         const EnvironmentVariable<std::string>& variable,
                         uint32_t fallback) {
    const auto value = env.GetTyped(variable);
    if (!value.has_value()) {
        return fallback;
    }
    return TryParseNvmeKvU32(value->c_str()).value_or(fallback);
}

}  // namespace

uint32_t NvmeKvExecutorConfig::ReadTransferAlignmentBytesFromEnvironment(
    const Environ& env) {
    return ReadNvmeKvU32Or(env,
                           NvmeKvExecutorEnvironmentVariables::
                               MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES,
                           NvmeKvExecutorConfig{}.transfer_alignment_bytes);
}

uint32_t NvmeKvExecutorConfig::ReadValueBlockUnitBytesFromEnvironment(
    const Environ& env) {
    return ReadNvmeKvU32Or(env,
                           NvmeKvExecutorEnvironmentVariables::
                               MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES,
                           NvmeKvExecutorConfig{}.value_block_unit_bytes);
}

uint32_t NvmeKvExecutorConfig::ReadProtocolMaxValueSizeFromEnvironment(
    const Environ& env) {
    return ReadNvmeKvU32Or(env,
                           NvmeKvExecutorEnvironmentVariables::
                               MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE,
                           NvmeKvExecutorConfig{}.protocol_max_value_size);
}

std::size_t NvmeKvExecutorConfig::ReadPlanBatchSizeFromEnvironment(
    const Environ& env) {
    const uint32_t read_plan_batch_size =
        ReadNvmeKvU32Or(env,
                        NvmeKvExecutorEnvironmentVariables::
                            MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE,
                        0);
    return read_plan_batch_size == 0
               ? NvmeKvExecutorConfig{}.read_plan_batch_size
               : std::min<std::size_t>(read_plan_batch_size, 1024);
}

NvmeKvExecutorConfig NvmeKvExecutorConfig::FromEnvironment(const Environ& env) {
    NvmeKvExecutorConfig config;
    config.transfer_alignment_bytes =
        ReadTransferAlignmentBytesFromEnvironment(env);
    config.value_block_unit_bytes = ReadValueBlockUnitBytesFromEnvironment(env);
    config.protocol_max_value_size =
        ReadProtocolMaxValueSizeFromEnvironment(env);
    config.read_plan_batch_size = ReadPlanBatchSizeFromEnvironment(env);
    return config;
}

}  // namespace mooncake
