#include "nvme_kv_io_concurrency_config.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>

#include "environ.h"
#include "environment_variables.h"
#include "nvme_kv_u32_parser.h"

namespace mooncake {
namespace {

constexpr std::size_t kDefaultMaxIoConcurrency = 256;
constexpr std::size_t kDefaultIoConcurrency = 18;
constexpr std::size_t kDefaultPrepareConcurrency = 12;
constexpr std::size_t kDefaultBatchSubmitConcurrency = 6;
constexpr std::size_t kDefaultRootSubmitConcurrency = 1;

std::optional<std::size_t> ReadOptionalPositive(
    const EnvironmentVariable<std::string>& variable) {
    const auto value = Environ::Read(variable);
    if (!value.has_value()) {
        return std::nullopt;
    }
    const auto parsed = TryParseNvmeKvU32(value->c_str());
    if (!parsed.has_value() || *parsed == 0) {
        return std::nullopt;
    }
    return static_cast<std::size_t>(*parsed);
}

std::size_t ReadPositiveOr(const EnvironmentVariable<std::string>& variable,
                           std::size_t fallback, std::size_t maximum) {
    const auto parsed = ReadOptionalPositive(variable);
    return parsed.has_value() ? std::min(*parsed, maximum) : fallback;
}

}  // namespace

NvmeKvIoConcurrencyConfig NvmeKvIoConcurrencyConfig::FromEnvironment(
    std::size_t device_queue_depth) {
    NvmeKvIoConcurrencyConfig config;
    config.max_io_concurrency = ReadPositiveOr(
        NvmeKvIoConcurrencyEnvironmentVariables::
            MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY,
        kDefaultMaxIoConcurrency, std::numeric_limits<uint32_t>::max());

    const auto configured_io =
        ReadOptionalPositive(NvmeKvIoConcurrencyEnvironmentVariables::
                                 MOONCAKE_NVME_KV_IO_CONCURRENCY);
    config.io_concurrency =
        configured_io.has_value()
            ? std::min(*configured_io, config.max_io_concurrency)
            : std::min(
                  std::max<std::size_t>(device_queue_depth, 1),
                  std::min(kDefaultIoConcurrency, config.max_io_concurrency));

    const std::size_t max_submit_concurrency =
        config.io_concurrency > 1 ? config.io_concurrency - 1 : 1;
    config.batch_submit_concurrency = ReadPositiveOr(
        NvmeKvIoConcurrencyEnvironmentVariables::
            MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY,
        std::min(kDefaultBatchSubmitConcurrency, max_submit_concurrency),
        max_submit_concurrency);
    config.root_submit_concurrency = ReadPositiveOr(
        NvmeKvIoConcurrencyEnvironmentVariables::
            MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY,
        std::min(kDefaultRootSubmitConcurrency, max_submit_concurrency),
        max_submit_concurrency);

    const std::size_t max_prepare_concurrency =
        config.io_concurrency > config.batch_submit_concurrency
            ? config.io_concurrency - config.batch_submit_concurrency
            : 1;
    config.prepare_concurrency = ReadPositiveOr(
        NvmeKvIoConcurrencyEnvironmentVariables::
            MOONCAKE_NVME_KV_PREPARE_CONCURRENCY,
        std::min(kDefaultPrepareConcurrency, max_prepare_concurrency),
        max_prepare_concurrency);
    return config;
}

}  // namespace mooncake
