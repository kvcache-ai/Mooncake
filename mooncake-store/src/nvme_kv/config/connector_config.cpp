#include "nvme_kv/connector_config.h"

#include <glog/logging.h>

#include <optional>
#include <string>

#include "environ.h"
#include "environment_variables.h"
#include "u32_parser.h"

namespace mooncake {
namespace {

uint32_t ReadNvmeKvU32Or(const EnvironmentVariable<std::string>& variable,
                         uint32_t fallback) {
    const auto value = Environ::Read(variable);
    if (!value.has_value()) {
        return fallback;
    }
    return TryParseNvmeKvU32(value->c_str()).value_or(fallback);
}

}  // namespace

tl::expected<NvmeKvTransport, ErrorCode> ParseNvmeKvTransport(
    std::string_view transport) {
    if (transport.empty() || transport == "auto") {
        return NvmeKvTransport::kAuto;
    }
    if (transport == "io_uring") {
        return NvmeKvTransport::kIoUring;
    }
    if (transport == "ioctl") {
        return NvmeKvTransport::kIoctl;
    }
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

const char* NvmeKvTransportName(NvmeKvTransport transport) {
    switch (transport) {
        case NvmeKvTransport::kAuto:
            return "auto";
        case NvmeKvTransport::kIoUring:
            return "io_uring";
        case NvmeKvTransport::kIoctl:
            return "ioctl";
    }
    return "unknown";
}

tl::expected<NvmeKvConnectorConfig, ErrorCode>
NvmeKvConnectorConfig::FromEnvironment() {
    NvmeKvConnectorConfig config;
    config.device_path =
        Environ::Read(
            NvmeKvConnectorEnvironmentVariables::MOONCAKE_NVME_KV_DEVICE_PATH)
            .value_or("");
    if (config.device_path.empty()) {
        LOG(ERROR) << "MOONCAKE_NVME_KV_DEVICE_PATH must not be empty";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    config.nsid = ReadNvmeKvU32Or(
        NvmeKvConnectorEnvironmentVariables::MOONCAKE_NVME_KV_NSID,
        config.nsid);
    config.queue_depth = ReadNvmeKvU32Or(
        NvmeKvConnectorEnvironmentVariables::MOONCAKE_NVME_KV_QUEUE_DEPTH,
        config.queue_depth);
    config.runtime_transfer_limit =
        ReadNvmeKvU32Or(NvmeKvConnectorEnvironmentVariables::
                            MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT,
                        config.runtime_transfer_limit);
    const auto transport_name =
        Environ::Read(
            NvmeKvConnectorEnvironmentVariables::MOONCAKE_NVME_KV_TRANSPORT)
            .value_or("");
    auto transport = ParseNvmeKvTransport(transport_name);
    if (!transport.has_value()) {
        LOG(ERROR) << "Unknown MOONCAKE_NVME_KV_TRANSPORT: " << transport_name;
        return tl::make_unexpected(transport.error());
    }
    config.transport = *transport;
    return config;
}

}  // namespace mooncake
