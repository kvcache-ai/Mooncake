#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "nvme_kv/executor_util.h"

namespace mooncake {

enum class NvmeKvTransport {
    kAuto,
    kIoUring,
    kIoctl,
};

tl::expected<NvmeKvTransport, ErrorCode> ParseNvmeKvTransport(
    std::string_view transport);

const char* NvmeKvTransportName(NvmeKvTransport transport);

struct NvmeKvConnectorConfig {
    std::string device_path;
    uint32_t nsid = 1;
    uint32_t queue_depth = kDefaultNvmeKvQueueDepth;
    uint32_t runtime_transfer_limit = kDefaultNvmeKvRuntimeTransferLimit;
    NvmeKvTransport transport = NvmeKvTransport::kAuto;

    static tl::expected<NvmeKvConnectorConfig, ErrorCode> FromEnvironment();
};

}  // namespace mooncake
