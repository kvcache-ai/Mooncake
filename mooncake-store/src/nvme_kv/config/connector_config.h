#pragma once

#include <cstdint>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

enum class NvmeKvTransport {
    kAuto,
    kIoUring,
    kIoctl,
};

const char* NvmeKvTransportName(NvmeKvTransport transport);

struct NvmeKvConnectorConfig {
    std::string device_path;
    uint32_t nsid = 1;
    uint32_t queue_depth = 256;
    uint32_t runtime_transfer_limit = 270336;
    NvmeKvTransport transport = NvmeKvTransport::kAuto;

    static tl::expected<NvmeKvConnectorConfig, ErrorCode> FromEnvironment();
};

}  // namespace mooncake
