#pragma once

#include <istream>
#include "nvme_kv/connector_config.h"

namespace mooncake {

struct SharedNvmeKvConfig {
    std::vector<NvmeKvConnectorConfig> namespaces;

    static tl::expected<SharedNvmeKvConfig, std::string> Parse(
        std::istream& input);
};

}  // namespace mooncake
