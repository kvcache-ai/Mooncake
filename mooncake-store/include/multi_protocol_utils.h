#pragma once

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

namespace mooncake {

// Parse a comma-separated protocol string (for example, "cxl,tcp").
[[nodiscard]] inline std::vector<std::string> parseProtocolList(
    const std::string& protocol) {
    std::vector<std::string> result;
    std::stringstream stream(protocol);
    std::string item;
    while (std::getline(stream, item, ',')) {
        if (!item.empty()) {
            result.push_back(item);
        }
    }
    return result;
}

// Multi-protocol tiering currently supports one CXL transport plus one RAM
// transport.
[[nodiscard]] inline bool isValidMultiProtocol(
    const std::vector<std::string>& protocols) {
    if (protocols.size() != 2) {
        return false;
    }
    const bool has_cxl =
        std::find(protocols.begin(), protocols.end(), "cxl") != protocols.end();
    const bool has_tcp =
        std::find(protocols.begin(), protocols.end(), "tcp") != protocols.end();
    const bool has_rdma = std::find(protocols.begin(), protocols.end(), "rdma") !=
                          protocols.end();
    return has_cxl && (has_tcp || has_rdma);
}

}  // namespace mooncake
