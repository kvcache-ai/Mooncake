#pragma once

#include <compare>
#include <cstdint>
#include <string>

namespace mooncake::detail {

struct NofEndpoint {
    std::string transport;
    std::string address;
    std::string service;
    std::string nqn;
    uint32_t nsid = 1;
    auto operator<=>(const NofEndpoint&) const = default;
};

bool ParseNofEndpoint(const std::string& text, NofEndpoint& endpoint,
                      std::string& error);
// Resolves only existing, unambiguous kernel connections. No device paths in
// user configuration and no shell invocation or fabric connection side effects.
std::string FindLinuxNvmeDevice(
    const NofEndpoint& endpoint, std::string& error,
    const std::string& subsystem_root = "/sys/class/nvme-subsystem");

}  // namespace mooncake::detail
