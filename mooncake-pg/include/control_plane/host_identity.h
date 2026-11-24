#ifndef MOONCAKE_PG_CONTROL_PLANE_HOST_IDENTITY_H
#define MOONCAKE_PG_CONTROL_PLANE_HOST_IDENTITY_H

#include <cstdlib>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>

#include <openssl/hmac.h>

#include "control_plane/control_types.h"

namespace mooncake {

inline std::optional<std::string> makePGHostId(std::string_view override_id,
                                               std::string_view boot_id,
                                               std::string_view machine_id) {
    if (!override_id.empty()) return "override:" + std::string(override_id);
    // A machine ID alone can be shared by images cloned onto different hosts.
    if (boot_id.empty()) return std::nullopt;
    auto host_id = "boot:" + std::string(boot_id) + ":";
    if (machine_id.empty()) return host_id;

    // Use an application-specific representation of the machine ID on the wire.
    constexpr std::string_view key = "mooncake-pg:host-id:v1";
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int size = 0;
    if (!HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
              reinterpret_cast<const unsigned char*>(machine_id.data()),
              machine_id.size(), digest, &size)) {
        return std::nullopt;
    }
    constexpr char hex[] = "0123456789abcdef";
    host_id.reserve(host_id.size() + size * 2);
    for (unsigned int index = 0; index < size; ++index) {
        host_id.push_back(hex[digest[index] >> 4]);
        host_id.push_back(hex[digest[index] & 0xf]);
    }
    return host_id;
}

inline std::optional<std::string> detectPGHostId() {
    if (const char* configured = std::getenv("MOONCAKE_PG_HOSTID");
        configured && *configured) {
        return makePGHostId(configured, {}, {});
    }
    std::string boot_id;
    std::string machine_id;
    std::ifstream("/proc/sys/kernel/random/boot_id") >> boot_id;
    std::ifstream("/etc/machine-id") >> machine_id;
    return makePGHostId({}, boot_id, machine_id);
}

inline uint64_t localHostPeerMask(const DeviceTransferEndpoint& local,
                                  const DeviceTransferSnapshot& snapshot) {
    static_assert(kMaxNumRanks <= 64);
    if (!local.host_id || local.host_id->empty()) return 0;
    uint64_t peers = 0;
    for (const auto rank : snapshot.participants) {
        if (rank < 0 ||
            static_cast<size_t>(rank) >= snapshot.endpoints.size() ||
            static_cast<uint32_t>(rank) >= kMaxNumRanks) {
            continue;
        }
        const auto& endpoint = snapshot.endpoints[rank];
        if (endpoint && endpoint->host_id == local.host_id) {
            peers |= uint64_t{1} << rank;
        }
    }
    return peers;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_HOST_IDENTITY_H
