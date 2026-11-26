#ifndef MOONCAKE_PG_CONTROL_PLANE_HOST_IDENTITY_H
#define MOONCAKE_PG_CONTROL_PLANE_HOST_IDENTITY_H

#include <cstdlib>
#include <fstream>
#include <limits.h>
#include <string>
#include <string_view>
#include <unistd.h>

namespace mooncake {

inline std::string makePGHostId(std::string_view override_id,
                                std::string_view boot_id,
                                std::string_view hostname) {
    if (!override_id.empty()) return "override:" + std::string(override_id);
    if (boot_id.empty() || hostname.empty()) return {};
    return "boot:" + std::string(boot_id) + ":" + std::string(hostname);
}

inline std::string detectPGHostId() {
    if (const char* configured = std::getenv("MOONCAKE_PG_HOSTID");
        configured && *configured) {
        return makePGHostId(configured, {}, {});
    }
    std::string boot_id;
    std::ifstream("/proc/sys/kernel/random/boot_id") >> boot_id;
    char hostname[HOST_NAME_MAX + 1] = {};
    if (gethostname(hostname, sizeof(hostname)) != 0) return {};
    return makePGHostId({}, boot_id, hostname);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_HOST_IDENTITY_H
