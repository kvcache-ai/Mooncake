#include "nvme_kv_u32_parser.h"

#include <cerrno>
#include <cstdlib>
#include <limits>

namespace mooncake {

std::optional<uint32_t> TryParseNvmeKvU32(const char* value) {
    if (value == nullptr) {
        return std::nullopt;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long parsed = std::strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed > std::numeric_limits<uint32_t>::max()) {
        return std::nullopt;
    }
    return static_cast<uint32_t>(parsed);
}

}  // namespace mooncake
