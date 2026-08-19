#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace mooncake {

enum class DummyClientStatus {
    HEALTH = 0,
    DISCONNECTION,
};

inline std::ostream& operator<<(std::ostream& os,
                                const DummyClientStatus& status) noexcept {
    static const std::unordered_map<DummyClientStatus, std::string_view>
        status_strings{{DummyClientStatus::HEALTH, "HEALTH"},
                       {DummyClientStatus::DISCONNECTION, "DISCONNECTION"}};
    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

}  // namespace mooncake