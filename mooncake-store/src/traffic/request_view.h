#pragma once

#include <cstdint>
#include <string_view>

namespace mooncake::traffic {

struct RequestView {
    std::uint32_t route_key = 0;
    std::string_view payload;
    std::uint64_t conn_id = 0;
    std::uint32_t seq_num = 0;
    std::uint32_t attach_length = 0;
};

}  // namespace mooncake::traffic
