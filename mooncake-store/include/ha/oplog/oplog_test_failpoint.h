#pragma once

#include <string_view>

namespace mooncake {

class TestFailPoint {
   public:
    static bool Wait(std::string_view name);
};

}  // namespace mooncake
