#pragma once

#include <string>

namespace mooncake {

struct NoFRegisterConfig {
    std::string transport_type = "RDMA";

    static NoFRegisterConfig FromEnvironment();
};

}  // namespace mooncake
