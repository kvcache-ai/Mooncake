#pragma once

#include <string>

namespace mooncake {

class Environ;

struct NoFRegisterConfig {
    std::string transport_type = "RDMA";

    static NoFRegisterConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
