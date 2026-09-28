#pragma once

namespace mooncake {

class Environ;

struct RpcProtocolConfig {
    bool use_rdma{false};

    static RpcProtocolConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
