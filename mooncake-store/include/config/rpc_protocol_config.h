#pragma once

namespace mooncake {

struct RpcProtocolConfig {
    bool use_rdma{false};

    static RpcProtocolConfig FromEnvironment();
};

}  // namespace mooncake
