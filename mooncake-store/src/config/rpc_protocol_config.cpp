#include "config/rpc_protocol_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

RpcProtocolConfig RpcProtocolConfig::FromEnvironment() {
    using Variables = RpcProtocolEnvironmentVariables;
    const auto value = Environ::Read(Variables::MC_RPC_PROTOCOL);
    return RpcProtocolConfig{.use_rdma = value && *value == "rdma"};
}

}  // namespace mooncake
