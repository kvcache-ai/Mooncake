#include "config/rpc_protocol_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

RpcProtocolConfig RpcProtocolConfig::FromEnvironment(const Environ& env) {
    using Variables = CommonEnvironmentVariables::Rpc;
    const auto value = env.GetTyped(Variables::MC_RPC_PROTOCOL);
    return RpcProtocolConfig{.use_rdma = value && *value == "rdma"};
}

}  // namespace mooncake
