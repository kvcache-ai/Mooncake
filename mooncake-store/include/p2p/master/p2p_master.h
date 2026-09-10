#pragma once

#include <functional>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "p2p/common/p2p_master_config.h"

namespace mooncake {

class P2PMasterRpcService;

class P2PMaster {
   public:
    explicit P2PMaster(const P2PMasterConfig& config);

    int Run();

   private:
    int RunStandalone();
    int RunWithHA();
    int RunActiveRpcServers(coro_rpc::coro_rpc_server& server,
                            P2PMasterRpcService& service,
                            std::function<void()> before_start = {});

    P2PMasterConfig config_;
};

}  // namespace mooncake
