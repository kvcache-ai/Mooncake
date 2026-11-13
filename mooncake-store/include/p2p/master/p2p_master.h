#pragma once

#include <atomic>
#include <csignal>
#include <cstdint>
#include <functional>
#include <string>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "p2p/common/p2p_master_config.h"

namespace mooncake {

class P2PMasterRpcService;
class OpLogManager;

class P2PMaster {
   public:
    explicit P2PMaster(const P2PMasterConfig& config);

    int Run();

    // A process intentionally owns one P2P master instance. The signal
    // handler and focused tests shutdown through this static request.
    static void RequestShutdown();
    static bool IsShutdownRequested();

   private:
    static void HandleSignal(int signal);
    int RunStandalone();
    int RunWithHA();
    static bool DrainOpLogBeforeStop(OpLogManager& manager, uint32_t timeout_ms,
                                     const char* reason = "stepdown");
    int RunActiveRpcServers(coro_rpc::coro_rpc_server& server,
                            P2PMasterRpcService& service,
                            std::function<void()> after_start = {},
                            std::function<void()> after_stop = {},
                            std::function<bool()> stopping = {},
                            std::function<bool()> stepping_down = {},
                            std::string* startup_error = nullptr);

    P2PMasterConfig config_;
    static std::atomic<bool> shutdown_requested_;
};

}  // namespace mooncake
