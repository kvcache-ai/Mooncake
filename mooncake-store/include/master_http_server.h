#pragma once

#include <atomic>
#include <csignal>
#include <cstdint>
#include <ylt/coro_http/coro_http_server.hpp>

namespace mooncake {

class WrappedMasterService;

class MasterHttpServer {
   public:
    explicit MasterHttpServer(uint16_t port, size_t thread_num = 4);
    ~MasterHttpServer();

    MasterHttpServer(const MasterHttpServer&) = delete;
    MasterHttpServer& operator=(const MasterHttpServer&) = delete;

    void Start();
    void Stop();

    void SetService(WrappedMasterService* service);
    void ClearService();

   private:
    void RegisterHandlers();

    coro_http::coro_http_server http_server_;
    std::atomic<WrappedMasterService*> service_{nullptr};
};

}  // namespace mooncake
