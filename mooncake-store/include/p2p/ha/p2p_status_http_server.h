#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <ylt/coro_http/coro_http_server.hpp>

namespace mooncake {

// Read-only diagnostic HTTP server owned by the P2P master supervisor loop.
// It starts before leader election so a standby keeps /ha/status, /ha/doctor
// and /metrics reachable while blocking on leadership, and stops right before
// the active RPC service binds the same metrics port.
class P2PStatusHttpServer {
   public:
    explicit P2PStatusHttpServer(uint16_t port,
                                 std::string host = "0.0.0.0")
        : port_(port), host_(std::move(host)) {}

    P2PStatusHttpServer(const P2PStatusHttpServer&) = delete;
    P2PStatusHttpServer& operator=(const P2PStatusHttpServer&) = delete;

    ~P2PStatusHttpServer() { Stop(); }

    void Start();
    void Stop();

    // Returns the actually bound port; useful when constructed with port 0.
    uint16_t port() const;

   private:
    uint16_t port_;
    std::string host_;
    std::unique_ptr<coro_http::coro_http_server> server_;
};

// Registers the read-only diagnostic handlers shared by the per-term status
// server and the active P2PMasterRpcService HTTP server: /ha/status,
// /ha/doctor, /server_info, /metrics, /metrics/summary and /health.
void RegisterStatusHttpHandlers(coro_http::coro_http_server& server);

}  // namespace mooncake
