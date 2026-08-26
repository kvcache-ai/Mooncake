// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_SERVER_H_
#define TENT_HIGH_PERFORMANCE_TCP_SERVER_H_

#include <asio.hpp>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
#include "tent/transport/tcp/high_performance_tcp_workers.h"

namespace mooncake::tent {

class HighPerformanceTcpServer {
   public:
    struct Config {
        std::string bind_address;
        uint16_t port{0};
        uint64_t max_transfer_bytes{1ULL << 30};
        size_t chunk_size{1ULL << 20};
        uint64_t progress_timeout_ms{30000};
        size_t max_connections{4096};
    };

    HighPerformanceTcpServer(Config config,
                             HighPerformanceTcpBufferRegistry* registry,
                             HighPerformanceTcpWorkers* workers);
    ~HighPerformanceTcpServer();

    HighPerformanceTcpServer(const HighPerformanceTcpServer&) = delete;
    HighPerformanceTcpServer& operator=(const HighPerformanceTcpServer&) =
        delete;

    Status start(uint16_t* bound_port);
    Status stopAccepting();
    Status cancelAll();
    Status stop();

    size_t activeSessionsForTest() const {
        return active_sessions_.load(std::memory_order_acquire);
    }

   private:
    class Session;

    Status startAccept();
    void installAcceptedSocket(size_t worker_id,
                               std::shared_ptr<asio::ip::tcp::socket> socket);
    bool reserveConnection();
    void onSessionClosed(size_t worker_id,
                         const std::shared_ptr<Session>& session);
    void cancelWorkerSessions(size_t worker_id);

    Config config_;
    HighPerformanceTcpBufferRegistry* registry_{nullptr};
    HighPerformanceTcpWorkers* workers_{nullptr};

    asio::io_context accept_io_;
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>>
        accept_guard_;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
    std::thread accept_thread_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<size_t> next_worker_{0};
    std::atomic<size_t> active_sessions_{0};

    // Each set is touched only on the corresponding worker context.
    std::vector<std::unordered_set<std::shared_ptr<Session>>> sessions_;
    mutable std::mutex sessions_wait_mutex_;
    std::condition_variable sessions_wait_cv_;
};

}  // namespace mooncake::tent

#endif  // TENT_HIGH_PERFORMANCE_TCP_SERVER_H_
