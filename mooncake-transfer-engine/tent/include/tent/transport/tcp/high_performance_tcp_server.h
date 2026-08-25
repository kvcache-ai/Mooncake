// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_SERVER_H_
#define TENT_HIGH_PERFORMANCE_TCP_SERVER_H_
#include <asio.hpp>
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include "tent/common/status.h"
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
namespace mooncake::tent {
class HighPerformanceTcpServer {
 public:
  struct Config { std::string bind_address; uint16_t port{0}; uint64_t max_transfer_bytes{1ULL<<30}; size_t chunk_size{1ULL<<20}; };
  HighPerformanceTcpServer(Config config, HighPerformanceTcpBufferRegistry* registry);
  ~HighPerformanceTcpServer();
  Status start(uint16_t* bound_port); Status stop();
 private:
  void acceptLoop(); void serve(asio::ip::tcp::socket socket);
  Config config_; HighPerformanceTcpBufferRegistry* registry_; asio::io_context io_; std::unique_ptr<asio::ip::tcp::acceptor> acceptor_; std::thread accept_thread_; std::mutex session_mutex_; std::vector<std::thread> sessions_; std::atomic<bool> stopping_{false};
};
}  // namespace mooncake::tent
#endif
