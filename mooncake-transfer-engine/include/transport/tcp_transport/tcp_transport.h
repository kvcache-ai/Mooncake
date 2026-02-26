// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TCP_TRANSPORT_H_
#define TCP_TRANSPORT_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <asio/ip/tcp.hpp>

#include "transfer_metadata.h"
#include "transport/transport.h"
#include "ylt/coro_io/coro_io.hpp"

namespace mooncake {
class TransferMetadata;
class TcpContext;
class TcpTransport;

// Pooled connection for reusing client-side connections
struct PooledConnection {
    std::shared_ptr<asio::ip::tcp::socket> socket;
    std::string host;
    uint16_t port;
    std::chrono::steady_clock::time_point last_used;
    bool in_use;

    PooledConnection(std::shared_ptr<asio::ip::tcp::socket> s,
                     const std::string &h, uint16_t p)
        : socket(std::move(s)),
          host(h),
          port(p),
          last_used(std::chrono::steady_clock::now()),
          in_use(true) {}
};

class TcpTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    TcpTransport();

    ~TcpTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo);

    int startHandshakeDaemon();

    int allocateLocalSegmentID(int tcp_data_port);

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata);

    int unregisterLocalMemory(void *addr, bool update_metadata = false);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location);

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    void worker();

    void startTransfer(Slice *slice);

    const char *getName() const override { return "tcp"; }

   private:
    TcpContext *context_;
    std::atomic_bool running_;
    std::thread thread_;
    bool enable_connection_pool_ =
        false;  // Use MC_TCP_ENABLE_CONNECTION_POOL=1 to enable connection pool

    // Client-side connection pool
    struct ConnectionKey {
        std::string host;
        uint16_t port;

        bool operator==(const ConnectionKey &other) const {
            return host == other.host && port == other.port;
        }
    };

    struct ConnectionKeyHash {
        std::size_t operator()(const ConnectionKey &key) const {
            return std::hash<std::string>()(key.host) ^
                   (std::hash<uint16_t>()(key.port) << 1);
        }
    };

    std::unordered_map<ConnectionKey,
                       std::deque<std::shared_ptr<PooledConnection>>,
                       ConnectionKeyHash>
        connection_pool_;
    std::mutex pool_mutex_;

    std::shared_ptr<asio::ip::tcp::socket> getConnection(
        const std::string &host, uint16_t port);
    void returnConnection(const std::string &host, uint16_t port,
                          std::shared_ptr<asio::ip::tcp::socket> socket);
    void cleanupIdleConnections();

    static constexpr std::chrono::seconds kConnectionIdleTimeout{60};
};
}  // namespace mooncake

#endif
