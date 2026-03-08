// Copyright 2025 KVCache.AI
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

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct TcpTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
};

struct TcpSubBatch : public Transport::SubBatch {
    std::vector<TcpTask> task_list;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

class TcpTransport : public Transport {
   public:
    TcpTransport();

    ~TcpTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status);

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options);

    virtual Status removeMemoryBuffer(BufferDesc &desc);

    virtual const char *getName() const { return "tcp"; }

    virtual bool supportNotification() const { return true; }

    virtual Status sendNotification(SegmentID target_id,
                                    const Notification &notify);

    virtual Status receiveNotification(std::vector<Notification> &notify_list);

   private:
    // Initiate an async transfer for one task via raw TCP socket.
    void startTransfer(TcpTask *task);

    // Resolve the remote segment to get the TCP data endpoint (host:dataport).
    Status findRemoteDataEndpoint(uint64_t dest_addr, uint64_t length,
                                  uint64_t target_id, std::string &host,
                                  uint16_t &data_port);

    // Server-side: start accepting incoming data connections.
    void doAccept();

    // Find an available TCP port and bind the acceptor.
    Status startDataServer();

    // Connection pool: acquire/release reusable TCP connections.
    asio::ip::tcp::socket acquireConnection(const std::string &host,
                                            uint16_t port);
    void releaseConnection(const std::string &key,
                           asio::ip::tcp::socket socket);

   private:
    bool installed_ = false;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;

    // ASIO data plane
    std::unique_ptr<asio::io_context> io_context_;
    std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
        work_guard_;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
    std::thread worker_thread_;
    std::atomic<bool> running_{false};
    uint16_t data_port_ = 0;

    // Connection pool for reusing TCP connections to remote endpoints.
    std::mutex pool_mutex_;
    std::unordered_map<std::string, std::vector<asio::ip::tcp::socket>>
        conn_pool_;

    RWSpinlock notify_lock_;
    std::vector<Notification> notify_list_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_TRANSPORT_H_
