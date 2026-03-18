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

#ifndef TCP_HP_TRANSPORT_H_
#define TCP_HP_TRANSPORT_H_

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include <asio/ip/tcp.hpp>
#include <asio/thread_pool.hpp>

#include "tent/common/concurrent/rw_spinlock.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/transport/tcp_hp/bounce_buffer_pool.h"
#include "tent/transport/tcp_hp/conn_manager.h"
#include "tent/transport/tcp_hp/hp_session.h"
#include "tent/transport/tcp_hp/inflight_controller.h"
#include "tent/transport/tcp_hp/io_context_pool.h"
#include "tent/transport/tcp_hp/striped_transfer.h"

namespace mooncake {
namespace tent {

struct TcpHpTask {
    Request request;
    std::atomic<TransferStatusEnum> status_word{TransferStatusEnum::INITIAL};
    std::atomic<size_t> transferred_bytes{0};
    uint64_t target_addr = 0;

    TcpHpTask() = default;
    TcpHpTask(TcpHpTask&& other) noexcept
        : request(std::move(other.request)),
          status_word(other.status_word.load(std::memory_order_relaxed)),
          transferred_bytes(other.transferred_bytes.load(std::memory_order_relaxed)),
          target_addr(other.target_addr) {}
    TcpHpTask& operator=(TcpHpTask&& other) noexcept {
        request = std::move(other.request);
        status_word.store(other.status_word.load(std::memory_order_relaxed), std::memory_order_relaxed);
        transferred_bytes.store(other.transferred_bytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
        target_addr = other.target_addr;
        return *this;
    }
    TcpHpTask(const TcpHpTask&) = delete;
    TcpHpTask& operator=(const TcpHpTask&) = delete;
};

struct TcpHpSubBatch : public Transport::SubBatch {
    std::vector<TcpHpTask> task_list;
    size_t max_size;
    std::atomic<size_t> outstanding{0};  // inflight transfers referencing tasks
    size_t size() const override { return task_list.size(); }
};

class TcpHpTransport : public Transport {
   public:
    TcpHpTransport();
    ~TcpHpTransport();

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;
    Status uninstall() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;
    Status freeSubBatch(SubBatchRef& batch) override;
    Status submitTransferTasks(
        SubBatchRef batch,
        const std::vector<Request>& request_list) override;
    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;
    Status removeMemoryBuffer(BufferDesc& desc) override;

    bool supportNotification() const override { return true; }
    Status sendNotification(SegmentID target_id,
                            const Notification& notify) override;
    Status receiveNotification(
        std::vector<Notification>& notify_list) override;

    const char* getName() const override { return "tcp_hp"; }

   private:
    void startTransfer(TcpHpSubBatch* batch, TcpHpTask* task);

    // Single-connection transfer (small buffers, GPU memory).
    void doSingleTransfer(TcpHpSubBatch* batch, TcpHpTask* task,
                          const std::string& host, uint16_t port);

    // Multi-connection striped transfer (large CPU buffers).
    // Splits the buffer into num_stripes_ contiguous sub-ranges, each sent
    // over an independent TCP connection in parallel.
    void doStripedTransfer(TcpHpSubBatch* batch, TcpHpTask* task,
                           const std::string& host, uint16_t port);

    Status findRemoteDataEndpoint(uint64_t dest_addr, uint64_t length,
                                  uint64_t target_id, std::string& host,
                                  uint16_t& data_port);
    void doAccept();
    Status startDataServer();

    bool installed_ = false;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;

    // I/O pool (Phase 1: size=1, Phase 2: configurable)
    std::unique_ptr<tcp_hp::IoContextPool> io_pool_;
    std::unique_ptr<tcp_hp::ConnManager> conn_mgr_;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
    std::atomic<bool> running_{false};
    uint16_t data_port_ = 0;
    size_t chunk_size_ = 65536;
    unsigned transfer_timeout_s_ = 30;  // 0 = no timeout

    // Multi-rail striping: split large CPU transfers across parallel connections.
    // Disabled for GPU memory (bounce-buffer path already limits parallelism).
    size_t stripe_threshold_ = 1 * 1024 * 1024;  // 0 = always stripe
    size_t num_stripes_ = 1;                       // 1 = disabled

    // Phase 2: bounce buffer pool for GPU transfers
    std::unique_ptr<tcp_hp::BounceBufferPool> bounce_pool_;

    // Phase 2: inflight transfer limiter
    std::unique_ptr<tcp_hp::InflightController> inflight_ctrl_;

    // Thread pool for blocking connection work (connect + handshake),
    // so io_context threads are never blocked by synchronous operations.
    std::unique_ptr<asio::thread_pool> connect_pool_;

    // Active server-side sessions: tracked so we can close their sockets
    // during shutdown before destroying metadata_ / SegmentManager.
    std::mutex server_sessions_mutex_;
    std::unordered_set<std::shared_ptr<tcp_hp::HpSession>> server_sessions_;

    // Notification (same as TcpTransport)
    RWSpinlock notify_lock_;
    std::vector<Notification> notify_list_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_TRANSPORT_H_
