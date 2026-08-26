// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_TRANSPORT_H_
#define TENT_HIGH_PERFORMANCE_TCP_TRANSPORT_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "tent/runtime/tcp_transport_config.h"
#include "tent/runtime/transport.h"
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
#include "tent/transport/tcp/high_performance_tcp_client.h"
#include "tent/transport/tcp/high_performance_tcp_server.h"
#include "tent/transport/tcp/high_performance_tcp_task.h"
#include "tent/transport/tcp/high_performance_tcp_workers.h"

namespace mooncake::tent {

struct HighPerformanceTcpSubBatch : Transport::SubBatch {
    std::vector<std::shared_ptr<HighPerformanceTcpTaskState>> tasks;
    size_t max_size{0};

    size_t size() const override { return tasks.size(); }
};

class HighPerformanceTcpTransport final : public Transport {
   public:
    HighPerformanceTcpTransport();
    explicit HighPerformanceTcpTransport(HighPerformanceTcpParams params);
    ~HighPerformanceTcpTransport() override;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> config) override;
    Status uninstall() override;
    Status quiesce() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;
    Status freeSubBatch(SubBatchRef& batch) override;
    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override;
    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    bool supportsCancellation() const override { return true; }
    Status cancelTransferTask(SubBatchRef batch, int task_id) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;
    Status removeMemoryBuffer(BufferDesc& desc) override;
    bool tracksLocalBuffer(const BufferDesc& desc) const override {
        return registry_.tracks(desc.addr, desc.length);
    }

    bool supportNotification() const override { return true; }
    Status sendNotification(SegmentID target_id,
                            const Notification& notification) override;
    Status receiveNotification(
        std::vector<Notification>& notifications) override;

    const char* getName() const override { return "tcp_high_performance"; }

   private:
    struct TaskPlan;

    Status validateParams() const;
    Status planTask(const Request& request, HighPerformanceTcpSubBatch* batch,
                    TaskPlan* plan);
    Status rollbackPublishedEndpoint(
        const std::optional<std::string>& previous_attr);
    Status stopRuntime(bool close_registry);
    std::string makeIncarnation() const;

    HighPerformanceTcpParams params_;
    std::shared_ptr<ControlService> metadata_;
    std::string local_segment_name_;
    std::string incarnation_;

    std::unique_ptr<HighPerformanceTcpAdmissionController> admission_;
    std::unique_ptr<HighPerformanceTcpWorkers> workers_;
    std::unique_ptr<HighPerformanceTcpClient> client_;
    std::unique_ptr<HighPerformanceTcpServer> server_;
    HighPerformanceTcpBufferRegistry registry_;

    std::atomic<uint64_t> next_request_id_{1};
    std::atomic<bool> installed_{false};
    std::atomic<bool> stopping_{false};
    mutable std::mutex lifecycle_mutex_;

    RWSpinlock notify_lock_;
    std::vector<Notification> notifications_;
};

}  // namespace mooncake::tent

#endif  // TENT_HIGH_PERFORMANCE_TCP_TRANSPORT_H_
