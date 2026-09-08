// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_UB_TRANSPORT_H_
#define TENT_TRANSPORT_UB_UB_TRANSPORT_H_

#include <memory>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake::tent {

namespace ub {
class UrmaAdapter;
struct UbTask;
}  // namespace ub

struct UbSubBatch final : public Transport::SubBatch {
    std::vector<std::shared_ptr<ub::UbTask>> task_list;
    size_t max_size{0};

    size_t size() const override { return task_list.size(); }
};

// TENT-native UB transport. The implementation owns its task/slice scheduler,
// endpoint store and URMA resources; it never converts requests to Classic TE
// types or calls the Classic UbTransport/UbWorkerPool data path.
class UbTransport final : public Transport {
   public:
    explicit UbTransport(std::shared_ptr<ub::UrmaAdapter> adapter = nullptr);
    ~UbTransport() override;

    UbTransport(const UbTransport&) = delete;
    UbTransport& operator=(const UbTransport&) = delete;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;
    Status uninstall() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;
    Status freeSubBatch(SubBatchRef& batch) override;
    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;
    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    bool supportsCancellation() const override { return true; }
    Status cancelTransferTask(SubBatchRef batch, int task_id) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;
    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override;
    Status removeMemoryBuffer(BufferDesc& desc) override;
    bool warmupMemory(void* addr, size_t length) override;

    const char* getName() const override { return "ub"; }
    double getEstimatedBandwidth() const override;
    bool supportNotification() const override { return false; }

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::tent

#endif  // TENT_TRANSPORT_UB_UB_TRANSPORT_H_
