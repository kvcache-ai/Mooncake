#ifndef MEMFABRIC_TRANSPORT_H_
#define MEMFABRIC_TRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "memfabric_types.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/transport/memfabric/memfabric_under_api.h"

namespace mooncake {
namespace tent {

struct MemFabricConfig {
    std::string store_url;
    uint32_t device_id = 0;
    std::optional<smem_bm_data_op_type> data_op_type;
    std::string data_op_type_str;
};

struct MemFabricTask {
    Request request;
    volatile TransferStatusEnum status_word = TransferStatusEnum::PENDING;
    uint64_t batch_id = 0;
    size_t length = 0;
    size_t transferred_bytes = 0;
};

struct MemFabricSubBatch : public Transport::SubBatch {
    std::vector<MemFabricTask> task_list;
    smem_trans_t handle = nullptr;
    std::unordered_map<uint64_t, TransferStatusEnum> batch_status_cache;
    size_t pending_batch_count = 0;
    size_t size() const override { return task_list.size(); }
    ~MemFabricSubBatch() override;
};

class MemFabricTransport : public Transport {
    friend class MemFabricTransportTest;

   public:
    MemFabricTransport();
    ~MemFabricTransport() override;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;
    Status uninstall() override;
    Status quiesce() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;
    Status freeSubBatch(SubBatchRef& batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;
    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override;
    Status removeMemoryBuffer(BufferDesc& desc) override;

    const char* getName() const override { return "memfabric"; }
    bool supportNotification() const override { return false; }

   private:
    Status loadConfig(const std::shared_ptr<Config>& conf);
    Status publishLocalDevices();
    Status resolveDestUrl(const Request& request, std::string& dest_url) const;
    // Caller holds inflight_mu_; shared by public polling and quiesce().
    Status getTransferStatusLocked(SubBatchRef batch, int task_id,
                                   TransferStatus& status);

    bool installed_{false};
    smem_trans_t handle_{nullptr};
    std::shared_ptr<ControlService> metadata_;
    std::string local_segment_name_;
    MemFabricConfig config_;

    std::mutex inflight_mu_;
    std::condition_variable inflight_cv_;
    // Non-owning batch index, protected together with submission and polling.
    std::unordered_set<MemFabricSubBatch*> batches_;
    bool stopping_ = false;
    std::atomic<int> inflight_{0};
    int32_t transfer_timeout_ms_{10000};

    static std::atomic<int> g_init_ref_;
    static std::mutex g_init_mutex_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // MEMFABRIC_TRANSPORT_H_
