#ifndef MEMFABRIC_TRANSPORT_H_
#define MEMFABRIC_TRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "memfabric_types.h"
#include "tent/common/concurrent/thread_pool.h"
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/transport/memfabric/memfabric_under_api.h"

namespace mooncake {
namespace tent {

struct MemFabricConfig {
    std::string store_url;
    std::string bind_url;
    std::string nic_url;
    int log_level = 1;
    int32_t device_id = -1;
    std::optional<smem_bm_data_op_type> data_op_type;
    std::string data_op_type_str;
};

struct MemFabricCopyCompletion {
    std::atomic<TransferStatusEnum> status{TransferStatusEnum::PENDING};
};
struct MemFabricTask {
    std::shared_ptr<MemFabricCopyCompletion> gva_completion;
    Request request;
    volatile TransferStatusEnum status_word = TransferStatusEnum::PENDING;
    uint64_t batch_id = 0;
    size_t length = 0;
    size_t transferred_bytes = 0;
};

struct MemFabricSubBatch : public Transport::SubBatch {
    std::vector<MemFabricTask> task_list;
    std::unordered_map<uint64_t, TransferStatusEnum> batch_status_cache;
    size_t pending_batch_count = 0;
    size_t capacity = 0;
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

    Status allocateLocalMemory(void**, size_t, MemoryOptions&) override;
    Status freeLocalMemory(void*, size_t) override;
    bool warmupMemory(void* address, size_t size) override;
    bool supportsGlobalAddressing() const override;
    Status mapRemoteMemory(SegmentID, void*, size_t) override;
    Status unmapRemoteMemory(SegmentID, void*) override;
    const char* getName() const override { return "memfabric"; }
    bool supportNotification() const override { return false; }

   private:
    struct GvaCopy {
        uint64_t source, destination;
        size_t size;
        uint32_t direction;
    };
    struct GvaCopyGroup {
        uint32_t direction;
        SegmentID target;
        Request::OpCode opcode;
        std::vector<MemFabricGvaCopy> copies;
        std::shared_ptr<MemFabricCopyCompletion> completion =
            std::make_shared<MemFabricCopyCompletion>();
    };
    Status submitGvaCopies(MemFabricSubBatch*, const std::vector<Request>&);
    Status resolveGvaCopies(const std::vector<Request>&, std::vector<GvaCopy>&);
    Status buildGvaCopyGroups(MemFabricSubBatch*, const std::vector<Request>&,
                              const std::vector<GvaCopy>&,
                              std::vector<std::shared_ptr<GvaCopyGroup>>&);
    void dispatchGvaCopyGroups(std::vector<std::shared_ptr<GvaCopyGroup>>&);
    std::vector<std::unique_ptr<ThreadPool>> copy_workers_;
    size_t next_copy_worker_ = 0;
    std::shared_ptr<std::atomic<size_t>> gva_pending_ =
        std::make_shared<std::atomic<size_t>>(0);
    size_t max_pending_copies_ = 256;
    Status loadConfig(const std::shared_ptr<Config>& conf);
    Status loadGvaConfig(const Config& conf, const std::string& protocol);
    Status finishInstall();
    // Called with inflight_mu_ held; also serializes registration and teardown.
    Status initializeLocked();
    Status bindDevice() const;
    Status publishLocalDevices();
    Status resolveDestUrl(const Request& request, std::string& dest_url) const;
    // Caller holds inflight_mu_; shared by public polling and quiesce().
    Status getTransferStatusLocked(SubBatchRef batch, int task_id,
                                   TransferStatus& status);

    // SMEM retains native registrations until handle destruction. Keep their
    // aligned coverage across logical remove/re-add calls as well.
    std::map<uint64_t, uint64_t>
        registered_ranges_;  // [begin, end), guarded by inflight_mu_

    bool installed_{false};
    smem_trans_t handle_{nullptr};
    std::shared_ptr<ControlService> metadata_;
    std::string local_segment_name_;
    MemFabricConfig config_;

    mutable std::mutex inflight_mu_;
    std::shared_ptr<MemFabricSharedMemory> gva_;
    MemFabricGvaConfig gva_config_;
    std::map<std::pair<SegmentID, uint64_t>, std::string> gva_remote_;
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
