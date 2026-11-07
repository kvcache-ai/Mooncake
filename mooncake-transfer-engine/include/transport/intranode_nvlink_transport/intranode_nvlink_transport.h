// Copyright 2024 KVCache.AI

#ifndef INTRANODE_NVLINK_TRANSPORT_H_
#define INTRANODE_NVLINK_TRANSPORT_H_

#include "cuda_alike.h"

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <utility>

#include "common/hash_utils.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/intranode_nvlink_transport/vmm_fd_channel.h"
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

class IntraNodeNvlinkTransport : public Transport {
   public:
    IntraNodeNvlinkTransport();

    ~IntraNodeNvlinkTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    static void* allocatePinnedLocalMemory(size_t length);

    static void freePinnedLocalMemory(void* addr);

   protected:
    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata = true) override;

    int unregisterLocalMemory(void* addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override;

    int relocateSharedMemoryAddress(uint64_t& dest_addr, uint64_t length,
                                    uint64_t target_id);

    const char* getName() const override { return "nvlink_intraNode"; }

   private:
    std::atomic_bool running_;

    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
        // Set when the peer buffer is VMM memory mapped with cuMemMap rather
        // than opened with cudaIpcOpenMemHandle.
        bool vmm = false;
        std::vector<intranode_vmm::VmmChunk> vmm_chunks;
        std::vector<uint64_t> vmm_handles;
    };

    // A registered VMM (cuMemCreate) buffer: the chunks that cover it and
    // one retained allocation handle per chunk.
    struct VmmExport {
        uint64_t base;
        uint64_t length;
        int device;
        std::vector<intranode_vmm::VmmChunk> chunks;
        std::vector<uint64_t> handles;
    };

    int registerVmmMemory(void* addr, size_t length, int device,
                          const std::string& location);

    // Returns false if `base` is not a registered VMM buffer; otherwise
    // unregisters it and stores the result in `rc`.
    bool unregisterVmmMemory(uint64_t base, bool update_metadata, int& rc);

    int exportVmmFds(uint64_t buffer_id, std::vector<int>& fds);

    int openVmmBuffer(const intranode_vmm::VmmBufferInfo& info, uint64_t length,
                      OpenedShmEntry& shm_entry);

    static void releaseOpenedEntry(OpenedShmEntry& entry);

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    // bool use_fabric_mem_;

    std::mutex register_mutex_;
    std::unordered_set<uint64_t> registered_base_addrs_;

    // VMM exports, keyed by buffer id and by base address. Guarded by
    // vmm_mutex_, which the fd server thread also takes.
    std::mutex vmm_mutex_;
    std::unordered_map<uint64_t, VmmExport> vmm_exports_;
    std::unordered_map<uint64_t, uint64_t> vmm_ids_by_base_;
    uint64_t next_vmm_buffer_id_ = 1;
    intranode_vmm::VmmFdServer vmm_fd_server_;
};

}  // namespace mooncake

#endif  // INTRANODE_NVLINK_TRANSPORT_H_
