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

#ifndef MNNVL_TRANSPORT_H_
#define MNNVL_TRANSPORT_H_

#include <cuda.h>
#include <cuda_runtime.h>
#include "tent/common/cuda_fabric_compat.h"

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/platform/cuda.h"

namespace mooncake {
namespace tent {

// A peer buffer imported into this process. Identified by the peer's base
// address AND the mnnvl_handle it was imported from: the peer may publish a
// new allocation at the same address (free + allocate, or a restart), and a
// mapping of the old allocation must then not be served again.
struct MnnvlMappingEntry {
    void *mnnvl_addr = nullptr;  // local VA the import is mapped at
    uint64_t length = 0;         // mapped extent (the peer's BufferDesc length)
    std::string mnnvl_handle;    // handle the mapping was imported from
    // Tasks using this mapping. A retired entry is unmapped only when this
    // drops to zero, so an in-flight copy never races with the teardown.
    std::atomic<int> pins{0};
    // Set (under relocate_lock_) once the entry left relocate_map_.
    bool stale = false;
};

struct MnnvlTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
    cudaEvent_t completion_event = nullptr;
    // Mapping that target_addr points into (remote targets only). Pinned at
    // submit, released when the task reaches a terminal state or the batch
    // is freed.
    std::shared_ptr<MnnvlMappingEntry> mapping;
};

struct MnnvlSubBatch : public Transport::SubBatch {
    std::vector<MnnvlTask> task_list;
    size_t max_size;
    CUDAStreamHandle sync_stream;
    CUDAStreamHandle async_stream;
    int stream_device_id = -1;
    // Completion events created in startTransfer (one per submit). Destroyed by
    // the destructor (RAII); Slab<T>::deallocate() invokes ~MnnvlSubBatch()
    // before reusing the storage, so this runs on every free.
    std::vector<cudaEvent_t> completion_events;
    ~MnnvlSubBatch() {
        for (auto event : completion_events) cudaEventDestroy(event);
    }
    virtual size_t size() const { return task_list.size(); }
};

class MnnvlTransport : public Transport {
    friend class MnnvlTransportTestPeer;

   public:
    MnnvlTransport();

    ~MnnvlTransport();

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

    virtual const char *getName() const { return "mnnvl"; }

    virtual Status allocateLocalMemory(void **addr, size_t size,
                                       MemoryOptions &options);

    virtual Status freeLocalMemory(void *addr, size_t size);

   private:
    void startTransfer(std::vector<MnnvlTask *> &tasks, MnnvlSubBatch *batch);

    void *createSharedMemory(const std::string &path, size_t size);

    using OpenedMnnvlEntry = MnnvlMappingEntry;
    using EntryRef = std::shared_ptr<OpenedMnnvlEntry>;
    using HashMap =
        std::unordered_map<SegmentID, std::unordered_map<uint64_t, EntryRef>>;

    // Translates a peer address into this process. The cached mapping is
    // used only if its mnnvl_handle matches the peer's current descriptor;
    // otherwise the old mapping is retired and the new allocation imported.
    // With `pinned` set, the resolved entry is pinned and handed back; the
    // caller must unpin() it once the transfer is done. Without it the
    // returned address is only safe while no other thread can retire the
    // mapping (single-threaded tests); real callers always pin.
    Status relocateSharedMemoryAddress(uint64_t &dest_addr, uint64_t length,
                                       uint64_t target_id,
                                       EntryRef *pinned = nullptr);

    // One resolution pass; `lost_race` is set when another thread published
    // a different generation of the buffer while this one was importing.
    Status relocateOnce(uint64_t &dest_addr, uint64_t length,
                        uint64_t target_id, EntryRef *pinned, bool &lost_race);

    // True if `entry` was imported from the allocation `mnnvl_handle` names.
    static bool sameGeneration(const OpenedMnnvlEntry &entry,
                               const std::string &mnnvl_handle);

    // Drops one pin; a retired entry whose last pin is released is unmapped.
    void unpin(EntryRef &entry);

    // Imports `mnnvl_handle` and maps it (reserve, map, set access). Nothing
    // is left behind on failure.
    Status importMapping(uint64_t peer_addr, uint64_t length,
                         const std::string &mnnvl_handle, EntryRef &entry);

    // Marks the entry at `slot` stale and removes it from relocate_map_
    // (caller holds the write lock; both iterators are invalidated); retired
    // entries with no pins are appended to `to_release`.
    void retireLocked(HashMap::iterator target,
                      HashMap::mapped_type::iterator slot,
                      std::vector<EntryRef> &to_release);

    // Retires every mapping of `target_id` that `segment` no longer
    // publishes: the address is gone, or carries a different handle. Run
    // only when `segment` is authoritative (the local snapshot, or a
    // descriptor that was just refetched), so a peer that restarted onto a
    // different address does not leave its old mappings behind until
    // uninstall(). Caller holds the write lock.
    void retireUnpublishedLocked(SegmentID target_id,
                                 const SegmentDesc &segment,
                                 std::vector<EntryRef> &to_release);

    // Moves retired entries whose pins dropped to zero into `to_release`
    // (caller holds the write lock).
    void collectReleasableLocked(std::vector<EntryRef> &to_release);

    // cuMemUnmap + cuMemAddressFree. Never called under relocate_lock_.
    static void releaseMappings(std::vector<EntryRef> &entries);

    Status setPeerAccess();

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    CudaPlatform *platform_;

    RWSpinlock relocate_lock_;
    HashMap relocate_map_;  // live mappings: target -> peer base addr -> entry
    // Mappings replaced or uninstalled while tasks still pin them; unmapped
    // as soon as the last pin drops.
    std::vector<EntryRef> retired_;
    std::shared_ptr<Config> conf_;

    std::string machine_id_;

    std::mutex allocate_mutex_;
    // VMM allocations owned by this transport: base address -> mapped extent
    // (rounded to the allocation granularity), needed to unmap on free.
    std::unordered_map<void *, size_t> allocate_set_;
    uint64_t async_memcpy_threshold_;
    bool supported_;
    CUmemAllocationHandleType handle_type_;

    bool host_register_;
    // transports/mnnvl/egm: export HOST_NUMA (EGM) allocations so peers can
    // read/write host memory over NVLink; adds dram_to_dram / gpu_to_dram.
    bool egm_enabled_ = false;
};
}  // namespace tent
}  // namespace mooncake

#endif  // MNNVL_TRANSPORT_H_
