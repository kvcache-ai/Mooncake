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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pybind11/pybind11.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <stack>
#include <unordered_set>
#include <vector>

#include "common/base/status.h"
#include "transfer_engine.h"
#include "transfer_engine_c.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/transport.h"

using namespace mooncake;

class ScatterTransferTicket;

const static size_t kDefaultBufferCapacity = 2ull * 1024 * 1024 * 1024;
const static size_t kSlabSizeKBTabLen = 16;
const static size_t kMaxClassId = kSlabSizeKBTabLen - 1;
const static size_t kSlabSizeKB[] = {
    8,         16,        32,         64,        128,      256,
    512,       1024,      2 * 1024,   4 * 1024,  8 * 1024, 16 * 1024,
    32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024};

class TransferEnginePy {
   public:
    enum class TransferOpcode { READ = 0, WRITE = 1 };
    struct TransferNotify {
        std::string name;
        std::string msg;
    };

   public:
    using BatchDesc = Transport::BatchDesc;

   public:
    TransferEnginePy();

    ~TransferEnginePy();

    int initialize(const char *local_hostname, const char *metadata_server,
                   const char *protocol, const char *device_name);

    int initializeExt(const char *local_hostname, const char *metadata_server,
                      const char *protocol, const char *device_name,
                      const char *metadata_type);

    int getRpcPort();

    uintptr_t allocateManagedBuffer(size_t length);

    int freeManagedBuffer(uintptr_t user_tensor, size_t length);

    int transferSyncWrite(const char *target_hostname, uintptr_t buffer,
                          uintptr_t peer_buffer_address, size_t length,
                          const std::string &transport_hint = "",
                          const std::string &nic_hint = "");

    batch_id_t transferSubmitWrite(const char *target_hostname,
                                   uintptr_t buffer,
                                   uintptr_t peer_buffer_address, size_t length,
                                   const std::string &transport_hint = "",
                                   const std::string &nic_hint = "");

    int transferCheckStatus(batch_id_t batch_id);

    int transferSyncRead(const char *target_hostname, uintptr_t buffer,
                         uintptr_t peer_buffer_address, size_t length,
                         const std::string &transport_hint = "",
                         const std::string &nic_hint = "");

    int batchTransferSyncWrite(const char *target_hostname,
                               std::vector<uintptr_t> buffers,
                               std::vector<uintptr_t> peer_buffer_addresses,
                               std::vector<size_t> lengths,
                               const std::string &transport_hint = "",
                               const std::string &nic_hint = "");

    int batchTransferSyncRead(const char *target_hostname,
                              std::vector<uintptr_t> buffers,
                              std::vector<uintptr_t> peer_buffer_addresses,
                              std::vector<size_t> lengths,
                              const std::string &transport_hint = "",
                              const std::string &nic_hint = "");

    std::shared_ptr<ScatterTransferTicket> scatterTransferSyncWriteWithTicket(
        const std::string &endpoint,
        const std::vector<uintptr_t> &local_base_addresses,
        const std::vector<size_t> &local_capacities,
        const std::vector<uint64_t> &remote_base_addresses,
        const std::vector<size_t> &remote_capacities,
        const std::vector<std::vector<size_t>> &local_offsets,
        const std::vector<std::vector<size_t>> &remote_offsets,
        const std::vector<std::vector<size_t>> &lengths);

    std::shared_ptr<ScatterTransferTicket> scatterTransferSyncReadWithTicket(
        const std::string &endpoint,
        const std::vector<uintptr_t> &local_base_addresses,
        const std::vector<size_t> &local_capacities,
        const std::vector<uint64_t> &remote_base_addresses,
        const std::vector<size_t> &remote_capacities,
        const std::vector<std::vector<size_t>> &local_offsets,
        const std::vector<std::vector<size_t>> &remote_offsets,
        const std::vector<std::vector<size_t>> &lengths);

    batch_id_t batchTransferAsyncWrite(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths,
        const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    batch_id_t batchTransferAsyncRead(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths,
        const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    // Buffer-protocol variants of the batch APIs. Each argument is a
    // contiguous 1-D array of 8-byte integers (a NumPy uint64/int64 array,
    // an array.array('Q'), ...). Large batches skip building three Python
    // lists of ints and converting them one element at a time.
    int batchTransferSyncWriteArrays(
        const char *target_hostname, const pybind11::buffer &buffers,
        const pybind11::buffer &peer_buffer_addresses,
        const pybind11::buffer &lengths, const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    int batchTransferSyncReadArrays(
        const char *target_hostname, const pybind11::buffer &buffers,
        const pybind11::buffer &peer_buffer_addresses,
        const pybind11::buffer &lengths, const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    batch_id_t batchTransferAsyncWriteArrays(
        const char *target_hostname, const pybind11::buffer &buffers,
        const pybind11::buffer &peer_buffer_addresses,
        const pybind11::buffer &lengths, const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    batch_id_t batchTransferAsyncReadArrays(
        const char *target_hostname, const pybind11::buffer &buffers,
        const pybind11::buffer &peer_buffer_addresses,
        const pybind11::buffer &lengths, const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    // Page-gathered batch transfer: `fields` is a C-contiguous [F, 5] array
    // of 8-byte integers, one row (src_base, src_stride, dst_base,
    // dst_stride, length) per field; `src_pages` / `dst_pages` are 1-D
    // arrays of P page ids. Descriptor (f, i) copies `length` bytes from
    // src_base + src_pages[i] * src_stride to dst_base + dst_pages[i] *
    // dst_stride. The F x P descriptors are expanded here, field-major, and
    // submitted in batches of at most `max_batch_size` (0: one batch).
    // Returns 0, or the first batch's nonzero status.
    int batchTransferSyncWritePages(const char *target_hostname,
                                    const pybind11::buffer &src_pages,
                                    const pybind11::buffer &dst_pages,
                                    const pybind11::buffer &fields,
                                    size_t max_batch_size = 0,
                                    const std::string &transport_hint = "",
                                    const std::string &nic_hint = "");

    int batchTransferSyncReadPages(const char *target_hostname,
                                   const pybind11::buffer &src_pages,
                                   const pybind11::buffer &dst_pages,
                                   const pybind11::buffer &fields,
                                   size_t max_batch_size = 0,
                                   const std::string &transport_hint = "",
                                   const std::string &nic_hint = "");

    int batchTransferSyncPages(const char *target_hostname,
                               const pybind11::buffer &src_pages,
                               const pybind11::buffer &dst_pages,
                               const pybind11::buffer &fields,
                               size_t max_batch_size, TransferOpcode opcode,
                               const std::string &transport_hint,
                               const std::string &nic_hint);

    // Engine-side gather + write: gathers scattered GPU segments
    // (gpu_segments[i], lengths[i]) into a pinned staging slot, then pushes
    // the slot to remote_base at the target as one contiguous async WRITE.
    // split > 1 splits that WRITE into `split` equal contiguous descriptors
    // (target offsets remote_base + chunk boundaries) within one batch, so
    // the TCP transport spreads them across lanes — a single multi-GB
    // descriptor would be pinned to one lane (~2.3GB/s vs ~4.9GB/s
    // aggregate). The remote bytes land identically either way, so the
    // consumer needs no protocol change.
    // Returns a batch_id to poll with transferCheckStatus, or 0 on failure
    // (no slot available, gather or submit failed) — the caller is expected
    // to fall back to a per-descriptor path. The slot stays in
    // gather_slots_ until the caller releases it with releaseGatherSlot
    // after observing a terminal transfer status; it is never released
    // internally, because polling the batch from a second thread would race
    // the caller's transferCheckStatus, which frees the batch on completion
    // (a concurrent reader of the freed BatchDesc corrupts the heap).
    batch_id_t submitGatherWrite(const char *target_hostname,
                                 const std::vector<uintptr_t> &gpu_segments,
                                 const std::vector<size_t> &lengths,
                                 uintptr_t remote_base, size_t split);

    // Hand the staging slot of a finished gather batch back to the pool.
    // Must be called at most once per batch, after transferCheckStatus
    // returned a terminal status (1/-1). Calling it while the transfer is
    // still in flight (e.g. TIMEOUT) is unsafe: the session may still read
    // from the slot.
    void releaseGatherSlot(batch_id_t batch_id);

    // Engine-side receive slot (the receive-side mirror of
    // submitGatherWrite): check out a pinned staging slot for one merged
    // remote WRITE to land in. The slot buffer is registered as local
    // memory on first checkout so a peer can target it, and stays
    // registered for the process lifetime. Returns (slot_addr, capacity),
    // or (0, 0) when no slot is free — the caller then advertises addr 0
    // and the producer falls back to per-descriptor transfers. The slot
    // stays checked out until releaseScatterSlot.
    std::pair<uintptr_t, size_t> acquireScatterSlot();

    // Scatter the bytes sitting in scatter slot `slot_addr` out to GPU
    // destinations: dst_segments[i] receives lengths[i] bytes, read
    // contiguously from the slot starting at offset 0, in segment order
    // (the exact mirror of the packing submitGatherWrite performs). Blocks
    // until the scatter is applied to device memory. Returns 0 on success.
    // Does not release the slot; call releaseScatterSlot when done.
    int submitScatterToDevice(uintptr_t slot_addr,
                              const std::vector<uintptr_t> &dst_segments,
                              const std::vector<size_t> &lengths);

    // Hand a scatter slot back to the pool. Must be called exactly once per
    // acquireScatterSlot, after the slot bytes are no longer needed (i.e.
    // after submitScatterToDevice returned, or the pull failed).
    void releaseScatterSlot(uintptr_t slot_addr);

    int transferSync(const char *target_hostname, uintptr_t buffer,
                     uintptr_t peer_buffer_address, size_t length,
                     TransferOpcode opcode, TransferNotify *notify = nullptr,
                     const std::string &transport_hint = "",
                     const std::string &nic_hint = "");

    // Known issue: in a few inference engines and benchmarks, accuracy
    // may be affected when using the batchTransferSync API. We currently
    // found this issue only in multi-node NVLink transfers.
    int batchTransferSync(const char *target_hostname,
                          std::vector<uintptr_t> buffers,
                          std::vector<uintptr_t> peer_buffer_addresses,
                          std::vector<size_t> lengths, TransferOpcode opcode,
                          TransferNotify *notify = nullptr,
                          const std::string &transport_hint = "",
                          const std::string &nic_hint = "");

    batch_id_t batchTransferAsync(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, TransferOpcode opcode,
        const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    int getBatchTransferStatus(const std::vector<batch_id_t> &batch_ids);

#ifdef USE_CUDA
    void batchTransferOnCuda(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, TransferOpcode opcode,
        uintptr_t stream_ptr = 0, const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    void transferWriteOnCuda(const char *target_hostname, uintptr_t buffer,
                             uintptr_t peer_buffer_address, size_t length,
                             uintptr_t stream_ptr = 0,
                             const std::string &transport_hint = "",
                             const std::string &nic_hint = "");

    void transferReadOnCuda(const char *target_hostname, uintptr_t buffer,
                            uintptr_t peer_buffer_address, size_t length,
                            uintptr_t stream_ptr = 0,
                            const std::string &transport_hint = "",
                            const std::string &nic_hint = "");

    void batchTransferWriteOnCuda(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, uintptr_t stream_ptr = 0,
        const std::string &transport_hint = "",
        const std::string &nic_hint = "");

    void batchTransferReadOnCuda(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, uintptr_t stream_ptr = 0,
        const std::string &transport_hint = "",
        const std::string &nic_hint = "");
#endif

    uintptr_t getFirstBufferAddress(const std::string &segment_name);

    // Pre-connect every (local_ctx, peer_nic) pair for `segment_name` so the
    // first submitTransfer does not stall on handshake RPC + fi_av_insert.
    // No-op on non-EFA builds or when the EFA transport is not installed.
    int warmupEfaSegment(const std::string &segment_name);

    int writeBytesToBuffer(uintptr_t dest_address, char *src_ptr,
                           size_t length) {
        memcpy((void *)dest_address, (void *)src_ptr, length);
        return 0;
    }

    pybind11::bytes readBytesFromBuffer(uintptr_t source_address,
                                        size_t length) {
        return pybind11::bytes(
            static_cast<const char *>(reinterpret_cast<void *>(source_address)),
            length);
    }

    // FOR EXPERIMENT ONLY
    int registerMemory(uintptr_t buffer_addr, size_t capacity,
                       const std::string &location = kWildcardLocation,
                       bool remote_accessible = true);

    // must be called before TransferEnginePy::~TransferEnginePy()
    int unregisterMemory(uintptr_t buffer_addr);

    int batchRegisterMemory(std::vector<uintptr_t> buffer_addresses,
                            std::vector<size_t> capacities,
                            const std::string &location = kWildcardLocation);

    int batchUnregisterMemory(std::vector<uintptr_t> buffer_addresses);

    std::string getLocalTopology(const char *device_name);

    std::vector<TransferNotify> getNotifies();

    int sendProbe(const std::string &peer_server_name);

    std::shared_ptr<TransferEngine> getEngine() const { return engine_; }

    uintptr_t getEnginePtr() const { return (uintptr_t)engine_.get(); }

   private:
    std::shared_ptr<ScatterTransferTicket> scatterTransferSyncWithTicket(
        const std::string &endpoint,
        const std::vector<uintptr_t> &local_base_addresses,
        const std::vector<size_t> &local_capacities,
        const std::vector<uint64_t> &remote_base_addresses,
        const std::vector<size_t> &remote_capacities,
        const std::vector<std::vector<size_t>> &local_offsets,
        const std::vector<std::vector<size_t>> &remote_offsets,
        const std::vector<std::vector<size_t>> &lengths, TransferOpcode opcode);

    int resolveNicHint(const std::string &name) const;

    char *allocateRawBuffer(size_t capacity);

    int findClassId(size_t size);

    int doBuddyAllocate(int class_id);

   private:
    std::shared_ptr<TransferEngine> engine_;
    Transport *xport_;

    std::mutex mutex_;
    std::vector<std::stack<char *>> free_list_;
    std::vector<char *> buffer_list_;
    std::unordered_set<char *> large_buffer_list_;
    std::unordered_map<std::string, Transport::SegmentHandle> handle_map_;

    // batch_id -> staging slot for in-flight submitGatherWrite batches
    // (stored as void* to keep GpuStagingPool out of this header).
    std::mutex gather_slot_mutex_;
    std::unordered_map<batch_id_t, void *> gather_slots_;
    // slot buffer addr -> staging slot for checked-out scatter slots, and
    // the set of slot buffers already registered as local memory.
    std::mutex scatter_slot_mutex_;
    std::unordered_map<uintptr_t, void *> scatter_slots_;
    std::unordered_set<uintptr_t> registered_scatter_slots_;
    bool auto_discovery_;

    uint64_t transfer_timeout_nsec_;
};
