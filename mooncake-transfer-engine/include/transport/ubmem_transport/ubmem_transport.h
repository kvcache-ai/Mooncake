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

// UbmemTransport — transport over UB shared-memory devices (obmm
// shmdev). Modeled after cxl_transport: whole-device mmap + offset
// semantics + synchronous inline copy. Cross-node accesses the peer
// segment through a local mirror device (identical offset space); mirror
// devices are O_SYNC (nc), the local pool device is cacheable.
//
// Env:
//   MC_UBMEM_DEV_PATH      local pool device (required)
//   MC_UBMEM_DEV_SIZE      device bytes (default: probe via lseek)
//   MC_UBMEM_PEER_DEV_MAP  "peerhost:peerdev=localdev,..." mirrors
//   MC_UBMEM_PEER_DEV_MAP_FILE  cluster-wide JSON mirror topology;
//                          every node keeps the entries where the
//                          reader host is itself (ignored when
//                          MC_UBMEM_PEER_DEV_MAP is set)
//   MC_UBMEM_FORCE_O_SYNC  force O_SYNC mappings (diagnostics)
//   MC_UBMEM_INLINE_MEMCPY 1=memcpy (default), 0=nc load pipeline

#ifndef UBMEM_TRANSPORT_H_
#define UBMEM_TRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class TransferMetadata;

class UbmemTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;

   public:
    UbmemTransport();

    ~UbmemTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    void* getUbmemBaseAddr() { return own_base_; }

    size_t getUbmemDevSize() { return own_size_; }

   private:
    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo);

    int allocateLocalSegmentID();

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata);

    int unregisterLocalMemory(void* addr, bool update_metadata = false);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry>& buffer_list,
        const std::string& location);

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override;

    const char* getName() const override { return "ubmem"; }

    // local pool device + lazily created cross-node mirrors
    struct Mapping {
        void* base = nullptr;
        uint64_t size = 0;
    };

    int ubmemDevInit();

    static uint64_t probeDevSize(const char* path);

    // 8 independent loads per iteration: glibc memcpy does not pipeline
    // on nc device memory.
    static void ubmemCopy(void* dest, const void* source, size_t size);

    static void* mapDevice(const char* path, uint64_t bytes);

    // target_base is the owner's device base (VA); request.target_offset
    // is an owner-side VA, subtract it to get the in-device offset.
    int ubmemExecuteOne(Mapping* mapping, const TransferRequest& request,
                        std::string& error, uint64_t target_base = 0,
                        bool nc = false);

    // Resident parallel chunk pool for large copies; copy() returns only
    // after the copy completes. MC_UBMEM_COPY_THREADS tunes workers
    // (default 8, 0 disables).
    class CopyPool {
       public:
        ~CopyPool();
        void start(int workers);
        void stop();
        bool ready() const { return !stop_flag_ && workers_ > 0; }
        void copy(void* dest, const void* src, size_t bytes);

       private:
        void workerMain(int index);

        std::vector<std::thread> threads_;
        std::mutex dispatch_mu_;  // at most one in-flight large copy
        std::mutex mu_;           // task slot + generation
        std::condition_variable wake_cv_;
        std::condition_variable done_cv_;
        const uint8_t* task_src_ = nullptr;
        uint8_t* task_dst_ = nullptr;
        size_t task_bytes_ = 0;
        uint64_t gen_ = 0;
        size_t done_count_ = 0;
        bool stop_flag_ = false;
        int workers_ = 0;
    };

    static const size_t kParallelThreshold = 1ull << 20;

    // Segment resolve cache. First request per segment resolves via
    // metadata, afterwards lock-free by segment id; ids beyond capacity
    // fall back to the slow path. Only successful resolutions are cached.
    // mappings_ is node-based, so entry pointers are stable.
    struct Resolved {
        Mapping* mapping;
        uint64_t target_base;
        bool nc;  // target mapping is O_SYNC strongly-ordered (mirror)
    };
    static constexpr size_t kResolveCacheSize = 4096;
    std::atomic<Resolved*> resolve_cache_[kResolveCacheSize]{};

    // read once in ubmemDevInit
    bool inline_memcpy_ = true;

    Mapping* resolveTargetMapping(const TransferRequest& request,
                                  std::string& error,
                                  uint64_t* target_base = nullptr,
                                  bool* nc = nullptr);

    Mapping* getOrCreateMapping(const std::string& path, uint64_t bytes);

    void loadPeerDevMap();

   private:
    std::string own_path_;
    void* own_base_ = nullptr;
    uint64_t own_size_ = 0;

    CopyPool copy_pool_;

    std::mutex mappings_mu_;
    // device path -> mapping (local pool + mirrors, read-mostly)
    std::unordered_map<std::string, Mapping> mappings_;

    // "peer_server_name\x1Fpeer_device_path" -> local mirror device path
    std::unordered_map<std::string, std::string> peer_dev_map_;
};
}  // namespace mooncake

#endif
