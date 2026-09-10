// Copyright 2026 KVCache.AI
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

#ifndef SHM_TRANSPORT_H_
#define SHM_TRANSPORT_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class ShmTransportTestPeer;

// POSIX shm_open requires a name that begins with '/'. The object itself is
// still "mooncake_*" so routing can distinguish it from GPU IPC blobs.
inline constexpr char kPosixShmNamePrefix[] = "/mooncake_";

inline bool isPosixShmName(const std::string& name) {
    std::string_view key = name;
    if (!key.empty() && key.front() == '/') key.remove_prefix(1);
    constexpr std::string_view bare = "mooncake_";
    return key.size() > bare.size() && key.substr(0, bare.size()) == bare;
}

class ShmTransport : public Transport {
   public:
    ShmTransport();

    ~ShmTransport() override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    void* allocateSharedMemory(size_t length);

    int freeSharedMemory(void* addr);

    bool getShmName(void* addr, std::string* name) const;

   private:
    struct OpenedShmEntry {
        void* shm_addr = nullptr;
        uint64_t length = 0;
        std::string shm_name;
        std::atomic<uint32_t> pin_count{0};
        std::atomic<bool> stale{false};
    };

   public:
    // Holds a relocate-cache mmap until memcpy finishes so prune/cap cannot
    // munmap it out from under an in-flight copy.
    class MappingPin {
       public:
        MappingPin() = default;
        MappingPin(MappingPin&& other) noexcept;
        MappingPin& operator=(MappingPin&& other) noexcept;
        ~MappingPin();
        MappingPin(const MappingPin&) = delete;
        MappingPin& operator=(const MappingPin&) = delete;
        void reset();
        explicit operator bool() const { return static_cast<bool>(entry_); }

       private:
        friend class ShmTransport;
        MappingPin(ShmTransport* transport,
                   std::shared_ptr<OpenedShmEntry> entry);
        ShmTransport* transport_ = nullptr;
        std::shared_ptr<OpenedShmEntry> entry_;
    };

   private:
    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void* addr, bool update_metadata) override;

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry>& buffer_list,
        const std::string& location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override;

    const char* getName() const override { return "shm"; }

    friend class ShmTransportTestPeer;

    struct AllocatedShmEntry {
        std::string name;
        size_t length = 0;
    };

    using RelocateMap =
        std::unordered_map<uint64_t, std::shared_ptr<OpenedShmEntry>>;
    using PendingUnmap = std::vector<std::pair<void*, uint64_t>>;

    void* createSharedMemory(const std::string& path, size_t size,
                             int* error = nullptr);

    Status relocateSharedMemoryAddress(uint64_t& dest_addr, uint64_t length,
                                       uint64_t target_id,
                                       MappingPin* pin = nullptr);

    static bool tryResolve(const RelocateMap& relocate_map, uint64_t& dest_addr,
                           uint64_t length, const std::string& shm_name,
                           std::shared_ptr<OpenedShmEntry>* out_entry);

    static void queueUnmap(OpenedShmEntry& entry, PendingUnmap* pending);
    static void unpinLocked(OpenedShmEntry& entry, PendingUnmap* pending);
    static void flushUnmaps(PendingUnmap& pending);
    void pruneAndCapLocked(RelocateMap& mappings,
                           const TransferMetadata::SegmentDesc& desc,
                           uint64_t keep_addr, PendingUnmap* pending);
    void pruneIfNeeded(SegmentID target_id,
                       const TransferMetadata::SegmentDesc& desc,
                       uint64_t keep_addr);
    void adoptPin(std::shared_ptr<OpenedShmEntry> entry, MappingPin* pin);

    Status copySlice(TransferTask& task, const TransferRequest& request,
                     uint64_t dest_addr);

    void failSlice(TransferTask& task, const TransferRequest& request);

    RWSpinlock relocate_lock_;
    std::unordered_map<SegmentID, RelocateMap> relocate_map_;

    mutable std::mutex shm_path_mutex_;
    std::unordered_map<void*, AllocatedShmEntry> shm_path_map_;

    static constexpr int kShmCreateMaxRetries = 8;
    static constexpr size_t kMaxMappingsPerTarget = 32;
};

}  // namespace mooncake

#endif  // SHM_TRANSPORT_H_
