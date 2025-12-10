// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include "cuda_alike.h"

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>
#include <utility>
#include <cuda.h>
#include <optional>

#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

struct ShareableHandle {
    int type;  // 2 = POSIX_FD
    union {
        int fd;
        uint8_t fabric[32];
    } value;
};
struct PairHash {
    template <typename T1, typename T2>
    std::size_t operator()(const std::pair<T1, T2>& p) const {
        std::size_t h1 = std::hash<T1>{}(p.first);
        std::size_t h2 = std::hash<T2>{}(p.second);
        return h1 ^ (h2 << 1);
    }
};

enum class MemoryBackend {
    FABRIC,
    IPC_POSIX_FD,
    UNKNOWN
    };

class NvlinkTransport : public Transport {
   public:
    void shutdown() {
        if (!server_running_.exchange(false)) {
            return;  // Already shutdown
        }
        LOG(INFO) << "Shutting down NVLink transport...";
        server_running_ = false;
        if (export_server_socket_ >= 0) {
            close(export_server_socket_);
            export_server_socket_ = -1;
        }
        if (export_server_thread_.joinable()) {
            export_server_thread_.join();
        }
        // Clean remap entries
        for (auto &entry : remap_entries_) {
            if (use_fabric_mem_) {
                freePinnedLocalMemory(entry.second.shm_addr);
            } else {
                cudaIpcCloseMemHandle(entry.second.shm_addr);
            }
        }
        remap_entries_.clear();
        LOG(INFO) << "NVLink transport shutdown complete.";
    }

    NvlinkTransport();

    ~NvlinkTransport(){
        shutdown();
    }

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

    const char* getName() const override { return "nvlink"; }

   private:

    void startExportServer();
    void exportServerLoop();
    void cleanupExportServer();
    std::optional<std::pair<uint64_t, std::string>> parseRequest(std::string_view req);
    void sendFdToClient(int sock, int fd, const std::string& client_path);
    void cleanupSocket(int sock, const std::string& path);
    static MemoryBackend detectMemoryBackend();
    std::string getSocketPath() const;

    std::atomic_bool running_;

    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
    };

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;

    std::mutex register_mutex_;
    std::atomic<bool> server_running_{false};
    std::thread export_server_thread_;
    int export_server_socket_ = -1;

    struct ExportedBuffer {
        void* base_addr;
        size_t size;
        CUmemGenericAllocationHandle alloc_handle;
    };
    std::unordered_map<void*, ExportedBuffer> exported_buffers_;
    std::mutex exported_mutex_;
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_