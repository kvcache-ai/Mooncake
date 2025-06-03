// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include <cuda_runtime.h>

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

class ThreadPool {
   public:
    ThreadPool(size_t threadCount)
        : ioService_(),
          work_(boost::asio::make_work_guard(ioService_)),
          stopped_(false) {
        for (size_t i = 0; i < threadCount; ++i) {
            threads_.create_thread(
                boost::bind(&boost::asio::io_service::run, &ioService_));
        }
    }

    ~ThreadPool() { stop(); }

    void submit(std::function<void()> task) {
        ioService_.post(std::move(task));
    }

    void stop() {
        if (!stopped_) {
            stopped_ = true;
            ioService_.stop();
            threads_.join_all();
        }
    }

   private:
    boost::asio::io_service ioService_;
    boost::asio::executor_work_guard<boost::asio::io_service::executor_type>
        work_;
    boost::thread_group threads_;
    bool stopped_;
};

class NvlinkTransport : public Transport {
   public:
    NvlinkTransport();

    ~NvlinkTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferRequest*>& request_list,
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

   protected:
    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    void startTransfer(Slice* slice);

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
    std::atomic_bool running_;
    ThreadPool thread_pool_;

    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
    };

    std::unordered_map<uint64_t, OpenedShmEntry> remap_entries_;
    std::mutex remap_mutex_;
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_