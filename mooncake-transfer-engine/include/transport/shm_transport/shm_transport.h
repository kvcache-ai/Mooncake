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

#ifndef SHM_TRANSPORT_H_
#define SHM_TRANSPORT_H_

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <functional>
#include <iostream>
#include <queue>

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

class ShmTransport : public Transport {
   public:
    ShmTransport();

    ~ShmTransport();

    Status submitTransferTask(
        const std::vector<TransferRequest *> &request_list,
        const std::vector<TransferTask *> &task_list) override;

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> metadata,
                std::shared_ptr<Topology> topology);

    void startTransfer(Slice *slice);

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata);

    int unregisterLocalMemory(void *addr, bool update_metadata = false);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location);

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    void *allocateLocalMemory(size_t length, const std::string &location);

    int deallocateLocalMemory(void *addr);

    void *createSharedMemory(const std::string &path, size_t size);

    int relocateSharedMemoryAddress(uint64_t &dest_addr, uint64_t length, uint64_t target_id);

    const char *getName() const override { return "shm"; }

   private:
    std::atomic_bool running_;
    ThreadPool thread_pool_;

    struct OpenedShmEntry {
        int shm_fd;
        void *shm_addr;
        uint64_t length;
    };

    std::unordered_map<void *, std::string> created_entries_;
    std::unordered_map<uint64_t, OpenedShmEntry> remap_entries_;
};
}  // namespace mooncake

#endif