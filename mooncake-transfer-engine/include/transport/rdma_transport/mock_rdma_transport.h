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

#ifndef MOCK_RDMA_TRANSPORT_H_
#define MOCK_RDMA_TRANSPORT_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "transport/transport.h"
#include "transport/tcp_transport/tcp_transport.h"

namespace mooncake {

/// MockRdmaTransport provides an RDMA-compatible transport that works
/// without actual RDMA hardware. It delegates all I/O to TcpTransport,
/// but registers segments as protocol "rdma" so the system sees an
/// RDMA transport. Controlled by MC_RDMA_MOCK=1 environment variable.
///
/// Additionally supports fault injection via:
///   MC_RDMA_MOCK_FAIL_RATE=<float>  (0.0-1.0, probability of submit failure)
///   MC_RDMA_MOCK_FAIL_AFTER=<int>   (fail after N submits, -1 to disable)
class MockRdmaTransport : public Transport {
   public:
    MockRdmaTransport();
    ~MockRdmaTransport() override;

    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    const char* getName() const override { return "mock_rdma"; }

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void* addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(
        const std::vector<BufferEntry>& buffer_list,
        const std::string& location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override;

    /// Check if a submit failure should be injected.
    bool shouldInjectFailure();

   private:
    std::shared_ptr<TcpTransport> tcp_impl_;
    std::atomic<int> submit_count_{0};
    double fail_rate_ = 0.0;
    int fail_after_ = -1;
};

/// Check whether mock RDMA mode is enabled (MC_RDMA_MOCK=1).
bool isMockRdmaEnabled();

}  // namespace mooncake

#endif  // MOCK_RDMA_TRANSPORT_H_
