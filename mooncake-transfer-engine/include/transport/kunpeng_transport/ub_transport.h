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

#ifndef UB_TRANSPORT_H
#define UB_TRANSPORT_H
#include <memory>
#include <string>
#include <vector>
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class UbContext;
class UbEndPoint;
class TransferMetadata;
class UbWorkerpool;

enum UB_ENDPOINT_TYPE { URMA_ENDPOINT = 0, OBMM_ENDPOINT = 1 };

// ub transport supports integration of endpoints that use UB protocol software.
// Currently, two types of endpoints are supported: urma and obmm.
// urma link : https://atomgit.com/openeuler/umdk
// obmm link : https://atomgit.com/openeuler/obmm
class UbTransport : public Transport {
    friend class UbContext;
    friend class UbEndPoint;
    friend class UbWorkerPool;

   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    UbTransport(UB_ENDPOINT_TYPE endpoint_type = URMA_ENDPOINT);

    ~UbTransport();

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

    const char* getName() const override { return "ub"; }

    // TRANSFER

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    SegmentID getSegmentID(const std::string& segment_name);

   private:
    int allocateLocalSegmentID();

   public:
    int onSetupConnections(const HandShakeDesc& peer_desc,
                           HandShakeDesc& local_desc);

    int sendHandshake(const std::string& peer_server_name,
                      const HandShakeDesc& local_desc,
                      HandShakeDesc& peer_desc) {
        return metadata_->sendHandshake(peer_server_name, local_desc,
                                        peer_desc);
    }

   private:
    static int init(UbTransport* transport);

    static void uninit(UbTransport* transport);

    static int initializeUbResources(UbTransport* transport);

    static std::shared_ptr<UbContext> buildContext(
        UbTransport* transport, const std::string& device_name,
        int max_endpoints);

    int startHandshakeDaemon(std::string& local_server_name);

   public:
    static int selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                            int& buffer_id, int& device_id, int retry_cnt = 0);
    static int selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                            std::string_view hint, int& buffer_id,
                            int& device_id, int retry_cnt = 0);

   private:
    std::vector<std::shared_ptr<UbContext>> context_list_;
    std::shared_ptr<Topology> local_topology_;
    UB_ENDPOINT_TYPE endpoint_type_;
};
}  // namespace mooncake

#endif  // UB_TRANSPORT_H
