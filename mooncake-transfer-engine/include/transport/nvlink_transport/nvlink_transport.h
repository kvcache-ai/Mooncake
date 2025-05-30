// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include <cuda_runtime.h>
#include <memory>
#include <string>
#include <vector>
#include "transport/transport.h"
#include "transfer_metadata.h"
#include "topology.h"

namespace mooncake {

class NvlinkTransport : public Transport {
public:
    NvlinkTransport();
    ~NvlinkTransport();

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char* getName() const override { return "nvlink"; }

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata = true) override;

    int unregisterLocalMemory(void* addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location) override;

    int unregisterLocalMemoryBatch(const std::vector<void*>& addr_list) override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;
};

} // namespace mooncake

#endif // NVLINK_TRANSPORT_H_