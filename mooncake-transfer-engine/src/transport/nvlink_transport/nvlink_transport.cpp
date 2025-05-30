#include "transport/nvlink_transport/nvlink_transport.h"
#include <cuda_runtime.h>
#include <glog/logging.h>

namespace mooncake {

NvlinkTransport::NvlinkTransport() {}

NvlinkTransport::~NvlinkTransport() {}

int NvlinkTransport::install(std::string& local_server_name,
                             std::shared_ptr<TransferMetadata> meta,
                             std::shared_ptr<Topology> topo) {
    local_server_name_ = local_server_name;
    metadata_ = meta;
    // Optionally store topo if needed
    return 0;
}

int NvlinkTransport::registerLocalMemory(void* addr, size_t length,
                                         const std::string& location, bool remote_accessible,
                                         bool update_metadata) {
    // For CUDA, memory registration is not always needed, but you may want to check device pointers
    cudaPointerAttributes attr;
    cudaError_t err = cudaPointerGetAttributes(&attr, addr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaPointerGetAttributes failed";
        return -1;
    }
    // Optionally, add to metadata
    return 0;
}

int NvlinkTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    // No-op for CUDA, unless you want to track allocations
    return 0;
}

int NvlinkTransport::registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                              const std::string& location) {
    for (const auto& entry : buffer_list) {
        registerLocalMemory(entry.addr, entry.length, location, true, false);
    }
    return 0;
}

int NvlinkTransport::unregisterLocalMemoryBatch(const std::vector<void*>& addr_list) {
    for (auto addr : addr_list) {
        unregisterLocalMemory(addr, false);
    }
    return 0;
}

Status NvlinkTransport::submitTransfer(BatchID batch_id,
                                       const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "NvlinkTransport: Exceed batch capacity";
        return Status::InvalidArgument("NvlinkTransport: Exceed batch capacity");
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (const auto& request : entries) {
        TransferTask& task = batch_desc.task_list[task_id++];
        cudaError_t err;
        if (request.opcode == TransferRequest::READ) {
            // Device to Host
            err = cudaMemcpy(request.source, (void*)request.target_id, request.length, cudaMemcpyDeviceToHost);
        } else {
            // Host to Device
            err = cudaMemcpy((void*)request.target_id, request.source, request.length, cudaMemcpyHostToDevice);
        }
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpy failed: " << cudaGetErrorString(err);
            task.failed_slice_count++;
            continue;
        }
        task.transferred_bytes = request.length;
        task.success_slice_count++;
        task.is_finished = true;
    }
    return Status::OK();
}

Status NvlinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (task_id >= batch_desc.task_list.size()) {
        return Status::InvalidArgument("NvlinkTransport: Invalid task_id");
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    if (task.success_slice_count + task.failed_slice_count == task.slice_count) {
        status.s = (task.failed_slice_count > 0) ? TransferStatusEnum::FAILED : TransferStatusEnum::COMPLETED;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

} // namespace mooncake