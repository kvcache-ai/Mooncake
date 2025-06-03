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
        // (Optional) Retrieve NVLinkBufferDesc (or just the IPC handle) from metadata.
        // (For brevity, assume you have a helper “getRemoteNVLinkBufferDesc” that returns a NVLinkBufferDesc.)
        TransferMetadata::NVLinkBufferDesc remote_desc = metadata_->getRemoteNVLinkBufferDesc(request.target_id); // (e.g., via metadata)
        cudaIpcMemHandle_t ipc_handle = remote_desc.getIpcHandle();
        void* remote_ptr = nullptr;
        cudaError_t err = cudaIpcOpenMemHandle(&remote_ptr, ipc_handle, cudaIpcMemLazyEnablePeerAccess);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaIpcOpenMemHandle failed: " << cudaGetErrorString(err);
            task.failed_slice_count++;
            continue;
        }
        // (Optional) Use cudaMemcpyAsync (or cudaMemcpy) with remote_ptr + offset.
        if (request.opcode == TransferRequest::READ) {
            // remote -> local (e.g., cudaMemcpy(local, remote + offset, size, cudaMemcpyDeviceToDevice))
            err = cudaMemcpy(request.source, (char*)remote_ptr + request.target_offset, request.length, cudaMemcpyDeviceToDevice);
        } else {
            // local -> remote (e.g., cudaMemcpy(remote + offset, local, size, cudaMemcpyDeviceToDevice))
            err = cudaMemcpy((char*)remote_ptr + request.target_offset, request.source, request.length, cudaMemcpyDeviceToDevice);
        }
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpy failed: " << cudaGetErrorString(err);
            task.failed_slice_count++;
        } else {
            task.transferred_bytes = request.length;
            task.success_slice_count++;
        }
        task.is_finished = true;
        // (Optional) Close the IPC handle (if you’re done with it) via cudaIpcCloseMemHandle(remote_ptr).
        cudaIpcCloseMemHandle(remote_ptr);
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