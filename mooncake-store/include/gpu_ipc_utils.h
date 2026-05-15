#pragma once

#include "cuda_alike.h"

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include <acl/acl_rt.h>
#endif

#include <cstddef>
#include <cstring>
#include <glog/logging.h>

namespace mooncake {
namespace gpu_ipc {

// IPC handle size for each platform, used for deserialization validation.
inline size_t IpcHandleSize() {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return sizeof(cudaIpcMemHandle_t);  // 64
#elif defined(USE_HIP)
    return sizeof(hipIpcMemHandle_t);  // 64
#elif defined(USE_ASCEND) || defined(USE_UBSHMEM)
    return 65;  // kIPCHandleKeyLength, consistent with ubshmem_transport.cpp
#else
    return 0;
#endif
}

// Open a remote accelerator memory IPC handle in the current process.
// handle_data: deserialized raw bytes
// handle_size: byte count (must match IpcHandleSize())
// device_ptr [out]: device virtual address accessible in this process
inline bool OpenIpcHandle(const void* handle_data, size_t handle_size,
                          void** device_ptr) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    if (handle_size != sizeof(cudaIpcMemHandle_t)) return false;
    cudaIpcMemHandle_t handle;
    std::memcpy(&handle, handle_data, sizeof(handle));
    return cudaIpcOpenMemHandle(device_ptr, handle,
                                cudaIpcMemLazyEnablePeerAccess) == cudaSuccess;
#elif defined(USE_HIP)
    if (handle_size != sizeof(hipIpcMemHandle_t)) return false;
    hipIpcMemHandle_t handle;
    std::memcpy(&handle, handle_data, sizeof(handle));
    return hipIpcOpenMemHandle(device_ptr, handle,
                               hipIpcMemLazyEnablePeerAccess) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_UBSHMEM)
    if (handle_size != 65) return false;
    char ipc_key[65] = {0};
    std::memcpy(ipc_key, handle_data, 65);
    return aclrtIpcMemImportByKey(
               device_ptr, ipc_key,
               ACL_RT_IPC_MEM_IMPORT_FLAG_ENABLE_PEER_ACCESS) == ACL_SUCCESS;
#else
    (void)handle_data;
    (void)handle_size;
    (void)device_ptr;
    return false;
#endif
}

// Close an opened IPC handle.
inline void CloseIpcHandle(void* device_ptr) {
    if (!device_ptr) return;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    cudaIpcCloseMemHandle(device_ptr);
#elif defined(USE_HIP)
    hipIpcCloseMemHandle(device_ptr);
#elif defined(USE_ASCEND) || defined(USE_UBSHMEM)
    aclrtIpcMemClose(device_ptr);
#else
    (void)device_ptr;
#endif
}

}  // namespace gpu_ipc
}  // namespace mooncake
