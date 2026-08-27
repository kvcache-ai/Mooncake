// Copyright 2025 Huawei Technologies Co., Ltd
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

#ifndef ASCEND_DIRECT_TRANSPORT_ADXL_COMPAT_H
#define ASCEND_DIRECT_TRANSPORT_ADXL_COMPAT_H

#include <graph/ascend_string.h>

#include <acl/acl.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#ifdef FUNC_VISIBILITY
#define ASCEND_FUNC_VISIBILITY __attribute__((visibility("default")))
#else
#define ASCEND_FUNC_VISIBILITY
#endif

namespace adxl {
using Status = uint32_t;
using AscendString = ge::AscendString;
using TransferReq = void*;
using MemHandle = void*;

constexpr Status SUCCESS = 0U;
constexpr Status PARAM_INVALID = 103900U;
constexpr Status TIMEOUT = 103901U;
constexpr Status NOT_CONNECTED = 103902U;
constexpr Status ALREADY_CONNECTED = 103903U;
constexpr Status NOTIFY_FAILED = 103904U;
constexpr Status UNSUPPORTED = 103905U;
constexpr Status FAILED = 503900U;
constexpr Status RESOURCE_EXHAUSTED = 203900U;

enum MemType { MEM_DEVICE, MEM_HOST };

enum TransferOp { READ, WRITE };

struct MemDesc {
    uintptr_t addr;
    size_t len;
    uint8_t reserved[128] = {};
};

struct TransferOpDesc {
    uintptr_t local_addr;
    uintptr_t remote_addr;
    size_t len;
};

enum class TransferStatus { WAITING, COMPLETED, TIMEOUT, FAILED };

struct TransferArgs {
    uint8_t reserved[128] = {};
};

// FeatureType values must be explicitly assigned. Append new capabilities at
// the end only.
enum FeatureType : int32_t {
    AUTO_CONNECT = 0,
    CLIENT_SERVER_COMM = 1,
};

using ShareableHandle = aclrtMemFabricHandle;

class ASCEND_FUNC_VISIBILITY AdxlEngine {
   public:
    /**
     * @brief Constructs an AdxlEngine instance.
     */
    AdxlEngine();

    /**
     * @brief Destroys the AdxlEngine instance.
     */
    ~AdxlEngine();

    /**
     * @brief Initializes AdxlEngine before any other operation.
     * @param [in] local_engine Unique engine identifier. IPv4 identifiers use
     * host_ip or host_ip:host_port; IPv6 identifiers use [host_ip] or
     * [host_ip]:host_port. A positive host_port makes this engine listen as a
     * server.
     * @param [in] options Initialization options.
     * @return SUCCESS on success, or another status on failure.
     */
    Status Initialize(const AscendString& local_engine,
                      const std::map<AscendString, AscendString>& options);

    /**
     * @brief Releases resources owned by AdxlEngine.
     */
    void Finalize();

    /**
     * @brief Registers memory with the engine.
     * @param [in] mem Description of the memory to register.
     * @param [in] type Type of the memory to register.
     * @param [out] mem_handle Handle used to deregister the memory.
     * @return SUCCESS on success, or another status on failure.
     */
    Status RegisterMem(const MemDesc& mem, MemType type, MemHandle& mem_handle);

    /**
     * @brief Deregisters memory from the engine.
     * @param [in] mem_handle Handle returned by RegisterMem.
     * @return SUCCESS on success, or another status on failure.
     */
    Status DeregisterMem(MemHandle mem_handle);

    /**
     * @brief Connects to a remote AdxlEngine.
     * @param [in] remote_engine Unique identifier of the remote engine.
     * @param [in] timeout_in_millis Connection timeout in milliseconds.
     * @return SUCCESS on success, or another status on failure.
     */
    Status Connect(const AscendString& remote_engine,
                   int32_t timeout_in_millis = 1000);

    /**
     * @brief Disconnects from a remote AdxlEngine.
     * @param [in] remote_engine Unique identifier of the remote engine.
     * @param [in] timeout_in_millis Disconnection timeout in milliseconds.
     * @return SUCCESS on success, or another status on failure.
     */
    Status Disconnect(const AscendString& remote_engine,
                      int32_t timeout_in_millis = 1000);

    /**
     * @brief Transfers memory synchronously with a remote AdxlEngine.
     * @param [in] remote_engine Unique identifier of the remote engine.
     * @param [in] operation Reads remote memory or writes local memory.
     * @param [in] op_descs Local and remote addresses for the batch.
     * @param [in] timeout_in_millis Transfer timeout in milliseconds.
     * @return SUCCESS on success, or another status on failure.
     */
    Status TransferSync(const AscendString& remote_engine, TransferOp operation,
                        const std::vector<TransferOpDesc>& op_descs,
                        int32_t timeout_in_millis = 1000);

    /**
     * @brief Submits a batch of asynchronous transfers.
     * @param [in] remote_engine Unique identifier of the remote engine.
     * @param [in] operation Reads remote memory or writes local memory.
     * @param [in] op_descs Local and remote addresses for the batch.
     * @param [in] optional_args Reserved optional arguments.
     * @param [out] req Request handle used to query transfer status.
     * @return SUCCESS on success, or another status on failure.
     */
    __attribute__((weak)) Status
    TransferAsync(const AscendString& remote_engine, TransferOp operation,
                  const std::vector<TransferOpDesc>& op_descs,
                  const TransferArgs& optional_args, TransferReq& req);

    /**
     * @brief Gets the status of an asynchronous transfer request.
     * @param [in] req Request handle returned by TransferAsync.
     * @param [out] status Current transfer status.
     * @return SUCCESS on success, or another status on failure.
     */
    __attribute__((weak)) Status GetTransferStatus(const TransferReq& req,
                                                   TransferStatus& status);

    /**
     * @brief Allocates memory through ACL virtual memory management.
     * @param [in] type Type of memory to allocate.
     * @param [in] size Allocation size in bytes.
     * @param [out] ptr Virtual address of the allocation.
     * @return SUCCESS on success, or another status on failure.
     */
    __attribute__((weak)) static Status MallocMem(MemType type, size_t size,
                                                  void** ptr);

    /**
     * @brief Exports memory allocated by MallocMem as a fabric shareable
     * handle.
     *
     * Each allocation is exported through ACL at most once. The first call
     * exports and caches the handle; subsequent calls return the cached handle.
     *
     * @param [in] addr Virtual address returned by MallocMem.
     * @param [out] handle Exported fabric shareable handle.
     * @return SUCCESS on success, PARAM_INVALID if addr was not returned by
     * MallocMem or has already been freed, and another error status otherwise.
     */
    __attribute__((weak)) static Status ExportToShareableHandle(
        void* addr, ShareableHandle& handle);

    /**
     * @brief Frees memory allocated by MallocMem.
     * @param [in] ptr Virtual address returned by MallocMem.
     * @return SUCCESS on success, or another status on failure.
     */
    __attribute__((weak)) static Status FreeMem(void* ptr);

    /**
     * @brief Queries a library capability.
     * @param [in] feature_type Capability to query.
     * @param [out] value 1 when supported, or 0 when unsupported.
     * @return SUCCESS on success, UNSUPPORTED for an unknown capability, or
     * PARAM_INVALID for an invalid argument.
     */
    __attribute__((weak)) static Status GetCapability(FeatureType feature_type,
                                                      int32_t& value);

   private:
    class AdxlEngineImpl;
    std::unique_ptr<AdxlEngineImpl> impl_;
};

inline bool IsAdxlFeatureSupported(FeatureType feature) {
    if (&AdxlEngine::GetCapability == nullptr) {
        return false;
    }
    int32_t val = 0;
    const Status st = AdxlEngine::GetCapability(feature, val);
    return st == SUCCESS && val == 1;
}
}  // namespace adxl

#endif  // ASCEND_DIRECT_TRANSPORT_ADXL_COMPAT_H
