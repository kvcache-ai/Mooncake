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

#include "transport/ascend_transport/ubshmem_transport/ubshmem_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <sys/syscall.h>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
namespace {
constexpr size_t kIPCHandleKeyLength = 65;
constexpr int kDefaultNumStreams = 4;
constexpr size_t kDefaultThreadPool = 8;
constexpr uint64_t kGetStreamTimeoutMillis = 3000;
}  // namespace

// Pool configuration constants
// These can be overridden via environment variable:
// - MC_UBSHMEM_NUM_STREAMS: max streams in ACL streams pool
// - MC_UBSHMEM_STREAMS_PER_TRANSFER: max ACL streams used in per transfer task
// - MC_UBSHMEM_THREAD_POOL_SIZE: thread pool size in UBshmem transport
// - MC_UBSHMEM_GET_STREAM_TIMEOUT: get stream timeout in milliseconds

static bool checkAcl(aclError result, const char *message) {
    if (result != ACL_ERROR_NONE) {
        const char *errMsg = aclGetRecentErrMsg();
        LOG(ERROR) << message << " (Error code: " << result << " - " << errMsg
                   << ")";
        return false;
    }
    return true;
}

static int openIPCHandle(const std::vector<unsigned char> &buffer,
                         void **shm_addr) {
    // Validate buffer size before copying
    if (buffer.size() != kIPCHandleKeyLength) {
        LOG(ERROR) << "UBShmemTransport: buffer size " << buffer.size()
                   << " does not match expected size " << kIPCHandleKeyLength;
        return -1;
    }

    // Copy IPC key from buffer
    char ipc_key[kIPCHandleKeyLength] = {0};
    memcpy(ipc_key, buffer.data(), kIPCHandleKeyLength);

    // Import IPC memory handle
    if (!checkAcl(aclrtIpcMemImportByKey(
                      shm_addr, ipc_key,
                      ACL_RT_IPC_MEM_IMPORT_FLAG_ENABLE_PEER_ACCESS),
                  "UBShmemTransport: aclrtIpcMemImportByKey failed")) {
        return -1;
    }
    return 0;
}

static int openShareableHandle(const std::vector<unsigned char> &buffer,
                               size_t length, void **shm_addr) {
    aclrtMemFabricHandle export_handle = {};
    memcpy(&export_handle, buffer.data(), sizeof(export_handle));

    aclrtDrvMemHandle handle;
    if (!checkAcl(
            aclrtMemImportFromShareableHandleV2(
                &export_handle, ACL_MEM_SHARE_HANDLE_TYPE_FABRIC,
                ACL_RT_VMM_EXPORT_FLAG_DEFAULT, &handle),
            "UBShmemTransport: aclrtMemImportFromShareableHandleV2 failed")) {
        return -1;
    }

    uint64_t page_type = 1;
    if (!checkAcl(
            aclrtReserveMemAddress(shm_addr, length, 0, nullptr, page_type),
            "UBShmemTransport: aclrtReserveMemAddress failed")) {
        (void)aclrtFreePhysical(handle);
        return -1;
    }

    if (!checkAcl(aclrtMapMem(*shm_addr, length, 0, handle, 0),
                  "UBShmemTransport: aclrtMemMap failed")) {
        (void)aclrtReleaseMemAddress(*shm_addr);
        (void)aclrtFreePhysical(handle);
        return -1;
    }

    return 0;
}

static int getDeviceFromPointer(void *ptr) {
    if (ptr == nullptr) {
        LOG(ERROR) << "UBShmemTransport: null pointer passed to "
                      "getDeviceFromPointer";
        return -1;
    }

    aclrtPtrAttributes attributes;
    auto err = aclrtPointerGetAttributes(ptr, &attributes);
    if (!checkAcl(err, "UBShmemTransport: aclrtPointerGetAttributes failed")) {
        return -1;
    }

    if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return attributes.location.id;
    } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        // Host memory - return -1 to indicate CPU memory
        // This is not an error, just indicates we should use current device
        // context
        if (globalConfig().trace) {
            LOG(INFO) << "UBShmemTransport: pointer " << ptr
                      << " is host memory (type: " << attributes.location.type
                      << "), will use current device context";
        }
        return -1;
    } else {
        LOG(WARNING) << "UBShmemTransport: unknown memory type "
                     << attributes.location.type << " for pointer " << ptr;
        return -1;
    }
}

static int setDeviceContext(void *source_ptr) {
    // Get device ID from source pointer
    int device_id = getDeviceFromPointer(source_ptr);

    // Set device context if we have NPU memory
    if (device_id >= 0) {
        if (!checkAcl(aclrtSetDevice(device_id),
                      "UBShmemTransport: failed to set device context")) {
            return -1;
        }
    }

    return 0;
}

static bool supportFabricMem() {
    const char *use_ipc = getenv("MC_USE_UBSHMEM_IPC");
    if (use_ipc != nullptr && strcmp(use_ipc, "1") == 0) {
        return false;
    }
    uint32_t num_devices = 0;
    if (!checkAcl(aclrtGetDeviceCount(&num_devices),
                  "UBShmemTransport: aclrtGetDeviceCount failed")) {
        return false;
    }

    if (num_devices == 0) {
        LOG(ERROR) << "UBShmemTransport: no device found";
        return false;
    }
    return true;
}

UBShmemTransport::UBShmemTransport()
    : use_fabric_mem_(supportFabricMem()),
      num_streams_per_transfer_(kDefaultNumStreams),  // Default: 4 streams per transfer
      thread_pool_size_(kDefaultThreadPool),          // Default: 8 threads
      stream_pool_timeout_ms_(kGetStreamTimeoutMillis)  // Default: 3000 ms
{
    // Read configuration from environment variables
    const char *num_streams_per_transfer_env =
        getenv("MC_UBSHMEM_STREAMS_PER_TRANSFER");
    if (num_streams_per_transfer_env != nullptr) {
        int env_value = atoi(num_streams_per_transfer_env);
        if (env_value > 0) {
            num_streams_per_transfer_ = env_value;
        }
    }

    const char *max_streams_env = getenv("MC_UBSHMEM_MAX_STREAMS");
    int max_streams = kDefaultNumStreams * kDefaultThreadPool;
    if (max_streams_env != nullptr) {
        int env_value = atoi(max_streams_env);
        if (env_value > 0) {
            max_streams = env_value;
        }
    }

    const char *thread_pool_size_env = getenv("MC_UBSHMEM_THREAD_POOL_SIZE");
    if (thread_pool_size_env != nullptr) {
        int env_value = atoi(thread_pool_size_env);
        if (env_value > 0 && env_value <= 64) {  // Limit to reasonable range
            thread_pool_size_ = static_cast<size_t>(env_value);
            LOG(INFO)
                << "UBShmemTransport: Using thread pool size "
                << thread_pool_size_
                << " from environment variable MC_UBSHMEM_THREAD_POOL_SIZE";
        } else {
            LOG(WARNING) << "UBShmemTransport: Invalid thrsead pool size "
                         << env_value << " (must be 1-64), using default "
                         << thread_pool_size_;
        }
    }

    // Read stream pool timeout configuration
    const char *stream_timeout_env = getenv("MC_UBSHMEM_GET_STREAM_TIMEOUT");
    if (stream_timeout_env != nullptr) {
        int env_value = atoi(stream_timeout_env);
        if (env_value > 0) {
            stream_pool_timeout_ms_ = static_cast<uint64_t>(env_value);
            LOG(INFO)
                << "UBShmemTransport: Using stream pool timeout "
                << stream_pool_timeout_ms_
                << "ms from environment variable MC_UBSHMEM_GET_STREAM_TIMEOUT";
        } else {
            LOG(WARNING) << "UBShmemTransport: Invalid stream pool timeout "
                         << env_value << "ms (must be > 0), using default "
                         << stream_pool_timeout_ms_ << "ms";
        }
    }

    // Initialize stream pool
    stream_pool_ = std::make_unique<StreamPool>(max_streams);
}

UBShmemTransport::~UBShmemTransport() {
    stopThreadPool();

    if (use_fabric_mem_) {
        for (auto &entry : remap_entries_) {
            freePinnedLocalMemory(entry.second.shm_addr);
        }
    } else {
        for (auto &entry : remap_entries_) {
            if (entry.second.key != nullptr) {
                (void)aclrtIpcMemClose(entry.second.key);
                delete[] entry.second.key;
            }
        }
    }
    remap_entries_.clear();
}

void UBShmemTransport::initializeThreadPool() {
    running_ = true;
    workers_.reserve(thread_pool_size_);
    for (auto i = 0U; i < thread_pool_size_; ++i) {
        workers_.emplace_back([this] { workerThread(); });
    }
    LOG(INFO) << "UBShmemTransport: Thread pool initialized with "
              << thread_pool_size_ << " threads";
}

void UBShmemTransport::stopThreadPool() {
    running_ = false;
    task_queue_cv_.notify_all();
    for (std::thread &worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    while (!task_queue_.empty()) {
        task_queue_.pop();
    }
}

void UBShmemTransport::workerThread() {
    while (running_) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(task_queue_mutex_);
            task_queue_cv_.wait(
                lock, [this] { return !running_ || !task_queue_.empty(); });

            if (!running_ && task_queue_.empty()) {
                return;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        if (task) {
            try {
                task();
            } catch (...) {
                LOG(ERROR) << "UBShmemTransport: Worker thread caught unknown "
                              "exception";
            }
        }
    }
}

int UBShmemTransport::install(std::string &local_server_name,
                              std::shared_ptr<TransferMetadata> metadata,
                              std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;

    desc->name = local_server_name_;
    desc->protocol = "ubshmem";

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));

    // Initialize thread pool
    initializeThreadPool();

    return 0;
}

void UBShmemTransport::submitSlices(std::vector<Slice *> &slice_list) {
    std::unordered_map<SegmentID, std::vector<Slice *>> seg_to_slices;
    for (auto slice : slice_list) {
        seg_to_slices[slice->target_id].push_back(slice);
    }
    for (auto &[seg_id, slices] : seg_to_slices) {
        // Enqueue the task to the thread pool
        std::unique_lock<std::mutex> queue_lock(task_queue_mutex_);
        if (!running_) {
            for (auto slice : slices) {
                slice->markFailed();
            }
            continue;
        }

        task_queue_.emplace([this, moved_slices = std::move(slices)]() {
            std::vector<aclrtStream> streams;
            try {
                if (moved_slices.empty()) {
                    return;
                }

                // Get device ID from first slice
                int rc = setDeviceContext(moved_slices[0]->source_addr);
                if (rc != 0) {
                    LOG(ERROR) << "Failed to set device context";
                    for (auto slice : moved_slices) {
                        slice->markFailed();
                    }
                    return;
                }

                int device_id =
                    getDeviceFromPointer(moved_slices[0]->source_addr);
                if (device_id < 0) {
                    int32_t temp_device_id;
                    if (!checkAcl(aclrtGetDevice(&temp_device_id),
                                  "UBShmemTransport: aclrtGetDevice failed")) {
                        LOG(ERROR) << "Failed to get current device context";
                        for (auto slice : moved_slices) {
                            slice->markFailed();
                        }
                        return;
                    }
                    device_id = temp_device_id;
                }

                if (!stream_pool_->tryGetStreams(num_streams_per_transfer_,
                                                 streams,
                                                 stream_pool_timeout_ms_)) {
                    LOG(ERROR) << "Failed to get streams from pool";
                    for (auto slice : moved_slices) {
                        slice->markFailed();
                    }
                    return;
                }

                // Process all slices in parallel
                for (size_t i = 0; i < moved_slices.size(); ++i) {
                    auto slice = moved_slices[i];
                    // Relocate shared memory address if needed
                    uint64_t dest_addr = slice->ubshmem.dest_addr;
                    if (slice->target_id != LOCAL_SEGMENT_ID) {
                        int rc = relocateSharedMemoryAddress(
                            dest_addr, slice->length, slice->target_id);
                        if (rc) {
                            LOG(ERROR) << "Device memory not registered";
                            slice->markFailed();
                            continue;
                        }
                    }
                    slice->ubshmem.dest_addr = dest_addr;

                    // Execute memory copy using stream in round-robin fashion
                    aclrtStream stream = streams[i % streams.size()];
                    aclError err;
                    if (slice->opcode == TransferRequest::READ) {
                        err = aclrtMemcpyAsync(
                            slice->source_addr, slice->length,
                            reinterpret_cast<void *>(slice->ubshmem.dest_addr),
                            slice->length, ACL_MEMCPY_DEFAULT, stream);
                    } else {
                        err = aclrtMemcpyAsync(
                            reinterpret_cast<void *>(slice->ubshmem.dest_addr),
                            slice->length, slice->source_addr, slice->length,
                            ACL_MEMCPY_DEFAULT, stream);
                    }

                    if (!checkAcl(
                            err, "UBShmemTransport: aclrtMemcpyAsync failed")) {
                        slice->markFailed();
                        continue;
                    }
                }

                // Synchronize all streams to ensure transfers complete
                for (auto stream : streams) {
                    if (!checkAcl(aclrtSynchronizeStream(stream),
                                  "UBShmemTransport: aclrtSynchronizeStream "
                                  "failed")) {
                        LOG(ERROR) << "Stream synchronization failed";
                        for (auto slice : moved_slices) {
                            if (slice->status == Slice::PENDING) {
                                slice->markFailed();
                            }
                        }
                        stream_pool_->releaseStreams(streams);
                        return;
                    }
                }

                // Mark all pending slices as success
                for (auto slice : moved_slices) {
                    if (slice->status == Slice::PENDING) {
                        slice->markSuccess();
                    }
                }

                stream_pool_->releaseStreams(streams);
            } catch (...) {
                LOG(ERROR) << "Unknown exception in transfer task";
                for (auto slice : moved_slices) {
                    if (slice->status == Slice::PENDING) {
                        slice->markFailed();
                    }
                }
                if (streams.size() > 0) {
                    stream_pool_->releaseStreams(streams);
                }
            }
        });

        task_queue_cv_.notify_one();
    }
}

Status UBShmemTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "UBShmemTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "UBShmemTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    std::vector<Slice *> slice_list;
    slice_list.reserve(entries.size());

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto &request = entries[i];
        auto &task = batch_desc.task_list[task_id + i];
        task.total_bytes = request.length;

        Slice *slice = getSliceCache().allocate();
        slice->source_addr = request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->ubshmem.dest_addr = request.target_offset;
        slice->task = &task;
        slice->status = Slice::PENDING;

        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }

    submitSlices(slice_list);
    return Status::OK();
}

Status UBShmemTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                           TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();

    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "UBShmemTransport::getTransferStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }

    // Get task and status info
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;

    // Determine completion status
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }

    return Status::OK();
}

Status UBShmemTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    if (task_list.empty()) {
        return Status::OK();
    }

    std::vector<Slice *> slice_list;
    slice_list.reserve(task_list.size());

    for (auto *task_ptr : task_list) {
        assert(task_ptr);
        assert(task_ptr->request);
        auto &request = *task_ptr->request;

        Slice *slice = getSliceCache().allocate();
        slice->source_addr = request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->ubshmem.dest_addr = request.target_offset;
        slice->task = task_ptr;
        slice->status = Slice::PENDING;

        task_ptr->slice_list.push_back(slice);
        __sync_fetch_and_add(&task_ptr->slice_count, 1);
        slice_list.push_back(slice);
    }

    submitSlices(slice_list);
    return Status::OK();
}

int UBShmemTransport::registerLocalMemory(void *addr, size_t length,
                                          const std::string &location,
                                          bool remote_accessible,
                                          bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);

    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }

    // IPC-based memory registration
    if (!use_fabric_mem_) {
        // Get IPC Mem export key
        char ipc_key[kIPCHandleKeyLength] = {0};
        if (!checkAcl(aclrtIpcMemGetExportKey(
                          addr, length, ipc_key, kIPCHandleKeyLength,
                          ACL_RT_IPC_MEM_EXPORT_FLAG_DISABLE_PID_VALIDATION),
                      "UBShmemTransport: aclrtIpcMemGetExportKey failed")) {
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name =
            serializeBinaryData((const void *)ipc_key, kIPCHandleKeyLength);
        return metadata_->addLocalMemoryBuffer(desc, true);
    }

    // Fabric memory registration
    else {
        // Retain allocation handle
        aclrtDrvMemHandle handle;
        auto ret = aclrtMemRetainAllocationHandle(addr, &handle);
        if (!checkAcl(
                ret,
                "UBShmemTransport: aclrtMemRetainAllocationHandle failed")) {
            return -1;
        }

        // Export shareable handle
        aclrtMemFabricHandle export_handle_raw = {};
        if (!checkAcl(
                aclrtMemExportToShareableHandleV2(
                    handle, ACL_RT_VMM_EXPORT_FLAG_DISABLE_PID_VALIDATION,
                    ACL_MEM_SHARE_HANDLE_TYPE_FABRIC, &export_handle_raw),
                "UBShmemTransport: aclrtMemExportToShareableHandleV2 failed")) {
            (void)aclrtFreePhysical(handle);
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name = serializeBinaryData((const void *)&export_handle_raw,
                                            sizeof(aclrtMemFabricHandle));
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int UBShmemTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int UBShmemTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                  uint64_t length,
                                                  uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);

    // Search for matching buffer entry
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            // Check if already remapped (shared lock)
            remap_lock_.lockShared();
            if (remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                auto shm_addr =
                    remap_entries_[std::make_pair(target_id, entry.addr)]
                        .shm_addr;
                remap_lock_.unlockShared();
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();

            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            if (!remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);

                void *shm_addr = nullptr;
                int rc = -1;

                if (!use_fabric_mem_) {
                    rc = openIPCHandle(output_buffer, &shm_addr);
                } else if (output_buffer.size() ==
                               sizeof(aclrtMemFabricHandle) &&
                           use_fabric_mem_) {
                    rc = openShareableHandle(output_buffer, entry.length,
                                             &shm_addr);
                } else {
                    LOG(ERROR) << "Mismatched UBShmem data transfer method";
                    return -1;
                }

                if (rc != 0) {
                    return -1;
                }

                OpenedShmEntry shm_entry;
                shm_entry.shm_addr = shm_addr;
                shm_entry.length = entry.length;

                // For IPC mode, we need to save the key for cleanup
                if (!use_fabric_mem_) {
                    shm_entry.key = new char[kIPCHandleKeyLength];
                    memcpy(shm_entry.key, output_buffer.data(),
                           kIPCHandleKeyLength);
                } else {
                    shm_entry.key = nullptr;
                }

                remap_entries_[std::make_pair(target_id, entry.addr)] =
                    shm_entry;
            }

            // Calculate relocated address
            auto shm_addr =
                remap_entries_[std::make_pair(target_id, entry.addr)].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int UBShmemTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int rc = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                     false);
        if (rc < 0) {
            return rc;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int UBShmemTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc < 0) {
            return rc;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

void *UBShmemTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void *ptr = nullptr;
        if (!checkAcl(aclrtMalloc(&ptr, size, ACL_MEM_MALLOC_HUGE_FIRST),
                      "UBShmemTransport: aclrtMalloc failed")) {
            return nullptr;
        }
        return ptr;
    }

    size_t granularity = 0;
    aclrtPhysicalMemProp prop = {};
    aclrtDrvMemHandle handle;
    void *ptr = nullptr;
    int aclDev;

    if (!checkAcl(aclrtGetDevice(&aclDev),
                  "UBShmemTransport: aclrtGetDevice failed")) {
        return nullptr;
    }

    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_HBM_MEM_HUGE;
    prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = aclDev;
    prop.reserve = 0;

    auto result = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (!checkAcl(
            result,
            "UBShmemTransport: Failed to allocate specific device memory")) {
        return nullptr;
    }

    uint64_t page_type = 1;
    result =
        aclrtReserveMemAddress(&ptr, size, granularity, nullptr, page_type);
    if (!checkAcl(result, "UBShmemTransport: aclrtReserveMemAddress failed")) {
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    result = aclrtMapMem(ptr, size, 0, handle, 0);
    if (!checkAcl(result, "UBShmemTransport: aclrtMapMem failed")) {
        (void)aclrtReleaseMemAddress(ptr);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    return ptr;
}

void UBShmemTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        aclrtFree(ptr);
        return;
    }

    aclrtDrvMemHandle handle;

    if (!checkAcl(aclrtMemRetainAllocationHandle(ptr, &handle),
                  "UBShmemTransport: aclrtMemRetainAllocationHandle failed")) {
        return;
    }

    (void)aclrtUnmapMem(ptr);
    (void)aclrtReleaseMemAddress(ptr);
    (void)aclrtFreePhysical(handle);
}
}  // namespace mooncake
