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

#include "transport/maca_transport/maca_transport.h"

#include <bits/stdint-uintn.h>
#include "cuda_alike.h"
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <memory>
#include <string>
#include <unordered_map>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

static bool checkCudaErrorReturn(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        return false;
    }
    return true;
}

namespace mooncake {
static int getNumDevices() {
    static int cached_num_devices = -1;
    if (cached_num_devices == -1) {
        if (!checkCudaErrorReturn(cudaGetDeviceCount(&cached_num_devices),
                                  "MacaTransport: cudaGetDeviceCount failed")) {
            return 0;
        }
    }
    return cached_num_devices;
}

static bool enableP2PAccess(int src_device_id, int dst_device_id) {
    int original_device;
    cudaGetDevice(&original_device);

    int canAccessPeer = 0;
    if (!checkCudaErrorReturn(cudaDeviceCanAccessPeer(
                                  &canAccessPeer, src_device_id, dst_device_id),
                              "MacaTransport: failed to query peer access")) {
        cudaSetDevice(original_device);
        return false;
    }

    if (!canAccessPeer) {
        LOG(ERROR) << "MacaTransport: device " << src_device_id
                   << " cannot p2p access device " << dst_device_id;
        cudaSetDevice(original_device);
        return false;
    }

    // enable src->dst p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(src_device_id),
                              "MacaTransport: failed to set device")) {
        cudaSetDevice(original_device);
        return false;
    }
    cudaError_t result = cudaDeviceEnablePeerAccess(dst_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR) << "MacaTransport: failed to enable p2p access (Error code: "
                   << result << " - " << cudaGetErrorString(result) << ")"
                   << std::endl;
        cudaSetDevice(original_device);
        return false;
    }

    // enable dst->src p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(dst_device_id),
                              "MacaTransport: failed to set device")) {
        cudaSetDevice(original_device);
        return false;
    }
    result = cudaDeviceEnablePeerAccess(src_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR) << "MacaTransport: failed to enable p2p access (Error code: "
                   << result << " - " << cudaGetErrorString(result) << ")"
                   << std::endl;
        cudaSetDevice(original_device);
        return false;
    }

    cudaSetDevice(original_device);
    return true;
}

static int getDeviceFromPointer(void *ptr) {
    if (!ptr) return -1;

    cudaPointerAttributes attr;
    cudaError_t err = cudaPointerGetAttributes(&attr, ptr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "MacaTransport: cudaPointerGetAttributes failed for "
                   << ptr;
        return -1;
    }

    if (attr.type == cudaMemoryTypeDevice) {
        return attr.device;
    }
    return -1;
}

enum class MacaCallerSyncMode {
    Host,
    Wait,
};

enum class MacaCopyApi {
    Auto,
    Default,
    BatchFlag,
};

struct CallerSync {
    MacaCallerSyncMode mode;
    cudaEvent_t event;
};

static MacaCallerSyncMode callerSyncMode() {
    static const MacaCallerSyncMode mode = [] {
        const char *env = std::getenv("MC_MACA_CALLER_SYNC");
        if (!env) return MacaCallerSyncMode::Host;

        std::string value(env);
        if (value == "host") return MacaCallerSyncMode::Host;
        if (value == "wait") return MacaCallerSyncMode::Wait;
        LOG(WARNING) << "MacaTransport: unknown MC_MACA_CALLER_SYNC=" << value
                     << ", falling back to host";
        return MacaCallerSyncMode::Host;
    }();
    return mode;
}

static const char *callerSyncModeName(MacaCallerSyncMode mode) {
    switch (mode) {
        case MacaCallerSyncMode::Host:
            return "host";
        case MacaCallerSyncMode::Wait:
            return "wait";
    }
    return "unknown";
}

static MacaCopyApi copyApi() {
    static const MacaCopyApi api = [] {
        const char *env = std::getenv("MC_MACA_COPY_API");
        if (!env) return MacaCopyApi::Auto;

        std::string value(env);
        if (value == "auto") return MacaCopyApi::Auto;
        if (value == "default") return MacaCopyApi::Default;
        if (value == "batchflag") return MacaCopyApi::BatchFlag;
        LOG(WARNING) << "MacaTransport: unknown MC_MACA_COPY_API=" << value
                     << ", falling back to auto";
        return MacaCopyApi::Auto;
    }();
    return api;
}

static const char *copyApiName(MacaCopyApi api) {
    switch (api) {
        case MacaCopyApi::Auto:
            return "auto";
        case MacaCopyApi::Default:
            return "default";
        case MacaCopyApi::BatchFlag:
            return "batchflag";
    }
    return "unknown";
}

static size_t batchFlagMinBytes() {
    static const size_t min_bytes = [] {
        constexpr size_t kDefaultMinBytes = 1024ULL * 1024ULL;
        const char *env = std::getenv("MC_MACA_BATCHFLAG_MIN_BYTES");
        if (!env) return kDefaultMinBytes;

        char *end = nullptr;
        unsigned long long value = std::strtoull(env, &end, 0);
        if (end == env || *end != '\0') {
            LOG(WARNING) << "MacaTransport: unknown "
                            "MC_MACA_BATCHFLAG_MIN_BYTES="
                         << env << ", falling back to " << kDefaultMinBytes;
            return kDefaultMinBytes;
        }
        return static_cast<size_t>(value);
    }();
    return min_bytes;
}

static bool shouldUseBatchFlag(MacaCopyApi api, size_t length) {
    if (api == MacaCopyApi::BatchFlag) return true;
    if (api == MacaCopyApi::Auto) return length >= batchFlagMinBytes();
    return false;
}

class PerDeviceStreamPool {
   public:
    cudaStream_t getOrCreate(int device_id) {
        auto iter = streams_.find(device_id);
        if (iter != streams_.end()) return iter->second;

        int original_device = -1;
        cudaGetDevice(&original_device);
        if (!checkCudaErrorReturn(cudaSetDevice(device_id),
                                  "MacaTransport: failed to set device")) {
            if (original_device >= 0) cudaSetDevice(original_device);
            return nullptr;
        }

        cudaStream_t stream = nullptr;
        cudaError_t err =
            cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
        if (original_device >= 0) cudaSetDevice(original_device);
        if (!checkCudaErrorReturn(
                err, "MacaTransport: cudaStreamCreateWithFlags failed")) {
            return nullptr;
        }

        streams_[device_id] = stream;
        return stream;
    }

    ~PerDeviceStreamPool() {
        int original_device = -1;
        cudaGetDevice(&original_device);
        for (auto &entry : streams_) {
            cudaSetDevice(entry.first);
            cudaStreamDestroy(entry.second);
        }
        if (original_device >= 0) cudaSetDevice(original_device);
    }

   private:
    std::unordered_map<int, cudaStream_t> streams_;
};

static thread_local PerDeviceStreamPool thread_local_stream_pool;

class PerDeviceEventPool {
   public:
    cudaEvent_t getOrCreate(int device_id) {
        auto iter = events_.find(device_id);
        if (iter != events_.end()) return iter->second;

        int original_device = -1;
        cudaGetDevice(&original_device);
        if (!checkCudaErrorReturn(cudaSetDevice(device_id),
                                  "MacaTransport: failed to set device")) {
            if (original_device >= 0) cudaSetDevice(original_device);
            return nullptr;
        }

        cudaEvent_t event = nullptr;
        cudaError_t err =
            cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
        if (original_device >= 0) cudaSetDevice(original_device);
        if (!checkCudaErrorReturn(
                err, "MacaTransport: cudaEventCreateWithFlags failed")) {
            return nullptr;
        }

        events_[device_id] = event;
        return event;
    }

    ~PerDeviceEventPool() {
        int original_device = -1;
        cudaGetDevice(&original_device);
        for (auto &entry : events_) {
            cudaSetDevice(entry.first);
            cudaEventDestroy(entry.second);
        }
        if (original_device >= 0) cudaSetDevice(original_device);
    }

   private:
    std::unordered_map<int, cudaEvent_t> events_;
};

static thread_local PerDeviceEventPool thread_local_event_pool;

static Status prepareCallerSync(CallerSync &sync) {
    sync.mode = callerSyncMode();
    sync.event = nullptr;

    int current_device = 0;
    cudaGetDevice(&current_device);
    cudaEvent_t event = thread_local_event_pool.getOrCreate(current_device);
    if (!event) {
        return Status::Context("MacaTransport: failed to create sync event");
    }

    cudaError_t err = cudaEventRecord(event, cudaStreamPerThread);
    if (err != cudaSuccess) {
        LOG(ERROR) << "MacaTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(err);
        return Status::Context("MacaTransport: cudaEventRecord failed");
    }

    sync.event = event;
    if (sync.mode == MacaCallerSyncMode::Host) {
        err = cudaEventSynchronize(event);
        if (err != cudaSuccess) {
            LOG(ERROR) << "MacaTransport: cudaEventSynchronize failed: "
                       << cudaGetErrorString(err);
            return Status::Context(
                "MacaTransport: cudaEventSynchronize failed");
        }
    }
    return Status::OK();
}

static void getCopyEndpoints(Transport::Slice *slice, void *&dst,
                             const void *&src) {
    dst = slice->local.dest_addr;
    src = slice->source_addr;
    if (slice->opcode == Transport::TransferRequest::READ) {
        dst = slice->source_addr;
        src = slice->local.dest_addr;
    }
}

static cudaError_t submitMemcpyAsync(Transport::Slice *slice,
                                     cudaStream_t stream) {
    void *dst;
    const void *src;
    getCopyEndpoints(slice, dst, src);
    return cudaMemcpyAsync(dst, src, slice->length, cudaMemcpyDefault, stream);
}

static cudaError_t submitBatchFlagAsync(std::vector<mcCopyFlag_t> &copy_batch,
                                        cudaStream_t stream) {
    if (copy_batch.empty()) return cudaSuccess;
    return mcExtBatchCopyFlagAndWaitV2(copy_batch.data(), copy_batch.size(),
                                       nullptr, 0, stream);
}

MacaTransport::MacaTransport() {
    int num_devices = getNumDevices();
    if (globalConfig().trace) {
        LOG(INFO) << "MacaTransport: num_devices: " << num_devices;
    }

    for (int src_device_id = 0; src_device_id < num_devices; ++src_device_id) {
        for (int dst_device_id = src_device_id + 1; dst_device_id < num_devices;
             ++dst_device_id) {
            if (enableP2PAccess(src_device_id, dst_device_id)) {
                if (globalConfig().trace) {
                    LOG(INFO)
                        << "MacaTransport: enabled p2p access between device "
                        << src_device_id << " and " << dst_device_id;
                }
            } else {
                LOG(ERROR)
                    << "MacaTransport: failed to enable p2p access between "
                       "device "
                    << src_device_id << " and " << dst_device_id;
            }
        }
    }
}

MacaTransport::~MacaTransport() {
    for (auto &entry : remap_entries_) {
        cudaIpcCloseMemHandle(entry.second.shm_addr);
    }
    remap_entries_.clear();
}

int MacaTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> metadata,
                           std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto old_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    if (old_desc) *desc = *old_desc;

    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (desc->protocol.empty()) {
        desc->protocol = "maca";
    } else if (desc->protocol.find("maca") == std::string::npos) {
        desc->protocol += ",maca";
    }
#else
    desc->protocol = "maca";
#endif
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status MacaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "MacaTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "MacaTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        task.batch_id = batch_id;
        task.transport_ = this;
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        // Set correct device context before memcpy
        int original_device = -1;
        cudaGetDevice(&original_device);
        int target_device = getDeviceFromPointer(request.source);
        if (target_device < 0)
            target_device = getDeviceFromPointer((void *)dest_addr);
        if (target_device >= 0) cudaSetDevice(target_device);

        cudaError_t err;
        if (slice->opcode == TransferRequest::READ)
            err = cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                             slice->length, cudaMemcpyDefault);
        else
            err = cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                             slice->length, cudaMemcpyDefault);
        if (err != cudaSuccess)
            slice->markFailed();
        else
            slice->markSuccess();

        if (original_device >= 0) cudaSetDevice(original_device);
    }

    return Status::OK();
}

Status MacaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                        TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "MacaTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    std::unordered_map<cudaStream_t, cudaError_t> stream_status_cache;
    for (auto *slice : task.slice_list) {
        if (slice && slice->status == Slice::POSTED) {
            cudaStream_t stream = (cudaStream_t)slice->local.cuda_stream;
            auto iter = stream_status_cache.find(stream);
            cudaError_t err;
            if (iter == stream_status_cache.end()) {
                err = cudaStreamQuery(stream);
                stream_status_cache[stream] = err;
            } else {
                err = iter->second;
            }

            if (err == cudaSuccess) {
                slice->markSuccess();
            } else if (err != cudaErrorNotReady) {
                LOG(ERROR) << "MacaTransport: cudaStreamQuery failed: "
                           << cudaGetErrorString(err);
                slice->markFailed();
            }
        }
    }
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
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

Status MacaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    MacaCallerSyncMode sync_mode = callerSyncMode();
    static const bool logged_sync_mode = [sync_mode] {
        LOG(INFO) << "MacaTransport: caller sync mode "
                  << callerSyncModeName(sync_mode);
        return true;
    }();
    (void)logged_sync_mode;
    MacaCopyApi api = copyApi();
    static const bool logged_copy_api = [api] {
        LOG(INFO) << "MacaTransport: copy api " << copyApiName(api);
        return true;
    }();
    (void)logged_copy_api;
    static const bool logged_batchflag_threshold = [api] {
        if (api == MacaCopyApi::Auto) {
            LOG(INFO) << "MacaTransport: batchflag min bytes "
                      << batchFlagMinBytes();
        }
        return true;
    }();
    (void)logged_batchflag_threshold;

    CallerSync caller_sync;
    Status sync_status = prepareCallerSync(caller_sync);
    if (!sync_status.ok()) return sync_status;

    struct DeviceStream {
        int device_id;
        cudaStream_t stream;
        bool ok;
        std::vector<mcCopyFlag_t> copy_batch;
        std::vector<Slice *> batch_slices;
    };

    std::vector<DeviceStream> streams;
    std::unordered_map<int, size_t> stream_index_by_device;
    Status first_error = Status::OK();
    bool has_batchflag_copies = false;

    int original_device = -1;
    cudaGetDevice(&original_device);
    int active_copy_device = -1;

    auto getStream = [&](int device_id, cudaStream_t &stream,
                         size_t &stream_index) -> bool {
        auto iter = stream_index_by_device.find(device_id);
        if (iter != stream_index_by_device.end()) {
            stream_index = iter->second;
            stream = streams[stream_index].stream;
            return true;
        }

        if (!checkCudaErrorReturn(cudaSetDevice(device_id),
                                  "MacaTransport: failed to set device")) {
            return false;
        }

        cudaStream_t new_stream =
            thread_local_stream_pool.getOrCreate(device_id);
        if (!new_stream) return false;

        if (caller_sync.mode == MacaCallerSyncMode::Wait && caller_sync.event) {
            cudaError_t wait_err =
                cudaStreamWaitEvent(new_stream, caller_sync.event, 0);
            if (wait_err != cudaSuccess) {
                LOG(ERROR) << "MacaTransport: cudaStreamWaitEvent failed: "
                           << cudaGetErrorString(wait_err);
                return false;
            }
        }

        stream_index = streams.size();
        streams.push_back({device_id, new_stream, true, {}, {}});
        stream_index_by_device[device_id] = stream_index;
        stream = new_stream;
        return true;
    };

    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        uint64_t dest_addr = request.target_offset;

        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts =
            globalConfig().slice_timeout > 0 ? getCurrentTimeInNano() : 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) {
                slice->local.dest_addr = nullptr;
                slice->markFailed();
                if (first_error.ok())
                    first_error =
                        Status::Memory("device memory not registered");
                continue;
            }
        }
        slice->local.dest_addr = (char *)dest_addr;

        int target_device = getDeviceFromPointer(request.source);
        if (target_device < 0)
            target_device = getDeviceFromPointer((void *)dest_addr);
        if (target_device < 0) {
            slice->markFailed();
            if (first_error.ok())
                first_error =
                    Status::InvalidArgument("Cannot infer MACA device");
            continue;
        }

        cudaStream_t stream = nullptr;
        size_t stream_index = 0;
        if (!getStream(target_device, stream, stream_index)) {
            slice->markFailed();
            if (first_error.ok())
                first_error =
                    Status::Memory("MacaTransport: failed to get MACA stream");
            continue;
        }

        if (active_copy_device != target_device) {
            if (!checkCudaErrorReturn(cudaSetDevice(target_device),
                                      "MacaTransport: failed to set device")) {
                slice->markFailed();
                streams[stream_index].ok = false;
                if (first_error.ok())
                    first_error =
                        Status::Context("MacaTransport: failed to set device");
                continue;
            }
            active_copy_device = target_device;
        }

        if (shouldUseBatchFlag(api, slice->length)) {
            void *dst;
            const void *src;
            getCopyEndpoints(slice, dst, src);

            mcCopyFlag_t copy;
            std::memset(&copy, 0, sizeof(copy));
            copy.dst = dst;
            copy.src = src;
            copy.engine = ParallelCopyEngineDefault;
            copy.count = slice->length;
            copy.waitNum = 0;
            copy.writeNum = 0;

            streams[stream_index].copy_batch.push_back(copy);
            streams[stream_index].batch_slices.push_back(slice);
            slice->local.cuda_stream = (void *)stream;
            has_batchflag_copies = true;
            continue;
        }

        cudaError_t err = submitMemcpyAsync(slice, stream);
        if (err != cudaSuccess) {
            LOG(ERROR) << "MacaTransport: async copy failed: "
                       << cudaGetErrorString(err);
            slice->markFailed();
            streams[stream_index].ok = false;
            if (first_error.ok())
                first_error =
                    Status::Memory("MacaTransport: async copy failed");
        } else {
            slice->local.cuda_stream = (void *)stream;
            slice->status = Slice::POSTED;
        }
    }

    if (has_batchflag_copies) {
        auto failBatchSlices = [](DeviceStream &entry) {
            for (auto *slice : entry.batch_slices) {
                if (slice->status == Slice::PENDING) {
                    slice->markFailed();
                }
            }
        };

        for (auto &entry : streams) {
            if (entry.copy_batch.empty()) continue;
            if (!entry.ok) {
                failBatchSlices(entry);
                if (first_error.ok())
                    first_error =
                        Status::Memory("MacaTransport: batch copy skipped");
                continue;
            }
            if (!checkCudaErrorReturn(cudaSetDevice(entry.device_id),
                                      "MacaTransport: failed to set device")) {
                entry.ok = false;
                failBatchSlices(entry);
                if (first_error.ok())
                    first_error =
                        Status::Context("MacaTransport: failed to set device");
                continue;
            }

            cudaError_t err =
                submitBatchFlagAsync(entry.copy_batch, entry.stream);
            if (err != cudaSuccess) {
                LOG(ERROR)
                    << "MacaTransport: mcExtBatchCopyFlagAndWaitV2 failed: "
                    << cudaGetErrorString(err);
                entry.ok = false;
                failBatchSlices(entry);
                if (first_error.ok())
                    first_error =
                        Status::Memory("MacaTransport: batch copy failed");
            }
        }

        for (auto &entry : streams) {
            if (entry.copy_batch.empty()) continue;
            for (auto *slice : entry.batch_slices) {
                if (slice->status != Slice::PENDING) continue;
                if (entry.ok)
                    slice->status = Slice::POSTED;
                else
                    slice->markFailed();
            }
        }
    }

    if (original_device >= 0) cudaSetDevice(original_device);
    return first_error;
}

int MacaTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &location,
                                       bool remote_accessible,
                                       bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }
    cudaPointerAttributes attr;
    cudaError_t err = cudaPointerGetAttributes(&attr, addr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "MacaTransport: cudaPointerGetAttributes failed";
        return -1;
    }

    if (attr.type != cudaMemoryTypeDevice) {
        LOG(ERROR) << "Unsupported memory type, " << addr << " " << attr.type;
        return -1;
    }

    // Resolve the true cudaMalloc base address. Framework caching allocators
    // (PyTorch, etc.) sub-allocate tensors within larger cudaMalloc segments.
    // cudaIpcGetMemHandle always returns a handle for the entire segment, so
    // we must register at segment granularity for correct IPC relocation.
    CUdeviceptr base_ptr = 0;
    size_t alloc_size = 0;
    CUresult cu_err =
        cuMemGetAddressRange(&base_ptr, &alloc_size, (CUdeviceptr)addr);
    if (cu_err != CUDA_SUCCESS) {
        LOG(ERROR) << "MacaTransport: cuMemGetAddressRange failed "
                   << "for addr " << addr << " (error " << cu_err << ")";
        return -1;
    }

    // Skip if this cudaMalloc block is already registered
    if (registered_base_addrs_.count((uint64_t)base_ptr)) {
        return 0;
    }

    cudaIpcMemHandle_t handle;
    err = cudaIpcGetMemHandle(&handle, (void *)base_ptr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "MacaTransport: cudaIpcGetMemHandle failed";
        return -1;
    }

    (void)remote_accessible;
    BufferDesc desc;
    desc.addr = (uint64_t)base_ptr;
    desc.length = alloc_size;
    desc.name = location;
    desc.shm_name = serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
#ifdef ENABLE_MULTI_PROTOCOL
    desc.protocol = "maca";
#endif
    int rc = metadata_->addLocalMemoryBuffer(desc, true);
    if (rc == 0) {
        registered_base_addrs_.insert((uint64_t)base_ptr);
    }
    return rc;
}

int MacaTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    CUdeviceptr base_ptr = 0;
    size_t alloc_size = 0;
    CUresult cu_err =
        cuMemGetAddressRange(&base_ptr, &alloc_size, (CUdeviceptr)addr);

    void *key_ptr = addr;
    if (cu_err == CUDA_SUCCESS) {
        key_ptr = (void *)base_ptr;
    } else {
        LOG(WARNING)
            << "MacaTransport: cuMemGetAddressRange failed for "
            << "addr " << addr << " during unregister (error " << cu_err
            << "). Memory may already be freed, using provided address.";
    }

    {
        std::lock_guard<std::mutex> lock(register_mutex_);
        registered_base_addrs_.erase((uint64_t)key_ptr);
    }
    return metadata_->removeLocalMemoryBuffer(key_ptr, update_metadata);
}

int MacaTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                               uint64_t length,
                                               uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
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
                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t)) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void *shm_addr = nullptr;
                    cudaError_t err = cudaIpcOpenMemHandle(
                        &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                    if (err != cudaSuccess) {
                        LOG(ERROR) << "MacaTransport: "
                                      "cudaIpcOpenMemHandle failed: "
                                   << cudaGetErrorString(err);
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else {
                    LOG(ERROR) << "Mismatched MACA data transfer method";
                    return -1;
                }
            }
            auto shm_addr =
                remap_entries_[std::make_pair(target_id, entry.addr)].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
        index++;
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int MacaTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int MacaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

void *MacaTransport::allocatePinnedLocalMemory(size_t size) {
    void *ptr = nullptr;
    cudaError_t res = cudaMalloc(&ptr, size);
    if (res == cudaSuccess) {
        LOG(INFO) << "MacaTransport: allocated device memory " << size
                  << " bytes";
        return ptr;
    } else {
        LOG(ERROR) << "MacaTransport: cudaMalloc failed: "
                   << cudaGetErrorString(res);
        return nullptr;
    }
}

void MacaTransport::freePinnedLocalMemory(void *ptr) {
    cudaFree(ptr);
    return;
}

}  // namespace mooncake
