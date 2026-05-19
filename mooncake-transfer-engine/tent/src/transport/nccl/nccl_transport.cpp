// Copyright 2026 KVCache.AI
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

#include "tent/transport/nccl/nccl_transport.h"

#include <glog/logging.h>

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace {

Status ncclStatus(ncclResult_t result, const char* expr) {
    if (result == ncclSuccess) return Status::OK();
    return Status::InternalError(std::string(expr) + ": " +
                                 ncclGetErrorString(result) + LOC_MARK);
}

#define CHECK_NCCL(call)                       \
    do {                                       \
        Status _s = ncclStatus(call, #call);   \
        if (!_s.ok()) return _s;               \
    } while (0)

int hexValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::string serializeUniqueId(const ncclUniqueId& id) {
    static constexpr char kHex[] = "0123456789abcdef";
    const auto* bytes = reinterpret_cast<const unsigned char*>(&id);
    std::string out;
    out.resize(sizeof(id) * 2);
    for (size_t i = 0; i < sizeof(id); ++i) {
        out[2 * i] = kHex[bytes[i] >> 4];
        out[2 * i + 1] = kHex[bytes[i] & 0xf];
    }
    return out;
}

Status deserializeUniqueId(const std::string& raw, ncclUniqueId& id) {
    if (raw.size() != sizeof(id) * 2) {
        return Status::InvalidArgument(
            "Invalid NCCL unique id size" LOC_MARK);
    }
    auto* bytes = reinterpret_cast<unsigned char*>(&id);
    for (size_t i = 0; i < sizeof(id); ++i) {
        int hi = hexValue(raw[2 * i]);
        int lo = hexValue(raw[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            return Status::InvalidArgument(
                "Invalid NCCL unique id encoding" LOC_MARK);
        }
        bytes[i] = static_cast<unsigned char>((hi << 4) | lo);
    }
    return Status::OK();
}

std::string makeSessionKey(const std::string& local_name,
                           const std::string& remote_name, int local_device,
                           int remote_device) {
    std::ostringstream ss;
    ss << local_name << "->" << remote_name << ":cuda" << local_device
       << "->cuda" << remote_device;
    return ss.str();
}

std::string makeWindowKey(const std::string& session_key,
                          const char* purpose, uint64_t base,
                          uint64_t length) {
    std::ostringstream ss;
    ss << session_key << ":window:" << purpose << ":" << std::hex << base
       << ":" << length;
    return ss.str();
}

Status setCudaDevice(int device, int& previous_device) {
    CHECK_CUDA(cudaGetDevice(&previous_device));
    CHECK_CUDA(cudaSetDevice(device));
    return Status::OK();
}

}  // namespace

struct NcclTransport::CommState {
    std::mutex mu;
    std::condition_variable cv;
    ncclComm_t comm = nullptr;
    Status status;
    bool initializing = false;
    bool ready = false;
    int device_index = -1;
    int local_rank = -1;
    int peer_rank = -1;
};

struct NcclTransport::WindowState {
    std::mutex mu;
    std::condition_variable cv;
    ncclWindow_t window = nullptr;
    void* local_buffer = nullptr;
    uint64_t length = 0;
    bool owns_local_buffer = false;
    bool initializing = false;
    bool ready = false;
    int device_index = -1;
    std::string session_key;
    Status status;
};

struct NcclTransport::TransferContext {
    SegmentID target_id = 0;
    std::string remote_segment_name;
    std::string remote_rpc_addr;
    uint64_t target_base = 0;
    uint64_t target_length = 0;
    uint64_t target_offset = 0;
    uint64_t source_base = 0;
    uint64_t source_length = 0;
    int local_device = -1;
    int remote_device = -1;
    std::string session_key;
    std::string window_key;
    std::string source_window_key;
};

NcclTransport::NcclTransport() = default;

NcclTransport::~NcclTransport() { uninstall(); }

Status NcclTransport::install(std::string& local_segment_name,
                              std::shared_ptr<ControlService> metadata,
                              std::shared_ptr<Topology> local_topology,
                              std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "NCCL transport has been installed" LOC_MARK);
    }

    if (Platform::getLoader().type() != "cuda") {
        return Status::InvalidArgument(
            "NCCL transport requires CUDA platform" LOC_MARK);
    }

    platform_ = dynamic_cast<CudaPlatform*>(&Platform::getLoader());
    if (!platform_) {
        return Status::InvalidArgument(
            "NCCL transport could not load CUDA platform" LOC_MARK);
    }

    CHECK_NCCL(ncclGetVersion(&nccl_version_));
    if (nccl_version_ < 22900) {
        return Status::InvalidArgument(
            "NCCL host-side RMA requires NCCL 2.29 or newer" LOC_MARK);
    }

    metadata_ = std::move(metadata);
    local_segment_name_ = local_segment_name;
    local_topology_ = std::move(local_topology);
    conf_ = std::move(conf);
    allow_external_window_buffers_ =
        conf_ ? conf_->get("transports/nccl/allow_external_window_buffers",
                           false)
              : false;

    metadata_->setBootstrapNcclCallback(
        [this](const NcclBootstrapDesc& request, NcclBootstrapDesc& response) {
            auto status = onBootstrapNccl(request, response);
            if (!status.ok()) response.reply_msg = status.ToString();
            return status.ok() ? 0 : -1;
        });
    metadata_->setNcclWindowCallback(
        [this](const NcclWindowDesc& request, NcclWindowDesc& response) {
            auto status = onRegisterNcclWindow(request, response);
            if (!status.ok()) response.reply_msg = status.ToString();
            return status.ok() ? 0 : -1;
        });
    metadata_->setNcclSignalCallback(
        [this](const NcclSignalDesc& request, NcclSignalDesc& response) {
            auto status = onWaitNcclSignal(request, response);
            if (!status.ok()) response.reply_msg = status.ToString();
            return status.ok() ? 0 : -1;
        });

    // Host-side NCCL RMA maps WRITE to ncclPutSignal. READ is skipped by the
    // scheduler until device-side GIN get or delegated put is added.
    caps.gpu_to_gpu = true;
    installed_ = true;

    LOG(INFO) << "NCCL transport installed: version=" << nccl_version_
              << " allow_external_window_buffers="
              << allow_external_window_buffers_;
    return Status::OK();
}

Status NcclTransport::uninstall() {
    if (!installed_) return Status::OK();

    if (metadata_) {
        metadata_->setBootstrapNcclCallback(nullptr);
        metadata_->setNcclWindowCallback(nullptr);
        metadata_->setNcclSignalCallback(nullptr);
    }

    std::vector<std::thread> background_threads;
    {
        std::lock_guard<std::mutex> lock(background_mutex_);
        background_threads.swap(background_threads_);
    }
    for (auto& thread : background_threads) {
        if (thread.joinable()) thread.join();
    }

    {
        std::lock_guard<std::mutex> lock(window_mutex_);
        for (auto& [_, window] : windows_) {
            if (!window) continue;
            std::unique_lock<std::mutex> state_lock(window->mu);
            if (window->ready && window->window) {
                int previous_device = 0;
                auto status = setCudaDevice(window->device_index,
                                            previous_device);
                if (status.ok()) {
                    auto result = ncclCommWindowDeregister(
                        comms_[window->session_key]->comm, window->window);
                    if (result != ncclSuccess)
                        LOG(WARNING) << "ncclCommWindowDeregister failed: "
                                     << ncclGetErrorString(result);
                    cudaSetDevice(previous_device);
                }
            }
            if (window->owns_local_buffer && window->local_buffer) {
                auto result = ncclMemFree(window->local_buffer);
                if (result != ncclSuccess)
                    LOG(WARNING) << "ncclMemFree window buffer failed: "
                                 << ncclGetErrorString(result);
            }
        }
        windows_.clear();
    }

    {
        std::lock_guard<std::mutex> lock(comm_mutex_);
        for (auto& [_, comm] : comms_) {
            if (comm && comm->ready && comm->comm) {
                auto result = ncclCommDestroy(comm->comm);
                if (result != ncclSuccess)
                    LOG(WARNING) << "ncclCommDestroy failed: "
                                 << ncclGetErrorString(result);
            }
        }
        comms_.clear();
    }

    std::lock_guard<std::mutex> lock(allocation_mutex_);
    if (!nccl_allocations_.empty()) {
        LOG(WARNING) << "NCCL transport uninstalling with "
                     << nccl_allocations_.size()
                     << " tracked ncclMemAlloc buffers still live";
    }
    metadata_.reset();
    local_topology_.reset();
    conf_.reset();
    platform_ = nullptr;
    installed_ = false;
    return Status::OK();
}

Status NcclTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto nccl_batch = Slab<NcclSubBatch>::Get().allocate();
    if (!nccl_batch)
        return Status::InternalError("Unable to allocate NCCL sub-batch");
    batch = nccl_batch;
    nccl_batch->task_list.reserve(max_size);
    nccl_batch->max_size = max_size;
    CHECK_STATUS(platform_->getStreamFromPool(nccl_batch->stream));
    return Status::OK();
}

Status NcclTransport::freeSubBatch(SubBatchRef& batch) {
    auto nccl_batch = dynamic_cast<NcclSubBatch*>(batch);
    if (!nccl_batch)
        return Status::InvalidArgument("Invalid NCCL sub-batch" LOC_MARK);
    for (auto& task : nccl_batch->task_list) {
        if (task.completion_event) {
            cudaEventDestroy(task.completion_event);
            task.completion_event = nullptr;
        }
    }
    Slab<NcclSubBatch>::Get().deallocate(nccl_batch);
    batch = nullptr;
    return Status::OK();
}

Status NcclTransport::markFailed(NcclTask& task, const std::string& reason) {
    LOG(WARNING) << "NCCL host RMA task failed: " << reason;
    task.transferred_bytes.store(0, std::memory_order_release);
    task.status_word.store(TransferStatusEnum::FAILED,
                           std::memory_order_release);
    return Status::OK();
}

Status NcclTransport::buildTransferContext(const Request& request,
                                           TransferContext& ctx) {
    if (request.target_id == LOCAL_SEGMENT_ID) {
        return Status::InvalidArgument(
            "NCCL host RMA expects a remote target segment" LOC_MARK);
    }
    if (Platform::getLoader().getMemoryType(request.source) != MTYPE_CUDA) {
        return Status::InvalidArgument(
            "NCCL host RMA source must be CUDA memory" LOC_MARK);
    }

    auto local_locations = Platform::getLoader().getLocation(request.source, 1);
    if (local_locations.empty()) {
        return Status::InvalidArgument(
            "Unable to resolve local CUDA source location" LOC_MARK);
    }
    LocationParser local_location(local_locations[0].location);
    if (local_location.type() != "cuda" || local_location.index() < 0) {
        return Status::InvalidArgument(
            "Unable to resolve local CUDA device" LOC_MARK);
    }

    BufferDesc target_buffer;
    Status status = metadata_->segmentManager().withCachedSegment(
        request.target_id, [&](SegmentDesc* segment) {
            if (segment->type != SegmentType::Memory) {
                return Status::NeedsRefreshCache(
                    "NCCL target segment is not memory" LOC_MARK);
            }
            auto* buffer = segment->findBuffer(request.target_offset,
                                              request.length);
            if (!buffer) {
                return Status::NeedsRefreshCache(
                    "Requested address is not in registered buffer" LOC_MARK);
            }
            target_buffer = *buffer;
            ctx.remote_segment_name = segment->name;
            ctx.remote_rpc_addr = segment->rpc_server_addr;
            return Status::OK();
        });
    if (!status.ok()) return status;

    LocationParser remote_location(target_buffer.location);
    if (remote_location.type() != "cuda" || remote_location.index() < 0) {
        return Status::InvalidArgument(
            "NCCL host RMA target must be CUDA memory" LOC_MARK);
    }

    ctx.target_id = request.target_id;
    ctx.target_base = target_buffer.addr;
    ctx.target_length = target_buffer.length;
    ctx.target_offset = request.target_offset - target_buffer.addr;
    ctx.source_base = reinterpret_cast<uint64_t>(request.source);
    ctx.source_length = request.length;
    ctx.local_device = local_location.index();
    ctx.remote_device = remote_location.index();
    ctx.session_key = makeSessionKey(local_segment_name_,
                                     ctx.remote_segment_name,
                                     ctx.local_device, ctx.remote_device);
    ctx.window_key = makeWindowKey(ctx.session_key, "target", ctx.target_base,
                                   ctx.target_length);
    ctx.source_window_key = makeWindowKey(ctx.session_key, "source",
                                          ctx.source_base, ctx.source_length);
    return Status::OK();
}

void NcclTransport::startBackground(std::function<void()> fn) {
    std::lock_guard<std::mutex> lock(background_mutex_);
    background_threads_.emplace_back(std::move(fn));
}

Status NcclTransport::waitForComm(const std::string& session_key,
                                  std::shared_ptr<CommState>& state) {
    {
        std::lock_guard<std::mutex> lock(comm_mutex_);
        auto it = comms_.find(session_key);
        if (it == comms_.end()) {
            return Status::InvalidArgument(
                "NCCL communicator session not found" LOC_MARK);
        }
        state = it->second;
    }

    std::unique_lock<std::mutex> lock(state->mu);
    state->cv.wait(lock, [&] { return state->ready || !state->status.ok(); });
    return state->status;
}

Status NcclTransport::ensureComm(const TransferContext& ctx,
                                 std::shared_ptr<CommState>& state) {
    bool should_init = false;
    {
        std::lock_guard<std::mutex> lock(comm_mutex_);
        auto& entry = comms_[ctx.session_key];
        if (!entry) entry = std::make_shared<CommState>();
        state = entry;
        std::lock_guard<std::mutex> state_lock(state->mu);
        if (!state->ready && !state->initializing) {
            state->initializing = true;
            state->device_index = ctx.local_device;
            state->local_rank = 0;
            state->peer_rank = 1;
            should_init = true;
        }
    }

    if (should_init) {
        ncclUniqueId unique_id;
        auto status = ncclStatus(ncclGetUniqueId(&unique_id),
                                 "ncclGetUniqueId");
        if (status.ok()) {
            NcclBootstrapDesc request;
            request.session_key = ctx.session_key;
            request.unique_id = serializeUniqueId(unique_id);
            request.device_index = ctx.remote_device;
            NcclBootstrapDesc response;
            status = ControlClient::bootstrapNccl(ctx.remote_rpc_addr,
                                                  request, response);
        }
        if (status.ok()) {
            int previous_device = 0;
            status = setCudaDevice(ctx.local_device, previous_device);
            if (status.ok()) {
                status = ncclStatus(
                    ncclCommInitRank(&state->comm, 2, unique_id, 0),
                    "ncclCommInitRank");
                cudaSetDevice(previous_device);
            }
        }

        {
            std::lock_guard<std::mutex> lock(state->mu);
            state->status = status;
            state->ready = status.ok();
            state->initializing = false;
        }
        state->cv.notify_all();
    }

    return waitForComm(ctx.session_key, state);
}

Status NcclTransport::ensureWindow(
    const TransferContext& ctx, const std::shared_ptr<CommState>& comm_state,
    std::shared_ptr<WindowState>& state) {
    bool should_init = false;
    {
        std::lock_guard<std::mutex> lock(window_mutex_);
        auto& entry = windows_[ctx.window_key];
        if (!entry) entry = std::make_shared<WindowState>();
        state = entry;
        std::lock_guard<std::mutex> state_lock(state->mu);
        if (!state->ready && !state->initializing) {
            state->initializing = true;
            state->length = ctx.target_length;
            state->device_index = ctx.local_device;
            state->session_key = ctx.session_key;
            should_init = true;
        }
    }

    if (should_init) {
        Status status;
        NcclWindowDesc request;
        request.session_key = ctx.session_key;
        request.window_key = ctx.window_key;
        request.addr = ctx.target_base;
        request.length = ctx.target_length;
        request.device_index = ctx.remote_device;
        request.win_flags = NCCL_WIN_DEFAULT;
        request.allocate_local = false;
        NcclWindowDesc response;
        status = ControlClient::registerNcclWindow(ctx.remote_rpc_addr,
                                                   request, response);

        int previous_device = 0;
        bool device_changed = false;
        if (status.ok()) {
            status = setCudaDevice(ctx.local_device, previous_device);
            device_changed = status.ok();
        }
        if (status.ok()) {
            status = ncclStatus(ncclMemAlloc(&state->local_buffer,
                                             ctx.target_length),
                                "ncclMemAlloc(window dummy)");
            state->owns_local_buffer = status.ok();
        }
        if (status.ok()) {
            status = ncclStatus(
                ncclCommWindowRegister(comm_state->comm, state->local_buffer,
                                       ctx.target_length, &state->window,
                                       NCCL_WIN_DEFAULT),
                "ncclCommWindowRegister");
        }
        if (device_changed) cudaSetDevice(previous_device);

        {
            std::lock_guard<std::mutex> lock(state->mu);
            state->status = status;
            state->ready = status.ok();
            state->initializing = false;
        }
        state->cv.notify_all();
    }

    std::unique_lock<std::mutex> lock(state->mu);
    state->cv.wait(lock, [&] { return state->ready || !state->status.ok(); });
    return state->status;
}

Status NcclTransport::ensureSourceWindow(
    const TransferContext& ctx, const std::shared_ptr<CommState>& comm_state,
    std::shared_ptr<WindowState>& state) {
    bool should_init = false;
    {
        std::lock_guard<std::mutex> lock(window_mutex_);
        auto& entry = windows_[ctx.source_window_key];
        if (!entry) entry = std::make_shared<WindowState>();
        state = entry;
        std::lock_guard<std::mutex> state_lock(state->mu);
        if (!state->ready && !state->initializing) {
            state->initializing = true;
            state->length = ctx.source_length;
            state->device_index = ctx.local_device;
            state->session_key = ctx.session_key;
            should_init = true;
        }
    }

    if (should_init) {
        Status status;
        NcclWindowDesc request;
        request.session_key = ctx.session_key;
        request.window_key = ctx.source_window_key;
        request.length = ctx.source_length;
        request.device_index = ctx.remote_device;
        request.win_flags = NCCL_WIN_COLL_SYMMETRIC;
        request.allocate_local = true;
        NcclWindowDesc response;
        status = ControlClient::registerNcclWindow(ctx.remote_rpc_addr,
                                                   request, response);

        int previous_device = 0;
        bool device_changed = false;
        if (status.ok()) {
            status = setCudaDevice(ctx.local_device, previous_device);
            device_changed = status.ok();
        }
        if (status.ok()) {
            status = ncclStatus(
                ncclCommWindowRegister(
                    comm_state->comm, reinterpret_cast<void*>(ctx.source_base),
                    ctx.source_length, &state->window,
                    NCCL_WIN_COLL_SYMMETRIC),
                "ncclCommWindowRegister(source)");
        }
        if (device_changed) cudaSetDevice(previous_device);

        {
            std::lock_guard<std::mutex> lock(state->mu);
            state->status = status;
            state->ready = status.ok();
            state->initializing = false;
        }
        state->cv.notify_all();
    }

    std::unique_lock<std::mutex> lock(state->mu);
    state->cv.wait(lock, [&] { return state->ready || !state->status.ok(); });
    return state->status;
}

Status NcclTransport::postRemoteWaitSignal(const TransferContext& ctx) {
    NcclSignalDesc request;
    request.session_key = ctx.session_key;
    request.peer = 0;
    request.op_count = 1;
    request.signal_index = 0;
    request.context = 0;
    request.device_index = ctx.remote_device;
    NcclSignalDesc response;
    return ControlClient::waitNcclSignal(ctx.remote_rpc_addr, request,
                                         response);
}

Status NcclTransport::onBootstrapNccl(const NcclBootstrapDesc& request,
                                      NcclBootstrapDesc& response) {
    response.session_key = request.session_key;
    response.unique_id = request.unique_id;
    response.device_index = request.device_index;

    bool should_init = false;
    std::shared_ptr<CommState> state;
    {
        std::lock_guard<std::mutex> lock(comm_mutex_);
        auto& entry = comms_[request.session_key];
        if (!entry) entry = std::make_shared<CommState>();
        state = entry;
        std::lock_guard<std::mutex> state_lock(state->mu);
        if (state->ready || state->initializing) return Status::OK();
        state->initializing = true;
        state->device_index = request.device_index;
        state->local_rank = 1;
        state->peer_rank = 0;
        should_init = true;
    }

    if (!should_init) return Status::OK();

    startBackground([state, request]() {
        Status status;
        ncclUniqueId unique_id;
        status = deserializeUniqueId(request.unique_id, unique_id);
        int previous_device = 0;
        bool device_changed = false;
        if (status.ok()) {
            status = setCudaDevice(request.device_index, previous_device);
            device_changed = status.ok();
        }
        if (status.ok()) {
            status = ncclStatus(ncclCommInitRank(&state->comm, 2, unique_id, 1),
                                "ncclCommInitRank(remote)");
        }
        if (device_changed) cudaSetDevice(previous_device);
        {
            std::lock_guard<std::mutex> lock(state->mu);
            state->status = status;
            state->ready = status.ok();
            state->initializing = false;
        }
        state->cv.notify_all();
    });
    return Status::OK();
}

Status NcclTransport::onRegisterNcclWindow(const NcclWindowDesc& request,
                                           NcclWindowDesc& response) {
    response.session_key = request.session_key;
    response.window_key = request.window_key;
    response.addr = request.addr;
    response.length = request.length;
    response.device_index = request.device_index;
    response.win_flags = request.win_flags;
    response.allocate_local = request.allocate_local;

    bool should_init = false;
    std::shared_ptr<WindowState> state;
    {
        std::lock_guard<std::mutex> lock(window_mutex_);
        auto& entry = windows_[request.window_key];
        if (!entry) entry = std::make_shared<WindowState>();
        state = entry;
        std::lock_guard<std::mutex> state_lock(state->mu);
        if (state->ready || state->initializing) return Status::OK();
        state->initializing = true;
        state->length = request.length;
        state->device_index = request.device_index;
        state->session_key = request.session_key;
        should_init = true;
    }

    if (!should_init) return Status::OK();

    startBackground([this, state, request]() {
        std::shared_ptr<CommState> comm_state;
        Status status = waitForComm(request.session_key, comm_state);
        int previous_device = 0;
        bool device_changed = false;
        if (status.ok()) {
            status = setCudaDevice(request.device_index, previous_device);
            device_changed = status.ok();
        }
        if (status.ok() && request.allocate_local) {
            status = ncclStatus(ncclMemAlloc(&state->local_buffer,
                                             request.length),
                                "ncclMemAlloc(remote window dummy)");
            state->owns_local_buffer = status.ok();
        }
        if (status.ok()) {
            void* buffer = request.allocate_local
                               ? state->local_buffer
                               : reinterpret_cast<void*>(request.addr);
            status = ncclStatus(
                ncclCommWindowRegister(comm_state->comm, buffer,
                                       request.length, &state->window,
                                       request.win_flags),
                "ncclCommWindowRegister(remote)");
        }
        if (device_changed) cudaSetDevice(previous_device);
        {
            std::lock_guard<std::mutex> lock(state->mu);
            state->status = status;
            state->ready = status.ok();
            state->initializing = false;
        }
        state->cv.notify_all();
    });
    return Status::OK();
}

Status NcclTransport::onWaitNcclSignal(const NcclSignalDesc& request,
                                       NcclSignalDesc& response) {
    response.session_key = request.session_key;
    response.peer = request.peer;
    response.op_count = request.op_count;
    response.signal_index = request.signal_index;
    response.context = request.context;
    response.device_index = request.device_index;

    startBackground([this, request]() {
        std::shared_ptr<CommState> comm_state;
        Status status = waitForComm(request.session_key, comm_state);
        int previous_device = 0;
        bool device_changed = false;
        if (status.ok()) {
            status = setCudaDevice(request.device_index, previous_device);
            device_changed = status.ok();
        }
        if (status.ok()) {
            ncclWaitSignalDesc_t desc{request.op_count, request.peer,
                                      request.signal_index, request.context};
            status = ncclStatus(ncclWaitSignal(1, &desc, comm_state->comm,
                                               nullptr),
                                "ncclWaitSignal(remote)");
        }
        if (device_changed) cudaSetDevice(previous_device);
        if (!status.ok()) {
            LOG(WARNING) << "NCCL remote wait-signal failed: "
                         << status.ToString();
        }
    });
    return Status::OK();
}

Status NcclTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto nccl_batch = dynamic_cast<NcclSubBatch*>(batch);
    if (!nccl_batch)
        return Status::InvalidArgument("Invalid NCCL sub-batch" LOC_MARK);
    if (request_list.size() + nccl_batch->task_list.size() >
        nccl_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    for (const auto& request : request_list) {
        nccl_batch->task_list.emplace_back();
        auto& task = nccl_batch->task_list.back();
        task.request = request;
        task.status_word.store(TransferStatusEnum::PENDING,
                               std::memory_order_release);

        if (request.opcode == Request::READ) {
            CHECK_STATUS(markFailed(
                task, "READ needs device-side ncclGinGet or delegated put"));
            continue;
        }

        TransferContext ctx;
        auto status = buildTransferContext(request, ctx);
        std::shared_ptr<CommState> comm_state;
        if (status.ok()) status = ensureComm(ctx, comm_state);
        std::shared_ptr<WindowState> window_state;
        if (status.ok()) status = ensureWindow(ctx, comm_state, window_state);
        std::shared_ptr<WindowState> source_window_state;
        if (status.ok()) {
            status = ensureSourceWindow(ctx, comm_state, source_window_state);
        }
        if (status.ok()) status = postRemoteWaitSignal(ctx);
        if (status.ok()) {
            int previous_device = 0;
            status = setCudaDevice(ctx.local_device, previous_device);
            bool device_changed = status.ok();
            if (status.ok()) {
                status = ncclStatus(
                    ncclPutSignal(request.source, request.length, ncclUint8,
                                  comm_state->peer_rank, window_state->window,
                                  ctx.target_offset, 0, 0, 0,
                                  comm_state->comm,
                                  nccl_batch->stream.get()),
                    "ncclPutSignal");
                if (status.ok()) {
                    auto err = cudaEventCreateWithFlags(
                        &task.completion_event, cudaEventDisableTiming);
                    if (err != cudaSuccess) {
                        status = Status::InternalError(
                            std::string("cudaEventCreateWithFlags: ") +
                            cudaGetErrorString(err) + LOC_MARK);
                    }
                }
                if (status.ok()) {
                    auto err = cudaEventRecord(task.completion_event,
                                               nccl_batch->stream.get());
                    if (err != cudaSuccess) {
                        status = Status::InternalError(
                            std::string("cudaEventRecord: ") +
                            cudaGetErrorString(err) + LOC_MARK);
                    }
                }
            }
            if (device_changed) cudaSetDevice(previous_device);
        }

        if (!status.ok()) CHECK_STATUS(markFailed(task, status.ToString()));
    }
    return Status::OK();
}

Status NcclTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                        TransferStatus& status) {
    auto nccl_batch = dynamic_cast<NcclSubBatch*>(batch);
    if (!nccl_batch)
        return Status::InvalidArgument("Invalid NCCL sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= (int)nccl_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }

    auto& task = nccl_batch->task_list[task_id];
    auto current = task.status_word.load(std::memory_order_acquire);
    if (current == TransferStatusEnum::PENDING && task.completion_event) {
        auto err = cudaEventQuery(task.completion_event);
        if (err == cudaSuccess) {
            task.transferred_bytes.store(task.request.length,
                                         std::memory_order_release);
            task.status_word.store(TransferStatusEnum::COMPLETED,
                                   std::memory_order_release);
            current = TransferStatusEnum::COMPLETED;
        } else if (err != cudaErrorNotReady) {
            task.status_word.store(TransferStatusEnum::FAILED,
                                   std::memory_order_release);
            current = TransferStatusEnum::FAILED;
        }
    }

    status.s = current;
    status.transferred_bytes =
        task.transferred_bytes.load(std::memory_order_acquire);
    return Status::OK();
}

Status NcclTransport::allocateLocalMemory(void** addr, size_t size,
                                          MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() != "cuda") {
        return Status::InvalidArgument(
            "NCCL transport only allocates CUDA memory" LOC_MARK);
    }

    int previous_device = 0;
    CHECK_CUDA(cudaGetDevice(&previous_device));
    CHECK_CUDA(cudaSetDevice(location.index()));
    auto status = ncclStatus(ncclMemAlloc(addr, size), "ncclMemAlloc");
    cudaSetDevice(previous_device);
    if (!status.ok()) return status;

    std::lock_guard<std::mutex> lock(allocation_mutex_);
    nccl_allocations_.insert(reinterpret_cast<uint64_t>(*addr));
    return Status::OK();
}

Status NcclTransport::freeLocalMemory(void* addr, size_t size) {
    {
        std::lock_guard<std::mutex> lock(allocation_mutex_);
        auto it = nccl_allocations_.find(reinterpret_cast<uint64_t>(addr));
        if (it == nccl_allocations_.end()) {
            return Platform::getLoader().free(addr, size);
        }
        nccl_allocations_.erase(it);
    }
    CHECK_NCCL(ncclMemFree(addr));
    return Status::OK();
}

bool NcclTransport::isNcclAllocated(uint64_t addr) const {
    std::lock_guard<std::mutex> lock(allocation_mutex_);
    return nccl_allocations_.count(addr) != 0;
}

bool NcclTransport::isCudaLocation(const std::string& location) const {
    return LocationParser(location).type() == "cuda";
}

Status NcclTransport::addMemoryBuffer(BufferDesc& desc,
                                      const MemoryOptions& options) {
    if (!isCudaLocation(desc.location)) return Status::OK();

    const bool nccl_allocated = isNcclAllocated(desc.addr);
    if (!nccl_allocated && !allow_external_window_buffers_) {
        if (options.type == NCCL) {
            return Status::InvalidArgument(
                "NCCL host RMA requires ncclMemAlloc/VMM-compatible CUDA "
                "buffers; set transports/nccl/allow_external_window_buffers "
                "only if the caller guarantees window registration support" LOC_MARK);
        }
        return Status::OK();
    }

    desc.transports.push_back(TransportType::NCCL);
    desc.transport_attrs[TransportType::NCCL] =
        nccl_allocated ? "allocator=ncclMemAlloc;window=deferred"
                       : "allocator=external;window=deferred";
    return Status::OK();
}

Status NcclTransport::removeMemoryBuffer(BufferDesc& desc) {
    desc.transport_attrs.erase(TransportType::NCCL);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
