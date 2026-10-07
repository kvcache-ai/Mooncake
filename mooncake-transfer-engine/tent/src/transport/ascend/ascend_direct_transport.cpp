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

#include "tent/transport/ascend/ascend_direct_transport.h"
#include "tent/transport/ascend/hixl_engine.h"
#include "tent/transport/ascend/local_copy_engine.h"
#include "tent/transport/ascend/resource_config.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <utility>
#include <variant>

#include <acl/acl.h>
#include <glog/logging.h>

#include "tent/common/status.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/slab.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

constexpr int64_t kMillisToNano = 1000000;
constexpr int kMaxInflightReqs = 100;

bool IsHostMem(const std::string& location, void* addr) {
    if (location.rfind("cpu", 0) == 0) {
        return true;
    }
    if (location.rfind("npu", 0) == 0) {
        return false;
    }
    aclrtPtrAttributes attributes{};
    if (aclrtPointerGetAttributes(addr, &attributes) != ACL_SUCCESS) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed, errmsg: "
                   << aclGetRecentErrMsg();
        return true;
    }
    return attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST;
}

TransferStatusEnum FromHixlState(HixlXferState state) {
    switch (state) {
        case HixlXferState::Completed:
            return TransferStatusEnum::COMPLETED;
        case HixlXferState::Timeout:
            return TransferStatusEnum::TIMEOUT;
        case HixlXferState::Failed:
            return TransferStatusEnum::FAILED;
        case HixlXferState::Waiting:
        default:
            return TransferStatusEnum::PENDING;
    }
}

}  // namespace

AscendDirectTransport::AscendDirectTransport() = default;

AscendDirectTransport::~AscendDirectTransport() { (void)uninstall(); }

Status AscendDirectTransport::install(std::string& local_segment_name,
                                      std::shared_ptr<ControlService> metadata,
                                      std::shared_ptr<Topology> local_topology,
                                      std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "AscendDirectTransport has been installed" LOC_MARK);
    }
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    options_ = LoadAscendDirectOptions(conf_);
    init_options_ = BuildHixlInitOptions(options_);
    transfer_timeout_ns_ =
        static_cast<int64_t>(options_.transfer_timeout_ms) * kMillisToNano;
    caps.dram_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;
    auto status = initEngines();
    if (!status.ok()) {
        finalizeEngines();
        return status;
    }
    status = publishLocalEngines();
    if (!status.ok()) {
        finalizeEngines();
        return status;
    }
    installed_ = true;
    LOG(INFO) << "Installed AscendDirectTransport engines=" << engines_.size()
              << " agent_mode=" << options_.agent_mode
              << " fabric_mem=" << options_.use_fabric_mem
              << " roce_mode=" << options_.roce_mode
              << " timeout_ms=" << options_.transfer_timeout_ms;
    return Status::OK();
}

Status AscendDirectTransport::initEngines() {
    // A failed install may be retried. Release any engines left by that
    // attempt before rebuilding the per-device set.
    finalizeEngines();
    aclrtContext saved = nullptr;
    if (aclrtGetCurrentContext(&saved) != ACL_ERROR_NONE) {
        return Status::InternalError(
            "Get device context failed, device may be not set." LOC_MARK);
    }

    std::vector<std::pair<int32_t, aclrtContext>> devices;
    if (options_.agent_mode) {
        uint32_t device_count = 0;
        if (aclrtGetDeviceCount(&device_count) != ACL_ERROR_NONE ||
            device_count == 0) {
            return Status::InternalError(
                "aclrtGetDeviceCount failed in agent mode" LOC_MARK);
        }
        for (uint32_t i = 0; i < device_count; ++i) {
            auto device_id = static_cast<int32_t>(i);
            if (aclrtSetDevice(device_id) != ACL_ERROR_NONE) {
                LOG(ERROR) << "aclrtSetDevice failed for device " << device_id
                           << ", errmsg: " << aclGetRecentErrMsg();
                (void)aclrtSetCurrentContext(saved);
                return Status::InternalError("aclrtSetDevice failed" LOC_MARK);
            }
            aclrtContext context = nullptr;
            if (aclrtGetCurrentContext(&context) != ACL_ERROR_NONE) {
                (void)aclrtSetCurrentContext(saved);
                return Status::InternalError(
                    "aclrtGetCurrentContext failed" LOC_MARK);
            }
            devices.emplace_back(device_id, context);
        }
        (void)aclrtSetCurrentContext(saved);
    } else {
        int32_t device_id = 0;
        if (aclrtGetDevice(&device_id) != ACL_ERROR_NONE) {
            return Status::InvalidArgument(
                "Get device id failed, device id may be not set." LOC_MARK);
        }
        devices.emplace_back(device_id, saved);
    }

    const auto host_ip = HostIpFromSegmentName(local_segment_name_);
    for (const auto& [device_id, context] : devices) {
        auto port = FindHixlListenPort(options_.base_port, device_id);
        if (port == 0) {
            return Status::InternalError("Find available port failed" LOC_MARK);
        }
        auto engine = std::make_unique<HixlEngine>();
        const auto name = MakeHixlEngineName(host_ip, port);
        auto init = init_options_;
        if (options_.roce_mode) {
            const auto roce_listen = HixlRoceListenPort(port);
            init = WithHixlListenPort(init_options_, roce_listen);
            LOG(INFO) << "HIXL engine " << name
                      << " roce listen_port=" << roce_listen;
        } else {
            LOG(INFO) << "HIXL engine " << name;
        }
        auto status = engine->initialize(name, context, device_id, init);
        if (!status.ok()) {
            engine->finalize();
            return status;
        }
        auto local_copy = std::make_unique<LocalCopyEngine>();
        auto copy_status = local_copy->initialize(engine->context(), device_id);
        if (!copy_status.ok()) {
            engine->finalize();
            return copy_status;
        }
        engines_.push_back(std::move(engine));
        local_copies_.push_back(std::move(local_copy));
    }
    return Status::OK();
}

void AscendDirectTransport::finalizeEngines() {
    // Local copy streams may still have ACL work queued, so tear them down
    // before dropping the HIXL engines and their contexts.
    for (auto& local_copy : local_copies_) {
        if (local_copy) {
            local_copy->finalize();
        }
    }
    local_copies_.clear();
    for (auto& engine : engines_) {
        if (engine) {
            engine->finalize();
        }
    }
    engines_.clear();
}

Status AscendDirectTransport::publishLocalEngines() {
    if (!metadata_) {
        return Status::InvalidArgument("metadata is null" LOC_MARK);
    }
    nlohmann::json names = nlohmann::json::array();
    for (const auto& engine : engines_) {
        names.push_back(engine->name());
    }
    return metadata_->segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            if (!std::holds_alternative<MemorySegmentDesc>(segment.detail)) {
                segment.detail = MemorySegmentDesc{};
            }
            auto& detail = std::get<MemorySegmentDesc>(segment.detail);
            if (!names.empty()) {
                detail.device_attrs[kHixlNameAttr] =
                    names.front().get<std::string>();
            }
            detail.device_attrs[kHixlNamesAttr] = names.dump();
            return Status::OK();
        });
}

size_t AscendDirectTransport::currentEngineIndex() const {
    if (engines_.size() <= 1) {
        return 0;
    }
    int32_t device_id = 0;
    if (aclrtGetDevice(&device_id) != ACL_ERROR_NONE) {
        return 0;
    }
    auto idx = static_cast<size_t>(device_id);
    if (idx >= engines_.size()) {
        return 0;
    }
    return idx;
}

Status AscendDirectTransport::uninstall() {
    (void)quiesce();
    finalizeEngines();
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        groups_.clear();
        route_groups_.clear();
    }
    inflight_.store(0, std::memory_order_release);
    inflight_cv_.notify_all();
    installed_ = false;
    metadata_.reset();
    return Status::OK();
}

Status AscendDirectTransport::quiesce() {
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(options_.transfer_timeout_ms);
    std::unique_lock<std::mutex> lock(inflight_mu_);
    while (inflight_.load(std::memory_order_acquire) > 0) {
        if (inflight_cv_.wait_until(lock, deadline) ==
            std::cv_status::timeout) {
            LOG(WARNING) << "AscendDirectTransport quiesce timed out with "
                         << inflight_.load() << " in-flight requests";
            break;
        }
    }
    return Status::OK();
}

Status AscendDirectTransport::allocateSubBatch(SubBatchRef& batch,
                                               size_t max_size) {
    auto* hixl_batch = Slab<HixlSubBatch>::Get().allocate();
    if (!hixl_batch) {
        return Status::InternalError("Unable to allocate HIXL sub-batch");
    }
    hixl_batch->task_list.clear();
    hixl_batch->task_list.reserve(max_size);
    hixl_batch->max_size = max_size;
    batch = hixl_batch;
    return Status::OK();
}

Status AscendDirectTransport::freeSubBatch(SubBatchRef& batch) {
    auto* hixl_batch = dynamic_cast<HixlSubBatch*>(batch);
    if (!hixl_batch) {
        return Status::InvalidArgument("Invalid HIXL sub-batch" LOC_MARK);
    }
    Slab<HixlSubBatch>::Get().deallocate(hixl_batch);
    batch = nullptr;
    return Status::OK();
}

Status AscendDirectTransport::resolveRequest(const Request& request,
                                             PreparedRequest& prepared) const {
    if (engines_.empty()) {
        return Status::InternalError(
            "HIXL engines are not initialized" LOC_MARK);
    }
    if (!metadata_) {
        return Status::InvalidArgument("metadata is null" LOC_MARK);
    }
    SegmentDescRef pin;
    SegmentDesc* desc = nullptr;
    auto status = metadata_->segmentManager().withCachedSegment(
        request.target_id, pin, [&](SegmentDesc* cached) {
            desc = cached;
            return Status::OK();
        });
    if (!status.ok() || desc == nullptr) {
        return Status::InvalidArgument("Cannot find target segment" LOC_MARK);
    }
    if (!std::holds_alternative<MemorySegmentDesc>(desc->detail)) {
        return Status::InvalidArgument(
            "Target is not a memory segment" LOC_MARK);
    }
    const auto& detail = std::get<MemorySegmentDesc>(desc->detail);
    prepared.request = request;
    prepared.local_engine_idx = currentEngineIndex();
    prepared.remote_hixl = ResolveRemoteHixlName(detail, request.target_offset);
    if (prepared.remote_hixl.empty()) {
        return Status::InvalidArgument("Missing remote hixl_name" LOC_MARK);
    }
    if (request.target_id != LOCAL_SEGMENT_ID &&
        prepared.local_engine_idx < engines_.size() &&
        engines_[prepared.local_engine_idx] &&
        prepared.remote_hixl == engines_[prepared.local_engine_idx]->name()) {
        LOG(WARNING) << "Remote segment id=" << request.target_id
                     << " resolved to local HIXL name " << prepared.remote_hixl;
    }
    return Status::OK();
}

Status AscendDirectTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* hixl_batch = dynamic_cast<HixlSubBatch*>(batch);
    if (!hixl_batch) {
        return Status::InvalidArgument("Invalid HIXL sub-batch" LOC_MARK);
    }
    if (request_list.size() + hixl_batch->task_list.size() >
        hixl_batch->max_size) {
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    }

    std::vector<PreparedRequest> prepared;
    prepared.reserve(request_list.size());
    for (const auto& request : request_list) {
        PreparedRequest item;
        CHECK_STATUS(resolveRequest(request, item));
        prepared.push_back(std::move(item));
    }

    const size_t start = hixl_batch->task_list.size();
    hixl_batch->task_list.resize(start + prepared.size());
    using GroupKey = std::pair<size_t, std::pair<std::string, bool>>;
    std::map<GroupKey, std::vector<HixlTask*>> groups;
    for (size_t i = 0; i < prepared.size(); ++i) {
        auto& task = hixl_batch->task_list[start + i];
        task.request = prepared[i].request;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
        task.req_handle = nullptr;
        task.local_engine_idx = prepared[i].local_engine_idx;
        task.remote_hixl = prepared[i].remote_hixl;
        const bool write = task.request.opcode == Request::WRITE;
        groups[{task.local_engine_idx, {task.remote_hixl, write}}].push_back(
            &task);
    }
    for (auto& [key, tasks] : groups) {
        startGroup(key.second.first, key.first, key.second.second, tasks);
    }
    return Status::OK();
}

void AscendDirectTransport::startGroup(const std::string& remote_hixl,
                                       size_t local_engine_idx, bool write,
                                       const std::vector<HixlTask*>& tasks) {
    auto fail_all = [&]() {
        for (auto* task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
        }
    };
    if (local_engine_idx >= engines_.size() || !engines_[local_engine_idx]) {
        fail_all();
        return;
    }
    if (remote_hixl == engines_[local_engine_idx]->name()) {
        startLocalCopy(local_engine_idx, write, tasks);
        return;
    }

    {
        std::unique_lock<std::mutex> lock(inflight_mu_);
        const auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(options_.transfer_timeout_ms);
        while (inflight_.load(std::memory_order_acquire) >= kMaxInflightReqs) {
            if (inflight_cv_.wait_until(lock, deadline) ==
                std::cv_status::timeout) {
                fail_all();
                return;
            }
        }
        inflight_.fetch_add(1, std::memory_order_acq_rel);
    }

    std::vector<HixlOpDesc> op_descs;
    op_descs.reserve(tasks.size());
    for (auto* task : tasks) {
        HixlOpDesc desc{};
        desc.local_addr = reinterpret_cast<uintptr_t>(task->request.source);
        desc.remote_addr = task->request.target_offset;
        desc.len = task->request.length;
        op_descs.push_back(desc);
    }
    void* req = nullptr;
    auto status = engines_[local_engine_idx]->transferAsync(remote_hixl, write,
                                                            op_descs, req);
    if (!status.ok() || req == nullptr) {
        releaseInflight();
        // AutoConnect already tore down the failed route inside HIXL.
        fail_all();
        return;
    }
    const auto now = getCurrentTimeInNano();
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        GroupState group;
        group.status = TransferStatusEnum::PENDING;
        group.remaining = static_cast<int>(tasks.size());
        group.engine_idx = local_engine_idx;
        group.remote = remote_hixl;
        group.counted = true;
        groups_[req] = std::move(group);
        route_groups_[{local_engine_idx, remote_hixl}].push_back(req);
    }
    for (auto* task : tasks) {
        task->req_handle = req;
        task->batch_size = tasks.size();
        task->start_time_ns = now;
        task->status_word = TransferStatusEnum::PENDING;
    }
}

void AscendDirectTransport::markGroupHandles(const std::vector<void*>& handles,
                                             TransferStatusEnum status) {
    int inflight_dec = 0;
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        for (void* handle : handles) {
            auto git = groups_.find(handle);
            if (git == groups_.end()) {
                continue;
            }
            if (git->second.status == TransferStatusEnum::PENDING) {
                git->second.status = status;
            }
            if (git->second.counted) {
                git->second.counted = false;
                inflight_dec++;
            }
        }
    }
    releaseInflight(inflight_dec);
}

void AscendDirectTransport::failLocalCopyStream(size_t engine_idx,
                                                TransferStatusEnum status) {
    if (engine_idx >= local_copies_.size() || !local_copies_[engine_idx]) {
        return;
    }
    auto handles = local_copies_[engine_idx]->failAndRecreate();
    markGroupHandles(handles, status);
}

void AscendDirectTransport::startLocalCopy(
    size_t local_engine_idx, bool write, const std::vector<HixlTask*>& tasks) {
    auto fail_all = [&]() {
        for (auto* task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
            task->local_copy = true;
        }
    };
    if (local_engine_idx >= local_copies_.size() ||
        !local_copies_[local_engine_idx]) {
        fail_all();
        return;
    }

    {
        std::unique_lock<std::mutex> lock(inflight_mu_);
        const auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(options_.transfer_timeout_ms);
        while (inflight_.load(std::memory_order_acquire) >= kMaxInflightReqs) {
            if (inflight_cv_.wait_until(lock, deadline) ==
                std::cv_status::timeout) {
                fail_all();
                return;
            }
        }
        inflight_.fetch_add(1, std::memory_order_acq_rel);
    }

    std::vector<HixlOpDesc> op_descs;
    op_descs.reserve(tasks.size());
    for (auto* task : tasks) {
        HixlOpDesc desc{};
        desc.local_addr = reinterpret_cast<uintptr_t>(task->request.source);
        desc.remote_addr = task->request.target_offset;
        desc.len = task->request.length;
        op_descs.push_back(desc);
    }

    void* req = nullptr;
    std::vector<void*> failed_handles;
    auto status = local_copies_[local_engine_idx]->submit(write, op_descs, req,
                                                          failed_handles);
    if (!failed_handles.empty()) {
        markGroupHandles(failed_handles, TransferStatusEnum::FAILED);
    }
    if (!status.ok() || req == nullptr) {
        releaseInflight();
        fail_all();
        return;
    }

    const auto now = getCurrentTimeInNano();
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        GroupState group;
        group.status = TransferStatusEnum::PENDING;
        group.remaining = static_cast<int>(tasks.size());
        group.engine_idx = local_engine_idx;
        group.remote = engines_[local_engine_idx]->name();
        group.counted = true;
        group.local_copy = true;
        groups_[req] = std::move(group);
    }
    for (auto* task : tasks) {
        task->req_handle = req;
        task->batch_size = tasks.size();
        task->start_time_ns = now;
        task->status_word = TransferStatusEnum::PENDING;
        task->local_copy = true;
        task->local_engine_idx = local_engine_idx;
    }
}

void AscendDirectTransport::releaseInflight(int n) {
    if (n <= 0) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(inflight_mu_);
        const int cur = inflight_.load(std::memory_order_acquire);
        inflight_.store(std::max(0, cur - n), std::memory_order_release);
    }
    inflight_cv_.notify_all();
}

bool AscendDirectTransport::applyGroupStatus(HixlTask& task,
                                             GroupState& group) {
    task.status_word = group.status;
    if (group.status == TransferStatusEnum::COMPLETED) {
        task.transferred_bytes = task.request.length;
    }
    if (group.remaining > 0) {
        group.remaining--;
    }
    if (group.counted && group.status != TransferStatusEnum::PENDING) {
        group.counted = false;
        return true;
    }
    return false;
}

void AscendDirectTransport::failEntireRoute(size_t engine_idx,
                                            const std::string& remote,
                                            TransferStatusEnum status,
                                            bool disconnect) {
    int inflight_dec = 0;
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        auto it = route_groups_.find({engine_idx, remote});
        std::vector<void*> handles;
        if (it != route_groups_.end()) {
            handles = it->second;
        }
        for (void* handle : handles) {
            auto git = groups_.find(handle);
            if (git == groups_.end()) {
                continue;
            }
            if (git->second.status == TransferStatusEnum::PENDING) {
                git->second.status = status;
            }
            if (git->second.counted) {
                git->second.counted = false;
                inflight_dec++;
            }
        }
    }
    // Never acquire inflight_mu_ while holding req_mutex_: submit waits on
    // inflight_mu_ and later takes req_mutex_.
    releaseInflight(inflight_dec);
    if (disconnect && engine_idx < engines_.size() && engines_[engine_idx]) {
        (void)engines_[engine_idx]->disconnectOnTimeout(
            remote, kAscendTimeoutDisconnectMs);
    }
}

bool AscendDirectTransport::finishTaskLocked(HixlTask& task,
                                             void** local_release_handle,
                                             size_t* local_release_engine) {
    auto git = groups_.find(task.req_handle);
    if (git == groups_.end()) {
        return false;
    }
    const bool release = applyGroupStatus(task, git->second);
    if (git->second.remaining > 0) {
        return release;
    }
    auto route_it =
        route_groups_.find({git->second.engine_idx, git->second.remote});
    if (route_it != route_groups_.end()) {
        auto& handles = route_it->second;
        handles.erase(
            std::remove(handles.begin(), handles.end(), task.req_handle),
            handles.end());
        if (handles.empty()) {
            route_groups_.erase(route_it);
        }
    }
    if (local_release_handle != nullptr && local_release_engine != nullptr &&
        git->second.local_copy) {
        *local_release_handle = task.req_handle;
        *local_release_engine = git->second.engine_idx;
    }
    groups_.erase(git);
    return release;
}

void AscendDirectTransport::completePolledTask(HixlTask& task) {
    bool release = false;
    void* local_handle = nullptr;
    size_t local_engine = 0;
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        auto git = groups_.find(task.req_handle);
        if (git != groups_.end() &&
            git->second.status != TransferStatusEnum::PENDING) {
            release = finishTaskLocked(task, &local_handle, &local_engine);
        }
    }
    // Release host flags outside req_mutex_ so timeout rebuild
    // (LocalCopyEngine mutex -> req_mutex_) cannot deadlock.
    if (local_handle != nullptr && local_engine < local_copies_.size() &&
        local_copies_[local_engine]) {
        local_copies_[local_engine]->release(local_handle);
    }
    if (release) {
        releaseInflight();
    }
}

Status AscendDirectTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                                TransferStatus& status) {
    auto* hixl_batch = dynamic_cast<HixlSubBatch*>(batch);
    if (!hixl_batch) {
        return Status::InvalidArgument("Invalid HIXL sub-batch" LOC_MARK);
    }
    if (task_id < 0 ||
        task_id >= static_cast<int>(hixl_batch->task_list.size())) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto& task = hixl_batch->task_list[task_id];
    if (task.status_word != TransferStatusEnum::PENDING) {
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }
    if (task.req_handle == nullptr) {
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    completePolledTask(task);
    if (task.status_word != TransferStatusEnum::PENDING) {
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    const auto now = getCurrentTimeInNano();
    if (now >= 0 && task.start_time_ns >= 0 &&
        (now - task.start_time_ns) > transfer_timeout_ns_) {
        if (task.local_copy) {
            failLocalCopyStream(task.local_engine_idx,
                                TransferStatusEnum::TIMEOUT);
        } else {
            failEntireRoute(task.local_engine_idx, task.remote_hixl,
                            TransferStatusEnum::TIMEOUT, /*disconnect=*/true);
        }
        completePolledTask(task);
        if (task.status_word == TransferStatusEnum::PENDING) {
            task.status_word = TransferStatusEnum::TIMEOUT;
        }
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    if (task.local_copy) {
        if (task.local_engine_idx >= local_copies_.size() ||
            !local_copies_[task.local_engine_idx]) {
            failLocalCopyStream(task.local_engine_idx,
                                TransferStatusEnum::FAILED);
        } else {
            const auto mapped =
                local_copies_[task.local_engine_idx]->poll(task.req_handle);
            if (mapped == TransferStatusEnum::COMPLETED ||
                mapped == TransferStatusEnum::FAILED) {
                std::lock_guard<std::mutex> lock(req_mutex_);
                auto git = groups_.find(task.req_handle);
                if (git != groups_.end() &&
                    git->second.status == TransferStatusEnum::PENDING) {
                    git->second.status = mapped;
                }
            }
        }
        completePolledTask(task);
        if (task.status_word == TransferStatusEnum::PENDING &&
            (task.local_engine_idx >= local_copies_.size() ||
             !local_copies_[task.local_engine_idx])) {
            task.status_word = TransferStatusEnum::FAILED;
        }
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    if (task.local_engine_idx >= engines_.size() ||
        !engines_[task.local_engine_idx]) {
        failEntireRoute(task.local_engine_idx, task.remote_hixl,
                        TransferStatusEnum::FAILED, /*disconnect=*/false);
        completePolledTask(task);
        if (task.status_word == TransferStatusEnum::PENDING) {
            task.status_word = TransferStatusEnum::FAILED;
        }
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    HixlXferState xfer_status = HixlXferState::Waiting;
    auto err = engines_[task.local_engine_idx]->getTransferStatus(
        task.req_handle, xfer_status);
    const auto mapped = FromHixlState(xfer_status);
    if (!err.ok() || mapped == TransferStatusEnum::FAILED) {
        failEntireRoute(task.local_engine_idx, task.remote_hixl,
                        TransferStatusEnum::FAILED, /*disconnect=*/false);
    } else if (mapped == TransferStatusEnum::TIMEOUT) {
        failEntireRoute(task.local_engine_idx, task.remote_hixl,
                        TransferStatusEnum::TIMEOUT, /*disconnect=*/true);
    } else if (mapped == TransferStatusEnum::COMPLETED) {
        std::lock_guard<std::mutex> lock(req_mutex_);
        auto git = groups_.find(task.req_handle);
        if (git != groups_.end() &&
            git->second.status == TransferStatusEnum::PENDING) {
            git->second.status = TransferStatusEnum::COMPLETED;
        }
    }

    completePolledTask(task);
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status AscendDirectTransport::addMemoryBuffer(BufferDesc& desc,
                                              const MemoryOptions& options) {
    (void)options;
    if (engines_.empty()) {
        return Status::InternalError(
            "HIXL engines are not initialized" LOC_MARK);
    }
    const size_t engine_idx = currentEngineIndex();
    const bool host =
        IsHostMem(desc.location, reinterpret_cast<void*>(desc.addr));
    if (engine_idx >= engines_.size() || !engines_[engine_idx]) {
        return Status::InternalError("Invalid HIXL engine index" LOC_MARK);
    }
    CHECK_STATUS(engines_[engine_idx]->registerMem(
        reinterpret_cast<void*>(desc.addr), desc.length, host));
    desc.transports.push_back(TransportType::AscendDirect);
    desc.transport_attrs[TransportType::AscendDirect] =
        std::to_string(engine_idx);
    return Status::OK();
}

Status AscendDirectTransport::addMemoryBuffer(
    std::vector<BufferDesc>& desc_list, const MemoryOptions& options) {
    std::vector<void*> registered;
    registered.reserve(desc_list.size());
    for (auto& desc : desc_list) {
        auto status = addMemoryBuffer(desc, options);
        if (!status.ok()) {
            for (void* addr : registered) {
                BufferDesc rollback{};
                rollback.addr = reinterpret_cast<uint64_t>(addr);
                (void)removeMemoryBuffer(rollback);
            }
            return status;
        }
        registered.push_back(reinterpret_cast<void*>(desc.addr));
    }
    return Status::OK();
}

Status AscendDirectTransport::removeMemoryBuffer(BufferDesc& desc) {
    Status first_error = Status::OK();
    for (auto& engine : engines_) {
        if (!engine) {
            continue;
        }
        auto status = engine->deregisterMem(reinterpret_cast<void*>(desc.addr));
        if (!status.ok() && first_error.ok()) {
            first_error = status;
        }
    }
    return first_error;
}

}  // namespace tent
}  // namespace mooncake
