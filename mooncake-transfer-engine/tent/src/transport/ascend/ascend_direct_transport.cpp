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
#include <cstring>
#include <exception>
#include <functional>
#include <map>
#include <thread>
#include <utility>
#include <variant>

#include <pthread.h>

#include <acl/acl.h>
#include <glog/logging.h>

#include "tent/common/status.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/slab.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "ascend_affinity.h"
#ifdef TENT_HAS_ASCEND_ALLOCATOR
#include "ascend_allocator.h"
#endif

namespace mooncake {
namespace tent {

namespace {

bool EnvFlagEnabled(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return false;
    }
    std::string text(value);
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return text == "1" || text == "true" || text == "yes" || text == "on";
}

}  // namespace
namespace {

constexpr int64_t kMillisToNano = 1000000;
constexpr int kMaxInflightReqs = 100;
// Monotonic nanoseconds for queued-timeout/cache windows. CLOCK_REALTIME can
// jump, so ages must not be derived from getCurrentTimeInNano().
int64_t SteadyTimeInNano() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Workers that hand remote groups to HIXL, and workers that issue one local
// copy stream each. Both are compile-time constants on purpose so every engine
// gets the same layout regardless of configuration. Eight transfer workers are
// enough to keep several peers' groups in flight while HIXL serializes first
// contact and status queries per peer; a larger pool would only add scheduling
// jitter around a submission call that is mostly a syscall. Revisit the value
// if a measurement shows submit latency dominating.
// One copy worker per stream matches LocalCopyEngine's six streams, so a group
// spread over them issues in parallel instead of serializing inside the pool.
// Keep the two counts in sync.
constexpr size_t kTransferThreadsPerEngine = 8;
constexpr size_t kSingleDeviceTransferThreads = 16;
constexpr size_t kCopyThreadsPerEngine = 6;
// Every task of a remote group shares one HIXL request, and HIXL serializes
// status queries per peer. Querying that request once per task would spend the
// poll loop inside GetTransferStatus instead of observing completion, so a
// group is queried at most once per window and its remaining tasks reuse the
// answer until the window expires.
constexpr int64_t kGroupPollCacheNs = 200000;

// The poller walks every task of a group on the same thread, so remembering
// the last request this thread queried lets the tasks that follow in the same
// window skip the group map and its lock entirely. The timestamps come from
// the monotonic clock, never CLOCK_REALTIME: a wall-clock jump would either
// freeze the cache window or make it expire instantly.
thread_local void* t_last_polled_handle = nullptr;
thread_local int64_t t_last_polled_ns = 0;

// HixlTask's status, byte count and request handle are shared between a
// transfer worker and the poller, and HixlTask must stay movable for
// std::vector, so the fields keep their plain types and every access goes
// through these helpers. Never mix them with plain reads or writes. The status
// is an enum, which the _n forms of the builtins do not accept, so it uses the
// generic (type-agnostic) form of the same primitives.
TransferStatusEnum LoadTaskStatus(HixlTask& task) {
    TransferStatusEnum status;
    __atomic_load(&task.status_word, &status, __ATOMIC_ACQUIRE);
    return status;
}

void StoreTaskStatus(HixlTask& task, TransferStatusEnum status) {
    __atomic_store(&task.status_word, &status, __ATOMIC_RELEASE);
}

size_t LoadTaskBytes(HixlTask& task) {
    return __atomic_load_n(&task.transferred_bytes, __ATOMIC_ACQUIRE);
}

void StoreTaskBytes(HixlTask& task, size_t bytes) {
    __atomic_store_n(&task.transferred_bytes, bytes, __ATOMIC_RELEASE);
}

void* LoadTaskHandle(HixlTask& task) {
    return __atomic_load_n(&task.req_handle, __ATOMIC_ACQUIRE);
}

// Publishes the group handle. Must be the last write to the task fields the
// poller reads after it: the release store makes them visible with it.
void StoreTaskHandle(HixlTask& task, void* handle) {
    __atomic_store_n(&task.req_handle, handle, __ATOMIC_RELEASE);
}

// One CPU set per engine: each NPU's own NUMA node when the process owns
// several NPUs, and the node of its only NPU otherwise. Empty means unpinned.
std::vector<std::vector<int>> EngineCpuSets(
    const std::vector<int32_t>& logic_device_ids) {
    const bool multi_device = logic_device_ids.size() > 1;
    AscendAffinityPlan plan = BuildAscendAffinityPlan(logic_device_ids);
    if (!multi_device) {
        return {plan.shared_cpus};
    }
    return std::move(plan.per_device_cpus);
}

// Allocation runs on the same per-engine transport pools that serve
// transfers, so its first touch follows the same NUMA plan as classic.
// Single-NPU processes publish the one shared pool; multi-NPU processes
// publish one runner per allocator device slot.
AllocationAffinityPools BuildAllocationPools(
    const std::vector<std::vector<int>>& engine_cpus,
    const std::vector<std::unique_ptr<AscendThreadPool>>& pools) {
    AllocationAffinityPools out;
    if (engine_cpus.size() <= 1) {
        if (!engine_cpus.empty() && !engine_cpus.front().empty() &&
            !pools.empty()) {
            out.shared = MakeAllocationRunner(pools.front().get());
        }
        return out;
    }
    out.per_device.reserve(pools.size());
    for (const auto& pool : pools) {
        out.per_device.push_back(MakeAllocationRunner(pool.get()));
    }
    return out;
}

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
    // HIXL only needs MEM_DEVICE for device memory; everything else is
    // host-accessible. Do not test for ACL_MEM_LOCATION_TYPE_HOST: current CANN
    // marks that enum reserved and returns UNREGISTERED/MANAGED/HOST_NUMA for
    // real host buffers, which would otherwise be misregistered as device.
    return attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE;
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
    use_shmem_ = EnvFlagEnabled("ASCEND_USE_SHMEM");
#ifdef TENT_HAS_ASCEND_ALLOCATOR
    // Fallback flags for consumers that load the allocator without a transfer
    // engine (tentpy): a process that links the engine registers a provider
    // reading globalConfig() live, which takes precedence over these.
    mooncake::ascend_allocator_set_runtime_flags(options_.agent_mode,
                                                 options_.use_fabric_mem);
#endif
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
    std::vector<int32_t> logic_device_ids;
    logic_device_ids.reserve(engines_.size());
    for (const auto& engine : engines_) {
        logic_device_ids.push_back(engine->deviceId());
    }
    const auto engine_cpus = EngineCpuSets(logic_device_ids);
    startPools(engine_cpus);
    PublishAllocationAffinityPools(
        this, BuildAllocationPools(engine_cpus, transfer_pools_));
    return Status::OK();
}

void AscendDirectTransport::startPools(
    const std::vector<std::vector<int>>& engine_cpus) {
    transfer_pools_.reserve(engines_.size());
    copy_pools_.reserve(engines_.size());
    for (size_t i = 0; i < engines_.size(); ++i) {
        std::vector<int> cpus;
        if (i < engine_cpus.size()) {
            cpus = engine_cpus[i];
        }
        const std::string index = std::to_string(i);
        const size_t transfer_threads = engines_.size() == 1
                                            ? kSingleDeviceTransferThreads
                                            : kTransferThreadsPerEngine;
        transfer_pools_.push_back(std::make_unique<AscendThreadPool>(
            transfer_threads, "tw" + index + "-", cpus));
        copy_pools_.push_back(std::make_unique<AscendThreadPool>(
            kCopyThreadsPerEngine, "tc" + index + "-", cpus));
        AscendThreadPool* copy_pool = copy_pools_.back().get();
        local_copies_[i]->setTaskRunner(
            [copy_pool](std::function<void()> task) {
                // A stopped pool runs the task here, so a copy being issued
                // during teardown still gets every stream issued.
                if (!copy_pool->tryEnqueue(task)) {
                    task();
                }
            });
    }
}

void AscendDirectTransport::stopPools() {
    // Stopping drains each queue, so every group already handed to a transfer
    // worker reaches HIXL, or fails, before the engines go away.
    for (auto& pool : transfer_pools_) {
        if (pool) {
            pool->stop();
        }
    }
    transfer_pools_.clear();
    for (auto& local_copy : local_copies_) {
        if (local_copy) {
            local_copy->setTaskRunner(nullptr);
        }
    }
    for (auto& pool : copy_pools_) {
        if (pool) {
            pool->stop();
        }
    }
    copy_pools_.clear();
}

void AscendDirectTransport::finalizeEngines() {
    stopPools();
    ClearAllocationAffinityPools(this);
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
    // A failed group is visible to the caller before its worker lets go of
    // the batch, so the batch cannot be recycled until that worker is done.
    while (hixl_batch->pending_submits.load(std::memory_order_acquire) > 0) {
        std::this_thread::yield();
    }
    Slab<HixlSubBatch>::Get().deallocate(hixl_batch);
    batch = nullptr;
    return Status::OK();
}

Status AscendDirectTransport::resolveRequestWithSegment(
    const Request& request, PreparedRequest& prepared,
    SegmentDesc* desc) const {
    if (desc == nullptr) {
        return Status::InvalidArgument("Cannot find target segment" LOC_MARK);
    }
    if (!std::holds_alternative<MemorySegmentDesc>(desc->detail)) {
        return Status::InvalidArgument(
            "Target is not a memory segment" LOC_MARK);
    }
    const auto& detail = std::get<MemorySegmentDesc>(desc->detail);
    prepared.request = request;
    prepared.local_engine_idx = currentEngineIndex();
    // A peer host segment allocated from a shared memfd can be mapped here, so
    // the copy becomes same-process and the ACL local copy engine can perform
    // it (H2D/D2H/H2H chosen by the copy engine), skipping HIXL entirely.
    // Anything else (no handle, another machine, stale handle, range escaping
    // the buffer) stays on the HIXL path below.
    void* local_addr = nullptr;
    if (use_shmem_ &&
        shared_host_mapper_.Translate(*desc, request.target_offset,
                                      request.length, &local_addr)) {
        prepared.shm_local = true;
        prepared.mapped_remote_addr = local_addr;
    } else {
        const auto& memory = std::get<MemorySegmentDesc>(desc->detail);
        size_t shm_buffers = 0;
        for (const auto& buffer : memory.buffers) {
            if (!buffer.shm_path.empty()) {
                ++shm_buffers;
            }
        }
        VLOG(1) << "TENT local-copy miss target_segment=" << request.target_id
                << " dest=" << request.target_offset
                << " len=" << request.length
                << " buffers=" << memory.buffers.size()
                << " shm_buffers=" << shm_buffers
                << " opcode=" << static_cast<int>(request.opcode);
    }
    // Only the HIXL route needs a remote engine name. Resolving it for a
    // mapped SHMEM request parses the segment's hixl_names JSON for every
    // request, which dominates the page-granular local-copy workload.
    if (!prepared.shm_local) {
        prepared.remote_hixl =
            ResolveRemoteHixlName(detail, request.target_offset);
        if (prepared.remote_hixl.empty()) {
            return Status::InvalidArgument("Missing remote hixl_name" LOC_MARK);
        }
        if (request.target_id != LOCAL_SEGMENT_ID &&
            prepared.local_engine_idx < engines_.size() &&
            engines_[prepared.local_engine_idx] &&
            prepared.remote_hixl ==
                engines_[prepared.local_engine_idx]->name()) {
            LOG(WARNING) << "Remote segment id=" << request.target_id
                         << " resolved to local HIXL name "
                         << prepared.remote_hixl;
        }
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

    // Batch-level segment pin cache: a get batch has thousands of requests for
    // the same target segment. Resolving the segment through the metadata cache
    // once per request dominated the page-granular submit path.
    std::unordered_map<SegmentID, SegmentDescRef> segment_pins;
    std::vector<PreparedRequest> prepared;
    prepared.reserve(request_list.size());
    for (const auto& request : request_list) {
        auto pin_it = segment_pins.find(request.target_id);
        if (pin_it == segment_pins.end()) {
            SegmentDescRef pin;
            SegmentDesc* desc = nullptr;
            auto status = metadata_->segmentManager().withCachedSegment(
                request.target_id, pin, [&](SegmentDesc* cached) {
                    desc = cached;
                    return Status::OK();
                });
            if (!status.ok() || desc == nullptr) {
                return Status::InvalidArgument(
                    "Cannot find target segment" LOC_MARK);
            }
            pin_it =
                segment_pins.emplace(request.target_id, std::move(pin)).first;
        }
        PreparedRequest item;
        CHECK_STATUS(
            resolveRequestWithSegment(request, item, pin_it->second.get()));
        prepared.push_back(std::move(item));
    }

    const size_t start = hixl_batch->task_list.size();
    hixl_batch->task_list.resize(start + prepared.size());
    // Stamped on the submitting thread with the same clock the timeout checks
    // use, so a task that never reaches a transfer worker (the pool is busy)
    // still times out instead of reporting PENDING forever.
    const int64_t accepted_time_ns = SteadyTimeInNano();
    using GroupKey = std::pair<size_t, std::pair<std::string, bool>>;
    std::map<GroupKey, std::vector<HixlTask*>> groups;
    std::map<std::pair<size_t, bool>, std::vector<HixlTask*>> mapped_groups;
    for (size_t i = 0; i < prepared.size(); ++i) {
        auto& task = hixl_batch->task_list[start + i];
        task.request = prepared[i].request;
        StoreTaskStatus(task, TransferStatusEnum::PENDING);
        StoreTaskBytes(task, 0);
        StoreTaskHandle(task, nullptr);
        task.accepted_time_ns = accepted_time_ns;
        task.start_time_ns = 0;
        task.local_engine_idx = prepared[i].local_engine_idx;
        task.remote_hixl = prepared[i].remote_hixl;
        task.local_copy = false;
        task.mapped_remote_addr = prepared[i].mapped_remote_addr;
        const bool write = task.request.opcode == Request::WRITE;
        if (prepared[i].shm_local) {
            mapped_groups[{task.local_engine_idx, write}].push_back(&task);
            continue;
        }
        groups[{task.local_engine_idx, {task.remote_hixl, write}}].push_back(
            &task);
    }
    // Remote groups go to the transfer workers first, so groups bound for
    // different peers reach HIXL together instead of one after another, and
    // none of them waits behind a local copy issued here.
    for (auto& [key, tasks] : groups) {
        if (!isLocalRoute(key.first, key.second.first)) {
            startGroup(hixl_batch, key.second.first, key.first,
                       key.second.second, std::move(tasks));
        }
    }
    for (auto& [key, tasks] : mapped_groups) {
        startLocalCopy(key.first, key.second, std::move(tasks),
                       /*use_mapped_remote=*/true);
    }
    for (auto& [key, tasks] : groups) {
        if (isLocalRoute(key.first, key.second.first)) {
            startLocalCopy(key.first, key.second.second, tasks);
        }
    }
    return Status::OK();
}

bool AscendDirectTransport::isLocalRoute(size_t engine_idx,
                                         const std::string& remote_hixl) const {
    return engine_idx < engines_.size() && engines_[engine_idx] &&
           remote_hixl == engines_[engine_idx]->name();
}

void AscendDirectTransport::startGroup(HixlSubBatch* batch,
                                       const std::string& remote_hixl,
                                       size_t local_engine_idx, bool write,
                                       std::vector<HixlTask*> tasks) {
    // A task the poller already timed out while it was queued keeps TIMEOUT;
    // only the ones still waiting for a worker are failed here.
    auto fail_all = [&]() {
        for (auto* task : tasks) {
            if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
                StoreTaskStatus(*task, TransferStatusEnum::FAILED);
            }
        }
    };
    if (local_engine_idx >= engines_.size() || !engines_[local_engine_idx]) {
        fail_all();
        return;
    }

    // The reservation stays on the caller, so a full window still pushes back
    // on whoever submits, and quiesce() already counts a group that is queued.
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

    batch->pending_submits.fetch_add(1, std::memory_order_relaxed);
    std::function<void()> issue = [this, batch, remote_hixl, local_engine_idx,
                                   write, tasks = std::move(tasks)]() {
        // A worker task must never let an exception escape (std::terminate),
        // and a failed submission must still mark the tasks and return the
        // group reservation exactly once.
        bool submitted = false;
        try {
            issueGroup(remote_hixl, local_engine_idx, write, tasks);
            submitted = true;
        } catch (const std::exception& e) {
            LOG(ERROR) << "AscendDirectTransport: submitting a group threw: "
                       << e.what();
        } catch (...) {
            LOG(ERROR)
                << "AscendDirectTransport: submitting a group threw an unknown "
                   "exception";
        }
        if (!submitted) {
            abandonGroup(tasks);
        }
        // Last touch of the batch: freeSubBatch may recycle it right after.
        batch->pending_submits.fetch_sub(1, std::memory_order_release);
    };
    AscendThreadPool* pool = local_engine_idx < transfer_pools_.size()
                                 ? transfer_pools_[local_engine_idx].get()
                                 : nullptr;
    if (pool == nullptr || !pool->tryEnqueue(issue)) {
        issue();
    }
}

void AscendDirectTransport::issueGroup(const std::string& remote_hixl,
                                       size_t local_engine_idx, bool write,
                                       const std::vector<HixlTask*>& tasks) {
    // Only tasks that are still queued are handed to HIXL. The poller cancels a
    // queued task under req_mutex_, and this claim runs under the same mutex,
    // so a task is either cancelled before the claim or part of the request.
    std::vector<HixlTask*> active;
    active.reserve(tasks.size());
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        for (auto* task : tasks) {
            if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
                active.push_back(task);
            }
        }
    }
    if (active.empty()) {
        // Every task was cancelled while it was queued. Nothing is sent to HIXL
        // and the reservation startGroup made is returned here.
        releaseInflight();
        return;
    }

    std::vector<HixlOpDesc> op_descs;
    op_descs.reserve(active.size());
    for (auto* task : active) {
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
        for (auto* task : active) {
            StoreTaskStatus(*task, TransferStatusEnum::FAILED);
        }
        return;
    }

    const auto now = SteadyTimeInNano();
    GroupState group;
    group.status = TransferStatusEnum::PENDING;
    group.engine_idx = local_engine_idx;
    group.remote = remote_hixl;
    group.counted = true;
    bool tracked = false;
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        // A poller may have cancelled a task while HIXL was accepting the
        // group. Such a task reports TIMEOUT and is never polled through the
        // group again, so it must not be counted in group.remaining and must
        // not get the handle published: counting it would keep remaining above
        // zero and leak the inflight reservation.
        int pending = 0;
        for (auto* task : active) {
            if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
                ++pending;
            }
        }
        if (pending > 0) {
            group.remaining = pending;
            const std::pair<size_t, std::string> route_key{local_engine_idx,
                                                           remote_hixl};
            auto& route = route_groups_[route_key];
            try {
                route.push_back(req);
                groups_.emplace(req, std::move(group));
            } catch (...) {
                // The handle is not published yet, so remove the half-inserted
                // route entry and let the worker guard fail the tasks and
                // return the reservation.
                route.erase(std::remove(route.begin(), route.end(), req),
                            route.end());
                if (route.empty()) {
                    route_groups_.erase(route_key);
                }
                throw;
            }
            for (auto* task : active) {
                if (LoadTaskStatus(*task) != TransferStatusEnum::PENDING) {
                    continue;
                }
                task->batch_size = active.size();
                task->start_time_ns = now;
                // Published last: a poller that sees the handle reads the
                // fields above.
                StoreTaskHandle(*task, req);
                tracked = true;
            }
        }
    }
    if (!tracked) {
        releaseInflight();
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

void AscendDirectTransport::startLocalCopy(size_t local_engine_idx, bool write,
                                           const std::vector<HixlTask*>& tasks,
                                           bool use_mapped_remote) {
    // Same contract as issueGroup: a queued task can be cancelled by the poller
    // only before it gets its handle, so the claim and the publication below
    // run under req_mutex_.
    auto fail_all = [&]() {
        for (auto* task : tasks) {
            task->local_copy = true;
            if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
                StoreTaskStatus(*task, TransferStatusEnum::FAILED);
            }
        }
    };
    if (local_engine_idx >= local_copies_.size() ||
        !local_copies_[local_engine_idx]) {
        fail_all();
        return;
    }
    // Read the name before reserving the inflight slot so a copy failure cannot
    // strand the reservation.
    const std::string local_remote = engines_[local_engine_idx]->name();

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

    std::vector<HixlTask*> active;
    active.reserve(tasks.size());
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        for (auto* task : tasks) {
            if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
                active.push_back(task);
            }
        }
    }
    if (active.empty()) {
        releaseInflight();
        return;
    }

    std::vector<HixlOpDesc> op_descs;
    op_descs.reserve(active.size());
    for (auto* task : active) {
        HixlOpDesc desc{};
        desc.local_addr = reinterpret_cast<uintptr_t>(task->request.source);
        desc.remote_addr =
            use_mapped_remote
                ? reinterpret_cast<uintptr_t>(task->mapped_remote_addr)
                : task->request.target_offset;
        desc.len = task->request.length;
        op_descs.push_back(desc);
    }
    if (VLOG_IS_ON(1)) {
        size_t total_bytes = 0;
        for (const auto& op : op_descs) {
            total_bytes += op.len;
        }
        VLOG(1) << "TENT local copy ops=" << op_descs.size()
                << " bytes=" << total_bytes
                << " mapped=" << (use_mapped_remote ? 1 : 0)
                << " write=" << (write ? 1 : 0);
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

    const auto now = SteadyTimeInNano();
    GroupState group;
    group.status = TransferStatusEnum::PENDING;
    group.engine_idx = local_engine_idx;
    group.remote = local_remote;
    group.counted = true;
    group.local_copy = true;
    bool tracked = false;
    bool publish_failed = false;
    {
        std::lock_guard<std::mutex> lock(req_mutex_);
        // As in issueGroup: a task cancelled while the streams were being
        // issued reports TIMEOUT and is never polled through the group, so it
        // is neither counted nor given the handle.
        int pending = 0;
        for (auto* task : active) {
            if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
                ++pending;
            }
        }
        if (pending > 0) {
            group.remaining = pending;
            try {
                groups_.emplace(req, std::move(group));
            } catch (...) {
                // No handle was published yet; the copy group's flags and the
                // reservation are released outside this lock below.
                publish_failed = true;
            }
            if (!publish_failed) {
                for (auto* task : active) {
                    if (LoadTaskStatus(*task) != TransferStatusEnum::PENDING) {
                        continue;
                    }
                    task->batch_size = active.size();
                    task->start_time_ns = now;
                    task->local_copy = true;
                    task->local_engine_idx = local_engine_idx;
                    // Published last.
                    StoreTaskHandle(*task, req);
                    tracked = true;
                }
            }
        }
    }
    if (publish_failed || !tracked) {
        // Release the host flags outside req_mutex_: the timeout path takes the
        // LocalCopyEngine mutex before req_mutex_, so the opposite order here
        // could deadlock.
        local_copies_[local_engine_idx]->release(req);
        releaseInflight();
    }
    if (publish_failed) {
        for (auto* task : active) {
            if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
                StoreTaskStatus(*task, TransferStatusEnum::FAILED);
            }
        }
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
    // Publish the byte count before the status: a poller that acquires the
    // terminal status must also see the bytes behind it.
    if (group.status == TransferStatusEnum::COMPLETED) {
        StoreTaskBytes(task, task.request.length);
    }
    StoreTaskStatus(task, group.status);
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
                                            TransferStatusEnum status) {
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
}

bool AscendDirectTransport::finishTaskLocked(HixlTask& task,
                                             void** local_release_handle,
                                             size_t* local_release_engine) {
    void* const handle = LoadTaskHandle(task);
    auto git = groups_.find(handle);
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
        handles.erase(std::remove(handles.begin(), handles.end(), handle),
                      handles.end());
        if (handles.empty()) {
            route_groups_.erase(route_it);
        }
    }
    if (local_release_handle != nullptr && local_release_engine != nullptr &&
        git->second.local_copy) {
        *local_release_handle = handle;
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
        auto git = groups_.find(LoadTaskHandle(task));
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

void AscendDirectTransport::abandonGroup(const std::vector<HixlTask*>& tasks) {
    for (auto* task : tasks) {
        if (LoadTaskStatus(*task) == TransferStatusEnum::PENDING) {
            StoreTaskStatus(*task, TransferStatusEnum::FAILED);
        }
    }
    releaseInflight();
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
    auto current = LoadTaskStatus(task);
    if (current != TransferStatusEnum::PENDING) {
        status = TransferStatus{current, LoadTaskBytes(task)};
        return Status::OK();
    }

    void* const handle = LoadTaskHandle(task);
    const auto now = SteadyTimeInNano();
    if (handle == nullptr) {
        // Queued for a transfer worker, which has not published the group yet.
        // A task that waits longer than the transfer timeout is timed out here
        // instead of reporting PENDING forever. The cancel is taken under
        // req_mutex_, the same lock the worker claims tasks under, so the
        // worker either drops the task or excludes it from group.remaining;
        // the inflight reservation is returned in both cases.
        const int64_t accepted = task.accepted_time_ns;
        if (accepted > 0 && now - accepted > transfer_timeout_ns_) {
            std::lock_guard<std::mutex> lock(req_mutex_);
            if (LoadTaskHandle(task) == nullptr &&
                LoadTaskStatus(task) == TransferStatusEnum::PENDING) {
                StoreTaskStatus(task, TransferStatusEnum::TIMEOUT);
                LOG(WARNING) << "AscendDirectTransport: task " << task_id
                             << " timed out while queued ("
                             << (now - accepted) / kMillisToNano << " ms > "
                             << options_.transfer_timeout_ms
                             << " ms), canceled before submission";
            }
        }
        status = TransferStatus{LoadTaskStatus(task), LoadTaskBytes(task)};
        return Status::OK();
    }

    if (!task.local_copy && handle == t_last_polled_handle &&
        t_last_polled_ns != 0 && now - t_last_polled_ns < kGroupPollCacheNs) {
        status = TransferStatus{LoadTaskStatus(task), LoadTaskBytes(task)};
        return Status::OK();
    }

    completePolledTask(task);
    current = LoadTaskStatus(task);
    if (current != TransferStatusEnum::PENDING) {
        status = TransferStatus{current, LoadTaskBytes(task)};
        return Status::OK();
    }

    if (task.start_time_ns > 0 &&
        (now - task.start_time_ns) > transfer_timeout_ns_) {
        if (task.local_copy) {
            failLocalCopyStream(task.local_engine_idx,
                                TransferStatusEnum::TIMEOUT);
        } else {
            failEntireRoute(task.local_engine_idx, task.remote_hixl,
                            TransferStatusEnum::TIMEOUT);
        }
        completePolledTask(task);
        if (LoadTaskStatus(task) == TransferStatusEnum::PENDING) {
            StoreTaskStatus(task, TransferStatusEnum::TIMEOUT);
        }
        status = TransferStatus{LoadTaskStatus(task), LoadTaskBytes(task)};
        return Status::OK();
    }

    if (!task.local_copy) {
        // Collapse the per-task polls of one HIXL request into one query per
        // short window: HIXL serializes status queries per peer, and a group
        // can hold thousands of tasks that all read the same completion.
        std::lock_guard<std::mutex> lock(req_mutex_);
        t_last_polled_handle = handle;
        t_last_polled_ns = now;
        auto git = groups_.find(handle);
        if (git != groups_.end()) {
            if (git->second.last_poll_ns != 0 &&
                now - git->second.last_poll_ns < kGroupPollCacheNs) {
                status =
                    TransferStatus{LoadTaskStatus(task), LoadTaskBytes(task)};
                return Status::OK();
            }
            git->second.last_poll_ns = now;
        }
    }

    if (task.local_copy) {
        if (task.local_engine_idx >= local_copies_.size() ||
            !local_copies_[task.local_engine_idx]) {
            failLocalCopyStream(task.local_engine_idx,
                                TransferStatusEnum::FAILED);
        } else {
            const auto mapped =
                local_copies_[task.local_engine_idx]->poll(handle);
            if (mapped == TransferStatusEnum::COMPLETED ||
                mapped == TransferStatusEnum::FAILED) {
                std::lock_guard<std::mutex> lock(req_mutex_);
                auto git = groups_.find(handle);
                if (git != groups_.end() &&
                    git->second.status == TransferStatusEnum::PENDING) {
                    git->second.status = mapped;
                }
            }
        }
        completePolledTask(task);
        if (LoadTaskStatus(task) == TransferStatusEnum::PENDING &&
            (task.local_engine_idx >= local_copies_.size() ||
             !local_copies_[task.local_engine_idx])) {
            StoreTaskStatus(task, TransferStatusEnum::FAILED);
        }
        status = TransferStatus{LoadTaskStatus(task), LoadTaskBytes(task)};
        return Status::OK();
    }

    if (task.local_engine_idx >= engines_.size() ||
        !engines_[task.local_engine_idx]) {
        failEntireRoute(task.local_engine_idx, task.remote_hixl,
                        TransferStatusEnum::FAILED);
        completePolledTask(task);
        if (LoadTaskStatus(task) == TransferStatusEnum::PENDING) {
            StoreTaskStatus(task, TransferStatusEnum::FAILED);
        }
        status = TransferStatus{LoadTaskStatus(task), LoadTaskBytes(task)};
        return Status::OK();
    }

    HixlXferState xfer_status = HixlXferState::Waiting;
    auto err =
        engines_[task.local_engine_idx]->getTransferStatus(handle, xfer_status);
    const auto mapped = FromHixlState(xfer_status);
    if (!err.ok() || mapped == TransferStatusEnum::FAILED) {
        failEntireRoute(task.local_engine_idx, task.remote_hixl,
                        TransferStatusEnum::FAILED);
    } else if (mapped == TransferStatusEnum::TIMEOUT) {
        failEntireRoute(task.local_engine_idx, task.remote_hixl,
                        TransferStatusEnum::TIMEOUT);
    } else if (mapped == TransferStatusEnum::COMPLETED) {
        std::lock_guard<std::mutex> lock(req_mutex_);
        auto git = groups_.find(handle);
        if (git != groups_.end() &&
            git->second.status == TransferStatusEnum::PENDING) {
            git->second.status = TransferStatusEnum::COMPLETED;
        }
    }

    completePolledTask(task);
    status = TransferStatus{LoadTaskStatus(task), LoadTaskBytes(task)};
    return Status::OK();
}

Status AscendDirectTransport::addMemoryBuffer(BufferDesc& desc,
                                              const MemoryOptions& options) {
    if (engines_.empty()) {
        return Status::InternalError(
            "HIXL engines are not initialized" LOC_MARK);
    }
    const size_t engine_idx = currentEngineIndex();
    bool host = IsHostMem(desc.location, reinterpret_cast<void*>(desc.addr));
#ifdef TENT_HAS_ASCEND_ALLOCATOR
    // Direct ACL VMM host memory (shm_helper's agent+fabric path) is not known
    // to HIXL's allocator bookkeeping, so HIXL only auto-corrects the MEM_TYPE
    // of buffers it allocated itself through adxl::MallocMem. Export it as
    // device memory, the same classification the classic transport uses.
    if (host && options_.use_fabric_mem &&
        mooncake::ascend_is_direct_vmm_memory(
            reinterpret_cast<void*>(desc.addr), desc.length)) {
        host = false;
    }
#endif
    if (engine_idx >= engines_.size() || !engines_[engine_idx]) {
        return Status::InternalError("Invalid HIXL engine index" LOC_MARK);
    }
    CHECK_STATUS(engines_[engine_idx]->registerMem(
        reinterpret_cast<void*>(desc.addr), desc.length, host,
        options.perm == kLocalReadWrite));
#ifdef TENT_HAS_ASCEND_ALLOCATOR
    if (host) {
        std::string shared_handle;
        if (mooncake::ascend_get_shared_host_handle(
                reinterpret_cast<void*>(desc.addr), &shared_handle)) {
            desc.shm_path = std::move(shared_handle);
        }
    }
#endif
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
