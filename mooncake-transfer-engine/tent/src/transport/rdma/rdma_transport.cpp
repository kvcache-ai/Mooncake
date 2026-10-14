// Copyright 2025 KVCache.AI
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

#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport/rdma/ibv_loader.h"
#include "tent/transport/rdma/quota.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iterator>
#include <limits>
#include <sstream>

#include "tent/common/status.h"
#include "tent/common/utils/ip.h"
#include "tent/transport/rdma/buffers.h"
#include "tent/transport/rdma/endpoint_store.h"
#include "tent/transport/rdma/workers.h"
#include "tent/common/utils/string_builder.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/topology.h"
#include "tent/common/utils/random.h"
#include "tent/metrics/tent_metrics.h"
#include "tent/thirdparty/nlohmann/json.h"

#define SET_DEVICE(key, param) \
    param = conf->get("transports/rdma/device/" #key, param)

#define SET_ENDPOINT(key, param) \
    param = conf->get("transports/rdma/endpoint/" #key, param)

#define SET_WORKERS(key, param) \
    param = conf->get("transports/rdma/workers/" #key, param)

namespace mooncake {
namespace tent {

namespace {

constexpr uint64_t kDefaultRdmaQuiesceTimeoutNs = 10000000000ull;

// EFA devices are listed by ibverbs but have no RC QPs; they are served by
// the fabric transport instead.
bool isEfaDevice(const std::string& name) {
    const std::string link = "/sys/class/infiniband/" + name + "/device/driver";
    char target[PATH_MAX];
    ssize_t len = readlink(link.c_str(), target, sizeof(target) - 1);
    if (len <= 0) return false;
    target[len] = '\0';
    const char* base = strrchr(target, '/');
    return strcmp(base ? base + 1 : target, "efa") == 0;
}

uint16_t getRdmaBindDefaultPort(const Config& config) {
    constexpr const char* kKey = "rpc_server_port";
    if (!config.contains(kKey)) return 0;

    json raw_value = config.get<json>(kKey, json());
    if (raw_value.is_number_integer() || raw_value.is_number_unsigned()) {
        long long value = raw_value.get<long long>();
        if (value >= 0 && value <= static_cast<long long>(
                                       std::numeric_limits<uint16_t>::max())) {
            return static_cast<uint16_t>(value);
        }
        return 0;
    }

    if (raw_value.is_string()) {
        const std::string string_value = raw_value.get<std::string>();
        char* end = nullptr;
        errno = 0;
        unsigned long value = std::strtoul(string_value.c_str(), &end, 10);
        if (errno == 0 && end != string_value.c_str() && *end == '\0' &&
            value <= std::numeric_limits<uint16_t>::max()) {
            return static_cast<uint16_t>(value);
        }
    }

    return 0;
}

}  // namespace

static Status configureLaneCount(std::shared_ptr<Config> conf,
                                 std::shared_ptr<RdmaParams> params) {
    constexpr int kUnset = -1;
    const int num_lanes = conf->get("transports/rdma/num_lanes", kUnset);
    const int num_cq_list =
        conf->get("transports/rdma/device/num_cq_list", kUnset);
    const int qp_mul_factor =
        conf->get("transports/rdma/endpoint/qp_mul_factor", kUnset);
    const int num_workers =
        conf->get("transports/rdma/workers/num_workers", kUnset);

    auto validate_positive = [](int value, const char* name) -> Status {
        if (value == kUnset || value > 0) return Status::OK();
        std::stringstream ss;
        ss << "Invalid RDMA " << name << ": " << value
           << ", expected a positive integer";
        return Status::InvalidArgument(ss.str() + LOC_MARK);
    };

    auto status = validate_positive(num_lanes, "num_lanes");
    if (!status.ok()) return status;
    status = validate_positive(num_cq_list, "device.num_cq_list");
    if (!status.ok()) return status;
    status = validate_positive(qp_mul_factor, "endpoint.qp_mul_factor");
    if (!status.ok()) return status;
    status = validate_positive(num_workers, "workers.num_workers");
    if (!status.ok()) return status;

    int lane_count = params->num_lanes;
    if (num_lanes != kUnset) {
        lane_count = num_lanes;
    } else if (num_cq_list != kUnset) {
        lane_count = num_cq_list;
    } else if (qp_mul_factor != kUnset) {
        lane_count = qp_mul_factor;
    } else if (num_workers != kUnset) {
        lane_count = num_workers;
    }

    auto validate_match = [lane_count](int value, const char* name) -> Status {
        if (value == kUnset || value == lane_count) return Status::OK();
        std::stringstream ss;
        ss << "Inconsistent RDMA lane configuration: " << name << "=" << value
           << " but expected lane count " << lane_count
           << " so worker/QP/CQ counts stay aligned";
        return Status::InvalidArgument(ss.str() + LOC_MARK);
    };

    status = validate_match(num_cq_list, "device.num_cq_list");
    if (!status.ok()) return status;
    status = validate_match(qp_mul_factor, "endpoint.qp_mul_factor");
    if (!status.ok()) return status;
    status = validate_match(num_workers, "workers.num_workers");
    if (!status.ok()) return status;

    if (num_cq_list != kUnset || qp_mul_factor != kUnset ||
        num_workers != kUnset) {
        LOG(WARNING) << "Legacy RDMA parallelism knobs "
                     << "(device.num_cq_list, endpoint.qp_mul_factor, "
                     << "workers.num_workers) are deprecated; prefer "
                     << "transports/rdma/num_lanes";
    }

    params->num_lanes = lane_count;
    params->device.num_cq_list = lane_count;
    params->endpoint.qp_mul_factor = lane_count;
    params->workers.num_workers = lane_count;
    return Status::OK();
}

static Status convertConfToRdmaParams(std::shared_ptr<Config> conf,
                                      std::shared_ptr<RdmaParams> params) {
    auto status = configureLaneCount(conf, params);
    if (!status.ok()) return status;

    SET_DEVICE(num_comp_channels, params->device.num_comp_channels);
    SET_DEVICE(port, params->device.port);
    SET_DEVICE(gid_index, params->device.gid_index);
    SET_DEVICE(max_cqe, params->device.max_cqe);

    SET_ENDPOINT(endpoint_store_cap, params->endpoint.endpoint_store_cap);
    SET_ENDPOINT(max_sge, params->endpoint.max_sge);
    SET_ENDPOINT(max_qp_wr, params->endpoint.max_qp_wr);
    SET_ENDPOINT(signal_interval, params->endpoint.signal_interval);
    SET_ENDPOINT(max_inline_bytes, params->endpoint.max_inline_bytes);
    SET_ENDPOINT(pkey_index, params->endpoint.pkey_index);
    SET_ENDPOINT(hop_limit, params->endpoint.hop_limit);
    SET_ENDPOINT(flow_label, params->endpoint.flow_label);
    SET_ENDPOINT(traffic_class, params->endpoint.traffic_class);
    SET_ENDPOINT(service_level, params->endpoint.service_level);
    SET_ENDPOINT(src_path_bits, params->endpoint.src_path_bits);
    SET_ENDPOINT(static_rate, params->endpoint.static_rate);
    SET_ENDPOINT(rq_psn, params->endpoint.rq_psn);
    SET_ENDPOINT(max_dest_rd_atomic, params->endpoint.max_dest_rd_atomic);
    SET_ENDPOINT(min_rnr_timer, params->endpoint.min_rnr_timer);
    SET_ENDPOINT(sq_psn, params->endpoint.sq_psn);
    SET_ENDPOINT(send_timeout, params->endpoint.send_timeout);
    SET_ENDPOINT(send_retry_count, params->endpoint.send_retry_count);
    SET_ENDPOINT(send_rnr_count, params->endpoint.send_rnr_count);
    SET_ENDPOINT(max_rd_atomic, params->endpoint.max_rd_atomic);
    params->endpoint.notify_proto =
        conf->get("notification/proto", params->endpoint.notify_proto);

    if (params->endpoint.signal_interval <= 0) {
        std::stringstream ss;
        ss << "Invalid RDMA endpoint.signal_interval: "
           << params->endpoint.signal_interval
           << ", expected a positive integer";
        return Status::InvalidArgument(ss.str() + LOC_MARK);
    }

    size_t mtu_val = conf->get("transports/rdma/endpoint/path_mtu", 4096);
    if (mtu_val == 4096)
        params->endpoint.path_mtu = IBV_MTU_4096;
    else if (mtu_val == 2048)
        params->endpoint.path_mtu = IBV_MTU_2048;
    else if (mtu_val == 1024)
        params->endpoint.path_mtu = IBV_MTU_1024;
    else
        params->endpoint.path_mtu = IBV_MTU_512;

    // Optional per-pool QP layout (RFC #2568 step 2). Each entry defines a
    // named pool with its own QP count and link-layer SL/TC;
    // SelectionPolicy.qp_pool references these by name. Absent/empty => single
    // default pool (unchanged). The pool SL/TC live here in the RDMA config,
    // not in SelectionPolicy, to keep the link-layer QoS definition in the
    // transport layer; policies only reference a pool by name.
    params->endpoint.qp_pools.clear();
    auto qp_pools_json =
        conf->getArray<nlohmann::json>("transports/rdma/endpoint/qp_pools");
    for (const auto& pool_json : qp_pools_json) {
        if (!pool_json.is_object()) {
            LOG(WARNING) << "Ignore non-object entry in qp_pools";
            continue;
        }
        if (!pool_json.contains("name") || !pool_json["name"].is_string()) {
            LOG(WARNING) << "Ignore qp_pool entry without a string 'name'";
            continue;
        }
        QpPoolSegment seg;
        seg.name = pool_json["name"].get<std::string>();
        seg.num_qp = pool_json.value("num_qp", 0);
        if (seg.num_qp <= 0) {
            LOG(WARNING) << "Ignore qp_pool '" << seg.name
                         << "' with non-positive num_qp " << seg.num_qp;
            continue;
        }
        seg.service_level = pool_json.value("service_level", -1);
        seg.traffic_class = pool_json.value("traffic_class", -1);
        params->endpoint.qp_pools.push_back(std::move(seg));
    }
    if (!params->endpoint.qp_pools.empty()) {
        LOG(INFO) << "Configured " << params->endpoint.qp_pools.size()
                  << " QP pool(s) for per-class link-layer isolation";
    }

    SET_WORKERS(max_retry_count, params->workers.max_retry_count);
    SET_WORKERS(block_size, params->workers.block_size);
    SET_WORKERS(grace_period_ns, params->workers.grace_period_ns);
    SET_WORKERS(rail_topo_path, params->workers.rail_topo_path);

    params->verbose = conf->get("verbose", false);
    params->log_slice_affinity =
        conf->get("transports/rdma/log_slice_affinity", false);
    return Status::OK();
}

static bool isGpuDirectRdmaSupported(std::shared_ptr<Config> conf) {
    auto disable_gpu_direct =
        conf->get("transports/rdma/disable_gpu_direct_rdma", false);
    if (disable_gpu_direct) {
        return false;
    }
    // Intel XPU: no peer-memory kernel module; device buffers are exported as
    // dma-bufs and registered with ibv_reg_dmabuf_mr, so the presence of that
    // verb is the capability signal. Buffers that fail to export at
    // registration time are simply not offered on RDMA and get staged.
    if (Platform::getLoader().type() == "xpu") {
        auto& loader = IbvLoader::Instance();
        return loader.available() && loader.sym().ibv_reg_dmabuf_mr != nullptr;
    }
    // Detect vendor GPUDirect/peer-memory drivers from /proc/modules.
    // NVIDIA: nvidia_peermem. AMD: peermem is built into amdgpu (linked with
    // ib_core), so the amdgpu module itself is the presence signal.
    // Hygon: the hycu driver provides the same peer-memory support.
    std::ifstream modules("/proc/modules");
    std::string line;
    while (std::getline(modules, line)) {
        const auto name_end = line.find(' ');
        const auto name =
            name_end == std::string::npos ? line : line.substr(0, name_end);
        if (name == "nvidia_peermem" || name == "amdgpu" || name == "hycu") {
            return true;
        }
    }
    return false;
}

RdmaTransport::RdmaTransport()
    : installed_(false),
      notify_worker_running_(false),
      notify_poll_interval_us_(10) {}  // Start at 10us

RdmaTransport::~RdmaTransport() { uninstall(); }

size_t RdmaTransport::initializeContexts() {
    context_set_.clear();
    context_name_lookup_.clear();
    // One slot per NicID: dev_id arrives as a NicID and subscripts both this
    // and BufferDesc::lkey, so a compacted layout would name the wrong RNIC.
    // Skipped NICs keep an inert context, which consumers reject via status().
    context_set_.reserve(local_topology_->getNicCount());
    size_t context_count = 0;
    for (size_t i = 0; i < local_topology_->getNicCount(); ++i) {
        auto entry = local_topology_->getNicEntry(i);
        if (entry->type == Topology::NIC_RDMA && isEfaDevice(entry->name)) {
            LOG(INFO) << "Skip EFA device " << entry->name
                      << " (not RC capable; use the fabric transport)";
        } else if (entry->type == Topology::NIC_RDMA) {
            auto context = std::make_shared<RdmaContext>(*this);
            if (context->construct(entry->name, params_) == 0) {
                context_name_lookup_[entry->name] = i;
                ++context_count;
                local_buffer_manager_.addDevice(context.get());
                context_set_.push_back(std::move(context));
                continue;
            }
            LOG(WARNING) << "Disable RDMA device " << entry->name << " because "
                         << "of initialization failure";
        }
        // A never-constructed context, not the one whose construct() failed:
        // the slot only has to stand in for the NicID, so it should not carry
        // a device name or an endpoint store it will never use.
        context_set_.push_back(std::make_shared<RdmaContext>(*this));
    }
    return context_count;
}

RdmaTransport::NotifyConfig RdmaTransport::readNotifyConfig(
    const Config& conf) {
    NotifyConfig config;
    config.rpc_fallback = conf.get("notification/rpc_fallback", true);
    config.path_probe = conf.get("notification/path_probe", true);
    // Clamped so the hold's deadline arithmetic cannot overflow.
    constexpr int64_t kMaxDeadlineMs = 24 * 3600 * 1000;
    const int64_t deadline_ms =
        conf.get("notification/resend_deadline_ms", int64_t{120000});
    if (deadline_ms > kMaxDeadlineMs) {
        LOG(WARNING) << "notification/resend_deadline_ms " << deadline_ms
                     << " clamped to " << kMaxDeadlineMs;
    }
    if (deadline_ms > 0) {
        config.resend_deadline =
            std::chrono::milliseconds(std::min(deadline_ms, kMaxDeadlineMs));
    }
    config.recv_backlog_max = loadNotifyRecvBacklogMax(conf);
    return config;
}

Status RdmaTransport::install(std::string& local_segment_name,
                              std::shared_ptr<ControlService> metadata,
                              std::shared_ptr<Topology> local_topology,
                              std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "RDMA transport has been installed" LOC_MARK);
    }

    if (!IbvLoader::Instance().available()) {
        return Status::InvalidArgument("RDMA transport not available" LOC_MARK);
    }

    if (local_topology == nullptr ||
        !local_topology->getNicCount(Topology::NIC_RDMA)) {
        return Status::DeviceNotFound(
            "No RDMA device found in topology" LOC_MARK);
    }

    conf_ = conf;
    params_ = std::make_shared<RdmaParams>();
    auto param_status = convertConfToRdmaParams(conf_, params_);
    if (!param_status.ok()) return param_status;
    metadata_ = metadata;
    const NotifyConfig notify = readNotifyConfig(*conf_);
    notify_rpc_fallback_ = notify.rpc_fallback;
    notify_path_probe_ = notify.path_probe;
    notify_resend_deadline_ = notify.resend_deadline;
    notify_backlog_max_ = notify.recv_backlog_max;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;

    // In dual-NIC environments (e.g. separate TCP and RDMA interfaces),
    // transports/rdma/bind_address allows NIC paths to use an
    // RDMA-reachable IP while local_segment_name_ keeps the
    // TCP-reachable address for P2P.
    const auto rdma_bind_addr = conf_->get("transports/rdma/bind_address", "");
    if (!rdma_bind_addr.empty()) {
        const uint16_t default_port = getRdmaBindDefaultPort(*conf_);
        auto [host_name, port] =
            parseHostNameWithPort(local_segment_name, default_port);
        rdma_server_name_ = rdma_bind_addr + ":" + std::to_string(port);
        LOG(INFO) << "RdmaTransport(TENT): using RDMA bind address "
                  << rdma_server_name_
                  << " (TCP address: " << local_segment_name_ << ")";
    } else {
        rdma_server_name_ = local_segment_name_;
    }

    local_buffer_manager_.setTopology(local_topology);
    const bool context_empty = initializeContexts() == 0;
    const bool topology_empty = local_topology_->empty();
    if (context_empty || topology_empty) {
        const char* error_message = "No RDMA device initialized successfully";
        uninstall();
        return Status::DeviceNotFound(std::string(error_message) + LOC_MARK);
    }

    if (conf_->get("verbose", false)) {
        local_topology_->print();
    }
    setupLocalSegment();

    metadata_->setBootstrapRdmaCallback(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2));

    workers_ = std::make_unique<Workers>(this);
    workers_->start();

    // A resend may bootstrap a new endpoint, so not on the completion poller.
    // Started before the notify worker: uninstall() joins it even when
    // install() fails later.
    notify_resend_running_ = true;
    notify_resend_worker_ =
        std::thread(&RdmaTransport::notifyResendThread, this);
    // Start notification worker thread
    notify_worker_running_ = true;
    notify_worker_ = std::thread(&RdmaTransport::notifyWorkerThread, this);

    installed_ = true;
    caps.dram_to_dram = true;
    if (isGpuDirectRdmaSupported(conf_)) {
        caps.dram_to_gpu = true;
        caps.gpu_to_dram = true;
        caps.gpu_to_gpu = true;
    }
    return Status::OK();
}

Status RdmaTransport::quiesce() {
    // Before the engine tears anything down: a resend still dials peers.
    {
        std::lock_guard<std::mutex> lock(notify_resend_mutex_);
        notify_resend_running_ = false;
        notify_resend_stopped_ = true;
    }
    notify_resend_cv_.notify_all();
    if (notify_resend_worker_.joinable()) notify_resend_worker_.join();
    uint64_t timeout_ns = kDefaultRdmaQuiesceTimeoutNs;
    if (conf_) {
        timeout_ns = conf_->get("transports/rdma/max_timeout_ns", timeout_ns);
    }
    Status drain = Status::OK();
    if (workers_) {
        drain = workers_->quiesce(timeout_ns);
        if (!drain.ok()) {
            LOG(ERROR) << "RDMA workers quiesce failed: " << drain.ToString();
        }
    }
    const Status sync =
        Platform::getLoader().synchronizeDevices(local_topology_.get());
    if (!sync.ok()) {
        LOG(WARNING) << "RDMA dest-GPU sync during quiesce failed: "
                     << sync.ToString();
    }
    return drain;
}

Status RdmaTransport::uninstall() {
    // ControlService may still receive BootstrapRdma RPCs while uninstall is
    // running. Unregister and drain the callback before destroying workers,
    // contexts, and other state used by onSetupRdmaConnections(). Keep this
    // outside installed_ so partially-installed transports are covered too.
    if (metadata_) metadata_->setBootstrapRdmaCallback(nullptr);

    // Drain CQ and dest-GPU visibility while MRs and QPs are still alive.
    // Idempotent when deconstruct() already called quiesce().
    (void)quiesce();

    if (installed_) {
        // Stop notification worker thread
        notify_worker_running_ = false;
        if (notify_worker_.joinable()) {
            notify_worker_.join();
        }

        workers_.reset();
        // Workers joined: nothing can name an orphan any more.
        {
            std::lock_guard<std::mutex> guard(orphan_slice_mutex_);
            for (auto& orphan : orphan_slices_)
                RdmaSliceStorage::Get().deallocate(orphan.slice);
            orphan_slices_.clear();
            orphans_pending_.store(false, std::memory_order_release);
        }
        metadata_.reset();
        local_buffer_manager_.clear();
        context_set_.clear();
        context_name_lookup_.clear();
        installed_ = false;
    }
    return Status::OK();
}

Status RdmaTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto rdma_batch = Slab<RdmaSubBatch>::Get().allocate();
    if (!rdma_batch)
        return Status::InternalError(
            "Unable to allocate RDMA sub-batch" LOC_MARK);
    batch = rdma_batch;
    rdma_batch->task_list.reserve(max_size);
    rdma_batch->max_size = max_size;
    return Status::OK();
}

Status RdmaTransport::freeSubBatch(SubBatchRef& batch) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch*>(batch);
    if (!rdma_batch)
        return Status::InvalidArgument("Invalid RDMA sub-batch" LOC_MARK);
    // Settle what an earlier free left behind before adding to it: an orphan
    // whose handler has since finished is freed here rather than waiting for
    // the monitor's next second.
    if (orphans_pending_.load(std::memory_order_acquire))
        reapOrphanSlices(/*on_tick=*/false);
    for (auto* task : rdma_batch->task_list) {
        task->deref();  // Release batch's reference to the task
    }
    rdma_batch->task_list.clear();
    // A slice still owed a completion cannot go back to the slab: its
    // address is a live work-request id, and the next transfer would get
    // that storage, so the stale completion would resolve somebody else's
    // slice and report bytes that never moved. The batch being terminal
    // does not rule this out -- a timeout resolves a slice while its work
    // request is live. Hand those over; the batch itself is freed now.
    std::vector<OrphanSlice> orphans;
    for (auto slice : rdma_batch->slice_chain) {
        while (slice) {
            auto next = slice->next;
            if (slice->completions_owed.load(std::memory_order_acquire) > 0) {
                slice->next = nullptr;  // it leaves the chain behind
                orphans.push_back({slice, 0});
            } else {
                RdmaSliceStorage::Get().deallocate(slice);
            }
            slice = next;
        }
    }
    if (!orphans.empty()) {
        std::lock_guard<std::mutex> guard(orphan_slice_mutex_);
        orphan_slices_.insert(orphan_slices_.end(), orphans.begin(),
                              orphans.end());
        orphans_pending_.store(true, std::memory_order_release);
    }
    Slab<RdmaSubBatch>::Get().deallocate(rdma_batch);
    batch = nullptr;
    return Status::OK();
}

Status RdmaTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch*>(batch);
    if (!rdma_batch)
        return Status::InvalidArgument("Invalid RDMA sub-batch" LOC_MARK);
    if (request_list.size() + rdma_batch->task_list.size() >
        rdma_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    const size_t default_block_size = params_->workers.block_size;
    const int num_workers = params_->workers.num_workers;
    std::vector<RdmaSliceList> slice_lists(num_workers);
    std::vector<RdmaSlice*> slice_tails(num_workers, nullptr);
    auto enqueue_ts = getCurrentTimeInNano();

    // Distribute starting worker across threads to avoid contention
    static std::atomic<int> g_caller_threads(0);
    thread_local int tl_caller_id = g_caller_threads.fetch_add(1);
    int next_worker_idx = tl_caller_id;
    for (auto& request : request_list) {
        auto opcode = request.opcode;
        auto type = Platform::getLoader().getMemoryType(request.source);
        size_t max_slice_count = 64;
        if (type == MTYPE_CUDA || opcode == Request::WRITE)
            max_slice_count = 32;
        auto* task = RdmaTaskStorage::Get().allocate();
        rdma_batch->task_list.push_back(task);
        task->request = request;
        task->device_mask = rdma_batch->device_mask;
        task->qp_pool = rdma_batch->qp_pool;  // RFC #2568 step 3
        task->num_slices = 0;
        task->status_word = PENDING;
        task->transferred_bytes = 0;
        task->success_slices.store(0, std::memory_order_relaxed);
        task->resolved_slices.store(0, std::memory_order_relaxed);
        task->first_error = PENDING;
        task->cancel_requested.store(false, std::memory_order_relaxed);
        task->ref();  // Batch holds a reference to the task

        const auto plan =
            planRdmaSlices(request.length, default_block_size, max_slice_count);
        const uint64_t block_size = plan.block_size;
        const uint64_t num_slices = plan.count;

        std::vector<int> slice_dev_ids;
        // Only if a single request is enough, we perform aggregated allocation
        if (num_slices >= max_slice_count / 2) {
            auto status = workers_->allocateDevices(
                request, static_cast<uint32_t>(num_slices), block_size,
                task->device_mask, slice_dev_ids);
            if (!status.ok()) {
                LOG(WARNING)
                    << "Device quota allocation failed: " << status.message();
            }
        }

        uint64_t offset = 0;
        for (uint64_t slice_idx = 0; slice_idx < num_slices; ++slice_idx) {
            // The last slice takes what is left, which is a block plus a
            // folded-in tail when the plan folded one; every other slice is
            // exactly a block. See planRdmaSlices().
            uint64_t length = (slice_idx + 1 == num_slices)
                                  ? request.length - offset
                                  : block_size;
            auto slice = RdmaSliceStorage::Get().allocate();
            slice->source_addr = (char*)request.source + offset;
            slice->target_addr = request.target_offset + offset;
            slice->length = length;
            slice->task = task;
            slice->retry_count = 0;
            slice->last_fallback_idx = -1;
            slice->charged_dev = -1;
            slice->posted_dev = -1;
            slice->counted_lane = -1;
            slice->ep_weak_ptr.reset();
            slice->word = PENDING;
            slice->next = nullptr;
            slice->enqueue_ts = enqueue_ts;
            slice->priority = request.priority;  // Copy priority from request
            task->num_slices++;
            task->ref();  // Each slice holds a reference to the task
            if (slice_idx < slice_dev_ids.size()) {
                slice->source_dev_id = slice_dev_ids[slice_idx];
                slice->charged_dev = slice->source_dev_id;
            }
            offset += length;
            int part_id = next_worker_idx % num_workers;
            auto& list = slice_lists[part_id];
            auto& tail = slice_tails[part_id];
            list.num_slices++;
            next_worker_idx++;
            if (list.first) {
                tail->next = slice;
                tail = slice;
            } else {
                list.first = tail = slice;
            }
        }
    }

    for (int i = 0; i < num_workers; ++i) {
        if (slice_lists[i].first) {
            rdma_batch->slice_chain.push_back(slice_lists[i].first);
            workers_->submit(slice_lists[i], i);
        }
    }
    return Status::OK();
}

void RdmaTransport::reapOrphanSlices(bool on_tick) {
    // Taken whole so the scan runs outside the lock: expired() reaches the
    // endpoint's control block, which is a cache miss per slice once a spell
    // of timeouts has filled this up. Survivors are compacted in place and
    // the same buffer goes back, so a tick allocates nothing. A batch freed
    // meanwhile appends to the emptied vector; those are kept too, and
    // nothing here reads the order.
    std::vector<OrphanSlice> taken;
    {
        std::lock_guard<std::mutex> guard(orphan_slice_mutex_);
        taken.swap(orphan_slices_);
    }
    if (taken.empty()) return;

    size_t kept = 0;
    size_t warned = 0;
    for (auto& orphan : taken) {
        auto* slice = orphan.slice;
        // The completion has been handled, or its endpoint is gone -- a
        // provider clears a queue pair's completions when it is destroyed
        // (mlx5 and mlx4 do), so none can name this slice afterwards.
        // Exactly zero: a handler pays on its way out and submitSlices()
        // counts the post under the queue pair lock that handler needs, so
        // the count never goes negative; if it ever did, holding on is the
        // safe way to be wrong.
        const int owed =
            slice->completions_owed.load(std::memory_order_acquire);
        if (owed == 0 || slice->ep_weak_ptr.expired()) {
            RdmaSliceStorage::Get().deallocate(slice);
            continue;
        }
        // Still held, so its endpoint is still alive: a queue pair the
        // teardown has not destroyed. Say so once, then keep holding -- an
        // age cap would free a slice the completion queue may yet name.
        if (on_tick && ++orphan.passes == orphan_warn_after_passes_) {
            ++warned;
            LOG(WARNING) << "Orphaned slice " << slice << " still owes " << owed
                         << " completion(s) after " << orphan.passes
                         << " reap passes and its endpoint is still alive; "
                            "held until the queue pair is destroyed -- see "
                            "the endpoint teardown log for why it is not";
        }
        taken[kept++] = orphan;
    }
    taken.resize(kept);

    std::lock_guard<std::mutex> guard(orphan_slice_mutex_);
    orphan_warnings_ += warned;
    if (!orphan_slices_.empty())
        taken.insert(taken.end(), orphan_slices_.begin(), orphan_slices_.end());
    orphan_slices_.swap(taken);
    orphans_pending_.store(!orphan_slices_.empty(), std::memory_order_release);
}

Status RdmaTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                        TransferStatus& status) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch*>(batch);
    if (task_id < 0 || task_id >= (int)rdma_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    }
    auto* task = rdma_batch->task_list[task_id];
    status = TransferStatus{task->status_word, task->transferred_bytes};
    return Status::OK();
}

Status RdmaTransport::cancelTransferTask(SubBatchRef batch, int task_id) {
    auto* rdma_batch = dynamic_cast<RdmaSubBatch*>(batch);
    if (!rdma_batch) {
        return Status::InvalidArgument("Invalid RDMA sub-batch" LOC_MARK);
    }
    if (task_id < 0 || task_id >= (int)rdma_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    }
    auto* task = rdma_batch->task_list[task_id];
    if (task->status_word != PENDING) return Status::OK();
    return workers_->cancel(task);
}

Status RdmaTransport::getNicLoadStats(std::vector<NicLoadStats>& stats) const {
    return workers_->getDeviceSelector()->getNicLoadStats(stats);
}

bool RdmaTransport::warmupMemory(void* addr, size_t length) {
    if (length < kMrWarmupMinBytes) return false;
    // Warm-up is a plain ibv_reg_mr. Intel XPU VRAM has no host pages behind
    // it and is registered through dma-buf export instead, so the temporary
    // ibv_reg_mr would only EFAULT.
    if (Platform::getLoader().getMemoryType(addr) == MTYPE_XPU) return false;
    unsigned hwc = std::thread::hardware_concurrency();
    if (hwc < 4) return false;
    RdmaContext* warmup_ctx = nullptr;
    for (auto& ctx : context_set_) {
        if (ctx && ctx->status() == RdmaContext::DEVICE_ENABLED) {
            warmup_ctx = ctx.get();
            break;
        }
    }
    if (!warmup_ctx) return false;
    int ret = warmupMrRegistrationParallel(warmup_ctx, addr, length);
    if (ret != 0) {
        LOG(WARNING) << "MR warm-up failed (rc=" << ret
                     << "), falling back to cold registration";
        return false;
    }
    VLOG(1) << "MR warm-up succeeded for " << length << " bytes";
    return true;
}

Status RdmaTransport::addMemoryBuffer(BufferDesc& desc,
                                      const MemoryOptions& options) {
    CHECK_STATUS(local_buffer_manager_.addBuffer(desc, options));
    desc.transports.push_back(TransportType::RDMA);
    return Status::OK();
}

Status RdmaTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                      const MemoryOptions& options) {
    CHECK_STATUS(local_buffer_manager_.addBuffer(desc_list, options));
    for (auto& desc : desc_list) {
        desc.transports.push_back(TransportType::RDMA);
    }
    return Status::OK();
}

Status RdmaTransport::removeMemoryBuffer(BufferDesc& desc) {
    return local_buffer_manager_.removeBuffer(desc);
}

Status RdmaTransport::refreshLocalDeviceDesc(const std::string& device_name,
                                             uint32_t lid,
                                             const std::string& gid) {
    if (!metadata_)
        return Status::InvalidArgument(
            "RDMA metadata is not initialized" LOC_MARK);

    auto& manager = metadata_->segmentManager();
    bool existed = false;
    uint32_t previous_lid = 0;
    std::string previous_gid;
    CHECK_STATUS(manager.updateLocal([&](SegmentDesc& segment) -> Status {
        if (segment.type != SegmentType::Memory)
            return Status::InvalidArgument(
                "Local segment is not a memory segment" LOC_MARK);
        auto* device = segment.findDevice(device_name);
        if (!device) {
            auto& detail = std::get<MemorySegmentDesc>(segment.detail);
            DeviceDesc added;
            added.name = device_name;
            added.lid = lid;
            added.gid = gid;
            detail.devices.push_back(std::move(added));
            return Status::OK();
        }
        existed = true;
        previous_lid = device->lid;
        previous_gid = device->gid;
        device->lid = lid;
        device->gid = gid;
        return Status::OK();
    }));

    auto status = manager.synchronizeLocal();
    if (status.ok()) return status;

    auto rollback = manager.updateLocal([&](SegmentDesc& segment) -> Status {
        if (segment.type != SegmentType::Memory)
            return Status::InvalidArgument(
                "Local segment is not a memory segment" LOC_MARK);
        auto& devices = std::get<MemorySegmentDesc>(segment.detail).devices;
        auto it = std::find_if(devices.begin(), devices.end(),
                               [&](const DeviceDesc& device) {
                                   return device.name == device_name;
                               });
        if (it == devices.end()) return Status::OK();
        if (!existed) {
            devices.erase(it);
        } else {
            it->lid = previous_lid;
            it->gid = previous_gid;
        }
        return Status::OK();
    });
    if (!rollback.ok()) {
        LOG(ERROR) << "Failed to roll back RDMA address metadata for "
                   << device_name << ": " << rollback.ToString();
    }
    return status;
}

Status RdmaTransport::setupLocalSegment() {
    auto& manager = metadata_->segmentManager();
    CHECK_STATUS(manager.updateLocal([&](SegmentDesc& segment) -> Status {
        // Store RDMA server name for dual-NIC setups; when it differs from
        // local_segment_name_ the peer will use it for NIC path construction.
        if (rdma_server_name_ != local_segment_name_) {
            segment.rdma_server_name = rdma_server_name_;
        }
        auto& detail = std::get<MemorySegmentDesc>(segment.detail);
        for (auto& context : context_set_) {
            if (context->status() != RdmaContext::DEVICE_ENABLED) continue;
            DeviceDesc device_desc;
            device_desc.name = context->name();
            const auto address = context->address();
            device_desc.lid = address.lid;
            device_desc.gid = address.gid;
            detail.devices.push_back(device_desc);
        }
        return Status::OK();
    }));
    return manager.synchronizeLocal();
}

int RdmaTransport::onSetupRdmaConnections(const BootstrapDesc& peer_desc,
                                          BootstrapDesc& local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty() || !context_name_lookup_.count(local_nic_name)) {
        std::stringstream ss;
        ss << "No device found in local segment: " << local_nic_name;
        LOG(ERROR) << ss.str();
        local_desc.reply_msg = ss.str();
        return -1;
    }
    auto index = context_name_lookup_[local_nic_name];
    auto context = context_set_[index];
    auto ctx_status = context->status();
    if (ctx_status != RdmaContext::DEVICE_ENABLED) {
        std::stringstream ss;
        ss << "Device is not ready: " << peer_desc.local_nic_path;
        LOG(ERROR) << ss.str();
        local_desc.reply_msg = ss.str();
        return -1;
    }
    // Endpoints are never reset. A peer process that reused the same nic path
    // (same IP:port after a restart) hits an EP_READY endpoint whose QPs no
    // longer exist. accept() retires it and the next getOrInsert() creates a
    // fresh one. Do that retry inside this RPC so the initiator receives a
    // valid GID instead of an empty bootstrap reply.
    auto store = context->endpointStore();
    constexpr int kMaxAcceptAttempts = 2;
    for (int attempt = 0; attempt < kMaxAcceptAttempts; ++attempt) {
        auto endpoint = store->getOrInsert(peer_desc.local_nic_path);
        if (!endpoint) {
            std::stringstream ss;
            ss << "Cannot allocate endpoint: " << peer_desc.local_nic_path;
            LOG(ERROR) << ss.str();
            local_desc.reply_msg = ss.str();
            return -1;
        }
        local_desc = BootstrapDesc();
        auto status = endpoint->accept(peer_desc, local_desc);
        if (status.ok()) {
            local_desc.reply_msg.clear();
            return 0;
        }
        const auto ep_status = endpoint->status();
        const bool retired = ep_status == RdmaEndPoint::EP_DESTROYING ||
                             ep_status == RdmaEndPoint::EP_DESTROYED;
        if (retired) {
            store->remove(endpoint.get());
            if (attempt + 1 < kMaxAcceptAttempts) {
                LOG(INFO) << "Retrying RDMA bootstrap after retiring stale "
                             "endpoint for "
                          << peer_desc.local_nic_path;
                continue;
            }
        }
        LOG(ERROR) << status.ToString();
        local_desc.reply_msg = status.ToString();
        return -1;
    }

    return -1;
}

std::shared_ptr<RdmaEndPoint> RdmaTransport::getEndpoint(SegmentID target_id,
                                                         int device_id,
                                                         Status* failure) {
    // context_set_ is NicID-indexed, so slot 0 may be inert; take the first
    // enabled context instead.
    for (size_t i = 0; i < context_set_.size(); ++i) {
        if (context_set_[i]->status() == RdmaContext::DEVICE_ENABLED)
            return getEndpointOn(i, target_id, device_id, failure);
    }
    if (failure)
        *failure = Status::DeviceNotFound("No enabled RDMA context" LOC_MARK);
    return nullptr;
}

std::shared_ptr<RdmaEndPoint> RdmaTransport::getEndpointOn(size_t local_ctx,
                                                           SegmentID target_id,
                                                           int device_id,
                                                           Status* failure) {
    std::string rpc_server_addr, target_seg_name, target_dev_name,
        target_nic_path_name;

    auto status = metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) {
            if (segment->type != SegmentType::Memory) {
                return Status::NeedsRefreshCache(
                    "Segment type is not Memory" LOC_MARK);
            }

            if (target_id != LOCAL_SEGMENT_ID) {
                rpc_server_addr = segment->rpc_server_addr;
            }

            auto topo = &std::get<MemorySegmentDesc>(segment->detail).topology;
            target_seg_name = segment->name;
            target_nic_path_name = segment->nicPathServerName();
            target_dev_name = topo->getNicName(device_id);
            if (target_seg_name.empty() || target_dev_name.empty()) {
                return Status::NeedsRefreshCache(
                    "Empty target segment or device name" LOC_MARK);
            }
            return Status::OK();
        });

    if (!status.ok()) {
        LOG(ERROR) << status.ToString();
        if (failure) *failure = status;
        return nullptr;
    }

    RdmaContext* context = context_set_[local_ctx].get();
    if (context->status() != RdmaContext::DEVICE_ENABLED) {
        if (failure) {
            *failure =
                Status::DeviceNotFound("RDMA context not enabled" LOC_MARK);
        }
        return nullptr;
    }
    std::shared_ptr<RdmaEndPoint> endpoint;
    std::string peer_name = MakeNicPath(target_nic_path_name, target_dev_name);
    endpoint = context->endpointStore()->getOrInsert(peer_name);
    if (!endpoint) {
        LOG(ERROR) << "Cannot allocate endpoint " << peer_name;
        if (failure) {
            *failure =
                Status::InternalError("Cannot allocate endpoint" LOC_MARK);
        }
        return nullptr;
    }
    if (endpoint->status() != RdmaEndPoint::EP_READY) {
        auto status = endpoint->connect(target_seg_name, target_dev_name,
                                        rpc_server_addr);
        if (!status.ok()) {
            thread_local uint64_t tl_last_output_ts = 0;
            uint64_t current_ts = getCurrentTimeInNano();
            if (current_ts - tl_last_output_ts > 10000000000ull) {
                tl_last_output_ts = current_ts;
                LOG(ERROR) << "Unable to connect endpoint " << peer_name << ": "
                           << status.ToString();
            }
            if (failure) *failure = status;
            return nullptr;
        }
    }
    return endpoint;
}

Status RdmaTransport::notifyStatusForEndpointFailure(const Status& failure) {
    if (failure.IsRpcServiceError()) {
        return Status::RpcServiceError(
            "RDMA notification endpoint bootstrap failed; peer control "
            "plane unreachable, not falling back to RPC: " +
            std::string{failure.message()} + LOC_MARK);
    }
    return Status::DeviceNotFound("RDMA notification endpoint unavailable: " +
                                  std::string{failure.message()} + LOC_MARK);
}

Status RdmaTransport::sendNotification(SegmentID target_id,
                                       const Notification& notify) {
    // Both failures below mean nothing left this host. Unless the bootstrap
    // RPC itself failed (see notifyStatusForEndpointFailure), the engine may
    // still reach the peer over the control plane, so they are reported with
    // codes TransferEngineImpl::sendNotification() recognizes as "channel
    // unavailable" rather than as a generic internal error.
    Status failure;
    auto endpoint = getEndpoint(target_id, LOCAL_SEGMENT_ID, &failure);
    Status status;
    bool stalled = false;
    if (!endpoint) {
        status = notifyStatusForEndpointFailure(failure);
    } else {
        // Notify QP not connected (the peer has none, or it was disabled
        // after a fault), or the post itself was refused.
        PendingNotify track;
        track.target = target_id;
        if (endpoint->sendNotification(notify, &track)) return Status::OK();
        stalled = endpoint->notifySendStalled();
        status =
            Status::RdmaError("RDMA notification channel unavailable" LOC_MARK);
    }
    // Then the other pairs, once, dialed if pair 0 could not be built. After
    // a stall, pairs already up go first, then those to the peer's other NICs
    // are dialed, and an RPC error is returned: the fallback would wait too.
    if (!notify_path_probe_ || status.IsRpcServiceError()) return status;
    const PendingNotify pending{target_id, notify};
    Status probed = sendNotificationOnAnyPath(pending, /*dial=*/!endpoint,
                                              /*probe=*/true, stalled);
    if (!probed.ok() && stalled) {
        probed = sendNotificationOnAnyPath(pending, /*dial=*/true,
                                           /*probe=*/true, stalled);
        if (probed.IsRpcServiceError()) return probed;
    }
    return probed.ok() ? Status::OK() : status;
}

Status RdmaTransport::sendNotificationIfConnected(SegmentID target_id,
                                                  const Notification& notify) {
    // The pairs sendNotification() takes, looked up only: a segment fetch or
    // a bootstrap would wait on the peer's control plane.
    SegmentDescRef segment;
    std::shared_ptr<RdmaEndPoint> endpoint;
    if (metadata_->segmentManager().peekRemote(segment, target_id).ok() &&
        segment->type == SegmentType::Memory) {
        for (size_t i = 0; i < context_set_.size(); ++i) {
            if (context_set_[i]->status() != RdmaContext::DEVICE_ENABLED)
                continue;
            endpoint = peekEndpoint(i, *segment, 0);
            break;
        }
    }
    Status status =
        Status::DeviceNotFound("notify endpoint not connected" LOC_MARK);
    bool stalled = false;
    if (endpoint) {
        PendingNotify track;
        track.target = target_id;
        if (endpoint->sendNotification(notify, &track)) return Status::OK();
        stalled = endpoint->notifySendStalled();
        status =
            Status::RdmaError("RDMA notification channel unavailable" LOC_MARK);
    }
    if (notify_path_probe_ &&
        sendNotificationOnAnyPath({target_id, notify}, /*dial=*/false,
                                  /*probe=*/true, stalled)
            .ok())
        return Status::OK();
    return status;
}

std::shared_ptr<RdmaEndPoint> RdmaTransport::peekEndpoint(
    size_t local_ctx, const SegmentDesc& peer, int device_id) {
    const auto& topology = std::get<MemorySegmentDesc>(peer.detail).topology;
    auto endpoint = context_set_[local_ctx]->endpointStore()->get(
        MakeNicPath(peer.nicPathServerName(), topology.getNicName(device_id)));
    if (endpoint && endpoint->status() != RdmaEndPoint::EP_READY) return {};
    return endpoint;
}

Status RdmaTransport::sendNotificationOnAnyPath(const PendingNotify& pending,
                                                bool dial, bool probe,
                                                bool stalled) {
    std::vector<size_t> locals;
    for (size_t i = 0; i < context_set_.size(); ++i) {
        if (context_set_[i]->status() == RdmaContext::DEVICE_ENABLED)
            locals.push_back(i);
    }
    Status last = Status::DeviceNotFound("No RDMA notification path" LOC_MARK);
    // No local NIC enabled, so no pair: the peer need not be looked up.
    if (locals.empty()) return last;
    std::vector<bool> rdma_nic;  // by the peer's NicID
    auto take_nics = [&](SegmentDesc* segment) {
        if (segment->type != SegmentType::Memory) {
            return Status::NeedsRefreshCache(
                "Segment type is not Memory" LOC_MARK);
        }
        const auto& topology =
            std::get<MemorySegmentDesc>(segment->detail).topology;
        for (size_t nic = 0; nic < topology.getNicCount(); ++nic)
            rdma_nic.push_back(topology.getNicType(nic) == Topology::NIC_RDMA);
        return Status::OK();
    };
    auto& segments = metadata_->segmentManager();
    SegmentDescRef segment;
    auto status =
        dial ? segments.withCachedSegment(pending.target, segment, take_nics)
             : segments.peekRemote(segment, pending.target);
    if (status.ok() && !dial) status = take_nics(segment.get());
    if (!status.ok()) return notifyStatusForEndpointFailure(status);
    // Start after the pair that just failed, like the data path; that pair
    // comes last for a resend, and not at all for a new notification.
    const size_t pairs = locals.size() * rdma_nic.size();
    const size_t start = static_cast<size_t>(pending.last_path + 1);
    size_t tries = pairs;
    if (probe && tries > 0) --tries;
    for (size_t n = 0; n < tries; ++n) {
        const size_t i = (start + n) % pairs;
        const size_t remote = i % rdma_nic.size();
        // A stalled pair 0 makes the peer's NIC 0 suspect, whatever the pair.
        if (!rdma_nic[remote] || (stalled && remote == 0)) continue;
        Status failure;
        std::shared_ptr<RdmaEndPoint> endpoint;
        if (dial) {
            endpoint =
                getEndpointOn(locals[i / rdma_nic.size()], pending.target,
                              static_cast<int>(remote), &failure);
        } else {
            endpoint = peekEndpoint(locals[i / rdma_nic.size()], *segment,
                                    static_cast<int>(remote));
        }
        if (!endpoint) {
            last = notifyStatusForEndpointFailure(failure);
            if (last.IsRpcServiceError()) return last;  // no pair will do
            continue;
        }
        const PendingNotify track{pending.target,
                                  {},
                                  pending.attempts,
                                  static_cast<int>(i),
                                  pending.first_failed};
        if (endpoint->sendNotification(pending.notifi, &track,
                                       /*wait_for_slot=*/!probe))
            return Status::OK();
        last =
            Status::RdmaError("RDMA notification channel unavailable" LOC_MARK);
    }
    return last;
}

void RdmaTransport::requeueNotifications(std::vector<PendingNotify>&& drained) {
    {
        std::lock_guard<std::mutex> lock(notify_resend_mutex_);
        // E.g. endpoints retired by uninstall(): nothing would resend them.
        if (notify_resend_stopped_ && !drained.empty()) {
            LOG(WARNING) << "Dropped " << drained.size()
                         << " notification resend(s) after quiesce, "
                         << (notify_resend_dropped_ += drained.size())
                         << " so far";
            TENT_RECORD_NOTIFY_DROPPED(TentMetrics::NotifyDropReason::Shutdown,
                                       drained.size());
            return;
        }
        for (auto& pending : drained)
            notify_resend_queue_.push_back(std::move(pending));
        setResendDepthsLocked();
    }
    notify_resend_cv_.notify_one();
}

void RdmaTransport::setResendDepthsLocked() {
#if TENT_METRICS_ENABLED
    size_t held = 0;
    for (const auto& peer : notify_resend_held_)
        held += peer.second.resends.size();
    TENT_SET_NOTIFY_QUEUE_DEPTH(TentMetrics::NotifyQueue::Resend,
                                notify_resend_queue_.size());
    TENT_SET_NOTIFY_QUEUE_DEPTH(TentMetrics::NotifyQueue::ResendHeld, held);
#endif
}

bool RdmaTransport::resendOne(PendingNotify& pending, Clock::time_point now) {
    Status status =
        Status::RdmaError("notification resend budget spent" LOC_MARK);
    if (++pending.attempts <= kMaxNotifyResendAttempts) {
        status = sendNotificationOnAnyPath(pending, /*dial=*/true,
                                           /*probe=*/false);
        if (status.ok()) return true;
    }
    // Past the RDMA rounds, or with none left: the control-plane RPC, the
    // same leg TcpTransport uses. Not for a peer whose control plane did
    // not answer.
    if (notify_rpc_fallback_ && !status.IsRpcServiceError() && metadata_) {
        status = metadata_->segmentManager().withPeerRpcAddr(
            pending.target, [&](const std::string& rpc_server_addr) {
                return ControlClient::notify(rpc_server_addr, pending.notifi);
            });
        if (status.ok()) return true;
    }
    // A stopped or restarting peer does not answer, or does not take it yet:
    // hold the resend rather than drop it, and do not count the round.
    if (holdsResend(status)) {
        if (now == Clock::time_point{}) now = Clock::now();
        if (pending.first_failed == Clock::time_point{})
            pending.first_failed = now;
        const auto give_up = pending.first_failed + notify_resend_deadline_;
        if (now <= give_up) {
            --pending.attempts;
            pending.next_try =
                now + std::min<Clock::duration>(kNotifyResendHoldInterval,
                                                give_up - now);
            return false;
        }
    }
    pending.next_try = {};
    const uint64_t dropped = ++notify_resend_dropped_;
    TENT_RECORD_NOTIFY_DROPPED(
        holdsResend(status) ? TentMetrics::NotifyDropReason::ResendDeadline
                            : TentMetrics::NotifyDropReason::ResendExhausted,
        1);
    LOG_EVERY_N(WARNING, 100)
        << "Dropped a notification to segment " << pending.target << " after "
        << pending.attempts << " attempt(s), " << dropped
        << " so far: " << status.ToString();
    return false;
}

void RdmaTransport::notifyResendThread() {
    std::unique_lock<std::mutex> lock(notify_resend_mutex_);
    auto next_due = [this] {
        auto due = Clock::time_point::max();
        for (const auto& held : notify_resend_held_)
            due = std::min(due, held.second.due);
        return due;
    };
    while (true) {
        setResendDepthsLocked();
        auto runnable = [&] {
            return !notify_resend_running_ || !notify_resend_queue_.empty() ||
                   next_due() <= Clock::now();
        };
        const auto due = next_due();
        if (due == Clock::time_point::max()) {
            notify_resend_cv_.wait(lock, runnable);
        } else {
            notify_resend_cv_.wait_until(lock, due, runnable);
        }
        if (!notify_resend_running_) {
            // Nothing restarts this thread: what it leaves is never sent.
            size_t left = notify_resend_queue_.size();
            for (const auto& held : notify_resend_held_)
                left += held.second.resends.size();
            if (left > 0) {
                LOG(WARNING) << "Dropped " << left
                             << " notification resend(s) on shutdown, "
                             << (notify_resend_dropped_ += left) << " so far";
                TENT_RECORD_NOTIFY_DROPPED(
                    TentMetrics::NotifyDropReason::Shutdown, left);
                TENT_SET_NOTIFY_QUEUE_DEPTH(TentMetrics::NotifyQueue::Resend,
                                            0);
                TENT_SET_NOTIFY_QUEUE_DEPTH(
                    TentMetrics::NotifyQueue::ResendHeld, 0);
            }
            return;
        }
        PendingNotify pending;
        auto held =
            std::find_if(notify_resend_held_.begin(), notify_resend_held_.end(),
                         [now = Clock::now()](const auto& entry) {
                             return entry.second.due <= now;
                         });
        if (held != notify_resend_held_.end()) {
            pending = std::move(held->second.resends.front());
            held->second.resends.pop_front();
        } else if (!notify_resend_queue_.empty()) {
            pending = std::move(notify_resend_queue_.front());
            notify_resend_queue_.pop_front();
            held = notify_resend_held_.find(pending.target);
            if (held != notify_resend_held_.end()) {
                if (pending.first_failed == Clock::time_point{})
                    pending.first_failed = Clock::now();
                held->second.resends.push_back(std::move(pending));
                continue;
            }
        } else {
            continue;
        }
        // The resend in hand may take an RPC timeout; it is in neither queue.
        setResendDepthsLocked();
        lock.unlock();
        bool delivered = false;
        pending.next_try = {};
        try {
            delivered = resendOne(pending);
            if (delivered) TENT_RECORD_NOTIFY_RESENT();
        } catch (const std::exception& e) {
            // E.g. a msg the RPC leg cannot encode: nothing above us catches.
            const uint64_t dropped = ++notify_resend_dropped_;
            TENT_RECORD_NOTIFY_DROPPED(
                TentMetrics::NotifyDropReason::ResendEncode, 1);
            LOG_EVERY_N(WARNING, 100)
                << "Dropped a notification to segment " << pending.target
                << ", " << dropped << " so far: " << e.what();
        }
        lock.lock();
        const SegmentID target = pending.target;
        if (!delivered && pending.next_try != Clock::time_point{}) {
            auto& peer = notify_resend_held_[target];
            peer.due = pending.next_try;
            peer.resends.push_front(std::move(pending));
            continue;
        }
        held = notify_resend_held_.find(target);
        if (held == notify_resend_held_.end()) continue;
        // The oldest is through: retry the rest, but not those out of time.
        const auto now = Clock::now();
        size_t expired = 0;
        for (auto& rest : held->second.resends) {
            if (!delivered && now - rest.first_failed > notify_resend_deadline_)
                ++expired;
            else
                notify_resend_queue_.push_back(std::move(rest));
        }
        notify_resend_held_.erase(held);
        if (expired > 0) {
            LOG(WARNING) << "Dropped " << expired
                         << " notification resend(s) to segment " << target
                         << " past the deadline, "
                         << (notify_resend_dropped_ += expired) << " so far";
            TENT_RECORD_NOTIFY_DROPPED(
                TentMetrics::NotifyDropReason::ResendDeadline, expired);
        }
    }
}

Status RdmaTransport::receiveNotification(
    std::vector<Notification>& notify_list) {
    std::unique_lock<std::mutex> lock(notify_mutex_);
    if (notify_list_.empty()) {
        return Status::OK();
    }
    // Arrivals wait on this lock: take the queue whole, move it out after.
    std::deque<Notification> taken;
    taken.swap(notify_list_);
    TENT_SET_NOTIFY_QUEUE_DEPTH(TentMetrics::NotifyQueue::RecvRdma, 0);
    lock.unlock();
    notify_list.assign(std::make_move_iterator(taken.begin()),
                       std::make_move_iterator(taken.end()));
    return Status::OK();
}

void RdmaTransport::addNotificationToQueue(const Notification& notifi) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    if (notify_list_.size() >= notify_backlog_max_) {
        notify_list_.pop_front();
        const uint64_t dropped = ++notify_backlog_dropped_;
        TENT_RECORD_NOTIFY_DROPPED(TentMetrics::NotifyDropReason::RecvBacklog,
                                   1);
        LOG_EVERY_N(WARNING, 1000)
            << "Notification receive queue recv_rdma full at "
            << notify_backlog_max_ << ", dropped the oldest unread, " << dropped
            << " so far";
    }
    notify_list_.push_back(notifi);
    TENT_RECORD_NOTIFY_RECEIVED(TentMetrics::NotifyQueue::RecvRdma,
                                notify_list_.size());
}

namespace {
// The notify QP carries its own host-memory send/recv buffers, so a local
// length/protection/WQE fault is confined to notification state. The data QPs
// of the same endpoint use separate WRs and MRs.
bool isNotifyLocalFault(ibv_wc_status status) {
    switch (status) {
        case IBV_WC_LOC_LEN_ERR:
        case IBV_WC_LOC_QP_OP_ERR:
        case IBV_WC_LOC_PROT_ERR:
        case IBV_WC_LOC_ACCESS_ERR:
        case IBV_WC_MW_BIND_ERR:
            return true;
        default:
            return false;
    }
}
}  // namespace

RdmaTransport::NotifyCompletionAction RdmaTransport::classifyNotifyCompletion(
    ibv_wc_status status, bool endpoint_alive, bool endpoint_ready) {
    // Every WR still posted on a retiring endpoint's notify QP flushes, which
    // is expected and must stay quiet.
    if (status == IBV_WC_WR_FLUSH_ERR && !endpoint_ready) {
        return NotifyCompletionAction::SkipSilently;
    }
    if (!endpoint_alive) return NotifyCompletionAction::ReportOnly;
    if (isNotifyLocalFault(status)) {
        return NotifyCompletionAction::DisableNotification;
    }
    return NotifyCompletionAction::RetireEndpoint;
}

int RdmaTransport::processNotifyCompletions() {
    int total_completions = 0;

    // Poll notification CQ from all contexts
    for (auto& context : context_set_) {
        auto notify_cq = context->notifyCq();
        if (!notify_cq) continue;

        ibv_wc wc[16];
        int completed = ibv_poll_cq(notify_cq->cq(), 16, wc);

        if (completed < 0) {
            PLOG(ERROR) << "Failed to poll notification CQ";
            continue;
        }

        if (completed == 0) continue;

        // Process each completion
        for (int i = 0; i < completed; ++i) {
            // Find endpoint by QP number before interpreting errors. The QP
            // stays published while the endpoint retires, so notifications
            // that landed before the retirement are still handed out; the
            // flushes that follow are told apart by the endpoint's state.
            std::shared_ptr<RdmaEndPoint> endpoint;
            {
                RWSpinlock::ReadGuard guard(notify_endpoint_map_lock_);
                auto it = notify_qp_to_endpoint_.find(wc[i].qp_num);
                if (it != notify_qp_to_endpoint_.end()) {
                    endpoint = it->second.lock();
                }
            }

            // Released only once this completion has been consumed: the
            // recv payload copied out of its slot, or the send / error path
            // finished. finishDestroy() waits for this count and
            // deconstructUnlocked() then frees the recv MR, so releasing it
            // earlier would let the buffers go while the CQE is still in
            // this batch. The ring depth and the CQ's completion order keep
            // that from happening today; this makes it hold by construction,
            // the way acknowledge() releases wr_depth on the data QPs only
            // after the completion is handled.
            struct NotifyInflightRelease {
                RdmaEndPoint* ep;
                ~NotifyInflightRelease() {
                    if (ep) ep->noteNotifyCompletion();
                }
            } inflight_release{endpoint.get()};

            if (wc[i].status != IBV_WC_SUCCESS) {
                // A failed completion leaves this notify QP unusable for good
                // and only the endpoint lifecycle builds a new one, so left
                // alone the endpoint stays EP_READY and every later
                // sendNotification() silently flushes. Retiring it also moves
                // the data QPs to ERR, so that is reserved for faults which may
                // mean the peer restarted or the path died. A notify QP that
                // is retiring or already disabled only flushes from here on,
                // and those completions stay quiet.
                const bool endpoint_ready =
                    endpoint && endpoint->status() == RdmaEndPoint::EP_READY &&
                    endpoint->notifyConnected();
                auto action = classifyNotifyCompletion(
                    wc[i].status, endpoint != nullptr, endpoint_ready);
                if (action == NotifyCompletionAction::SkipSilently) continue;

                LOG(ERROR) << "Notification completion failed: " << wc[i].status
                           << ", qp_num=" << wc[i].qp_num;
                if (action == NotifyCompletionAction::DisableNotification) {
                    endpoint->disableNotification(
                        "notify QP local completion error");
                } else if (action == NotifyCompletionAction::RetireEndpoint) {
                    endpoint->resetConnection("notify QP completion error");
                }
                continue;
            }

            if (!endpoint) {
                // The peer's NIC acked it, so the sender will not resend it.
                if (wc[i].opcode == IBV_WC_RECV)
                    TENT_RECORD_NOTIFY_DROPPED(
                        TentMetrics::NotifyDropReason::RecvEndpointGone, 1);
                LOG(WARNING) << "Received notification from unknown QP: "
                             << wc[i].qp_num;
                continue;
            }

            // Handle RECV completions: parse and add to transport queue
            if (wc[i].opcode == IBV_WC_RECV) {
                endpoint->handleNotifyRecv(wc[i].wr_id, wc[i].byte_len);
            } else if (wc[i].opcode == IBV_WC_SEND) {
                // Handle SEND completions: cleanup pending sends
                endpoint->handleNotifySendComplete(wc[i].wr_id);
            }
        }
    }

    return total_completions;
}

void RdmaTransport::registerNotifyQp(
    uint32_t qp_num, const std::shared_ptr<RdmaEndPoint>& endpoint) {
    RWSpinlock::WriteGuard guard(notify_endpoint_map_lock_);
    notify_qp_to_endpoint_[qp_num] = endpoint;
}

void RdmaTransport::unregisterNotifyQp(uint32_t qp_num) {
    RWSpinlock::WriteGuard guard(notify_endpoint_map_lock_);
    notify_qp_to_endpoint_.erase(qp_num);
}

void RdmaTransport::notifyWorkerThread() {
    while (notify_worker_running_) {
        processNotifyCompletions();
        usleep(notify_poll_interval_us_);
    }
}

double RdmaTransport::getEstimatedBandwidth() const {
    if (!workers_) return -1.0;
    auto* sel = workers_->getDeviceSelector();
    if (!sel) return -1.0;
    // The transmit estimate, not the selection EWMA: the admission queue
    // asks "how fast do bytes move once they are sent", and adds the wait
    // behind earlier work itself (DeadlineMlu's bytes_ahead), so the rate
    // must not fold that wait in the way the selection sample does.
    return sel->getAggregateTransmitBandwidth();
}

}  // namespace tent
}  // namespace mooncake
