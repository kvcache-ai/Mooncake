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

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
#endif

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <future>
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

#ifdef USE_CUDA
// Driver-API probe: true iff this process already has a primary context on
// `device`. Does not create one. Topology lists every visible GPU for NIC
// affinity; cudaSetDevice on those names would allocate idle-card contexts
// (Tone runs --gpus all without CUDA_VISIBLE_DEVICES).
bool cudaPrimaryContextIsActive(int device) {
    if (cuInit(0) != CUDA_SUCCESS) return false;
    CUdevice cu_dev = 0;
    if (cuDeviceGet(&cu_dev, device) != CUDA_SUCCESS) return false;
    unsigned int flags = 0;
    int active = 0;
    if (cuDevicePrimaryCtxGetState(cu_dev, &flags, &active) != CUDA_SUCCESS) {
        return false;
    }
    return active != 0;
}

bool cudaHasCurrentContext() {
    if (cuInit(0) != CUDA_SUCCESS) return false;
    CUcontext ctx = nullptr;
    return cuCtxGetCurrent(&ctx) == CUDA_SUCCESS && ctx != nullptr;
}

// Walk topology CUDA devices that already have a primary context. Restores
// the caller's current device only when this thread already had one — a
// bare cudaGetDevice() can implicitly create GPU 0. `op` is a log tag
// (e.g. "quiesce", "gdr-flush").
template <typename Fn>
void forEachTopologyCudaDevice(const Topology* topology, const char* op,
                               Fn&& on_device) {
    if (!topology) return;

    const size_t mem_count = topology->getMemCount();
    bool any_cuda = false;
    for (size_t i = 0; i < mem_count; ++i) {
        const auto* mem =
            topology->getMemEntry(static_cast<Topology::MemID>(i));
        if (mem && mem->type == Topology::MEM_CUDA) {
            any_cuda = true;
            break;
        }
    }
    if (!any_cuda) return;

    int device_count = 0;
    cudaError_t err = cudaGetDeviceCount(&device_count);
    if (err != cudaSuccess || device_count <= 0) {
        if (err != cudaSuccess) {
            LOG(WARNING) << "RDMA " << op << " cudaGetDeviceCount failed: "
                         << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
        return;
    }

    int saved = 0;
    const bool have_saved =
        cudaHasCurrentContext() && cudaGetDevice(&saved) == cudaSuccess;
    if (!have_saved) (void)cudaGetLastError();

    for (size_t i = 0; i < mem_count; ++i) {
        const auto* mem =
            topology->getMemEntry(static_cast<Topology::MemID>(i));
        if (!mem || mem->type != Topology::MEM_CUDA) continue;
        LocationParser parser(mem->name);
        const int device = parser.index();
        if (device < 0 || device >= device_count) continue;
        if (!cudaPrimaryContextIsActive(device)) continue;
        err = cudaSetDevice(device);
        if (err != cudaSuccess) {
            LOG(WARNING) << "RDMA " << op << " cudaSetDevice(" << device
                         << ") failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
            continue;
        }
        on_device(device);
    }
    if (have_saved) {
        err = cudaSetDevice(saved);
        if (err != cudaSuccess) {
            LOG(WARNING) << "RDMA " << op << " restore cudaSetDevice(" << saved
                         << ") failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
    }
}
#endif

// After a transfer-done notify, make GPUDirect RDMA writes visible to the dest
// GPU owner before the caller consumes the buffer. Host-side flush, not a
// full device synchronize. Empty topology / unsupported devices are no-ops.
Status flushDestGpuDirectWrites(const Topology* topology) {
#ifdef USE_CUDA
#if defined(CUDART_VERSION) && CUDART_VERSION >= 11030
    forEachTopologyCudaDevice(topology, "gdr-flush", [](int device) {
        int options = 0;
        cudaError_t err = cudaDeviceGetAttribute(
            &options, cudaDevAttrGPUDirectRDMAFlushWritesOptions, device);
        if (err != cudaSuccess) {
            LOG(WARNING) << "RDMA gdr-flush cudaDeviceGetAttribute device "
                         << device << " failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
            return;
        }
        if ((options & cudaFlushGPUDirectRDMAWritesOptionHost) == 0) {
            LOG_FIRST_N(WARNING, 8)
                << "RDMA gdr-flush host option unsupported on device "
                << device;
            return;
        }
        err = cudaDeviceFlushGPUDirectRDMAWrites(
            cudaFlushGPUDirectRDMAWritesTargetCurrentDevice,
            cudaFlushGPUDirectRDMAWritesToOwner);
        if (err != cudaSuccess) {
            LOG(WARNING) << "RDMA gdr-flush device " << device
                         << " failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
            return;
        }
        LOG_FIRST_N(INFO, 1)
            << "RDMA dest GPUDirect write flush enabled on device " << device;
    });
#else
    (void)topology;
#endif
#else
    (void)topology;
#endif
    return Status::OK();
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
    params->with_nvidia_peermem =
        conf->get("transports/rdma/with_nvidia_peermem", false);
    return Status::OK();
}

static bool isPeermemModuleLoaded() {
    // NVIDIA: nvidia_peermem. AMD: peermem is built into amdgpu (linked with
    // ib_core), so the amdgpu module itself is the presence signal.
    std::ifstream modules("/proc/modules");
    std::string line;
    while (std::getline(modules, line)) {
        const auto name_end = line.find(' ');
        const auto name =
            name_end == std::string::npos ? line : line.substr(0, name_end);
        if (name == "nvidia_peermem" || name == "amdgpu") {
            return true;
        }
    }
    return false;
}

static bool isGpuDirectRdmaSupported(std::shared_ptr<Config> conf) {
    auto disable_gpu_direct =
        conf->get("transports/rdma/disable_gpu_direct_rdma", false);
    if (disable_gpu_direct) {
        return false;
    }
    const bool with_nvidia_peermem =
        conf->get("transports/rdma/with_nvidia_peermem", false);
    if (with_nvidia_peermem) {
        return isPeermemModuleLoaded();
    }
    // Default: DMA-BUF first. Some GPUs/drivers advertise no DMA-BUF (H20
    // here: CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED=0, export returns 801) and
    // still do GDR through nvidia-peermem. Keep gpu_to_gpu on in that case so
    // registration can fall back to ibv_reg_mr instead of host-bounce.
    return RdmaContext::dmaBufRegistrationAvailable() ||
           isPeermemModuleLoaded();
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
        if (entry->type == Topology::NIC_RDMA) {
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

    // Start notification worker thread
    notify_worker_running_ = true;
    notify_worker_ = std::thread(&RdmaTransport::notifyWorkerThread, this);

    installed_ = true;
    caps.dram_to_dram = true;
    if (isGpuDirectRdmaSupported(conf_)) {
        caps.dram_to_gpu = true;
        caps.gpu_to_dram = true;
        caps.gpu_to_gpu = true;
        if (params_->with_nvidia_peermem) {
            LOG(INFO) << "RdmaTransport(TENT): GPUDirect RDMA via "
                         "nvidia-peermem/amdgpu (WITH_NVIDIA_PEERMEM)";
        } else if (RdmaContext::dmaBufRegistrationAvailable()) {
            LOG(INFO) << "RdmaTransport(TENT): GPUDirect RDMA via DMA-BUF";
        } else {
            LOG(INFO) << "RdmaTransport(TENT): GPUDirect RDMA via "
                         "nvidia-peermem (DMA-BUF not available on this GPU)";
        }
    }
    return Status::OK();
}

Status RdmaTransport::quiesce() {
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
            std::string source_location = kWildcardLocation;
            auto source_locations =
                Platform::getLoader().getLocation(request.source, 1, true);
            if (!source_locations.empty()) {
                source_location = source_locations[0].location;
            }
            auto device_selector = workers_->getDeviceSelector();
            if (device_selector) {
                auto status = device_selector->allocate(
                    request.length, static_cast<uint32_t>(num_slices),
                    block_size, source_location, slice_dev_ids,
                    request.priority, batch->device_mask);
                if (!status.ok() || slice_dev_ids.empty()) {
                    LOG(WARNING) << "Device quota allocation failed: "
                                 << status.message();
                }
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
                                             uint16_t lid,
                                             const std::string& gid) {
    if (!metadata_)
        return Status::InvalidArgument(
            "RDMA metadata is not initialized" LOC_MARK);

    auto& manager = metadata_->segmentManager();
    bool existed = false;
    uint16_t previous_lid = 0;
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

    // context_set_ is NicID-indexed, so slot 0 may be inert; take the first
    // enabled context instead.
    RdmaContext* context = nullptr;
    for (auto& ctx : context_set_) {
        if (ctx->status() == RdmaContext::DEVICE_ENABLED) {
            context = ctx.get();
            break;
        }
    }
    if (!context) {
        if (failure) {
            *failure =
                Status::DeviceNotFound("No enabled RDMA context" LOC_MARK);
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
    if (!endpoint) return notifyStatusForEndpointFailure(failure);
    // Notify QP not connected (the peer has none, or it was disabled after a
    // fault), or the post itself was refused.
    if (!endpoint->sendNotification(notify.name, notify.msg)) {
        return Status::RdmaError(
            "RDMA notification channel unavailable" LOC_MARK);
    }
    return Status::OK();
}

Status RdmaTransport::receiveNotification(
    std::vector<Notification>& notify_list) {
    {
        std::lock_guard<std::mutex> lock(notify_mutex_);
        if (notify_list_.empty()) {
            return Status::OK();
        }
        notify_list = std::move(notify_list_);
        notify_list_.clear();
    }
    // Empty polls are the hot path. Flush only after a real transfer-done
    // notify, and never while holding notify_mutex_ (this thread may enter
    // CUDA; the notify worker is a different path).
    bool flush = caps.gpu_to_gpu;
    if (conf_) {
        flush =
            flush &&
            conf_->get("transports/rdma/flush_gpu_direct_rdma_writes", true);
    }
    if (flush) {
        (void)flushDestGpuDirectWrites(local_topology_.get());
    }
    return Status::OK();
}

void RdmaTransport::addNotificationToQueue(const std::string& name,
                                           const std::string& msg) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    notify_list_.emplace_back(name, msg);
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
