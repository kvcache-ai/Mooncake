#include "tent/transport/memfabric/memfabric_transport.h"

#include <acl/acl.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <limits>
#include <map>
#include <optional>
#include <utility>

#include "tent/common/utils/ip.h"

namespace mooncake {
namespace tent {

namespace {

std::optional<smem_bm_data_op_type> parseDataOpType(const std::string& str) {
    if (str == "device_sdma") return SMEMB_DATA_OP_SDMA;
    if (str == "device_rdma") return SMEMB_DATA_OP_DEVICE_RDMA;
    if (str == "host_rdma") return SMEMB_DATA_OP_HOST_RDMA;
    if (str == "device_urma") return SMEMB_DATA_OP_DEVICE_URMA;
    if (str == "device_uboe") return SMEMB_DATA_OP_DEVICE_UBOE;
    return std::nullopt;
}

TransferStatusEnum mapBatchStatus(int32_t query_ret, int32_t batch_result) {
    if (query_ret == SM_NEED_RETRY) return TransferStatusEnum::PENDING;
    if (query_ret != SM_OK) return TransferStatusEnum::FAILED;
    switch (static_cast<SMErrorCode>(batch_result)) {
        case SM_OK:
            return TransferStatusEnum::COMPLETED;
        case SM_TIMEOUT:
            return TransferStatusEnum::TIMEOUT;
        case SM_INVALID_PARAM:
            return TransferStatusEnum::INVALID;
        default:
            return TransferStatusEnum::FAILED;
    }
}

}  // namespace

std::atomic<int> MemFabricTransport::g_init_ref_{0};
std::mutex MemFabricTransport::g_init_mutex_;

MemFabricSubBatch::~MemFabricSubBatch() {
    if (pending_batch_count > 0)
        LOG(WARNING) << "~MemFabricSubBatch: " << pending_batch_count
                     << " batch(es) still PENDING out of " << task_list.size()
                     << " tasks";
}

MemFabricTransport::MemFabricTransport() = default;
MemFabricTransport::~MemFabricTransport() { (void)uninstall(); }

Status MemFabricTransport::loadConfig(const std::shared_ptr<Config>&) {
    const char* protocol_env = std::getenv("MF_PROTOCOL");
    config_.data_op_type_str = protocol_env ? protocol_env : "";
    if (config_.data_op_type_str.empty()) {
        LOG(ERROR) << "MF_PROTOCOL env var not set";
        return Status::InvalidArgument(
            "MF_PROTOCOL must be set (device_sdma/device_rdma/...)" LOC_MARK);
    }
    config_.data_op_type = parseDataOpType(config_.data_op_type_str);
    if (!config_.data_op_type.has_value()) {
        LOG(ERROR) << "Invalid data_op_type: " << config_.data_op_type_str;
        return Status::InvalidArgument(
            "Invalid data_op_type: " + config_.data_op_type_str + LOC_MARK);
    }
    LOG(INFO) << "data_op_type=" << config_.data_op_type_str;

    transfer_timeout_ms_ = 10000;
    const char* timeout_env = std::getenv("MF_TRANSFER_TIMEOUT");
    if (timeout_env && timeout_env[0] != '\0') {
        try {
            transfer_timeout_ms_ =
                static_cast<int32_t>(std::stoll(timeout_env));
        } catch (const std::exception&) {
            LOG(WARNING) << "MF_TRANSFER_TIMEOUT is not a valid integer: "
                         << timeout_env;
        }
    }
    if (transfer_timeout_ms_ <= 0) transfer_timeout_ms_ = 10000;

    return Status::OK();
}

Status MemFabricTransport::install(std::string& local_segment_name,
                                   std::shared_ptr<ControlService> metadata,
                                   std::shared_ptr<Topology> local_topology,
                                   std::shared_ptr<Config> conf) {
    LOG(INFO) << "install: segment_name=" << local_segment_name;

    local_segment_name_ = local_segment_name;
    metadata_ = metadata;
    CHECK_STATUS(loadConfig(conf));

    if (!metadata_) {
        return Status::InvalidArgument("metadata is null" LOC_MARK);
    }
    const auto local_segment = metadata_->segmentManager().getLocal();
    if (!local_segment) {
        return Status::InvalidArgument("local segment is missing" LOC_MARK);
    }
    const auto [host, rpc_port] =
        parseHostNameWithPort(local_segment->rpc_server_addr, 0);
    if (host.empty() || rpc_port == 0 || host == "0.0.0.0" || host == "::") {
        return Status::InvalidArgument(
            "Invalid local RPC address: " + local_segment->rpc_server_addr +
            LOC_MARK);
    }
    const bool ipv6 = host.find(':') != std::string::npos;
    // Segment names are logical identifiers. Reuse only the RPC host and
    // let MemFabric allocate its own listening port.
    const auto bind_url = buildIpAddrWithPort(host, 0, ipv6);
    const auto nic_url = "tcp://" + buildIpAddrWithPort(host, rpc_port, ipv6);
    const bool device_rdma =
        config_.data_op_type.value() & SMEMB_DATA_OP_DEVICE_RDMA;
    smem_trans_config_t cfg;
    // Reserve room for any assigned port instead of silently truncating an
    // endpoint in the vendor's fixed-size fields.
    if (buildIpAddrWithPort(host, std::numeric_limits<uint16_t>::max(), ipv6)
                .size() >= sizeof(cfg.url) ||
        (device_rdma && nic_url.size() >= sizeof(cfg.nic))) {
        return Status::InvalidArgument(
            "Local RPC address exceeds MemFabric endpoint capacity" LOC_MARK);
    }

    int32_t device_id = 0;
    if (aclrtGetDevice(&device_id) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetDevice failed, device may be not set";
        return Status::InternalError(
            "aclrtGetDevice failed, device may be not set" LOC_MARK);
    }
    LOG(INFO) << "aclrtGetDevice: device_id=" << device_id;

    {
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        // Keep the library loaded until this installation owns an init
        // reference.
        if (!MemFabricUnderApi::LoadLibrary()) {
            LOG(ERROR)
                << "MemFabricUnderApi::LoadLibrary failed, libmf_smem.so "
                   "unavailable";
            return Status::DeviceNotFound("libmf_smem.so unavailable" LOC_MARK);
        }
        LOG(INFO) << "LoadLibrary: ok";

        auto log_level_str = conf->get("log_level", "info");
        int mf_log_level = 1;
        if (log_level_str == "warning")
            mf_log_level = 2;
        else if (log_level_str == "error")
            mf_log_level = 3;
        MemFabricUnderApi::SmemSetLogLevel(mf_log_level);

        if (g_init_ref_.load() == 0) {
            auto ret = MemFabricUnderApi::SmemTransInit(device_id, 0);
            LOG(INFO) << "smem_trans_init: ret=" << ret
                      << ", ref=" << g_init_ref_.load();
            if (ret != 0) {
                LOG(ERROR) << "smem_trans_init failed, ret=" << ret;
                MemFabricUnderApi::CleanupLibrary();
                return Status::InternalError("smem_trans_init failed" LOC_MARK);
            }
        }
        g_init_ref_.fetch_add(1);
    }

    if (MemFabricUnderApi::SmemTransConfigInit(&cfg) != 0) {
        LOG(ERROR) << "smem_trans_config_init failed";
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return Status::InternalError("smem_trans_config_init failed" LOC_MARK);
    }
    cfg.role = SMEM_TRANS_BOTH;
    cfg.deviceId = device_id;
    cfg.dataOpType = config_.data_op_type.value();

    std::memcpy(cfg.url, bind_url.c_str(), bind_url.size() + 1);
    if (device_rdma) {
        std::memcpy(cfg.nic, nic_url.c_str(), nic_url.size() + 1);
        LOG(INFO) << "RDMA mode: nic=" << cfg.nic;
    }
    LOG(INFO) << "trans config: url=" << cfg.url << ", nic=" << cfg.nic
              << ", deviceId=" << cfg.deviceId
              << ", dataOpType=" << config_.data_op_type_str;

    handle_ = MemFabricUnderApi::SmemTransCreate(&cfg, 0);
    LOG(INFO) << "smem_trans_create: handle=" << handle_;
    if (!handle_) {
        LOG(ERROR) << "smem_trans_create returned null handle";
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return Status::DeviceNotFound("smem_trans_create failed" LOC_MARK);
    }

    uint16_t actual_port = 0;
    if (MemFabricUnderApi::SmemTransGetRpcPort(handle_, &actual_port) == 0 &&
        actual_port != 0) {
        config_.store_url = buildIpAddrWithPort(host, actual_port, ipv6);
        LOG(INFO) << "smem_trans_get_rpc_port: port=" << actual_port
                  << ", store_url=" << config_.store_url;
    } else {
        LOG(ERROR) << "smem_trans_get_rpc_port failed";
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return Status::InternalError("smem_trans_get_port failed" LOC_MARK);
    }

    if (auto s = publishLocalDevices(); !s.ok()) {
        LOG(ERROR) << "publishLocalDevices failed";
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return s;
    }

    caps = Capabilities{};
    caps.dram_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;

    {
        std::lock_guard<std::mutex> lock(inflight_mu_);
        stopping_ = false;
    }
    installed_ = true;
    LOG(INFO) << "MemFabricTransport installed: url=" << config_.store_url
              << ", dataOpType=" << config_.data_op_type_str;
    return Status::OK();
}

Status MemFabricTransport::publishLocalDevices() {
    LOG(INFO) << "publishLocalDevices: store_url=" << config_.store_url;
    if (!metadata_) {
        LOG(ERROR) << "metadata is null in publishLocalDevices";
        return Status::InvalidArgument("metadata is null" LOC_MARK);
    }
    return metadata_->segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            if (!std::holds_alternative<MemorySegmentDesc>(segment.detail)) {
                segment.detail = MemorySegmentDesc{};
            }
            auto& detail = std::get<MemorySegmentDesc>(segment.detail);
            detail.device_attrs["memfabric_unique_id"] = config_.store_url;
            return Status::OK();
        });
}

Status MemFabricTransport::resolveDestUrl(const Request& request,
                                          std::string& dest_url) const {
    if (!metadata_) {
        LOG(ERROR) << "metadata is null in resolveDestUrl";
        return Status::InternalError("metadata is null" LOC_MARK);
    }
    SegmentDescRef pin;
    return metadata_->segmentManager().withCachedSegment(
        request.target_id, pin, [&](SegmentDesc* desc) -> Status {
            if (!desc) {
                LOG(ERROR) << "Segment not found for target_id="
                           << request.target_id;
                return Status::InvalidArgument("segment not found" LOC_MARK);
            }
            auto& detail = std::get<MemorySegmentDesc>(desc->detail);
            auto it = detail.device_attrs.find("memfabric_unique_id");
            if (it == detail.device_attrs.end() || it->second.empty()) {
                LOG(ERROR) << "memfabric_unique_id missing for target_id="
                           << request.target_id;
                return Status::InvalidArgument(
                    "memfabric_unique_id missing for segment" LOC_MARK);
            }
            dest_url = it->second;
            return Status::OK();
        });
}

Status MemFabricTransport::allocateSubBatch(SubBatchRef& batch,
                                            size_t max_size) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (stopping_)
        return Status::InternalError(
            "MemFabric transport is stopping" LOC_MARK);
    auto mf_batch = std::make_unique<MemFabricSubBatch>();
    mf_batch->handle = handle_;
    mf_batch->task_list.reserve(max_size);
    batches_.insert(mf_batch.get());
    batch = mf_batch.release();
    return Status::OK();
}

Status MemFabricTransport::freeSubBatch(SubBatchRef& batch) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch) {
        LOG(ERROR) << "Invalid sub-batch type in freeSubBatch";
        return Status::InvalidArgument("invalid sub-batch" LOC_MARK);
    }
    batches_.erase(mf_batch);
    delete mf_batch;
    batch = nullptr;
    return Status::OK();
}

Status MemFabricTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (stopping_)
        return Status::InternalError(
            "MemFabric transport is stopping" LOC_MARK);
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch) {
        LOG(ERROR) << "Invalid sub-batch type in submitTransferTasks";
        return Status::InvalidArgument("invalid sub-batch" LOC_MARK);
    }

    // Resolve every destination before changing the sub-batch so an error
    // leaves existing tasks untouched and dispatches no new work.
    std::vector<std::string> dest_urls(request_list.size());
    for (size_t i = 0; i < request_list.size(); ++i) {
        CHECK_STATUS(resolveDestUrl(request_list[i], dest_urls[i]));
    }

    using GroupKey = std::pair<std::string, bool>;  // {dest_url, is_write}
    std::map<GroupKey, std::vector<size_t>> groups;
    for (size_t i = 0; i < request_list.size(); ++i) {
        bool write = (request_list[i].opcode == Request::WRITE);
        groups[{dest_urls[i], write}].push_back(i);
    }

    const size_t start = mf_batch->task_list.size();
    mf_batch->task_list.resize(start + request_list.size());

    for (auto& [key, indices] : groups) {
        const auto& dest_url = key.first;
        bool is_write = key.second;
        const uint32_t batch_size = static_cast<uint32_t>(indices.size());
        std::vector<void*> src_list(batch_size);
        std::vector<void*> dest_list(batch_size);
        std::vector<size_t> size_list(batch_size);

        for (uint32_t j = 0; j < batch_size; ++j) {
            const auto& req = request_list[indices[j]];
            src_list[j] =
                const_cast<void*>(static_cast<const void*>(req.source));
            dest_list[j] = reinterpret_cast<void*>(req.target_offset);
            size_list[j] = req.length;
        }

        smem_trans_batch_params_t params = {};
        params.destUrl = dest_url.c_str();
        params.srcList = src_list.data();
        params.destList = dest_list.data();
        params.sizeList = size_list.data();
        params.batchSize = batch_size;
        params.opcode = is_write ? SMEM_TRANS_OP_WRITE : SMEM_TRANS_OP_READ;
        params.stream = nullptr;
        params.flags = 0;

        uint64_t batch_id = 0;
        auto ret =
            MemFabricUnderApi::SmemTransBatchCopy(handle_, &params, &batch_id);
        if (ret != 0) {
            LOG(ERROR) << "smem_trans_batch_copy failed, ret=" << ret
                       << ", dest_url=" << dest_url
                       << ", batch_size=" << batch_size;
            for (size_t idx : indices) {
                auto& task = mf_batch->task_list[start + idx];
                task.status_word = TransferStatusEnum::FAILED;
            }
            continue;
        }

        for (size_t idx : indices) {
            auto& task = mf_batch->task_list[start + idx];
            task.request = request_list[idx];
            task.batch_id = batch_id;
            task.length = request_list[idx].length;
            task.status_word = TransferStatusEnum::PENDING;
        }
        inflight_.fetch_add(1, std::memory_order_acq_rel);
        mf_batch->pending_batch_count++;
    }

    return Status::OK();
}

Status MemFabricTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                             TransferStatus& status) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    return getTransferStatusLocked(batch, task_id, status);
}

Status MemFabricTransport::getTransferStatusLocked(SubBatchRef batch,
                                                   int task_id,
                                                   TransferStatus& status) {
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch || task_id < 0 ||
        task_id >= static_cast<int>(mf_batch->task_list.size())) {
        LOG(ERROR) << "Invalid task_id=" << task_id << " in getTransferStatus";
        return Status::InvalidArgument("invalid task" LOC_MARK);
    }

    auto& task = mf_batch->task_list[task_id];
    if (task.status_word != TransferStatusEnum::PENDING) {
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    auto cache_it = mf_batch->batch_status_cache.find(task.batch_id);
    if (cache_it != mf_batch->batch_status_cache.end() &&
        cache_it->second != TransferStatusEnum::PENDING) {
        task.status_word = cache_it->second;
        if (task.status_word == TransferStatusEnum::COMPLETED)
            task.transferred_bytes = task.length;
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    int32_t batch_result = 0;
    auto query_ret = MemFabricUnderApi::SmemTransBatchStatusQuery(
        handle_, task.batch_id, &batch_result, 0);
    VLOG(1) << "batch_status_query: batch_id=" << task.batch_id
            << ", query_ret=" << query_ret << ", batch_result=" << batch_result;

    if (query_ret != SM_OK && query_ret != SM_NEED_RETRY) {
        status = TransferStatus{TransferStatusEnum::PENDING, 0};
        return Status::InternalError("MemFabric completion unknown" LOC_MARK);
    }

    TransferStatusEnum batch_status = mapBatchStatus(query_ret, batch_result);
    if (batch_status != TransferStatusEnum::PENDING &&
        batch_status != TransferStatusEnum::COMPLETED) {
        LOG(WARNING) << "smem_trans_batch_status_query: query_ret=" << query_ret
                     << ", batch_result=" << batch_result
                     << ", batch_id=" << task.batch_id
                     << ", mapped=" << static_cast<int>(batch_status);
    }

    if (batch_status != TransferStatusEnum::PENDING) {
        if (mf_batch->pending_batch_count > 0) mf_batch->pending_batch_count--;
        if (inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            inflight_cv_.notify_all();
        }
        mf_batch->batch_status_cache[task.batch_id] = batch_status;
        for (auto& t : mf_batch->task_list) {
            if (t.batch_id == task.batch_id &&
                t.status_word == TransferStatusEnum::PENDING) {
                t.status_word = batch_status;
                if (t.status_word == TransferStatusEnum::COMPLETED)
                    t.transferred_bytes = t.length;
            }
        }
    }

    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status MemFabricTransport::addMemoryBuffer(BufferDesc& desc,
                                           const MemoryOptions& options) {
    (void)options;
    LOG(INFO) << "addMemoryBuffer: addr=" << desc.addr
              << ", length=" << desc.length;
    if (!handle_) {
        LOG(ERROR) << "addMemoryBuffer called before install";
        return Status::InternalError("not installed" LOC_MARK);
    }
    auto ret = MemFabricUnderApi::SmemTransRegisterMem(
        handle_, reinterpret_cast<void*>(desc.addr), desc.length, 0);
    if (ret != 0) {
        LOG(ERROR) << "smem_trans_register_mem failed, ret=" << ret
                   << ", addr=" << desc.addr << ", length=" << desc.length;
        return Status::InternalError("register_mem failed" LOC_MARK);
    }
    desc.transports.push_back(MEMFABRIC);
    desc.transport_attrs[MEMFABRIC] = "";
    LOG(INFO) << "register_mem: ok, addr=" << desc.addr;
    return Status::OK();
}

Status MemFabricTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                           const MemoryOptions& options) {
    for (auto& desc : desc_list) {
        CHECK_STATUS(addMemoryBuffer(desc, options));
    }
    return Status::OK();
}

Status MemFabricTransport::removeMemoryBuffer(BufferDesc& desc) {
    if (!handle_) return Status::OK();
    LOG(INFO) << "removeMemoryBuffer: addr=" << desc.addr;
    auto ret = MemFabricUnderApi::SmemTransDeregisterMem(
        handle_, reinterpret_cast<void*>(desc.addr));
    if (ret != 0) {
        LOG(ERROR) << "smem_trans_deregister_mem failed, ret=" << ret
                   << ", addr=" << desc.addr;
        return Status::InternalError("deregister_mem failed" LOC_MARK);
    }
    return Status::OK();
}

Status MemFabricTransport::quiesce() {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(transfer_timeout_ms_);
    std::unique_lock<std::mutex> lock(inflight_mu_);
    stopping_ = true;
    while (inflight_.load(std::memory_order_acquire) > 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            LOG(WARNING) << "MemFabricTransport quiesce timed out with "
                         << inflight_.load() << " in-flight batches";
            return Status::InternalError(
                "MemFabric quiesce timed out" LOC_MARK);
        }
        for (auto* batch : batches_) {
            if (std::chrono::steady_clock::now() >= deadline) break;
            if (batch->pending_batch_count == 0) continue;
            for (size_t i = 0; i < batch->task_list.size(); ++i) {
                if (std::chrono::steady_clock::now() >= deadline) break;
                if (batch->task_list[i].status_word !=
                    TransferStatusEnum::PENDING)
                    continue;
                TransferStatus status{};
                // Query errors leave the task pending; retry until the
                // deadline.
                (void)getTransferStatusLocked(batch, static_cast<int>(i),
                                              status);
            }
        }
        if (inflight_.load(std::memory_order_acquire) == 0) break;
        // Release the lock between polls so normal status queries and batch
        // release can proceed. Submission remains blocked by stopping_.
        inflight_cv_.wait_until(
            lock, std::min(deadline, std::chrono::steady_clock::now() +
                                         std::chrono::milliseconds(1)));
    }
    return Status::OK();
}

Status MemFabricTransport::uninstall() {
    if (!installed_) return Status::OK();
    (void)quiesce();
    LOG(INFO) << "uninstall: handle=" << handle_;
    if (handle_) {
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        LOG(INFO) << "smem_trans_destroy: done";
    }
    {
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
            LOG(INFO) << "smem_trans_uninit + cleanup_library: ref=0";
        }
    }
    installed_ = false;
    metadata_.reset();
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
