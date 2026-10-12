#include "tent/transport/memfabric/memfabric_transport.h"

#include <acl/acl.h>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <unordered_map>

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

}  // namespace

std::atomic<int> MemFabricTransport::g_init_ref_{0};
std::mutex MemFabricTransport::g_init_mutex_;

MemFabricSubBatch::~MemFabricSubBatch() {
    if (pending_batch_count > 0)
        LOG(WARNING) << "~MemFabricSubBatch: " << pending_batch_count
                     << " batch(es) still PENDING out of "
                     << task_list.size() << " tasks";
}

MemFabricTransport::MemFabricTransport() = default;
MemFabricTransport::~MemFabricTransport() { (void)uninstall(); }

Status MemFabricTransport::install(std::string& local_segment_name,
                                   std::shared_ptr<ControlService> metadata,
                                   std::shared_ptr<Topology> local_topology,
                                   std::shared_ptr<Config> conf) {
    LOG(INFO) << "install: segment_name=" << local_segment_name;

    if (!MemFabricUnderApi::LoadLibrary()) {
        LOG(ERROR) << "MemFabricUnderApi::LoadLibrary failed, libmf_smem.so unavailable";
        return Status::DeviceNotFound("libmf_smem.so unavailable" LOC_MARK);
    }
    LOG(INFO) << "LoadLibrary: ok";

    local_segment_name_ = local_segment_name;
    metadata_ = metadata;

    config_.data_op_type_str = conf->get("transports/memfabric/data_op_type", "");
    if (config_.data_op_type_str.empty()) {
        LOG(ERROR) << "transports/memfabric/data_op_type not set in config";
        return Status::InvalidArgument(
            "data_op_type must be set (device_sdma/device_rdma/...)" LOC_MARK);
    }
    config_.data_op_type = parseDataOpType(config_.data_op_type_str);
    if (!config_.data_op_type.has_value()) {
        LOG(ERROR) << "Invalid data_op_type: " << config_.data_op_type_str;
        return Status::InvalidArgument(
            "Invalid data_op_type: " + config_.data_op_type_str + LOC_MARK);
    }
    LOG(INFO) << "data_op_type=" << config_.data_op_type_str;

    auto log_level_str = conf->get("log_level", "info");
    int mf_log_level = 1;
    if (log_level_str == "warning") mf_log_level = 2;
    else if (log_level_str == "error") mf_log_level = 3;
    MemFabricUnderApi::SmemSetLogLevel(mf_log_level);

    int32_t device_id = 0;
    if (aclrtGetDevice(&device_id) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetDevice failed, device may be not set";
        return Status::InternalError(
            "aclrtGetDevice failed, device may be not set" LOC_MARK);
    }
    LOG(INFO) << "aclrtGetDevice: device_id=" << device_id;

    {
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_add(1) == 0) {
            auto ret = MemFabricUnderApi::SmemTransInit(device_id, 0);
            LOG(INFO) << "smem_trans_init: ret=" << ret << ", ref=" << g_init_ref_.load();
            if (ret != 0) {
                LOG(ERROR) << "smem_trans_init failed, ret=" << ret;
                g_init_ref_.fetch_sub(1);
                return Status::InternalError("smem_trans_init failed" LOC_MARK);
            }
        }
    }

    smem_trans_config_t cfg;
    if (MemFabricUnderApi::SmemTransConfigInit(&cfg) != 0) {
        LOG(ERROR) << "smem_trans_config_init failed";
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) MemFabricUnderApi::SmemTransUninit(0);
        return Status::InternalError("smem_trans_config_init failed" LOC_MARK);
    }
    cfg.role = SMEM_TRANS_BOTH;
    cfg.deviceId = device_id;
    cfg.dataOpType = config_.data_op_type.value();

    {
        char host_ip[64] = {};
        auto pos = local_segment_name.find(':');
        if (pos != std::string::npos) {
            auto len = std::min(pos, sizeof(host_ip) - 1);
            std::memcpy(host_ip, local_segment_name.c_str(), len);
            host_ip[len] = '\0';
        }
        std::snprintf(cfg.url, sizeof(cfg.url), "%s:0", host_ip);
        if (config_.data_op_type.value() & SMEMB_DATA_OP_DEVICE_RDMA) {
            std::snprintf(cfg.nic, sizeof(cfg.nic), "tcp://%s",
                          local_segment_name.c_str());
            LOG(INFO) << "RDMA mode: nic=" << cfg.nic;
        }
    }
    LOG(INFO) << "trans config: url=" << cfg.url
              << ", nic=" << cfg.nic
              << ", deviceId=" << cfg.deviceId
              << ", dataOpType=" << config_.data_op_type_str;

    handle_ = MemFabricUnderApi::SmemTransCreate(&cfg, 0);
    LOG(INFO) << "smem_trans_create: handle=" << handle_;
    if (!handle_) {
        LOG(ERROR) << "smem_trans_create returned null handle";
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) MemFabricUnderApi::SmemTransUninit(0);
        return Status::DeviceNotFound("smem_trans_create failed" LOC_MARK);
    }

    uint16_t actual_port = 0;
    if (MemFabricUnderApi::SmemTransGetRpcPort(handle_, &actual_port) == 0 && actual_port != 0) {
        char host_ip[64] = {};
        auto pos = local_segment_name.find(':');
        if (pos != std::string::npos) {
            auto len = std::min(pos, sizeof(host_ip) - 1);
            std::memcpy(host_ip, local_segment_name.c_str(), len);
            host_ip[len] = '\0';
        }
        char url_buf[64];
        std::snprintf(url_buf, sizeof(url_buf), "%s:%u", host_ip, actual_port);
        config_.store_url = url_buf;
        LOG(INFO) << "smem_trans_get_rpc_port: port=" << actual_port
                  << ", store_url=" << config_.store_url;
    } else {
        LOG(ERROR) << "smem_trans_get_rpc_port failed";
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) MemFabricUnderApi::SmemTransUninit(0);
        return Status::InternalError("smem_trans_get_port failed" LOC_MARK);
    }

    if (auto s = publishLocalDevices(); !s.ok()) {
        LOG(ERROR) << "publishLocalDevices failed";
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) MemFabricUnderApi::SmemTransUninit(0);
        return s;
    }

    caps = Capabilities{};
    caps.dram_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;

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
                LOG(ERROR) << "Segment not found for target_id=" << request.target_id;
                return Status::InvalidArgument("segment not found" LOC_MARK);
            }
            auto& detail = std::get<MemorySegmentDesc>(desc->detail);
            auto it = detail.device_attrs.find("memfabric_unique_id");
            if (it == detail.device_attrs.end() || it->second.empty()) {
                LOG(ERROR) << "memfabric_unique_id missing for target_id=" << request.target_id;
                return Status::InvalidArgument(
                    "memfabric_unique_id missing for segment" LOC_MARK);
            }
            dest_url = it->second;
            return Status::OK();
        });
}

Status MemFabricTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto* mf_batch = new MemFabricSubBatch();
    mf_batch->handle = handle_;
    mf_batch->task_list.reserve(max_size);
    batch = mf_batch;
    return Status::OK();
}

Status MemFabricTransport::freeSubBatch(SubBatchRef& batch) {
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch) {
        LOG(ERROR) << "Invalid sub-batch type in freeSubBatch";
        return Status::InvalidArgument("invalid sub-batch" LOC_MARK);
    }
    delete mf_batch;
    batch = nullptr;
    return Status::OK();
}

Status MemFabricTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch) {
        LOG(ERROR) << "Invalid sub-batch type in submitTransferTasks";
        return Status::InvalidArgument("invalid sub-batch" LOC_MARK);
    }

    const size_t start = mf_batch->task_list.size();
    mf_batch->task_list.resize(start + request_list.size());

    std::vector<std::string> dest_urls(request_list.size());
    for (size_t i = 0; i < request_list.size(); ++i) {
        CHECK_STATUS(resolveDestUrl(request_list[i], dest_urls[i]));
    }

    std::unordered_map<std::string, std::vector<size_t>> groups;
    for (size_t i = 0; i < request_list.size(); ++i) {
        groups[dest_urls[i]].push_back(i);
    }

    for (auto& [dest_url, indices] : groups) {
        const uint32_t batch_size = static_cast<uint32_t>(indices.size());
        std::vector<void*> src_list(batch_size);
        std::vector<void*> dest_list(batch_size);
        std::vector<size_t> size_list(batch_size);

        bool is_write = true;
        for (uint32_t j = 0; j < batch_size; ++j) {
            const auto& req = request_list[indices[j]];
            src_list[j] = const_cast<void*>(static_cast<const void*>(req.source));
            dest_list[j] = reinterpret_cast<void*>(req.target_offset);
            size_list[j] = req.length;
            is_write = (req.opcode == Request::WRITE);
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
        auto ret = MemFabricUnderApi::SmemTransBatchCopy(handle_, &params, &batch_id);
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
        mf_batch->pending_batch_count++;
    }

    return Status::OK();
}

Status MemFabricTransport::getTransferStatus(SubBatchRef batch, int task_id,
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

    auto ret = MemFabricUnderApi::SmemTransBatchStatusQuery(handle_, task.batch_id, 0);
    VLOG(1) << "batch_status_query: batch_id=" << task.batch_id
            << ", ret=" << ret
            << (ret == SMEM_TRANS_BATCH_DONE ? " (DONE)" :
                ret == SMEM_TRANS_BATCH_PENDING ? " (PENDING)" : " (FAILED)");

    TransferStatusEnum batch_status;
    if (ret == SMEM_TRANS_BATCH_DONE) {
        batch_status = TransferStatusEnum::COMPLETED;
    } else if (ret == SMEM_TRANS_BATCH_PENDING) {
        batch_status = TransferStatusEnum::PENDING;
    } else {
        LOG(WARNING) << "smem_trans_batch_status_query error, ret=" << ret
                     << ", batch_id=" << task.batch_id;
        batch_status = TransferStatusEnum::FAILED;
    }

    if (batch_status != TransferStatusEnum::PENDING) {
        if (mf_batch->pending_batch_count > 0)
            mf_batch->pending_batch_count--;
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
    LOG(INFO) << "addMemoryBuffer: addr=" << desc.addr << ", length=" << desc.length;
    if (!handle_) {
        LOG(ERROR) << "addMemoryBuffer called before install";
        return Status::InternalError("not installed" LOC_MARK);
    }
    auto ret = MemFabricUnderApi::SmemTransRegisterMem(handle_, reinterpret_cast<void*>(desc.addr), desc.length, 0);
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
    (void)MemFabricUnderApi::SmemTransDeregisterMem(handle_, reinterpret_cast<void*>(desc.addr));
    return Status::OK();
}

Status MemFabricTransport::uninstall() {
    if (!installed_) return Status::OK();
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
