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

#include "tent/transport/ascend/ascend_direct_transport.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <random>

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"

namespace mooncake {
namespace tent {
namespace {
constexpr size_t MEM_CPY_LIMIT = 4096;
constexpr int BASE_PORT = 20000;
uint16_t findListenPort(int32_t dev_id) {
    char *rt_visible_devices = std::getenv("ASCEND_RT_VISIBLE_DEVICES");
    if (rt_visible_devices) {
        std::vector<std::string> device_list;
        std::stringstream ss(rt_visible_devices);
        std::string item;
        while (std::getline(ss, item, ',')) {
            device_list.push_back(item);
        }
        if (dev_id < static_cast<int32_t>(device_list.size())) {
            try {
                dev_id = std::stoi(device_list[dev_id]);
            } catch (const std::exception &e) {
                LOG(WARNING) << "ASCEND_RT_VISIBLE_DEVICES is not valid, value:"
                             << rt_visible_devices;
            }
        } else {
            LOG(WARNING) << "Device id is " << dev_id
                         << ", ASCEND_RT_VISIBLE_DEVICES is "
                         << rt_visible_devices << ", which is unexpected.";
        }
    }
    static std::random_device rand_gen;
    const int min_port = BASE_PORT + dev_id * 1000;
    const int max_port = BASE_PORT + (dev_id + 1) * 1000;
    LOG(INFO) << "Find available between " << min_port << " and " << max_port;
    const int max_attempts = 500;
    std::uniform_int_distribution<> rand_dist(min_port, max_port);
    int sockfd;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        int port = rand_dist(rand_gen);
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            continue;
        }
        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        sockaddr_in bind_address;
        memset(&bind_address, 0, sizeof(sockaddr_in));
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(port);
        bind_address.sin_addr.s_addr = INADDR_ANY;
        if (bind(sockfd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) < 0) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        close(sockfd);
        return port;
    }
    return 0;
}
}  // namespace
AscendDirectTransport::AscendDirectTransport() : installed_(false) {}

void AscendDirectTransport::workerThread() {
    LOG(INFO) << "AscendDirectTransport worker thread started";
    auto ret = aclrtSetCurrentContext(rt_context_);
    if (ret) {
        LOG(ERROR) << "Call aclrtSetCurrentContext failed, ret: " << ret;
        return;
    }
    while (running_) {
        std::vector<HixlTask *> task_list;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(
                lock, [this] { return !running_ || !task_queue_.empty(); });
            if (!running_) {
                break;
            }
            task_list = std::move(task_queue_.front());
            task_queue_.pop();
        }
        if (task_list.empty()) {
            LOG(ERROR) << "Empty transfer request batch";
            continue;
        }
        startTransfer(task_list[0]->request.target_id,
                      task_list[0]->request.opcode, task_list, true);
    }
    LOG(INFO) << "AscendDirectTransport worker thread stopped";
}

AscendDirectTransport::~AscendDirectTransport() {
    running_ = false;
    queue_cv_.notify_one();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    hixl_->Finalize();
    uninstall();
}

Status AscendDirectTransport::install(std::string &local_segment_name,
                                      std::shared_ptr<ControlService> metadata,
                                      std::shared_ptr<Topology> local_topology,
                                      std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "Hixl transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    installed_ = true;
    caps.dram_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;
    transfer_timeout_ =
        conf->get("transports/ascend_direct/transfer_timeout_ns", 3000000UL);
    connect_timeout_ =
        conf->get("transports/ascend_direct/connect_timeout_ns", 3000000UL);
    return initHixl(conf);
}

Status AscendDirectTransport::initHixl(const std::shared_ptr<Config> &conf) {
    auto ret = aclrtGetDevice(&device_logic_id_);
    if (ret) {
        LOG(ERROR) << "Call aclrtGetDevice failed, ret: " << ret;
        return Status::InvalidArgument(
            "Get device id failed, device id may be not set.");
    }
    ret = aclrtGetCurrentContext(&rt_context_);
    if (ret) {
        LOG(ERROR) << "Call aclrtGetCurrentContext failed, ret: " << ret;
        return Status::InternalError("Get rt context failed.");
    }
    auto host_ip = local_segment_name_;
    const size_t colon_pos = local_segment_name_.find(':');
    if (colon_pos != std::string::npos) {
        host_ip = local_segment_name_.substr(0, colon_pos);
    }
    auto port = findListenPort(device_logic_id_);
    auto hixl_name = host_ip + ":" + std::to_string(port);
    local_hixl_name_ = hixl_name;

    auto segment = metadata_->segmentManager().getLocal();
    auto &detail = std::get<MemorySegmentDesc>(segment->detail);
    detail.device_attrs["hixl_name"] = hixl_name;

    hixl_ = std::make_unique<hixl::Hixl>();
    if (!hixl_) return Status::InternalError("Create hixl failed.");
    std::map<hixl::AscendString, hixl::AscendString> options;
    std::string rdma_tc = conf->get("transports/ascend_direct/rdma_tc", "");
    if (!rdma_tc.empty()) {
        options["RdmaTrafficClass"] = rdma_tc.c_str();
        LOG(INFO) << "Set RdmaTrafficClass to:" << rdma_tc;
    } else {
        auto rdma_tc_env = std::getenv("HCCL_RDMA_TC");
        if (rdma_tc_env) {
            options["RdmaTrafficClass"] = rdma_tc_env;
            LOG(INFO) << "Set RdmaTrafficClass to:" << rdma_tc_env;
        }
    }
    std::string rdma_sl = conf->get("transports/ascend_direct/rdma_sl", "");
    if (!rdma_sl.empty()) {
        options["RdmaServiceLevel"] = rdma_sl.c_str();
        LOG(INFO) << "Set RdmaServiceLevel to:" << rdma_sl;
    } else {
        auto rdma_sl_env = std::getenv("HCCL_RDMA_SL");
        if (rdma_sl_env) {
            options["RdmaServiceLevel"] = rdma_sl_env;
            LOG(INFO) << "Set RdmaServiceLevel to:" << rdma_sl_env;
        }
    }
    std::string auto_connect =
        conf->get("transports/ascend_direct/auto_connect", "");
    auto auto_connect_opt = parseFromString<int32_t>(auto_connect);
    if (auto_connect_opt.has_value()) {
        auto_connect_ = (*auto_connect_opt == 1);
        options["AutoConnect"] = auto_connect_ ? "1" : "0";
        LOG(INFO) << "Set AutoConnect to: " << auto_connect;
    }
    std::string buffer_pool =
        conf->get("transports/ascend_direct/buffer_pool", "");
    if (!buffer_pool.empty()) {
        options["BufferPool"] = buffer_pool.c_str();
        use_buffer_pool_ = true;
        LOG(INFO) << "Set BufferPool to:" << buffer_pool;
    }
    auto status =
        hixl_->Initialize(hixl::AscendString(hixl_name.c_str()), options);
    if (status != hixl::SUCCESS) {
        LOG(ERROR) << "Failed to initialize AdxlEngine, status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("Initialize hixl failed.");
    }
    ret = aclrtCreateStreamWithConfig(
        &stream_, 0, ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "AscendDirectTransport: cannot create stream, ret: "
                   << ret;
        return Status::InternalError("Create stream failed.");
    }
    LOG(INFO) << "Success to initialize hixl engine:" << hixl_name
              << " with device_id:" << device_logic_id_;
    running_ = true;
    worker_thread_ = std::thread(&AscendDirectTransport::workerThread, this);
    return Status::OK();
}

Status AscendDirectTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status AscendDirectTransport::allocateSubBatch(SubBatchRef &batch,
                                               size_t max_size) {
    auto hixl_batch = Slab<HixlSubBatch>::Get().allocate();
    if (!hixl_batch)
        return Status::InternalError("Unable to allocate TCP sub-batch");
    batch = hixl_batch;
    hixl_batch->task_list.reserve(max_size);
    hixl_batch->max_size = max_size;
    return Status::OK();
}

Status AscendDirectTransport::freeSubBatch(SubBatchRef &batch) {
    auto hixl_batch = dynamic_cast<HixlSubBatch *>(batch);
    if (!hixl_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    Slab<HixlSubBatch>::Get().deallocate(hixl_batch);
    batch = nullptr;
    return Status::OK();
}

Status AscendDirectTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto hixl_batch = dynamic_cast<HixlSubBatch *>(batch);
    if (!hixl_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    if (request_list.size() + hixl_batch->task_list.size() >
        hixl_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    std::map<SegmentID, std::map<Request::OpCode, std::vector<HixlTask *>>>
        seg_to_tasks;
    for (auto &request : request_list) {
        hixl_batch->task_list.push_back(HixlTask{});
        auto &task = hixl_batch->task_list[hixl_batch->task_list.size() - 1];
        uint64_t target_addr = request.target_offset;
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.sync = use_buffer_pool_;
        task.transferred_bytes = 0;
        seg_to_tasks[task.request.target_id][task.request.opcode].push_back(
            &task);
    }
    for (auto &[seg_id, op_to_tasks] : seg_to_tasks) {
        for (auto &[opcode, tasks] : op_to_tasks) {
            if (use_buffer_pool_) {
                {
                    std::lock_guard<std::mutex> lock(queue_mutex_);
                    task_queue_.push(tasks);
                }
                queue_cv_.notify_one();
            } else {
                startTransfer(seg_id, opcode, tasks);
            }
        }
    }
    return Status::OK();
}

Status AscendDirectTransport::checkAndConnect(const std::string &remote_hixl) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto it = connected_segments_.find(remote_hixl);
    if (it != connected_segments_.end()) {
        VLOG(1) << "Already connected to target hixl engine: " << remote_hixl;
    } else {
        auto status = hixl_->Connect(
            remote_hixl.c_str(), static_cast<int32_t>(connect_timeout_ / 1000));
        if (status == hixl::TIMEOUT) {
            LOG(ERROR) << "Connect timeout to: " << remote_hixl
                       << ", you can increase the timeout duration to reduce "
                          "the failure rate by configuring "
                          "the ASCEND_CONNECT_TIMEOUT environment variable"
                       << ", errmsg: " << aclGetRecentErrMsg();
            return Status::InternalError("Connect to target timed out.");
        } else if (status != hixl::SUCCESS) {
            LOG(ERROR) << "Failed to connect to target: " << remote_hixl
                       << ", status: " << status
                       << ", errmsg: " << aclGetRecentErrMsg();
            return Status::InternalError("Connect to target failed.");
        }
        connected_segments_.emplace(remote_hixl);
        LOG(INFO) << "Connected to segment: " << remote_hixl;
    }
    return Status::OK();
}

void AscendDirectTransport::startTransfer(SegmentID target_id,
                                          Request::OpCode opcode,
                                          const std::vector<HixlTask *> &tasks,
                                          bool sync) {
    std::string rpc_server_addr;
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) {
        for (auto &task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    auto remote_hixl = detail.device_attrs["hixl_name"];
    if (remote_hixl == local_hixl_name_) {
        VLOG(1) << "Target is local.";
        for (auto &task : tasks) {
            task->sync = true;
        }
        auto start = std::chrono::steady_clock::now();
        localCopy(opcode, tasks);
        LOG(INFO) << "Local copy cost: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << " us.";
        return;
    } else {
        if (!auto_connect_) {
            auto ret = checkAndConnect(remote_hixl);
            if (!ret.ok()) {
                return;
            }
        }
    }
    auto op = (opcode == Request::WRITE) ? hixl::WRITE : hixl::READ;
    std::vector<hixl::TransferOpDesc> op_descs;
    op_descs.reserve(tasks.size());
    for (auto &task : tasks) {
        hixl::TransferOpDesc op_desc{};
        op_desc.local_addr = reinterpret_cast<uintptr_t>(task->request.source);
        op_desc.remote_addr =
            reinterpret_cast<uintptr_t>(task->request.target_offset);
        op_desc.len = task->request.length;
        op_descs.emplace_back(op_desc);
    }
    hixl::Status hixl_ret;
    hixl::TransferReq req_handle;
    if (sync) {
        hixl_ret = hixl_->TransferSync(
            hixl::AscendString(remote_hixl.c_str()), op, op_descs,
            static_cast<int32_t>(transfer_timeout_ / 1000));
    } else {
        hixl_ret =
            hixl_->TransferAsync(hixl::AscendString(remote_hixl.c_str()), op,
                                 op_descs, hixl::TransferArgs(), req_handle);
    }
    if (hixl_ret != hixl::SUCCESS) {
        LOG(ERROR) << "Failed to transfer to: " << remote_hixl
                   << ", status: " << hixl_ret
                   << ", errmsg: " << aclGetRecentErrMsg();
        // disconnect to remote when transfer fail
        disconnect(remote_hixl, 10);
        for (auto &task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }
    for (auto &task : tasks) {
        task->start_time = getCurrentTimeInNano();
        task->req_handle = req_handle;
        task->batch_size = tasks.size();
        task->remote_hixl = remote_hixl;
        task->status_word =
            sync ? TransferStatusEnum::COMPLETED : TransferStatusEnum::PENDING;
    }
}

Status AscendDirectTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                                TransferStatus &status) {
    auto hixl_batch = dynamic_cast<HixlSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)hixl_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = hixl_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    if (task.sync) {
        return Status::OK();
    }
    if (task.status_word == TransferStatusEnum::PENDING) {
        std::lock_guard<std::mutex> lock(req_mutex_);
        auto it = req_map_.find(task.req_handle);
        if (it != req_map_.end()) {
            auto xfer_status = it->second.first;
            task.status_word = xfer_status;
            if (xfer_status == TransferStatusEnum::COMPLETED) {
                task.transferred_bytes = task.request.length;
            }
            if (--req_map_[task.req_handle].second == 0) {
                req_map_.erase(task.req_handle);
            }
            return Status::OK();
        }
        uint64_t current_ts = getCurrentTimeInNano();
        if ((current_ts - task.start_time) > transfer_timeout_) {
            disconnect(task.remote_hixl, 10);
            task.status_word = TransferStatusEnum::TIMEOUT;
            if (task.batch_size > 1) {
                req_map_[task.req_handle] =
                    std::make_pair(task.status_word, task.batch_size - 1);
            }
            return Status::OK();
        }
        hixl::TransferStatus xfer_status;
        auto err = hixl_->GetTransferStatus(task.req_handle, xfer_status);
        if (err != hixl::SUCCESS) {
            // call failed equal to transfer failed;
            xfer_status = hixl::TransferStatus::FAILED;
        }
        if (xfer_status == hixl::TransferStatus::WAITING) {
            return Status::OK();
        }
        if (xfer_status == hixl::TransferStatus::COMPLETED) {
            task.transferred_bytes = task.request.length;
            task.status_word = TransferStatusEnum::COMPLETED;
        } else if (xfer_status == hixl::TransferStatus::FAILED) {
            LOG(ERROR) << "Get transfer status failed, ret: " << xfer_status
                       << ", errmsg: " << aclGetRecentErrMsg();
            disconnect(task.remote_hixl, 10);
            task.status_word = TransferStatusEnum::FAILED;
        }
        req_map_[task.req_handle] =
            std::make_pair(task.status_word, task.batch_size - 1);
    }
    return Status::OK();
}

void AscendDirectTransport::disconnect(const std::string &remote_hixl,
                                       int32_t timeout_in_millis) {
    if (auto_connect_) {
        auto status = hixl_->Disconnect(remote_hixl.c_str(), timeout_in_millis);
        if (status != hixl::SUCCESS) {
            LOG(ERROR) << "Failed to disconnect to: " << remote_hixl
                       << ", status: " << status
                       << ", errmsg: " << aclGetRecentErrMsg();
        }
        return;
    }
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto it = connected_segments_.find(remote_hixl);
    if (it == connected_segments_.end()) {
        LOG(INFO) << "Target hixl engine: " << remote_hixl
                  << " is not connected.";
    } else {
        auto status = hixl_->Disconnect(remote_hixl.c_str(), timeout_in_millis);
        connected_segments_.erase(remote_hixl);
        if (status != hixl::SUCCESS) {
            LOG(ERROR) << "Failed to disconnect to: " << remote_hixl
                       << ", status: " << status
                       << ", errmsg: " << aclGetRecentErrMsg();
        }
    }
}

Status AscendDirectTransport::addMemoryBuffer(BufferDesc &desc,
                                              const MemoryOptions &options) {
    desc.transports.push_back(TransportType::AscendDirect);
    hixl::MemDesc mem_desc{};
    mem_desc.addr =
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(desc.addr));
    mem_desc.len = desc.length;
    hixl::MemType mem_type;
    if (desc.location.starts_with("cpu")) {
        mem_type = hixl::MEM_HOST;
    } else if (desc.location.starts_with("npu")) {
        mem_type = hixl::MEM_DEVICE;
    } else if (desc.location == kWildcardLocation) {
        aclrtPtrAttributes attributes;
        auto ret = aclrtPointerGetAttributes(
            reinterpret_cast<void *>(desc.addr), &attributes);
        if (ret != ACL_SUCCESS) {
            LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
            return Status::InvalidArgument("Invalid location of addr:" +
                                           std::to_string(desc.addr));
        }
        if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
            mem_type = hixl::MEM_HOST;
        } else {
            mem_type = hixl::MEM_DEVICE;
        }
    } else {
        LOG(ERROR) << "location:" << desc.location << " is not supported.";
        return Status::InvalidArgument("Invalid location of addr:" +
                                       std::to_string(desc.addr));
    }
    hixl::MemHandle mem_handle;
    auto hixl_ret = hixl_->RegisterMem(mem_desc, mem_type, mem_handle);
    if (hixl_ret != hixl::SUCCESS) {
        LOG(ERROR) << "hixl_ret:" << hixl_ret
                   << ", errmsg: " << aclGetRecentErrMsg();
        return Status::InternalError("Register failed for addr:" +
                                     std::to_string(desc.addr));
    }
    LOG(INFO) << "AscendDirectTransport register mem addr:" << desc.addr
              << ", length:" << desc.length << ", location:" << desc.location
              << ", mem type:" << mem_type;
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    addr_to_mem_handle_[desc.addr] = mem_handle;
    return Status::OK();
}

Status AscendDirectTransport::removeMemoryBuffer(BufferDesc &desc) {
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    auto addr = desc.addr;
    if (addr_to_mem_handle_.find(addr) != addr_to_mem_handle_.end()) {
        (void)hixl_->DeregisterMem(addr_to_mem_handle_[addr]);
        addr_to_mem_handle_.erase(addr);
    }
    return Status::OK();
}

void AscendDirectTransport::localCopy(Request::OpCode opcode,
                                      const std::vector<HixlTask *> &tasks) {
    aclrtMemcpyKind kind;
    auto &first_task = tasks[0];
    auto remote_ptr =
        reinterpret_cast<void *>(first_task->request.target_offset);
    aclrtPtrAttributes attributes;
    auto ret =
        aclrtPointerGetAttributes(first_task->request.source, &attributes);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
        for (auto &task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }
    aclrtPtrAttributes dst_attributes;
    ret = aclrtPointerGetAttributes(remote_ptr, &dst_attributes);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
        for (auto &task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }
    if (attributes.location.type != ACL_MEM_LOCATION_TYPE_HOST &&
        attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE) {
        LOG(ERROR) << "location of local addr is not supported.";
        for (auto &task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }
    if (dst_attributes.location.type != ACL_MEM_LOCATION_TYPE_HOST &&
        dst_attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE) {
        LOG(ERROR) << "location of remote addr is not supported.";
        for (auto &task : tasks) {
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }
    if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST &&
        dst_attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        kind = ACL_MEMCPY_HOST_TO_HOST;
    } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE &&
               dst_attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        kind = ACL_MEMCPY_DEVICE_TO_DEVICE;
    } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        kind = (opcode == Request::OpCode::WRITE) ? ACL_MEMCPY_HOST_TO_DEVICE
                                                  : ACL_MEMCPY_DEVICE_TO_HOST;
    } else {
        kind = (opcode == Request::OpCode::WRITE) ? ACL_MEMCPY_DEVICE_TO_HOST
                                                  : ACL_MEMCPY_HOST_TO_DEVICE;
    }
    if (kind == ACL_MEMCPY_HOST_TO_HOST) {
        return copyWithSync(opcode, tasks, kind);
    }
    if (kind == ACL_MEMCPY_DEVICE_TO_DEVICE) {
        return copyWithAsync(opcode, tasks, kind);
    }
    auto left_num = tasks.size();
    size_t task_index = 0;
    while (left_num > 0) {
        auto batch_num = std::min(left_num, MEM_CPY_LIMIT);
        ret = copyWithBatch(opcode, tasks, kind, batch_num, task_index);
        if (ret == ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
            return copyWithAsync(opcode, tasks, kind);
        }
        left_num -= batch_num;
        task_index += batch_num;
    }
}

aclError AscendDirectTransport::copyWithBatch(
    Request::OpCode opcode, const std::vector<HixlTask *> &tasks,
    aclrtMemcpyKind kind, size_t batch_num, size_t task_index) const {
    std::vector<void *> void_remote_addrs(batch_num);
    std::vector<void *> void_local_addrs(batch_num);
    std::vector<aclrtMemcpyBatchAttr> attrs(batch_num);
    std::vector<size_t> attrsIds(batch_num);
    std::vector<size_t> sizes(batch_num);
    size_t idx = 0;
    for (size_t i = 0; i < batch_num; i++) {
        auto device_loc = aclrtMemLocation{
            static_cast<uint32_t>(device_logic_id_),
            aclrtMemLocationType::ACL_MEM_LOCATION_TYPE_DEVICE};
        auto host_loc = aclrtMemLocation{
            0, aclrtMemLocationType::ACL_MEM_LOCATION_TYPE_HOST};
        if (kind == ACL_MEMCPY_DEVICE_TO_HOST) {
            attrs[i] = aclrtMemcpyBatchAttr{host_loc, device_loc, {}};
        } else {
            attrs[i] = aclrtMemcpyBatchAttr{device_loc, host_loc, {}};
        }
        attrsIds[i] = idx++;
        auto &task = tasks[task_index + i];
        void_local_addrs[i] = task->request.source;
        void_remote_addrs[i] =
            reinterpret_cast<void *>(task->request.target_offset);
        sizes[i] = task->request.length;
    }
    size_t fail_idx;
    aclError ret;
    if (opcode == Request::OpCode::WRITE) {
        ret = aclrtMemcpyBatch(void_remote_addrs.data(), sizes.data(),
                               void_local_addrs.data(), sizes.data(),
                               sizes.size(), attrs.data(), attrsIds.data(),
                               attrs.size(), &fail_idx);
    } else {
        ret = aclrtMemcpyBatch(void_local_addrs.data(), sizes.data(),
                               void_remote_addrs.data(), sizes.data(),
                               sizes.size(), attrs.data(), attrsIds.data(),
                               attrs.size(), &fail_idx);
    }
    if (ret != ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
        if (ret == ACL_ERROR_NONE) {
            VLOG(1) << "Copy with aclrtMemcpyBatch suc.";
            for (size_t i = 0; i < batch_num; i++) {
                auto &task = tasks[task_index + i];
                task->status_word = TransferStatusEnum::COMPLETED;
            }
        } else {
            for (size_t i = 0; i < batch_num; i++) {
                auto &task = tasks[task_index + i];
                task->status_word = TransferStatusEnum::FAILED;
            }
        }
    }
    return ret;
}

void AscendDirectTransport::copyWithSync(Request::OpCode opcode,
                                         const std::vector<HixlTask *> &tasks,
                                         aclrtMemcpyKind kind) {
    for (auto &task : tasks) {
        auto local_ptr = task->request.source;
        auto remote_ptr = reinterpret_cast<void *>(task->request.target_offset);
        auto len = task->request.length;
        aclError ret;
        if (opcode == Request::OpCode::WRITE) {
            ret = aclrtMemcpy(remote_ptr, len, local_ptr, len, kind);
        } else {
            ret = aclrtMemcpy(local_ptr, len, remote_ptr, len, kind);
        }
        if (ret == ACL_ERROR_NONE) {
            VLOG(1) << "Copy with aclrtMemcpy suc.";
            task->status_word = TransferStatusEnum::COMPLETED;
        } else {
            LOG(ERROR) << "aclrtMemcpy failed, ret:" << ret;
            task->status_word = TransferStatusEnum::FAILED;
        }
    }
}
void AscendDirectTransport::copyWithAsync(Request::OpCode opcode,
                                          const std::vector<HixlTask *> &tasks,
                                          aclrtMemcpyKind kind) {
    std::vector<HixlTask *> async_list;
    aclError ret;
    for (auto &task : tasks) {
        auto local_ptr = task->request.source;
        auto remote_ptr = reinterpret_cast<void *>(task->request.target_offset);
        auto len = task->request.length;
        if (opcode == Request::OpCode::WRITE) {
            ret = aclrtMemcpyAsync(remote_ptr, len, local_ptr, len, kind,
                                   stream_);
        } else {
            ret = aclrtMemcpyAsync(local_ptr, len, remote_ptr, len, kind,
                                   stream_);
        }
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret;
            task->status_word = TransferStatusEnum::FAILED;
            continue;
        }
        async_list.emplace_back(task);
    }
    ret = aclrtSynchronizeStreamWithTimeout(
        stream_, static_cast<int32_t>(transfer_timeout_));
    if (ret == ACL_ERROR_NONE) {
        VLOG(1) << "Copy with aclrtMemcpyAsync suc.";
        for (auto &task : async_list) {
            task->status_word = TransferStatusEnum::COMPLETED;
        }
    } else {
        LOG(ERROR) << "Memory copy failed.";
        (void)aclrtStreamAbort(stream_);
        for (auto &task : async_list) {
            task->status_word = TransferStatusEnum::FAILED;
        }
    }
}

}  // namespace tent
}  // namespace mooncake
