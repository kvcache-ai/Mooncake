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

#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <exception>
#include <random>

#include "common.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

namespace mooncake {
namespace {
constexpr size_t kMemcpyBatchLimit = 4096U;
constexpr int64_t kMillisToNano = 1000000;
constexpr size_t kDefaultThreadPoolSize = 8U;
constexpr size_t kBufferModeThreadPoolSize = 1U;
constexpr size_t kAsyncTaskLimit = 100U;
constexpr int32_t kPortRange = 100;
constexpr int32_t kDefaultDisconnectTime = 1000;
constexpr int32_t kMaxGenPortAttempts = 500;
constexpr const char *kAutoConnect = "AutoConnect";
constexpr const char *kEnabled = "1";
constexpr const char *kDisabled = "0";
}  // namespace

AscendDirectTransport::AscendDirectTransport() : running_(false) {}

AscendDirectTransport::~AscendDirectTransport() {
    LOG(INFO) << "AscendDirectTransport destructor called";

    // Stop worker thread
    running_ = false;
    query_cv_.notify_all();
    async_task_cv_.notify_all();

    if (query_thread_.joinable()) {
        query_thread_.join();
    }

    // stop thread pool
    thread_pool_condition_.notify_all();
    for (std::thread &worker : workers_) {
        if (worker.joinable()) worker.join();
    }

    // Disconnect all connections
    std::lock_guard<std::mutex> lock(connection_mutex_);
    if (!connected_segments_.empty()) {
        for (auto &connected_segment : connected_segments_) {
            auto status =
                adxl_->Disconnect(connected_segment.c_str(), connect_timeout_);
            if (status != adxl::SUCCESS) {
                LOG(ERROR) << "Failed to disconnect AdxlEngine:"
                           << connected_segment;
            } else {
                LOG(INFO) << "Success to disconnect AdxlEngine:"
                          << connected_segment;
            }
        }
        connected_segments_.clear();
    }
    adxl_->Finalize();
}

int AscendDirectTransport::install(std::string &local_server_name,
                                   std::shared_ptr<TransferMetadata> meta,
                                   std::shared_ptr<Topology> topo) {
    LOG(INFO) << "install AscendDirectTransport for: " << local_server_name;
    // Call base class install method
    int ret = Transport::install(local_server_name, meta, topo);
    if (ret != 0) {
        LOG(ERROR) << "Failed to install base transport";
        return ret;
    }
    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR)
            << "AscendDirectTransport: cannot allocate local segment, ret: "
            << ret;
        return ret;
    }
    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "HcclTransport: cannot publish segments, "
                      "check the availability of metadata storage, ret: "
                   << ret;
        return ret;
    }
    ret = InitAdxlEngine();
    if (ret) {
        LOG(ERROR) << "AscendDirectTransport: InitAdxlEngine failed, ret: "
                   << ret;
        return ret;
    }
    char *connect_timeout_str = std::getenv("ASCEND_CONNECT_TIMEOUT");
    if (connect_timeout_str) {
        std::optional<int32_t> connect_timeout =
            parseFromString<int32_t>(connect_timeout_str);
        if (connect_timeout.has_value()) {
            connect_timeout_ = connect_timeout.value();
            LOG(INFO) << "Set connection timeout to:" << connect_timeout_;
        }
    }
    char *connect_transfer_str = std::getenv("ASCEND_TRANSFER_TIMEOUT");
    if (connect_transfer_str) {
        std::optional<int32_t> transfer_timeout =
            parseFromString<int32_t>(connect_transfer_str);
        if (transfer_timeout.has_value()) {
            transfer_timeout_ = transfer_timeout.value();
            LOG(INFO) << "Set transfer timeout to:" << transfer_timeout_;
        }
    }
    char *use_short_connection_str = std::getenv("ASCEND_USE_SHORT_CONNECTION");
    if (use_short_connection_str) {
        std::optional<int32_t> use_short_connection =
            parseFromString<int32_t>(use_short_connection_str);
        if (use_short_connection.has_value()) {
            use_short_connection_ =
                static_cast<bool>(use_short_connection.value());
            LOG(INFO) << "Set use enable short connection to:"
                      << use_short_connection_;
        }
    }
    transfer_timeout_in_nano_ = transfer_timeout_ * kMillisToNano;
    ret = aclrtCreateStreamWithConfig(
        &stream_, 0, ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "AscendDirectTransport: cannot create stream, ret: "
                   << ret;
        return FAILED;
    }
    running_ = true;
    query_thread_ = std::thread(&AscendDirectTransport::queryThread, this);
    size_t thread_pool_size =
        use_buffer_pool_ ? kBufferModeThreadPoolSize : kDefaultThreadPoolSize;
    char *ascend_thread_pool_size_str = std::getenv("ASCEND_THREAD_POOL_SIZE");
    if (ascend_thread_pool_size_str) {
        std::optional<int32_t> ascend_thread_pool_size_opt =
            parseFromString<int32_t>(ascend_thread_pool_size_str);
        if (ascend_thread_pool_size_opt.has_value()) {
            auto ascend_thread_pool_size =
                static_cast<size_t>(ascend_thread_pool_size_opt.value());
            if (ascend_thread_pool_size <= 16) {
                thread_pool_size = ascend_thread_pool_size;
                LOG(INFO) << "Set thread pool size to:" << thread_pool_size;
            } else {
                LOG(WARNING)
                    << "Invalid thread pool size:" << ascend_thread_pool_size;
            }
        }
    }
    // add thread pool
    for (size_t i = 0; i < thread_pool_size; ++i) {
        workers_.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(
                        this->thread_pool_queue_mutex_);
                    this->thread_pool_condition_.wait(lock, [this] {
                        return !this->running_ || !this->tasks_.empty();
                    });

                    if (!this->running_ && this->tasks_.empty()) return;

                    task = std::move(this->tasks_.front());
                    this->tasks_.pop();
                }
                task();
            }
        });
    }
    return 0;
}

int AscendDirectTransport::InitAdxlEngine() {
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    std::string host_ip = local_segment_desc->rank_info.hostIp;
    uint16_t host_port = local_segment_desc->rank_info.hostPort;
    adxl_ = std::make_unique<adxl::AdxlEngine>();
    if (!adxl_) return ERR_MEMORY;
    std::map<adxl::AscendString, adxl::AscendString> options;
    char *rdma_tc = std::getenv("ASCEND_RDMA_TC");
    if (rdma_tc) {
        options["adxl.RdmaTrafficClass"] = rdma_tc;
        LOG(INFO) << "Set RdmaTrafficClass to:" << rdma_tc;
    } else {
        rdma_tc = std::getenv("HCCL_RDMA_TC");
        if (rdma_tc) {
            options["adxl.RdmaTrafficClass"] = rdma_tc;
            LOG(INFO) << "Set RdmaTrafficClass to:" << rdma_tc;
        }
    }
    char *rdma_sl = std::getenv("ASCEND_RDMA_SL");
    if (rdma_sl) {
        options["adxl.RdmaServiceLevel"] = rdma_sl;
        LOG(INFO) << "Set RdmaServiceLevel to:" << rdma_sl;
    } else {
        rdma_sl = std::getenv("HCCL_RDMA_SL");
        if (rdma_sl) {
            options["adxl.RdmaServiceLevel"] = rdma_sl;
            LOG(INFO) << "Set RdmaServiceLevel to:" << rdma_sl;
        }
    }
    char *local_comm_res = std::getenv("ASCEND_LOCAL_COMM_RES");
    if (local_comm_res) {
        options["adxl.LocalCommRes"] = local_comm_res;
        LOG(INFO) << "Set LocalCommRes to:" << local_comm_res;
    }
    // check set async transfer
    char *use_async = std::getenv("ASCEND_USE_ASYNC_TRANSFER");
    if (use_async) {
#ifndef EXIST_ADXL_ASYNC_METHOD
        LOG(ERROR) << "ASCEND_USE_ASYNC_TRANSFER is set, but async transfer is "
                      "not available, please upgrade cann package.";
        return -1;
#endif
        LOG(INFO) << "Use async transfer";
        use_async_transfer_ = true;
    }
    char *auto_connect = std::getenv("ASCEND_AUTO_CONNECT");
    if (auto_connect) {
        auto auto_connect_opt = parseFromString<int32_t>(auto_connect);
        if (auto_connect_opt.has_value()) {
            auto_connect_ = (*auto_connect_opt == 1);
            options[kAutoConnect] = auto_connect_ ? kEnabled : kDisabled;
            LOG(INFO) << "Set AutoConnect to: " << auto_connect;
        }
    }
    // set default buffer pool
    options["adxl.BufferPool"] = "0:0";
    use_buffer_pool_ = false;
    char *buffer_pool = std::getenv("ASCEND_BUFFER_POOL");
    if (buffer_pool) {
        options["adxl.BufferPool"] = buffer_pool;
        if (std::strcmp(buffer_pool, "0:0") != 0) {
            LOG(INFO) << "Set adxl.BufferPool to:" << buffer_pool;
            use_buffer_pool_ = true;
            if (use_async_transfer_) {
                LOG(ERROR) << "Buffer pool mode do not support async transfer.";
                return -1;
            }
        }
    }
    if (globalConfig().ascend_use_fabric_mem) {
        if (use_buffer_pool_) {
            LOG(ERROR) << "Buffer pool and fabric mem mode can not be enabled "
                          "simultaneously.";
            return -1;
        }
        options["EnableUseFabricMem"] = "1";
        LOG(INFO) << "Fabric mem mode is enabled.";
    }
    char *global_resource_config = std::getenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
    if (global_resource_config) {
        options["GlobalResourceConfig"] = global_resource_config;
        LOG(INFO) << "Set GlobalResourceConfig to:" << global_resource_config;
    }
    std::string engine_name_str =
        (globalConfig().use_ipv6 ? ("[" + host_ip + "]") : host_ip) + ":" +
        std::to_string(host_port);
    auto adxl_engine_name = adxl::AscendString(engine_name_str.c_str());
    LOG(INFO) << "Set adxl engine name to " << adxl_engine_name.GetString();
    auto status = adxl_->Initialize(adxl_engine_name, options);
    if (status != adxl::SUCCESS) {
        LOG(ERROR) << "Failed to initialize AdxlEngine, status: " << status;
        return -1;
    }
    LOG(INFO) << "Success to initialize adxl engine:"
              << adxl_engine_name.GetString()
              << " with device_id:" << device_logic_id_ << ", pid:" << getpid();
    return 0;
}

template <class F, class... Args>
void AscendDirectTransport::enqueue(F &&f, Args &&...args) {
    auto task = std::make_shared<std::function<void()>>(
        [f = std::forward<F>(f),
         ... args = std::forward<Args>(args)]() mutable {
            std::invoke(f, args...);
        });
    {
        std::unique_lock<std::mutex> lock(thread_pool_queue_mutex_);
        if (!running_) {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }
        tasks_.emplace([task] { (*task)(); });
    }
    thread_pool_condition_.notify_one();  ///< Wake one waiting worker
}

void AscendDirectTransport::submitSlices(std::vector<Slice *> &slice_list) {
    std::unordered_map<SegmentID, std::vector<Slice *>> seg_to_slices;
    for (auto slice : slice_list) {
        seg_to_slices[slice->target_id].push_back(slice);
    }
    for (auto &[seg_id, slices] : seg_to_slices) {
        enqueue([this, moved_slices = std::move(slices)] {
            static thread_local bool context_set = false;
            if (!context_set) {
                auto ret = aclrtSetCurrentContext(rt_context_);
                if (ret) {
                    LOG(ERROR)
                        << "Call aclrtSetCurrentContext failed, ret: " << ret;
                    return;
                }
                context_set = true;
            }
            processSliceList(moved_slices);
        });
    }
}

Status AscendDirectTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "AscendDirectTransport: Exceed the limitation of current "
                      "batch's capacity";
        return Status::InvalidArgument(
            "AscendDirectTransport: Exceed the limitation of capacity, batch "
            "id: " +
            std::to_string(batch_id));
    }

    auto cur_task_size = batch_desc.task_list.size();
    batch_desc.task_list.resize(cur_task_size + entries.size());
    std::vector<Slice *> slice_list;
    slice_list.reserve(entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[cur_task_size];
        ++cur_task_size;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->ascend_direct.dest_addr = request.target_offset;
        slice->task = &task;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }
    submitSlices(slice_list);
    return Status::OK();
}

Status AscendDirectTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::vector<Slice *> slice_list;
    slice_list.reserve(task_list.size());

    for (auto index : task_list) {
        assert(index);
        auto &task = *index;
        assert(task.request);
        auto &request = *task.request;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->ascend_direct.dest_addr = request.target_offset;
        slice->task = &task;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }

    submitSlices(slice_list);
    return Status::OK();
}

Status AscendDirectTransport::getTransferStatus(BatchID batch_id,
                                                size_t task_id,
                                                TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "HcclTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
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

int AscendDirectTransport::registerLocalMemory(void *addr, size_t length,
                                               const std::string &location,
                                               bool remote_accessible,
                                               bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = location;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = (uint64_t)length;

    int ret;
    adxl::MemDesc mem_desc{};
    mem_desc.addr = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(addr));
    mem_desc.len = length;
    adxl::MemType mem_type;
    if (location.starts_with("cpu")) {
        mem_type = adxl::MEM_HOST;
    } else if (location.starts_with("npu")) {
        mem_type = adxl::MEM_DEVICE;
    } else if (location == kWildcardLocation) {
        aclrtPtrAttributes attributes;
        ret = aclrtPointerGetAttributes(addr, &attributes);
        if (ret != ACL_SUCCESS) {
            LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
            return -1;
        }
        if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
            mem_type = adxl::MEM_HOST;
        } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
            mem_type = adxl::MEM_DEVICE;
        } else {
            LOG(ERROR) << "location:" << location << " is not supported.";
            return ERR_INVALID_ARGUMENT;
        }
    } else {
        LOG(ERROR) << "location:" << location << " is not supported.";
        return ERR_INVALID_ARGUMENT;
    }
    LOG(INFO) << "AscendDirectTransport register mem addr:" << addr
              << ", length:" << length << ", location:" << location
              << ", mem type:" << mem_type;
    ret = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (ret) {
        LOG(ERROR) << "HcclTransport: addLocalMemoryBuffer failed, ret: "
                   << ret;
        return ret;
    }
    // memory type is HOST and use buffer pool, do not register to ADXL
    if (mem_type == adxl::MEM_HOST && use_buffer_pool_) {
        return 0;
    }
    adxl::MemHandle mem_handle;
    auto adxl_ret = adxl_->RegisterMem(mem_desc, mem_type, mem_handle);
    if (adxl_ret != adxl::SUCCESS) {
        LOG(ERROR) << "Register mem ret:" << adxl_ret << ".";
        return -1;
    }
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    addr_to_mem_handle_[addr] = mem_handle;
    return 0;
}

int AscendDirectTransport::unregisterLocalMemory(void *addr,
                                                 bool update_metadata) {
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    if (addr_to_mem_handle_.find(addr) != addr_to_mem_handle_.end()) {
        (void)adxl_->DeregisterMem(addr_to_mem_handle_[addr]);
        addr_to_mem_handle_.erase(addr);
    }
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int AscendDirectTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    LOG(INFO) << "AscendDirectTransport::registerLocalMemoryBatch called with "
                 "buffer count: "
              << buffer_list.size() << ", location: " << location;

    for (const auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret != 0) {
            LOG(ERROR) << "Failed to register memory in batch, addr: "
                       << buffer.addr;
            return ret;
        }
    }

    // Update metadata once for the entire batch
    return metadata_->updateLocalSegmentDesc();
}

int AscendDirectTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    LOG(INFO) << "AscendDirectTransport::unregisterLocalMemoryBatch called "
                 "with addr count: "
              << addr_list.size();

    for (void *addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret != 0) {
            LOG(ERROR) << "Failed to unregister memory in batch, addr: "
                       << addr;
            return ret;
        }
    }

    // Update metadata once for the entire batch
    return metadata_->updateLocalSegmentDesc();
}

int AscendDirectTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "ascend";

    // Parse local server name to get host IP and port
    auto [host_ip, host_port] = parseHostNameWithPort(local_server_name_);
    auto ret = aclrtGetDevice(&device_logic_id_);
    if (ret) {
        LOG(ERROR) << "Call aclrtGetDevice failed, ret: " << ret;
        return ret;
    }
    ret = aclrtGetCurrentContext(&rt_context_);
    if (ret) {
        LOG(ERROR) << "Call aclrtGetCurrentContext failed, ret: " << ret;
        return ret;
    }
    desc->rank_info.hostIp = host_ip;
    desc->rank_info.hostPort = findAdxlListenPort();
    if (desc->rank_info.hostPort == 0) {
        LOG(ERROR) << "Find available port failed.";
        return FAILED;
    }
    local_adxl_engine_name_ =
        (globalConfig().use_ipv6 ? ("[" + host_ip + "]") : host_ip) + ":" +
        std::to_string(desc->rank_info.hostPort);

    LOG(INFO) << "AscendDirectTransport set segment desc: host_ip=" << host_ip
              << ", host_port=" << desc->rank_info.hostPort
              << ", deviceLogicId=" << device_logic_id_;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

uint16_t AscendDirectTransport::findAdxlListenPort() {
    char *adxl_base_port = std::getenv("ASCEND_BASE_PORT");
    if (adxl_base_port) {
        std::optional<int32_t> base_port =
            parseFromString<int32_t>(adxl_base_port);
        if (base_port.has_value()) {
            base_port_ = base_port.value();
            LOG(INFO) << "Set base port to:" << base_port_;
        } else {
            LOG(WARNING) << "ASCEND_BASE_PORT is not valid, value:"
                         << adxl_base_port;
        }
    }
    int32_t dev_id = device_logic_id_;
    char *rt_visible_devices = std::getenv("ASCEND_RT_VISIBLE_DEVICES");
    if (rt_visible_devices) {
        std::vector<std::string> device_list;
        std::stringstream ss(rt_visible_devices);
        std::string item;
        while (std::getline(ss, item, ',')) {
            device_list.push_back(item);
        }
        if (dev_id < static_cast<int32_t>(device_list.size())) {
            std::optional<int32_t> dev_id_opt =
                parseFromString<int32_t>(device_list[dev_id]);
            if (dev_id_opt.has_value()) {
                dev_id = dev_id_opt.value();
                LOG(INFO) << "Set device id to:" << dev_id;
            } else {
                LOG(WARNING) << "Device id is " << dev_id
                             << ", ASCEND_RT_VISIBLE_DEVICES is "
                             << rt_visible_devices << ", which is unexpected.";
            }
        }
    }
    static std::random_device rand_gen;
    std::uniform_int_distribution rand_dist;
    const int min_port = base_port_ + dev_id * kPortRange;
    const int max_port = base_port_ + (dev_id + 1) * kPortRange;
    LOG(INFO) << "Find available between " << min_port << " and " << max_port;
    bool use_ipv6 = globalConfig().use_ipv6;
    int sockfd;
    for (int attempt = 0; attempt < kMaxGenPortAttempts; ++attempt) {
        int port = min_port + rand_dist(rand_gen) % (max_port - min_port + 1);
        sockfd = socket(use_ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
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
        sockaddr_storage bind_address_storage;
        memset(&bind_address_storage, 0, sizeof(bind_address_storage));
        auto *bind_addr = reinterpret_cast<sockaddr *>(&bind_address_storage);
        socklen_t addr_len;
        if (use_ipv6) {
            auto *addr6 = reinterpret_cast<sockaddr_in6 *>(bind_addr);
            addr6->sin6_family = AF_INET6;
            addr6->sin6_port = htons(port);
            addr6->sin6_addr = IN6ADDR_ANY_INIT;
            addr_len = sizeof(*addr6);
        } else {
            auto *addr4 = reinterpret_cast<sockaddr_in *>(bind_addr);
            addr4->sin_family = AF_INET;
            addr4->sin_port = htons(port);
            addr4->sin_addr.s_addr = INADDR_ANY;
            addr_len = sizeof(*addr4);
        }
        if (bind(sockfd, bind_addr, addr_len) < 0) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        close(sockfd);
        return port;
    }
    return 0;
}

void AscendDirectTransport::queryThread() {
#ifdef EXIST_ADXL_ASYNC_METHOD
    LOG(INFO) << "AscendDirectTransport query thread started";
    std::vector<std::vector<Slice *>> pending_batches;
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(query_mutex_);
            if (pending_batches.empty()) {
                query_cv_.wait(lock, [this] {
                    return !running_ || !query_slice_queue_.empty();
                });
            }
            if (!running_) {
                break;
            }
            while (!query_slice_queue_.empty()) {
                pending_batches.emplace_back(
                    std::move(query_slice_queue_.front()));
                query_slice_queue_.pop();
            }
        }

        if (pending_batches.empty()) {
            continue;
        }

        auto it = pending_batches.begin();
        while (it != pending_batches.end()) {
            auto &slice_list = *it;
            if (slice_list.empty()) {
                it = pending_batches.erase(it);
                continue;
            }
            auto handle = static_cast<adxl::TransferReq>(
                slice_list[0]->ascend_direct.handle);
            adxl::TransferStatus task_status;
            auto ret = adxl_->GetTransferStatus(handle, task_status);
            bool task_finished = true;
            if (ret != adxl::SUCCESS ||
                task_status == adxl::TransferStatus::FAILED) {
                LOG(ERROR) << "Get transfer status failed, ret: " << ret;
                for (auto &slice : slice_list) {
                    slice->markFailed();
                }
                it = pending_batches.erase(it);
            } else if (task_status == adxl::TransferStatus::COMPLETED) {
                auto now = getCurrentTimeInNano();
                auto duration = now - slice_list[0]->ascend_direct.start_time;
                auto target_segment_desc =
                    metadata_->getSegmentDescByID(slice_list[0]->target_id);
                if (target_segment_desc) {
                    auto target_adxl_engine_name =
                        (globalConfig().use_ipv6
                             ? ("[" + target_segment_desc->rank_info.hostIp +
                                "]")
                             : target_segment_desc->rank_info.hostIp) +
                        ":" +
                        std::to_string(target_segment_desc->rank_info.hostPort);
                    VLOG(1) << "Transfer to " << target_adxl_engine_name
                            << " time: " << duration / 1000 << "us";
                }
                for (auto &slice : slice_list) {
                    slice->markSuccess();
                }
                it = pending_batches.erase(it);
            } else {
                auto now = getCurrentTimeInNano();
                if (now - slice_list[0]->ascend_direct.start_time >
                    transfer_timeout_in_nano_) {
                    LOG(ERROR)
                        << "Transfer timeout, you can increase the timeout "
                           "duration to reduce "
                           "the failure rate by configuring "
                           "the ASCEND_TRANSFER_TIMEOUT environment variable.";
                    for (auto &slice : slice_list) {
                        slice->markFailed();
                    }
                    it = pending_batches.erase(it);
                } else {
                    task_finished = false;
                    ++it;
                }
            }
            if (task_finished) {
                std::lock_guard<std::mutex> lock(async_task_mutex_);
                if (active_async_tasks_ > 0) {
                    active_async_tasks_--;
                }
                async_task_cv_.notify_one();
            }
        }

        if (!pending_batches.empty()) {
            // Avoid busy loop
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    }
    LOG(INFO) << "AscendDirectTransport query thread stopped";
#endif
}

void AscendDirectTransport::processSliceList(
    const std::vector<Slice *> &slice_list) {
    if (slice_list.empty()) {
        return;
    }
    auto it = need_update_metadata_segs_.find(slice_list[0]->target_id);
    auto target_segment_desc = metadata_->getSegmentDescByID(
        slice_list[0]->target_id, (it != need_update_metadata_segs_.end()));
    if (!target_segment_desc) {
        LOG(ERROR) << "Cannot find segment descriptor for target_id: "
                   << slice_list[0]->target_id;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    if (it != need_update_metadata_segs_.end()) {
        need_update_metadata_segs_.erase(it);
    }
    auto target_adxl_engine_name =
        (globalConfig().use_ipv6
             ? ("[" + target_segment_desc->rank_info.hostIp + "]")
             : target_segment_desc->rank_info.hostIp) +
        ":" + std::to_string(target_segment_desc->rank_info.hostPort);
    adxl::TransferOp operation;
    if (slice_list[0]->opcode == TransferRequest::WRITE) {
        operation = adxl::WRITE;
    } else if (slice_list[0]->opcode == TransferRequest::READ) {
        operation = adxl::READ;
    } else {
        LOG(ERROR) << "Unsupported opcode: " << slice_list[0]->opcode;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    if (target_adxl_engine_name == local_adxl_engine_name_) {
        auto start = std::chrono::steady_clock::now();
        localCopy(slice_list[0]->opcode, slice_list);
        VLOG(1) << "Local copy time: "
                << std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count()
                << "us";
        return;
    }
    return connectAndTransfer(target_adxl_engine_name, operation, slice_list);
}

void AscendDirectTransport::connectAndTransfer(
    const std::string &target_adxl_engine_name, adxl::TransferOp operation,
    const std::vector<Slice *> &slice_list, int32_t times) {
    if (!auto_connect_) {
        int ret = checkAndConnect(target_adxl_engine_name);
        if (ret != 0) {
            for (auto &slice : slice_list) {
                slice->markFailed();
            }
            return;
        }
    }
    auto start = std::chrono::steady_clock::now();
    std::vector<adxl::TransferOpDesc> op_descs;
    op_descs.reserve(slice_list.size());
    for (auto &slice : slice_list) {
        adxl::TransferOpDesc op_desc{};
        op_desc.local_addr = reinterpret_cast<uintptr_t>(slice->source_addr);
        op_desc.remote_addr =
            reinterpret_cast<uintptr_t>(slice->ascend_direct.dest_addr);
        op_desc.len = slice->length;
        op_descs.emplace_back(op_desc);
    }
    if (use_async_transfer_) {
        return TransferWithAsync(target_adxl_engine_name, operation, slice_list,
                                 op_descs);
    }
    auto status = adxl_->TransferSync(target_adxl_engine_name.c_str(),
                                      operation, op_descs, transfer_timeout_);
    if (status == adxl::SUCCESS) {
        for (auto &slice : slice_list) {
            slice->markSuccess();
        }
        VLOG(1) << "Transfer to:" << target_adxl_engine_name << ", cost: "
                << std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count()
                << " us";
        if (use_short_connection_) {
            disconnect(target_adxl_engine_name, connect_timeout_);
        }
    } else {
        if (status == adxl::TIMEOUT) {
            LOG(ERROR) << "Transfer timeout to: " << target_adxl_engine_name
                       << ", you can increase the timeout duration to reduce "
                          "the failure rate by configuring "
                          "the ASCEND_TRANSFER_TIMEOUT environment variable.";
        } else {
            LOG(ERROR) << "Transfer slice failed with status: " << status;
        }
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        // the connection is probably broken.
        // set small timeout to just release local res.
        LOG(INFO) << "transfer failed and disconnect to:"
                  << target_adxl_engine_name;
        disconnect(target_adxl_engine_name, kDefaultDisconnectTime);
        need_update_metadata_segs_.emplace(slice_list[0]->target_id);
    }
}

void AscendDirectTransport::TransferWithAsync(
    const std::string &target_adxl_engine_name, adxl::TransferOp operation,
    const std::vector<Slice *> &slice_list,
    const std::vector<adxl::TransferOpDesc> &op_descs) {
#ifdef EXIST_ADXL_ASYNC_METHOD
    auto start_time = getCurrentTimeInNano();
    for (auto &slice : slice_list) {
        slice->ascend_direct.start_time = start_time;
    }

    {
        std::unique_lock<std::mutex> lock(async_task_mutex_);
        async_task_cv_.wait(lock, [this] {
            return !running_ || active_async_tasks_ < kAsyncTaskLimit;
        });
        if (!running_) {
            for (auto &slice : slice_list) {
                slice->markFailed();
            }
            return;
        }
        active_async_tasks_++;
    }

    adxl::TransferReq req_handle;
    auto status =
        adxl_->TransferAsync(target_adxl_engine_name.c_str(), operation,
                             op_descs, adxl::TransferArgs(), req_handle);
    if (status == adxl::SUCCESS) {
        for (auto &slice : slice_list) {
            slice->ascend_direct.handle = req_handle;
        }
        {
            std::unique_lock<std::mutex> lock(query_mutex_);
            query_slice_queue_.push(slice_list);
        }
        query_cv_.notify_one();
    } else {
        {
            std::lock_guard<std::mutex> lock(async_task_mutex_);
            if (active_async_tasks_ > 0) {
                active_async_tasks_--;
            }
            async_task_cv_.notify_one();
        }
        LOG(ERROR) << "Call transfer async failed with status: " << status;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        // the connection is probably broken.
        // set small timeout to just release local res.
        disconnect(target_adxl_engine_name, kDefaultDisconnectTime);
        need_update_metadata_segs_.emplace(slice_list[0]->target_id);
    }
#endif
}

void AscendDirectTransport::localCopy(TransferRequest::OpCode opcode,
                                      const std::vector<Slice *> &slice_list) {
    aclrtMemcpyKind kind;
    auto &first_slice = slice_list[0];
    auto remote_ptr =
        reinterpret_cast<void *>(first_slice->ascend_direct.dest_addr);
    aclrtPtrAttributes attributes;
    auto ret = aclrtPointerGetAttributes(first_slice->source_addr, &attributes);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
    }
    aclrtPtrAttributes dst_attributes;
    ret = aclrtPointerGetAttributes(remote_ptr, &dst_attributes);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
    }
    if (attributes.location.type != ACL_MEM_LOCATION_TYPE_HOST &&
        attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE) {
        LOG(ERROR) << "location of local addr is not supported.";
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
    }
    if (dst_attributes.location.type != ACL_MEM_LOCATION_TYPE_HOST &&
        dst_attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE) {
        LOG(ERROR) << "location of remote addr is not supported.";
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
    }
    if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST &&
        dst_attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        kind = ACL_MEMCPY_HOST_TO_HOST;
    } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE &&
               dst_attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        kind = ACL_MEMCPY_DEVICE_TO_DEVICE;
    } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        kind = (opcode == TransferRequest::WRITE) ? ACL_MEMCPY_HOST_TO_DEVICE
                                                  : ACL_MEMCPY_DEVICE_TO_HOST;
    } else {
        kind = (opcode == TransferRequest::WRITE) ? ACL_MEMCPY_DEVICE_TO_HOST
                                                  : ACL_MEMCPY_HOST_TO_DEVICE;
    }
    if (kind == ACL_MEMCPY_HOST_TO_HOST) {
        return copyWithSync(opcode, slice_list, kind);
    }
    if (kind == ACL_MEMCPY_DEVICE_TO_DEVICE) {
        return copyWithAsync(opcode, slice_list, kind);
    }
    auto left_num = slice_list.size();
    size_t slice_index = 0;
    while (left_num > 0) {
        auto batch_num = std::min(left_num, kMemcpyBatchLimit);
        ret = copyWithBatch(opcode, slice_list, kind, batch_num, slice_index);
        if (ret == ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
            return copyWithAsync(opcode, slice_list, kind);
        }
        left_num -= batch_num;
        slice_index += batch_num;
    }
}

aclError AscendDirectTransport::copyWithBatch(
    TransferRequest::OpCode opcode, const std::vector<Slice *> &slice_list,
    aclrtMemcpyKind kind, size_t batch_num, size_t slice_index) const {
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
        auto &slice = slice_list[slice_index + i];
        void_local_addrs[i] = slice->source_addr;
        void_remote_addrs[i] =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
        sizes[i] = slice->length;
    }
    size_t fail_idx;
    aclError ret;
    if (opcode == TransferRequest::WRITE) {
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
            for (size_t i = 0; i < 0 + batch_num; i++) {
                auto &slice = slice_list[slice_index + i];
                slice->markSuccess();
            }
        } else {
            for (size_t i = 0; i < batch_num; i++) {
                auto &slice = slice_list[slice_index + i];
                slice->markFailed();
            }
        }
    }
    return ret;
}

void AscendDirectTransport::copyWithSync(TransferRequest::OpCode opcode,
                                         const std::vector<Slice *> &slice_list,
                                         aclrtMemcpyKind kind) {
    for (auto &slice : slice_list) {
        auto local_ptr = slice->source_addr;
        auto remote_ptr =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
        auto len = slice->length;
        aclError ret;
        if (opcode == TransferRequest::WRITE) {
            ret = aclrtMemcpy(remote_ptr, len, local_ptr, len, kind);
        } else {
            ret = aclrtMemcpy(local_ptr, len, remote_ptr, len, kind);
        }
        if (ret == ACL_ERROR_NONE) {
            VLOG(1) << "Copy with aclrtMemcpy suc.";
            slice->markSuccess();
        } else {
            LOG(ERROR) << "aclrtMemcpy failed, ret:" << ret;
            slice->markFailed();
        }
    }
}

void AscendDirectTransport::copyWithAsync(
    TransferRequest::OpCode opcode, const std::vector<Slice *> &slice_list,
    aclrtMemcpyKind kind) {
    std::vector<Slice *> async_list;
    aclError ret;
    for (auto &slice : slice_list) {
        auto local_ptr = slice->source_addr;
        auto remote_ptr =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
        auto len = slice->length;
        if (opcode == TransferRequest::WRITE) {
            ret = aclrtMemcpyAsync(remote_ptr, len, local_ptr, len, kind,
                                   stream_);
        } else {
            ret = aclrtMemcpyAsync(local_ptr, len, remote_ptr, len, kind,
                                   stream_);
        }
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret;
            slice->markFailed();
            continue;
        }
        async_list.emplace_back(slice);
    }
    ret = aclrtSynchronizeStreamWithTimeout(stream_, transfer_timeout_);
    if (ret == ACL_ERROR_NONE) {
        VLOG(1) << "Copy with aclrtMemcpyAsync suc.";
        for (auto &slice : async_list) {
            slice->markSuccess();
        }
    } else {
        LOG(ERROR) << "Memory copy failed.";
        (void)aclrtStreamAbort(stream_);
        for (auto &slice : async_list) {
            slice->markFailed();
        }
    }
}

int AscendDirectTransport::checkAndConnect(
    const std::string &target_adxl_engine_name) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto it = connected_segments_.find(target_adxl_engine_name);
    if (it != connected_segments_.end()) {
        VLOG(1) << "Already connected to target adxl engine: "
                << target_adxl_engine_name;
        return 0;
    }
    auto start = std::chrono::steady_clock::now();
    auto status =
        adxl_->Connect(target_adxl_engine_name.c_str(), connect_timeout_);
    if (status == adxl::TIMEOUT) {
        LOG(ERROR) << "Connect timeout to: " << target_adxl_engine_name
                   << ", you can increase the timeout duration to reduce "
                      "the ASCEND_CONNECT_TIMEOUT environment variable.";
    } else if (status != adxl::SUCCESS) {
        LOG(ERROR) << "Failed to connect to target: " << target_adxl_engine_name
                   << ", status: " << status;
        return -1;
    }
    connected_segments_.emplace(target_adxl_engine_name);
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();
    LOG(INFO) << "Connected to segment: " << target_adxl_engine_name
              << ", cost:" << count << " us.";
    return 0;
}

int AscendDirectTransport::disconnect(
    const std::string &target_adxl_engine_name, int32_t timeout_in_millis,
    bool force) {
    if (auto_connect_) {
        auto status = adxl_->Disconnect(target_adxl_engine_name.c_str(),
                                        timeout_in_millis);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to disconnect to: " << target_adxl_engine_name
                       << ", status: " << status;
            return -1;
        }
        return 0;
    }
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto it = connected_segments_.find(target_adxl_engine_name);
    if (it == connected_segments_.end()) {
        LOG(INFO) << "Target adxl engine: " << target_adxl_engine_name
                  << " is not connected.";
        return 0;
    }
    if (!force) {
        auto status = adxl_->Disconnect(target_adxl_engine_name.c_str(),
                                        timeout_in_millis);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to disconnect to: " << target_adxl_engine_name
                       << ", status: " << status;
            connected_segments_.erase(target_adxl_engine_name);
            return -1;
        }
    }
    connected_segments_.erase(target_adxl_engine_name);
    return 0;
}
}  // namespace mooncake