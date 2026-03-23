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

#include "transport/ascend_transport/ascend_direct_transport/transfer_executor_base.h"
#include "transport/ascend_transport/ascend_direct_transport/async_transfer_executor.h"
#include "transport/ascend_transport/ascend_direct_transport/local_copy_engine.h"
#include "transport/ascend_transport/ascend_direct_transport/sync_transfer_executor.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <unistd.h>

#include "ascend_allocator.h"
#include "common.h"
#include "config.h"
#include "transfer_metadata.h"

namespace mooncake {
namespace {
constexpr const char* kAutoConnect = "AutoConnect";
constexpr const char* kEnabled = "1";
constexpr const char* kDisabled = "0";
constexpr int32_t kTransferRetryTimes = 2;

void markSlicesFailed(const std::vector<Transport::Slice*>& slice_list) {
    for (auto* slice : slice_list) {
        slice->markFailed();
    }
}
}  // namespace

TransferExecutorBase::TransferExecutorBase(const InitParams& params)
    : params_(params),
      local_engine_contexts_(params.local_engine_contexts),
      metadata_(params.metadata) {}

TransferExecutorBase::~TransferExecutorBase() = default;

std::unique_ptr<TransferExecutorBase> TransferExecutorBase::Create(
    const InitParams& params) {
    InitParams parsed = params;
    ParseExecutorEnvIntoInitParams(parsed);
    if (parsed.use_async_transfer) {
        return std::make_unique<AsyncTransferExecutor>(parsed);
    }
    return std::make_unique<SyncTransferExecutor>(parsed);
}

void TransferExecutorBase::ParseExecutorEnvIntoInitParams(InitParams& params) {
    char* connect_timeout_str = std::getenv("ASCEND_CONNECT_TIMEOUT");
    if (connect_timeout_str) {
        auto connect_timeout = parseFromString<int32_t>(connect_timeout_str);
        if (connect_timeout.has_value()) {
            params.connect_timeout = connect_timeout.value();
            LOG(INFO) << "Set connection timeout to:" << params.connect_timeout;
        }
    }
    char* transfer_timeout_str = std::getenv("ASCEND_TRANSFER_TIMEOUT");
    if (transfer_timeout_str) {
        auto transfer_timeout = parseFromString<int32_t>(transfer_timeout_str);
        if (transfer_timeout.has_value()) {
            params.transfer_timeout = transfer_timeout.value();
            LOG(INFO) << "Set transfer timeout to:" << params.transfer_timeout;
        }
    }
    char* use_short_connection_str = std::getenv("ASCEND_USE_SHORT_CONNECTION");
    if (use_short_connection_str) {
        auto use_short_connection =
            parseFromString<int32_t>(use_short_connection_str);
        if (use_short_connection.has_value()) {
            params.use_short_connection =
                static_cast<bool>(use_short_connection.value());
            LOG(INFO) << "Set use short connection to:"
                      << params.use_short_connection;
        }
    }
    char* use_async = std::getenv("ASCEND_USE_ASYNC_TRANSFER");
    if (use_async) {
        params.use_async_transfer = true;
    }
    char* auto_connect = std::getenv("ASCEND_AUTO_CONNECT");
    if (auto_connect) {
        auto auto_connect_opt = parseFromString<int32_t>(auto_connect);
        if (auto_connect_opt.has_value() && *auto_connect_opt == 1) {
            params.auto_connect = true;
        }
    }
    char* buffer_pool = std::getenv("ASCEND_BUFFER_POOL");
    if (buffer_pool && std::strcmp(buffer_pool, "0:0") != 0) {
        params.use_buffer_pool = true;
    }
}

int TransferExecutorBase::initEngines() {
    if (params_.use_buffer_pool && params_.use_async_transfer) {
        LOG(ERROR) << "Buffer pool mode does not support async transfer.";
        return -1;
    }
    local_copy_engine_ = std::make_unique<LocalCopyEngine>();
    if (!local_copy_engine_) {
        return ERR_MEMORY;
    }
    int ret = local_copy_engine_->Initialize(params_.transfer_timeout);
    if (ret != 0) {
        LOG(ERROR) << "Failed to initialize LocalCopyEngine, ret: " << ret;
        return ret;
    }
    aclrtContext saved_ctx = nullptr;
    if (aclrtGetCurrentContext(&saved_ctx) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return -1;
    }
    MAKE_GUARD(ctx_restore,
               [saved_ctx]() { (void)aclrtSetCurrentContext(saved_ctx); });

    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!local_segment_desc) {
        LOG(ERROR) << "Cannot get local segment descriptor";
        return -1;
    }

    std::map<adxl::AscendString, adxl::AscendString> options;
    char* rdma_tc = std::getenv("ASCEND_RDMA_TC");
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
    char* rdma_sl = std::getenv("ASCEND_RDMA_SL");
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
    char* local_comm_res = std::getenv("ASCEND_LOCAL_COMM_RES");
    if (local_comm_res) {
        options["adxl.LocalCommRes"] = local_comm_res;
        LOG(INFO) << "Set LocalCommRes to:" << local_comm_res;
    }

    char* auto_connect = std::getenv("ASCEND_AUTO_CONNECT");
    if (auto_connect) {
        auto auto_connect_opt = parseFromString<int32_t>(auto_connect);
        if (auto_connect_opt.has_value()) {
            options[kAutoConnect] =
                (*auto_connect_opt == 1) ? kEnabled : kDisabled;
            LOG(INFO) << "Set AutoConnect to: " << auto_connect;
        }
    }

    options["adxl.BufferPool"] = "0:0";
    char* buffer_pool = std::getenv("ASCEND_BUFFER_POOL");
    if (buffer_pool) {
        options["adxl.BufferPool"] = buffer_pool;
        if (std::strcmp(buffer_pool, "0:0") != 0) {
            LOG(INFO) << "Set adxl.BufferPool to:" << buffer_pool;
            if (params_.use_async_transfer) {
                LOG(ERROR) << "Buffer pool mode do not support async transfer.";
                return -1;
            }
        }
    }

    if (globalConfig().ascend_use_fabric_mem) {
        options["EnableUseFabricMem"] = "1";
        LOG(INFO) << "Fabric mem mode is enabled.";
    }

    char* global_resource_config = std::getenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
    if (global_resource_config) {
        options["GlobalResourceConfig"] = global_resource_config;
        LOG(INFO) << "Set GlobalResourceConfig to:" << global_resource_config;
    }

    if (params_.use_async_transfer) {
        LOG(INFO) << "Use async transfer";
    }

    const auto& endpoints = local_segment_desc->rank_info.endpoints;
    for (size_t idx = 0; idx < endpoints.size(); ++idx) {
        if (idx >= local_engine_contexts_.size()) {
            LOG(ERROR) << "Endpoint count exceeds local_engine_contexts size";
            return -1;
        }
        CHECK_ACL(aclrtSetCurrentContext(local_engine_contexts_[idx]));
        const auto& adxl_engine_name = endpoints[idx];
        auto engine = std::make_shared<adxl::AdxlEngine>();
        if (!engine) {
            return ERR_MEMORY;
        }
        auto status = engine->Initialize(adxl_engine_name.c_str(), options);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to initialize AdxlEngine, status: " << status
                       << ", errmsg: " << aclGetRecentErrMsg();
            return -1;
        }
        params_.local_adxl_engine_names.push_back(adxl_engine_name);
        adxl_engines_.emplace_back(std::move(engine));
        LOG(INFO) << "Success to initialize adxl engine:" << adxl_engine_name
                  << ", pid:" << getpid();
    }
    return 0;
}

void TransferExecutorBase::finalizeEngines() {
    for (auto& engine : adxl_engines_) {
        if (engine != nullptr) {
            engine->Finalize();
        }
    }
    if (local_copy_engine_) {
        local_copy_engine_->Finalize();
        local_copy_engine_.reset();
    }
}

int TransferExecutorBase::checkAndConnect(
    size_t engine_idx, const std::string& target_adxl_engine_name) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto& engine_connections = connected_segments_[engine_idx];
    auto it = engine_connections.find(target_adxl_engine_name);
    if (it != engine_connections.end()) {
        VLOG(1) << "Already connected to target adxl engine: "
                << target_adxl_engine_name;
        return 0;
    }
    auto status = adxl_engines_[engine_idx]->Connect(
        target_adxl_engine_name.c_str(), params_.connect_timeout);
    if (status == adxl::TIMEOUT) {
        LOG(ERROR) << "Connect timeout to: " << target_adxl_engine_name
                   << ", errmsg: " << aclGetRecentErrMsg();
        return -1;
    }
    if (status != adxl::SUCCESS) {
        LOG(ERROR) << "Failed to connect to target: " << target_adxl_engine_name
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return -1;
    }
    engine_connections.emplace(target_adxl_engine_name);
    LOG(INFO) << "Connected to segment: " << target_adxl_engine_name;
    return 0;
}

int TransferExecutorBase::disconnect(size_t engine_idx,
                                     const std::string& target_adxl_engine_name,
                                     int32_t timeout_in_millis) {
    if (params_.auto_connect) {
        auto status = adxl_engines_[engine_idx]->Disconnect(
            target_adxl_engine_name.c_str(), timeout_in_millis);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to disconnect to: " << target_adxl_engine_name
                       << ", status: " << status
                       << ", errmsg: " << aclGetRecentErrMsg();
            return -1;
        }
        return 0;
    }
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto& engine_connections = connected_segments_[engine_idx];
    auto it = engine_connections.find(target_adxl_engine_name);
    if (it == engine_connections.end()) {
        LOG(INFO) << "Target adxl engine: " << target_adxl_engine_name
                  << " is not connected.";
        return 0;
    }
    auto status = adxl_engines_[engine_idx]->Disconnect(
        target_adxl_engine_name.c_str(), timeout_in_millis);
    engine_connections.erase(it);
    if (status != adxl::SUCCESS) {
        LOG(ERROR) << "Failed to disconnect to: " << target_adxl_engine_name
                   << ", status: " << status
                   << ", errmsg: " << aclGetRecentErrMsg();
        return -1;
    }
    return 0;
}

void TransferExecutorBase::disconnectAllForEngine(size_t engine_idx) {
    if (engine_idx >= adxl_engines_.size() ||
        engine_idx >= local_engine_contexts_.size() ||
        adxl_engines_[engine_idx] == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto it = connected_segments_.find(engine_idx);
    if (it == connected_segments_.end()) {
        return;
    }
    (void)aclrtSetCurrentContext(local_engine_contexts_[engine_idx]);
    for (const auto& connected_segment : it->second) {
        auto status = adxl_engines_[engine_idx]->Disconnect(
            connected_segment.c_str(), params_.connect_timeout);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to disconnect AdxlEngine: "
                       << connected_segment
                       << ", errmsg: " << aclGetRecentErrMsg();
        } else {
            LOG(INFO) << "Success to disconnect AdxlEngine:"
                      << connected_segment;
        }
    }
    connected_segments_.erase(it);
}

void TransferExecutorBase::cleanupConnections() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    if (!connected_segments_.empty()) {
        for (auto& [engine_idx, endpoint_set] : connected_segments_) {
            if (engine_idx >= adxl_engines_.size() ||
                adxl_engines_[engine_idx] == nullptr) {
                continue;
            }
            (void)aclrtSetCurrentContext(local_engine_contexts_[engine_idx]);
            for (const auto& connected_segment : endpoint_set) {
                auto status = adxl_engines_[engine_idx]->Disconnect(
                    connected_segment.c_str(), params_.connect_timeout);
                if (status != adxl::SUCCESS) {
                    LOG(ERROR) << "Failed to disconnect AdxlEngine: "
                               << connected_segment
                               << ", errmsg: " << aclGetRecentErrMsg();
                } else {
                    LOG(INFO) << "Success to disconnect AdxlEngine:"
                              << connected_segment;
                }
            }
        }
        connected_segments_.clear();
    }
}

void TransferExecutorBase::rollbackRegisteredMem(
    const std::vector<EngineMemHandle>& registered_mem_handles) {
    for (const auto& [engine_idx, mem_handle] : registered_mem_handles) {
        if (engine_idx >= adxl_engines_.size() ||
            engine_idx >= local_engine_contexts_.size()) {
            continue;
        }
        if (aclrtSetCurrentContext(local_engine_contexts_[engine_idx]) !=
            ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to restore context for rollback, engine_idx: "
                       << engine_idx << ", errmsg: " << aclGetRecentErrMsg();
            continue;
        }
        auto status = adxl_engines_[engine_idx]->DeregisterMem(mem_handle);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Rollback deregister failed, engine_idx: "
                       << engine_idx << ", errmsg: " << aclGetRecentErrMsg();
        }
    }
}

int TransferExecutorBase::registerMem(void* addr, size_t length,
                                      adxl::MemType mem_type,
                                      bool use_buffer_pool, bool roce_mode,
                                      bool dummy_real_mode) {
    if (mem_type == adxl::MEM_HOST && use_buffer_pool) {
        LOG(INFO) << "Ignore register host mem:" << addr
                  << " when buffer pool is enabled.";
        return 0;
    }
    if (adxl_engines_.empty()) {
        LOG(ERROR) << "Adxl engine is not available.";
        return -1;
    }

    aclrtContext saved_ctx = nullptr;
    if (aclrtGetCurrentContext(&saved_ctx) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return -1;
    }
    MAKE_GUARD(ctx_restore,
               [saved_ctx]() { (void)aclrtSetCurrentContext(saved_ctx); });

    std::vector<size_t> engine_indices;
    bool register_to_all =
        (roce_mode && dummy_real_mode && ascend_is_store_memory(addr, length));
    if (register_to_all || adxl_engines_.size() == 1U) {
        engine_indices.resize(adxl_engines_.size());
        std::iota(engine_indices.begin(), engine_indices.end(), 0);
    } else {
        int32_t current_device_id = 0;
        CHECK_ACL(aclrtGetDevice(&current_device_id));
        size_t engine_idx = static_cast<size_t>(current_device_id);
        if (engine_idx >= adxl_engines_.size()) {
            LOG(ERROR) << "Invalid device id:" << current_device_id;
            return -1;
        }
        engine_indices = {engine_idx};
    }

    adxl::MemDesc mem_desc{};
    mem_desc.addr = reinterpret_cast<uintptr_t>(addr);
    mem_desc.len = length;

    std::vector<EngineMemHandle> registered_mem_handles;
    registered_mem_handles.reserve(engine_indices.size());
    for (size_t engine_idx : engine_indices) {
        if (engine_idx >= local_engine_contexts_.size()) {
            LOG(ERROR) << "Engine idx " << engine_idx
                       << " exceeds local_engine_contexts size";
            rollbackRegisteredMem(registered_mem_handles);
            return -1;
        }
        auto ctx_ret =
            aclrtSetCurrentContext(local_engine_contexts_[engine_idx]);
        if (ctx_ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtSetCurrentContext failed, engine_idx: "
                       << engine_idx << ", errmsg: " << aclGetRecentErrMsg();
            rollbackRegisteredMem(registered_mem_handles);
            return -1;
        }
        adxl::MemHandle mem_handle = nullptr;
        auto adxl_ret = adxl_engines_[engine_idx]->RegisterMem(
            mem_desc, mem_type, mem_handle);
        if (adxl_ret != adxl::SUCCESS) {
            LOG(ERROR) << "Register mem ret: " << adxl_ret
                       << ", errmsg: " << aclGetRecentErrMsg();
            rollbackRegisteredMem(registered_mem_handles);
            return -1;
        }
        registered_mem_handles.emplace_back(engine_idx, mem_handle);
        LOG(INFO) << "TransferExecutor register mem addr:" << addr
                  << ", length:" << length << ", mem type:"
                  << (mem_type == adxl::MEM_HOST ? "host" : "device")
                  << ", engine index:" << engine_idx;
    }
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    addr_to_mem_handles_[addr] = std::move(registered_mem_handles);
    return 0;
}

int TransferExecutorBase::deregisterMem(void* addr) {
    aclrtContext saved_ctx = nullptr;
    if (aclrtGetCurrentContext(&saved_ctx) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return -1;
    }
    MAKE_GUARD(ctx_restore,
               [saved_ctx]() { (void)aclrtSetCurrentContext(saved_ctx); });

    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    auto it = addr_to_mem_handles_.find(addr);
    if (it == addr_to_mem_handles_.end()) {
        return 0;
    }

    if (params_.dummy_real_mode) {
        std::set<size_t> engine_indices;
        for (const auto& [engine_idx, mem_handle] : it->second) {
            (void)mem_handle;
            engine_indices.insert(engine_idx);
        }
        for (size_t engine_idx : engine_indices) {
            disconnectAllForEngine(engine_idx);
        }
    }

    for (const auto& [engine_idx, mem_handle] : it->second) {
        if (engine_idx >= local_engine_contexts_.size()) {
            continue;
        }
        CHECK_ACL(aclrtSetCurrentContext(local_engine_contexts_[engine_idx]));
        (void)adxl_engines_[engine_idx]->DeregisterMem(mem_handle);
    }
    addr_to_mem_handles_.erase(it);
    return 0;
}

std::string TransferExecutorBase::resolveTargetAdxlEngineName(
    const std::shared_ptr<TransferMetadata::SegmentDesc>& segment_desc,
    size_t engine_idx) const {
    const auto& endpoints = segment_desc->rank_info.endpoints;
    if (params_.dummy_real_mode && params_.roce_mode) {
        if (engine_idx >= endpoints.size()) {
            return {};
        }
        return endpoints[engine_idx];
    }
    if (!endpoints.empty()) {
        return endpoints.front();
    }
    return {};
}

void TransferExecutorBase::processSliceList(
    const std::vector<Transport::Slice*>& slice_list) {
    if (slice_list.empty()) {
        return;
    }
    size_t local_engine_idx =
        params_.dummy_real_mode ? slice_list[0]->ascend_direct.engine_id : 0;
    VLOG(1) << "processSliceList for dev:" << local_engine_idx;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!local_segment_desc ||
        local_engine_idx >= local_segment_desc->rank_info.endpoints.size()) {
        LOG(ERROR) << "Invalid local segment or engine idx: "
                   << local_engine_idx;
        markSlicesFailed(slice_list);
        return;
    }
    const std::string& local_engine_name =
        local_segment_desc->rank_info.endpoints[local_engine_idx];
    adxl::TransferOp operation;
    if (slice_list[0]->opcode == TransferRequest::WRITE) {
        operation = adxl::WRITE;
    } else if (slice_list[0]->opcode == TransferRequest::READ) {
        operation = adxl::READ;
    } else {
        LOG(ERROR) << "Unsupported opcode: " << slice_list[0]->opcode;
        markSlicesFailed(slice_list);
        return;
    }

    ExecuteResult result;
    std::string target_adxl_engine_name;
    bool force_update = false;
    for (int32_t retry = 0; retry < kTransferRetryTimes; ++retry) {
        auto target_segment_desc = metadata_->getSegmentDescByID(
            slice_list[0]->target_id, force_update);
        if (!target_segment_desc) {
            LOG(ERROR) << "Cannot find segment descriptor for target_id: "
                       << slice_list[0]->target_id;
            markSlicesFailed(slice_list);
            return;
        }
        target_adxl_engine_name =
            resolveTargetAdxlEngineName(target_segment_desc, local_engine_idx);
        if (target_adxl_engine_name.empty()) {
            LOG(ERROR) << "Invalid local_engine_idx: " << local_engine_idx
                       << " target endpoint size:"
                       << target_segment_desc->rank_info.endpoints.size();
            markSlicesFailed(slice_list);
            return;
        }

        auto need_local_copy = !globalConfig().ascend_use_fabric_mem &&
                               (target_adxl_engine_name == local_engine_name);
        if (need_local_copy && local_copy_engine_) {
            auto start = std::chrono::steady_clock::now();
            local_copy_engine_->Copy(slice_list[0]->opcode, slice_list);
            VLOG(1) << "Local copy time: "
                    << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count()
                    << "us";
            return;
        }

        result = execute(local_engine_idx, target_adxl_engine_name, operation,
                         slice_list);
        if (result.ret == 0 || !result.retryable ||
            retry + 1 >= kTransferRetryTimes) {
            break;
        }
        force_update = true;
        LOG(INFO) << "Retry transfer to:" << target_adxl_engine_name;
    }

    if (result.ret == 0) {
        return;
    }
    if (result.status == adxl::TIMEOUT) {
        LOG(ERROR) << "Transfer timeout to: " << target_adxl_engine_name
                   << ", errmsg: " << aclGetRecentErrMsg();
    } else {
        LOG(ERROR) << "Transfer failed to: " << target_adxl_engine_name
                   << ", errmsg: " << aclGetRecentErrMsg();
    }
    markSlicesFailed(slice_list);
}
}  // namespace mooncake
