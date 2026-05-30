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
#include "transport/ascend_transport/ascend_direct_transport/context_manager.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

#include <glog/logging.h>

#include <cassert>
#include <memory>
#include <numeric>
#include <string>
#include <random>

#include "ascend_allocator.h"
#include "common.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
namespace {

int32_t ResolveCurrentEngineId(bool dummy_real_mode) {
    if (!dummy_real_mode) {
        return 0;
    }
    int32_t current_device_id = 0;
    if (aclrtGetDevice(&current_device_id) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetDevice failed, errmsg: " << aclGetRecentErrMsg();
        return -1;
    }
    return current_device_id;
}

void InitializeSlice(const Transport::TransferRequest &request,
                     int32_t current_engine_id, Transport::TransferTask *task,
                     Transport::Slice *slice) {
    slice->source_addr = request.source;
    slice->length = request.length;
    slice->opcode = request.opcode;
    slice->target_id = request.target_id;
    slice->ascend_direct.dest_addr = request.target_offset;
    slice->ascend_direct.engine_id = current_engine_id;
    slice->task = task;
    slice->status = Transport::Slice::PENDING;
    slice->ts = 0;
}

}  // namespace

AscendDirectTransport::AscendDirectTransport() = default;

AscendDirectTransport::~AscendDirectTransport() {
    LOG(INFO) << "AscendDirectTransport destructor called";

    if (dispatcher_) {
        dispatcher_->stop();
    }

    if (transfer_executor_) {
        transfer_executor_->finalize();
    }
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
        LOG(ERROR) << "cannot publish segments, "
                      "check the availability of metadata storage, ret: "
                   << ret;
        return ret;
    }

    TransferExecutorBase::InitParams exec_params;
    exec_params.metadata = metadata_;
    exec_params.local_engine_contexts = local_engine_contexts_;
    exec_params.dummy_real_mode = dummy_real_mode_;
    exec_params.roce_mode = roce_mode_;

    transfer_executor_ = TransferExecutorBase::Create(exec_params);
    ret = transfer_executor_->initialize();
    if (ret) {
        LOG(ERROR)
            << "AscendDirectTransport: TransferExecutor init failed, ret: "
            << ret;
        return ret;
    }

    if (dummy_real_mode_ && roce_mode_) {
        dispatcher_ = std::make_unique<RoceDummyRealSliceDispatcher>(
            transfer_executor_.get(), local_engine_contexts_);
    } else {
        dispatcher_ = std::make_unique<DefaultSliceDispatcher>(
            transfer_executor_.get(), local_engine_contexts_);
    }
    return 0;
}

int AscendDirectTransport::addEngineToSegmentDesc(int32_t device_id,
                                                  aclrtContext context,
                                                  const std::string &host_ip,
                                                  SegmentDesc *desc) {
    uint16_t listen_port = FindAdxlListenPort(base_port_, device_id);
    if (listen_port == 0) {
        LOG(ERROR) << "Find available port failed for device: " << device_id;
        return FAILED;
    }
    local_engine_contexts_.push_back(context);
    desc->rank_info.endpoints.push_back(
        GenAdxlEngineName(host_ip, listen_port));
    return 0;
}

int AscendDirectTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "ascend";

    dummy_real_mode_ = globalConfig().ascend_agent_mode;
    char *roce_enable_str = std::getenv("HCCL_INTRA_ROCE_ENABLE");
    if (roce_enable_str) {
        std::optional<int32_t> roce_enable =
            parseFromString<int32_t>(roce_enable_str);
        if (roce_enable.has_value() && roce_enable.value() == 1) {
            roce_mode_ = true;
            LOG(INFO) << "Roce mode is enabled.";
        } else {
            LOG(WARNING) << "HCCL_INTRA_ROCE_ENABLE is not valid, value:"
                         << roce_enable_str;
        }
    }
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
    const auto [host_ip, port] = parseHostNameWithPort(local_server_name_);
    (void)port;
    uint32_t device_count = 0;
    CHECK_ACL(aclrtGetDeviceCount(&device_count));
    if (dummy_real_mode_) {
        auto &ctx_mgr = ContextManager::getInstance();
        if (!ctx_mgr.isInitialized()) {
            LOG(ERROR) << "ContextManager is not initialized.";
            return -1;
        }
        if (device_count != ctx_mgr.getDeviceCount()) {
            LOG(WARNING) << "ACL device count " << device_count
                         << " differs from ContextManager device count "
                         << ctx_mgr.getDeviceCount();
        }
        for (uint32_t device_id = 0; device_id < ctx_mgr.getDeviceCount();
             ++device_id) {
            aclrtContext engine_context =
                ctx_mgr.getContext(static_cast<int32_t>(device_id));
            auto ret =
                addEngineToSegmentDesc(static_cast<int32_t>(device_id),
                                       engine_context, host_ip, desc.get());
            if (ret != 0) return ret;
        }
    } else {
        // get device from user context
        int32_t device_logic_id = 0;
        CHECK_ACL(aclrtGetDevice(&device_logic_id));
        aclrtContext engine_context = nullptr;
        CHECK_ACL(aclrtGetCurrentContext(&engine_context));
        auto ret = addEngineToSegmentDesc(device_logic_id, engine_context,
                                          host_ip, desc.get());
        if (ret != 0) return ret;
    }
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
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

    const int32_t current_engine_id = ResolveCurrentEngineId(dummy_real_mode_);
    if (current_engine_id < 0) {
        return Status::Context("aclrtGetDevice failed");
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
        InitializeSlice(request, current_engine_id, &task, slice);
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }
    if (dispatcher_) {
        dispatcher_->enqueue(std::move(slice_list));
    }
    return Status::OK();
}

Status AscendDirectTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    const int32_t current_engine_id = ResolveCurrentEngineId(dummy_real_mode_);
    if (current_engine_id < 0) {
        return Status::Context("aclrtGetDevice failed");
    }

    std::vector<Slice *> slice_list;
    slice_list.reserve(task_list.size());

    for (auto index : task_list) {
        assert(index);
        auto &task = *index;
        assert(task.request);
        auto &request = *task.request;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        InitializeSlice(request, current_engine_id, &task, slice);
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }

    if (dispatcher_) {
        dispatcher_->enqueue(std::move(slice_list));
    }
    return Status::OK();
}

// actually not called, just use getTransferStatus in multi_transport
Status AscendDirectTransport::getTransferStatus(BatchID batch_id,
                                                size_t task_id,
                                                TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "getTransportStatus invalid argument, batch id: " +
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
    aclrtContext saved_ctx = nullptr;
    if (aclrtGetCurrentContext(&saved_ctx) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return -1;
    }
    MAKE_GUARD(ctx_restore,
               [saved_ctx]() { (void)aclrtSetCurrentContext(saved_ctx); });

    BufferDesc buffer_desc;
    buffer_desc.name = location;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = (uint64_t)length;

    adxl::MemType mem_type;
    if (location.starts_with("cpu")) {
        mem_type = adxl::MEM_HOST;
    } else if (location.starts_with("npu")) {
        mem_type = adxl::MEM_DEVICE;
    } else if (location == kWildcardLocation) {
        aclrtPtrAttributes attributes;
        CHECK_ACL(aclrtPointerGetAttributes(addr, &attributes));
        if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
            mem_type = adxl::MEM_HOST;
        } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
            mem_type = adxl::MEM_DEVICE;
        } else {
            LOG(ERROR) << "mem addr:" << addr
                       << " can not be recognized, try set to host mem.";
            mem_type = adxl::MEM_HOST;
        }
    } else {
        LOG(ERROR) << "location:" << location << " is not supported.";
        return ERR_INVALID_ARGUMENT;
    }

    int ret = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (ret) {
        LOG(ERROR) << "addLocalMemoryBuffer failed, ret: " << ret;
        return ret;
    }

    const int register_ret = transfer_executor_->registerMem(
        addr, length, mem_type, transfer_executor_->getUseBufferPool(),
        roce_mode_, dummy_real_mode_);
    if (register_ret == 0) {
        return 0;
    }

    const int rollback_ret =
        metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rollback_ret != 0) {
        LOG(ERROR) << "removeLocalMemoryBuffer rollback failed, ret: "
                   << rollback_ret;
    }
    return register_ret;
}

int AscendDirectTransport::unregisterLocalMemory(void *addr,
                                                 bool update_metadata) {
    aclrtContext saved_ctx = nullptr;
    if (aclrtGetCurrentContext(&saved_ctx) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return -1;
    }
    MAKE_GUARD(ctx_restore,
               [saved_ctx]() { (void)aclrtSetCurrentContext(saved_ctx); });

    int ret = transfer_executor_->deregisterMem(addr);
    if (ret != 0) {
        return ret;
    }
    (void)metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    return 0;
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
}  // namespace mooncake
