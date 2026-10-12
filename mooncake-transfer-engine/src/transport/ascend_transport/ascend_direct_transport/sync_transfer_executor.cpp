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

#include "transport/ascend_transport/ascend_direct_transport/sync_transfer_executor.h"

#include <glog/logging.h>

#include "common.h"
#include "transfer_metadata.h"

// Optional audit hooks. Undefined weak symbols leave normal behavior unchanged.
extern "C" uint64_t mc_qos_audit_begin(int) __attribute__((weak));
extern "C" void mc_qos_audit_desc(uint64_t, uint64_t, uint64_t, uint64_t)
    __attribute__((weak));
extern "C" void mc_qos_audit_end(uint64_t, int) __attribute__((weak));

namespace mooncake {
namespace {
constexpr int32_t kDefaultDisconnectTime = 1000;
}  // namespace

SyncTransferExecutor::SyncTransferExecutor(const InitParams& params)
    : TransferExecutorBase(params) {}

SyncTransferExecutor::~SyncTransferExecutor() {
    SyncTransferExecutor::finalize();
}

int SyncTransferExecutor::initialize() { return initEngines(); }

void SyncTransferExecutor::finalize() {
    if (finalized_) {
        return;
    }
    cleanupConnections();
    finalizeEngines();
    finalized_ = true;
}

TransferExecutorBase::ExecuteResult SyncTransferExecutor::execute(
    size_t local_engine_idx, const std::string& target_adxl_engine_name,
    adxl::TransferOp operation,
    const std::vector<Transport::Slice*>& slice_list) {
    if (local_engine_idx >= adxl_engines_.size()) {
        LOG(ERROR) << "Invalid local_engine_idx: " << local_engine_idx;
        return {.ret = -1, .status = adxl::FAILED, .retryable = false};
    }

    auto* engine = adxl_engines_[local_engine_idx].get();
    if (!params_.auto_connect) {
        int ret = checkAndConnect(local_engine_idx, target_adxl_engine_name);
        if (ret != 0) {
            return {.ret = -1, .status = adxl::FAILED, .retryable = true};
        }
    }

    std::vector<adxl::TransferOpDesc> op_descs;
    op_descs.reserve(slice_list.size());
    for (const auto* slice : slice_list) {
        adxl::TransferOpDesc op_desc{};
        op_desc.local_addr = reinterpret_cast<uintptr_t>(slice->source_addr);
        op_desc.remote_addr =
            reinterpret_cast<uintptr_t>(slice->ascend_direct.dest_addr);
        op_desc.len = slice->length;
        op_descs.emplace_back(op_desc);
    }

    uint64_t audit_id = 0;
    if (mc_qos_audit_begin && mc_qos_audit_desc && mc_qos_audit_end) {
        audit_id = mc_qos_audit_begin(operation == adxl::READ ? 0 : 1);
        for (const auto& desc : op_descs) {
            mc_qos_audit_desc(audit_id, desc.local_addr, desc.remote_addr,
                              desc.len);
        }
    }
    auto status =
        engine->TransferSync(target_adxl_engine_name.c_str(), operation,
                             op_descs, params_.transfer_timeout);

    if (audit_id) mc_qos_audit_end(audit_id, static_cast<int>(status));

    if (status != adxl::SUCCESS) {
        if (!params_.auto_connect) {
            disconnect(local_engine_idx, target_adxl_engine_name,
                       kDefaultDisconnectTime);
        }
        return {.ret = -1, .status = status, .retryable = true};
    }

    recordConnectedSegment(local_engine_idx, target_adxl_engine_name);

    if (params_.use_short_connection) {
        disconnect(local_engine_idx, target_adxl_engine_name,
                   params_.connect_timeout);
    }
    for (auto* slice : slice_list) {
        slice->markSuccess();
    }
    return {.ret = 0, .status = status, .retryable = false};
}

}  // namespace mooncake
