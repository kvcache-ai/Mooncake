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

#ifndef TRANSFER_EXECUTOR_BASE_H
#define TRANSFER_EXECUTOR_BASE_H

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <acl/acl.h>
#include "adxl_compat.h"

#include "transport/transport.h"
#include "transfer_metadata.h"

namespace mooncake {

class LocalCopyEngine;

/**
 * TransferExecutorBase: Base class with common ADXL logic shared by
 * sync and async transfer. Holds engines, connection state, memory
 * registration, and connect/disconnect.
 */
class TransferExecutorBase {
   public:
    struct ExecuteResult {
        int ret = 0;
        adxl::Status status = adxl::SUCCESS;
        bool retryable = false;
    };

    struct InitParams {
        std::shared_ptr<TransferMetadata> metadata;
        std::vector<std::string> local_adxl_engine_names;
        std::vector<aclrtContext> local_engine_contexts;
        int32_t connect_timeout = 10000;
        int32_t transfer_timeout = 10000;
        bool use_async_transfer = false;
        bool auto_connect = false;
        bool use_short_connection = false;
        bool use_buffer_pool = false;
        bool dummy_real_mode = false;
        bool roce_mode = false;
    };

    explicit TransferExecutorBase(const InitParams& params);
    virtual ~TransferExecutorBase();

    static std::unique_ptr<TransferExecutorBase> Create(
        const InitParams& params);

    virtual int initialize() = 0;
    virtual void finalize() = 0;
    virtual ExecuteResult execute(
        size_t local_engine_idx, const std::string& target_adxl_engine_name,
        adxl::TransferOp operation,
        const std::vector<Transport::Slice*>& slice_list) = 0;

    void processSliceList(const std::vector<Transport::Slice*>& slice_list);

    int registerMem(void* addr, size_t length, adxl::MemType mem_type,
                    bool use_buffer_pool, bool roce_mode, bool dummy_real_mode);
    int deregisterMem(void* addr);

    const size_t getNumEngines() const { return adxl_engines_.size(); }
    const std::vector<aclrtContext>& getLocalEngineContexts() const {
        return local_engine_contexts_;
    }
    bool getUseBufferPool() const { return params_.use_buffer_pool; }

    static void ParseExecutorEnvIntoInitParams(InitParams& params);

   protected:
    int initEngines();
    void cleanupConnections();
    void finalizeEngines();
    void disconnectAllForEngine(size_t engine_idx);

    int checkAndConnect(size_t engine_idx,
                        const std::string& target_adxl_engine_name);
    int disconnect(size_t engine_idx,
                   const std::string& target_adxl_engine_name,
                   int32_t timeout_in_millis);
    std::string resolveTargetAdxlEngineName(
        const std::shared_ptr<TransferMetadata::SegmentDesc>& segment_desc,
        size_t engine_idx) const;

    InitParams params_;
    std::unique_ptr<LocalCopyEngine> local_copy_engine_;
    std::vector<std::shared_ptr<adxl::AdxlEngine>> adxl_engines_;
    std::vector<aclrtContext> local_engine_contexts_;
    std::mutex mem_handle_mutex_;

    std::unordered_map<size_t, std::set<std::string>> connected_segments_;
    std::mutex connection_mutex_;

    std::shared_ptr<TransferMetadata> metadata_;

   private:
    using EngineMemHandle = std::pair<size_t, adxl::MemHandle>;

    void rollbackRegisteredMem(
        const std::vector<EngineMemHandle>& registered_mem_handles);
    std::map<void*, std::vector<EngineMemHandle>> addr_to_mem_handles_;
};

}  // namespace mooncake

#endif  // TRANSFER_EXECUTOR_BASE_H
