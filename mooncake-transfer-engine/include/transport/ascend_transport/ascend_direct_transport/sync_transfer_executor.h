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

#ifndef SYNC_TRANSFER_EXECUTOR_H
#define SYNC_TRANSFER_EXECUTOR_H

#include "transfer_executor_base.h"
#include "transport/transport.h"

namespace mooncake {

/**
 * SyncTransferExecutor: Sync-only ADXL transfer. Extends Base with
 * TransferSync-based execution.
 */
class SyncTransferExecutor : public TransferExecutorBase {
   public:
    explicit SyncTransferExecutor(const InitParams& params);
    ~SyncTransferExecutor() override;

    int initialize() override;
    void finalize() override;
    ExecuteResult execute(
        size_t local_engine_idx, const std::string& target_adxl_engine_name,
        adxl::TransferOp operation,
        const std::vector<Transport::Slice*>& slice_list) override;

   private:
    bool finalized_{false};
};

}  // namespace mooncake

#endif  // SYNC_TRANSFER_EXECUTOR_H
