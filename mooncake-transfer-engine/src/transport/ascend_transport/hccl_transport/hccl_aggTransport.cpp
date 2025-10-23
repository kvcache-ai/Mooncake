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

#include <cassert>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <cstdlib>
#include <string>
#include <algorithm>
#include "transport/ascend_transport/hccl_transport/hccl_transport.h"

namespace mooncake {
int HcclTransport::aggTransport(std::vector<Slice *> &slice_list,
                                aclrtStream stream) {
    auto start = std::chrono::high_resolution_clock::now();
    int ret = prepareTransport(slice_list);
    if (ret) {
        LOG(ERROR) << "HcclTransport: prepareTransport failed" << ret;
        return ret;
    }
    std::vector<MemBlock> localMemPool;
    std::vector<MemBlock> remoteMemPool;
    localMemPool.reserve(slice_list.size());
    remoteMemPool.reserve(slice_list.size());

    for (auto slice : slice_list) {
        uint64_t len = slice->length;
        uint64_t source_addr = reinterpret_cast<uint64_t>(slice->source_addr);
        uint64_t dest_addr = slice->hccl.dest_addr;
        auto addr_type = slice->hccl.dest_addr_type;

        if (len > PER_HUGE_BUFFER_SIZE) {
            uint64_t remaining = len;
            uint64_t current_src = source_addr;
            uint64_t current_dest = dest_addr;

            while (remaining > 0) {
                uint64_t chunk_size =
                    std::min(remaining, (uint64_t)PER_HUGE_BUFFER_SIZE);

                localMemPool.emplace_back(current_src, chunk_size, addr_type);
                remoteMemPool.emplace_back(current_dest, chunk_size, addr_type);

                current_src += chunk_size;
                current_dest += chunk_size;
                remaining -= chunk_size;
            }
        } else {
            localMemPool.emplace_back(source_addr, len, addr_type);
            remoteMemPool.emplace_back(dest_addr, len, addr_type);
        }
    }

    ret = aggTransportMemTask(
        &local_rank_info_, &remote_rank_info_, localMemPool, remoteMemPool,
        slice_list[0]->opcode, stream, slice_list[0]->hccl.dest_addr_type);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aggTransportMemTask error, local "
                        "devicePhyId: "
                    << local_rank_info_.devicePhyId
                    << ", remote devicePhyId: "
                    << remote_rank_info_.devicePhyId << ", ret: " << ret;
        for (auto slice : slice_list) {
            slice->markFailed();
            slice->task->transferred_bytes = slice->length;
        }
        return ret;
    }

    for (auto slice : slice_list) {
        slice->markSuccess();
        slice->task->transferred_bytes = slice->length;
    }

    auto stop = std::chrono::high_resolution_clock::now();
    if (enableAscendLogging()) {
        pid_t pid = getpid();
        auto duration_call =
            std::chrono::duration_cast<std::chrono::microseconds>(stop -
                                                                    start);
        LOG(INFO) << "pid: " << pid
                    << ", target hostIp: " << remote_rank_info_.hostIp
                    << ", local devicePhyId: " << local_rank_info_.devicePhyId
                    << ", target devicePhyId: "
                    << remote_rank_info_.devicePhyId
                    << ", batch transfersync spent: " << duration_call.count()
                    << "us";
    } else {
        (void)start;
        (void)stop;
    }

    return 0;
}

void HcclTransport::aggInitiatorLoop(int deviceLogicId) {
    aclrtStream stream;
    int ret = aclrtSetDevice(deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice error, ret: " << ret;
    }

    ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtCreateStream error, ret: " << ret;
    }

    while (running_) {
        std::unique_lock<std::mutex> lock(initiator_mutex_);
        initiator_cond_.wait(
            lock, [this] { return !allReqQueues_.empty() || !running_; });
        auto slice_list = std::move(allReqQueues_.front());
        allReqQueues_.pop();
        lock.unlock();
        if (slice_list.empty()) {
            LOG(ERROR) << "HcclTransport: empty transfer request batch";
        }
        bool isAgg = true;

        if (!isAgg) {
            ret = nonAggTransport(slice_list, stream);
            if (ret) {
                LOG(ERROR) << "HcclTransport: nonAggTransport error, ret: "
                           << ret;
            }
        } else {
            ret = aggTransport(slice_list, stream);
            if (ret) {
                LOG(ERROR) << "HcclTransport: aggTransport error, ret: " << ret;
            }
        }
    }
}

void HcclTransport::aggInitiatorTransferLoop(int deviceLogicId) {
    aclrtStream stream;
    int ret = aclrtSetDevice(deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice error, ret: " << ret;
    }

    ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtCreateStream error, ret: " << ret;
    }

    while (running_) {
        ret = aggTransportMemTransfer(stream);
        if (ret) {
            LOG(ERROR) << "HcclTransport: aggTransportMemTransfer error";
        }
    }
}

void HcclTransport::aggTargetAcceptLoop(int deviceLogicId) {
    int ret = aclrtSetDevice(deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice failed ret: " << ret;
    }

    while (running_) {
        ret = transportMemAccept(&local_rank_info_, aggregateEnabled_);
        if (ret) {
            LOG(ERROR) << "HcclTransport: transportMemAccept failed ret: "
                       << ret;
        }
    }
}

// Target-side Aggregation/Splitting Processing Thread
void HcclTransport::aggTargetLoop(int deviceLogicId) {
    aclrtStream stream;
    int ret = aclrtSetDevice(deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice failed ret:" << ret;
    }

    ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtCreateStream error, ret:" << ret;
    }

    while (running_) {
        ret = aggTransportMemTarget(stream);
        if (ret) {
            LOG(ERROR) << "HcclTransport: aggTransportMemTarget failed ret:"
                       << ret;
        }
    }
}

int HcclTransport::startAggThreads() {
    pid_t pid = getpid();
    int ret = 0;
    int deviceLogicId;
    ret = aclrtGetDevice(&deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtGetDevice failed, ret: " << ret;
        return ret;
    }

    aggInitiatorThread_ =
        std::thread(&HcclTransport::aggInitiatorLoop, this, deviceLogicId);
    aggInitiatorTransferThread_ = std::thread(
        &HcclTransport::aggInitiatorTransferLoop, this, deviceLogicId);
    aggTargetAcceptThread_ =
        std::thread(&HcclTransport::aggTargetAcceptLoop, this, deviceLogicId);
    aggTargetThread_ =
        std::thread(&HcclTransport::aggTargetLoop, this, deviceLogicId);

    LOG(INFO) << "HcclTransport: startAggThreads, pid: " << pid
              << ", deviceLogicId: " << deviceLogicId;
    return 0;
}

}  // namespace mooncake
