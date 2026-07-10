// Copyright 2026 KVCache.AI
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

#include <cuda_runtime.h>
#include <glog/logging.h>
#include <nccl.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "transfer_engine.h"

namespace {

using mooncake::BatchID;
using mooncake::SegmentHandle;
using mooncake::TransferEngine;
using mooncake::TransferRequest;
using mooncake::TransferStatus;
using mooncake::TransferStatusEnum;

constexpr size_t kBufferBytes = 1 << 20;
constexpr size_t kTransferBytes = 256 << 10;

bool checkCuda(cudaError_t result, const char* operation) {
    if (result == cudaSuccess) return true;
    LOG(ERROR) << operation << " failed: " << cudaGetErrorString(result);
    return false;
}

bool checkNccl(ncclResult_t result, const char* operation) {
    if (result == ncclSuccess) return true;
    LOG(ERROR) << operation << " failed: " << ncclGetErrorString(result);
    return false;
}

bool waitForTransfer(TransferEngine* engine, BatchID batch) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline) {
        TransferStatus status{};
        auto result = engine->getTransferStatus(batch, 0, status);
        if (!result.ok()) {
            LOG(ERROR) << result.ToString();
            return false;
        }
        if (status.s == TransferStatusEnum::COMPLETED) return true;
        if (status.s == TransferStatusEnum::FAILED ||
            status.s == TransferStatusEnum::TIMEOUT) {
            LOG(ERROR) << "Transfer failed with status " << status.s;
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    LOG(ERROR) << "Timed out waiting for NCCL host transfer";
    return false;
}

bool submitOne(TransferEngine* engine, SegmentHandle target, void* local,
               uint64_t remote, TransferRequest::OpCode opcode) {
    BatchID batch = engine->allocateBatchID(1);
    if (batch == mooncake::INVALID_BATCH_ID) return false;

    TransferRequest request{};
    request.opcode = opcode;
    request.source = local;
    request.target_id = target;
    request.target_offset = remote;
    request.length = kTransferBytes;
    auto result = engine->submitTransfer(batch, {request});
    if (!result.ok()) {
        LOG(ERROR) << result.ToString();
        return false;
    }
    if (!waitForTransfer(engine, batch)) return false;
    result = engine->freeBatchID(batch);
    if (!result.ok()) {
        LOG(ERROR) << result.ToString();
        return false;
    }
    return true;
}

bool verifyPattern(void* device_ptr, int device, uint8_t expected) {
    std::vector<uint8_t> host(kTransferBytes);
    if (!checkCuda(cudaSetDevice(device), "cudaSetDevice")) return false;
    if (!checkCuda(cudaMemcpy(host.data(), device_ptr, host.size(),
                              cudaMemcpyDeviceToHost),
                   "cudaMemcpy device-to-host")) {
        return false;
    }
    for (uint8_t value : host) {
        if (value != expected) {
            LOG(ERROR) << "Pattern mismatch: expected "
                       << static_cast<int>(expected) << ", got "
                       << static_cast<int>(value);
            return false;
        }
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    int device_count = 0;
    if (!checkCuda(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount") ||
        device_count < 2) {
        LOG(ERROR) << "The NCCL host example requires two GPUs";
        return 1;
    }

    void* buffers[2] = {nullptr, nullptr};
    std::unique_ptr<TransferEngine> engines[2];
    std::string names[2];

    for (int rank = 0; rank < 2; ++rank) {
        if (!checkCuda(cudaSetDevice(rank), "cudaSetDevice") ||
            !checkNccl(ncclMemAlloc(&buffers[rank], kBufferBytes),
                       "ncclMemAlloc") ||
            !checkCuda(cudaMemset(buffers[rank], 0, kBufferBytes),
                       "cudaMemset")) {
            return 1;
        }

        engines[rank] = std::make_unique<TransferEngine>(false);
        if (engines[rank]->init(P2PHANDSHAKE, "127.0.0.1:0",
                                "127.0.0.1", 0) != 0 ||
            !engines[rank]->installTransport("nccl", nullptr) ||
            engines[rank]->registerLocalMemory(
                buffers[rank], kBufferBytes,
                "cuda:" + std::to_string(rank)) != 0) {
            LOG(ERROR) << "Failed to initialize NCCL TE rank " << rank;
            return 1;
        }
        names[rank] = engines[rank]->getLocalIpAndPort();
    }

    SegmentHandle peer_on_rank0 = engines[0]->openSegment(names[1]);
    SegmentHandle peer_on_rank1 = engines[1]->openSegment(names[0]);
    if (peer_on_rank0 == static_cast<SegmentHandle>(-1) ||
        peer_on_rank1 == static_cast<SegmentHandle>(-1)) {
        LOG(ERROR) << "Failed to exchange P2P segment metadata";
        return 1;
    }

    std::atomic<bool> writes_ok{true};
    std::vector<std::thread> writers;
    if (!checkCuda(cudaSetDevice(0), "cudaSetDevice") ||
        !checkCuda(cudaMemset(buffers[0], 0x5a, kTransferBytes),
                   "cudaMemset write source")) {
        return 1;
    }
    for (int i = 0; i < 4; ++i) {
        writers.emplace_back([&] {
            if (!submitOne(engines[0].get(), peer_on_rank0, buffers[0],
                           reinterpret_cast<uint64_t>(buffers[1]),
                           TransferRequest::WRITE)) {
                writes_ok.store(false, std::memory_order_relaxed);
            }
        });
    }
    for (auto& writer : writers) writer.join();
    if (!writes_ok.load(std::memory_order_relaxed) ||
        !verifyPattern(buffers[1], 1, 0x5a)) {
        return 1;
    }
    LOG(INFO) << "NCCL host WRITE validation passed";

    if (!checkCuda(cudaSetDevice(1), "cudaSetDevice") ||
        !checkCuda(cudaMemset(buffers[1], 0xa5, kTransferBytes),
                   "cudaMemset read source") ||
        !checkCuda(cudaSetDevice(0), "cudaSetDevice") ||
        !checkCuda(cudaMemset(buffers[0], 0, kTransferBytes),
                   "cudaMemset read destination") ||
        !submitOne(engines[0].get(), peer_on_rank0, buffers[0],
                   reinterpret_cast<uint64_t>(buffers[1]),
                   TransferRequest::READ) ||
        !verifyPattern(buffers[0], 0, 0xa5)) {
        return 1;
    }
    LOG(INFO) << "NCCL host READ validation passed";

    engines[0].reset();
    engines[1].reset();
    for (int rank = 0; rank < 2; ++rank) {
        cudaSetDevice(rank);
        if (!checkNccl(ncclMemFree(buffers[rank]), "ncclMemFree")) return 1;
    }
    return 0;
}
