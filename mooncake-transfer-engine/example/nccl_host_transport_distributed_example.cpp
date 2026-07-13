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
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <nccl.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "transfer_engine.h"

DEFINE_int32(rank, -1, "Process rank, 0 or 1");
DEFINE_int32(gpu_id, 0, "CUDA device local to this process");
DEFINE_string(peer_host, "", "Hostname of the other rank");
DEFINE_int32(base_port, 19000, "Rank 0 TE port; rank 1 uses base_port + 1");
DEFINE_int32(timeout_seconds, 120, "Peer connection and transfer timeout");
DEFINE_uint64(transfer_bytes, 1 << 20,
              "Bytes transferred by each correctness and benchmark request");
DEFINE_int32(iterations, 100,
             "Steady-state WRITE iterations measured by rank 0");

namespace {

using mooncake::BatchID;
using mooncake::SegmentHandle;
using mooncake::TransferEngine;
using mooncake::TransferRequest;
using mooncake::TransferStatus;
using mooncake::TransferStatusEnum;

constexpr size_t kAllocationBytes = 16 << 20;
constexpr size_t kControlBytes = 4096;
constexpr size_t kBufferBytes = kAllocationBytes - kControlBytes;

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

bool waitForDeviceByte(const void* device_ptr, uint8_t expected) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_timeout_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        uint8_t value = 0;
        if (!checkCuda(
                cudaMemcpy(&value, device_ptr, 1, cudaMemcpyDeviceToHost),
                "cudaMemcpy control byte")) {
            return false;
        }
        if (value == expected) return true;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    LOG(ERROR) << "Timed out waiting for control value "
               << static_cast<int>(expected);
    return false;
}

bool waitForTransfer(TransferEngine* engine, BatchID batch) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_timeout_seconds);
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
        std::this_thread::yield();
    }
    LOG(ERROR) << "Timed out waiting for transfer";
    return false;
}

bool submitOne(TransferEngine* engine, SegmentHandle target, void* local,
               uint64_t remote, size_t length, TransferRequest::OpCode opcode) {
    BatchID batch = engine->allocateBatchID(1);
    if (batch == mooncake::INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate transfer batch";
        return false;
    }
    TransferRequest request{};
    request.opcode = opcode;
    request.source = local;
    request.target_id = target;
    request.target_offset = remote;
    request.length = length;
    auto result = engine->submitTransfer(batch, {request});
    if (!result.ok() || !waitForTransfer(engine, batch)) {
        if (!result.ok()) LOG(ERROR) << result.ToString();
        return false;
    }
    result = engine->freeBatchID(batch);
    return result.ok();
}

bool expectReadRejected(TransferEngine* engine, SegmentHandle target,
                        void* local, uint64_t remote) {
    BatchID batch = engine->allocateBatchID(1);
    if (batch == mooncake::INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate READ rejection batch";
        return false;
    }
    TransferRequest request{};
    request.opcode = TransferRequest::READ;
    request.source = local;
    request.target_id = target;
    request.target_offset = remote;
    request.length = FLAGS_transfer_bytes;
    auto result = engine->submitTransfer(batch, {request});
    const bool rejected = result.IsNotSupportedTransport();
    if (!rejected) {
        LOG(ERROR) << "NCCL host READ returned " << result.ToString()
                   << ", expected NotSupportedTransport";
    }

    TransferStatus status{};
    auto status_result = engine->getTransferStatus(batch, 0, status);
    const bool failed =
        status_result.ok() && status.s == TransferStatusEnum::FAILED;
    if (!failed) {
        LOG(ERROR) << "Rejected NCCL host READ did not reach FAILED status";
    }

    auto free_result = engine->freeBatchID(batch);
    if (!free_result.ok()) LOG(ERROR) << free_result.ToString();
    return rejected && failed && free_result.ok();
}

bool verifyPattern(void* device_ptr, uint8_t expected) {
    std::vector<uint8_t> host(FLAGS_transfer_bytes);
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

bool openPeerSegment(TransferEngine* engine, const std::string& endpoint,
                     SegmentHandle* segment, uint64_t* buffer_addr) {
    if (!engine || !segment || !buffer_addr) return false;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_timeout_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        SegmentHandle candidate = engine->openSegment(endpoint);
        if (candidate != static_cast<SegmentHandle>(-1)) {
            auto desc = engine->getMetadata()->getSegmentDescByID(candidate);
            if (desc) {
                for (const auto& buffer : desc->buffers) {
                    if (buffer.addr && buffer.length && buffer.device_id >= 0) {
                        *segment = candidate;
                        *buffer_addr = buffer.addr;
                        return true;
                    }
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOG(ERROR) << "Timed out opening NCCL peer segment " << endpoint;
    return false;
}

double elapsedMilliseconds(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - start)
        .count();
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    if ((FLAGS_rank != 0 && FLAGS_rank != 1) || FLAGS_peer_host.empty() ||
        FLAGS_base_port < 1024 || FLAGS_base_port >= 65535) {
        LOG(ERROR) << "--rank=0|1, --peer_host, and --base_port in "
                      "[1024, 65534] are required";
        return 1;
    }
    if (!checkCuda(cudaSetDevice(FLAGS_gpu_id), "cudaSetDevice") ||
        FLAGS_transfer_bytes == 0 || FLAGS_transfer_bytes > kBufferBytes ||
        FLAGS_iterations <= 0) {
        LOG(ERROR) << "--transfer_bytes must be in [1, " << kBufferBytes
                   << "] and --iterations must be positive";
        return 1;
    }

    void* buffer = nullptr;
    if (!checkNccl(ncclMemAlloc(&buffer, kAllocationBytes), "ncclMemAlloc") ||
        !checkCuda(cudaMemset(buffer, 0, kAllocationBytes), "cudaMemset")) {
        return 1;
    }
    auto* tx_control = static_cast<uint8_t*>(buffer) + kBufferBytes;
    auto* rx_control = tx_control + 1;

    auto engine = std::make_unique<TransferEngine>(false);
    const std::string local_host = mooncake::getHostname();
    const int local_port = FLAGS_base_port + FLAGS_rank;
    const int peer_port = FLAGS_base_port + 1 - FLAGS_rank;
    const std::string local_endpoint =
        local_host + ":" + std::to_string(local_port);
    const std::string peer_endpoint =
        FLAGS_peer_host + ":" + std::to_string(peer_port);
    if (engine->init(P2PHANDSHAKE, local_endpoint, local_host, local_port) !=
            0 ||
        !engine->installTransport("nccl", nullptr) ||
        engine->registerLocalMemory(buffer, kAllocationBytes,
                                    "cuda:" + std::to_string(FLAGS_gpu_id)) !=
            0) {
        LOG(ERROR) << "Failed to initialize NCCL host transport";
        return 1;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    SegmentHandle peer_segment = static_cast<SegmentHandle>(-1);
    uint64_t peer_buffer_addr = 0;
    if (!openPeerSegment(engine.get(), peer_endpoint, &peer_segment,
                         &peer_buffer_addr)) {
        return 1;
    }

    if (!expectReadRejected(engine.get(), peer_segment, buffer,
                            peer_buffer_addr)) {
        return 1;
    }
    if (FLAGS_rank == 0) {
        LOG(INFO) << "NCCL host READ rejection validation passed";
    }

    if (FLAGS_rank == 0) {
        if (!checkCuda(cudaMemset(buffer, 0x5a, FLAGS_transfer_bytes),
                       "cudaMemset write source")) {
            return 1;
        }
        const auto first_write_start = std::chrono::steady_clock::now();
        if (!submitOne(engine.get(), peer_segment, buffer, peer_buffer_addr,
                       FLAGS_transfer_bytes, TransferRequest::WRITE)) {
            return 1;
        }
        const double first_write_ms = elapsedMilliseconds(first_write_start);

        const auto control_start = std::chrono::steady_clock::now();
        if (!checkCuda(cudaMemset(buffer, 0, FLAGS_transfer_bytes),
                       "cudaMemset reverse write destination") ||
            !checkCuda(cudaMemset(tx_control, 1, 1), "cudaMemset control") ||
            !submitOne(engine.get(), peer_segment, tx_control,
                       peer_buffer_addr + kBufferBytes + 1, 1,
                       TransferRequest::WRITE) ||
            !waitForDeviceByte(rx_control, 1) || !verifyPattern(buffer, 0xa5)) {
            return 1;
        }
        const double reverse_write_ms = elapsedMilliseconds(control_start);

        const auto benchmark_start = std::chrono::steady_clock::now();
        for (int i = 0; i < FLAGS_iterations; ++i) {
            if (!submitOne(engine.get(), peer_segment, buffer, peer_buffer_addr,
                           FLAGS_transfer_bytes, TransferRequest::WRITE)) {
                return 1;
            }
        }
        const double seconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                          benchmark_start)
                .count();
        const double gib = static_cast<double>(FLAGS_transfer_bytes) *
                           FLAGS_iterations / static_cast<double>(1ULL << 30);
        const double usec_per_op =
            seconds * 1.0e6 / std::max(1, FLAGS_iterations);
        LOG(INFO) << "Distributed NCCL host validation passed: cold WRITE "
                  << first_write_ms << " ms, reverse WRITE and control sync "
                  << reverse_write_ms << " ms; steady WRITE "
                  << FLAGS_iterations << " x " << FLAGS_transfer_bytes
                  << " bytes, " << gib / seconds << " GiB/s, " << usec_per_op
                  << " us/op";
        if (!checkCuda(cudaMemset(tx_control, 2, 1), "cudaMemset control") ||
            !submitOne(engine.get(), peer_segment, tx_control,
                       peer_buffer_addr + kBufferBytes + 1, 1,
                       TransferRequest::WRITE)) {
            return 1;
        }
    } else {
        if (!waitForDeviceByte(rx_control, 1) || !verifyPattern(buffer, 0x5a) ||
            !checkCuda(cudaMemset(buffer, 0xa5, FLAGS_transfer_bytes),
                       "cudaMemset reverse write source") ||
            !submitOne(engine.get(), peer_segment, buffer, peer_buffer_addr,
                       FLAGS_transfer_bytes, TransferRequest::WRITE) ||
            !checkCuda(cudaMemset(tx_control, 1, 1), "cudaMemset control") ||
            !submitOne(engine.get(), peer_segment, tx_control,
                       peer_buffer_addr + kBufferBytes + 1, 1,
                       TransferRequest::WRITE) ||
            !waitForDeviceByte(rx_control, 2)) {
            return 1;
        }
    }

    engine.reset();
    if (!checkNccl(ncclMemFree(buffer), "ncclMemFree")) return 1;
    return 0;
}
