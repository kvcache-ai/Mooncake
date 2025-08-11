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

// How to run:
// etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls
// http://10.0.0.1:2379
// ./rdma_transport_test --mode=target  --metadata_server=127.0.0.1:2379
//   --local_server_name=127.0.0.2:12345 --device_name=erdma_0
// ./rdma_transport_test --metadata_server=127.0.0.1:2379
//   --segment_id=127.0.0.2:12345 --local_server_name=127.0.0.3:12346
//   --device_name=erdma_1

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>

#ifdef USE_NVMEOF
#include <cufile.h>
#endif

#include <cassert>

static void checkCudaError(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        exit(EXIT_FAILURE);
    }
}
#endif

#define NR_SOCKETS (1)

DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "192.168.3.77:2379", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "read", "Operation type: read or write");

DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp");

DEFINE_string(device_name, "mlx5_2",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(nic_priority_matrix, "",
              "Path to RDMA NIC priority matrix file (Advanced)");

DEFINE_string(segment_id, "192.168.3.76", "Segment ID to access data");

#ifdef USE_CUDA
DEFINE_bool(use_vram, true, "Allocate memory from GPU VRAM");
DEFINE_int32(gpu_id, 0, "GPU ID to use");
#endif

using namespace mooncake;

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
#ifdef USE_CUDA
    if (from_vram) {
        int gpu_id = FLAGS_gpu_id;
        void *d_buf;
        checkCudaError(cudaSetDevice(gpu_id), "Failed to set device");
        checkCudaError(cudaMalloc(&d_buf, size),
                       "Failed to allocate device memory");
        return d_buf;
    }
#endif
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) {
#ifdef USE_CUDA
    // check pointer on GPU
    cudaPointerAttributes attributes;
    checkCudaError(cudaPointerGetAttributes(&attributes, addr),
                   "Failed to get pointer attributes");

    if (attributes.type == cudaMemoryTypeDevice) {
        cudaFree(addr);
    } else if (attributes.type == cudaMemoryTypeHost) {
        numa_free(addr, size);
    } else {
        LOG(ERROR) << "Unknown memory type";
    }
#else
    numa_free(addr, size);
#endif
}

int initiatorWorker(TransferEngine *engine, SegmentID segment_id, int thread_id,
                    void *addr) {
    bindToSocket(0);
    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;
    const size_t kDataLength = 4096000;
    {
        LOG(INFO) << "Stage 1: Write Data";
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;

        LOG(INFO) << "Write Data: " << std::string((char *)(addr), 16) << "...";

        auto batch_id = engine->allocateBatchID(1);
        Status s;

        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = engine->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(INFO) << "FAILED";
                completed = true;
            }
        }
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    {
        LOG(INFO) << "Stage 2: Read Data";
        auto batch_id = engine->allocateBatchID(1);
        Status s;

        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = engine->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(INFO) << "FAILED";
                completed = true;
            }
        }
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    int ret =
        memcmp((uint8_t *)(addr), (uint8_t *)(addr) + kDataLength, kDataLength);
    LOG(INFO) << "Read Data: " << std::string((char *)(addr) + kDataLength, 16)
              << "...";
    LOG(INFO) << "Compare: " << (ret == 0 ? "OK" : "FAILED");

    return 0;
}

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::string loadNicPriorityMatrix() {
    if (!FLAGS_nic_priority_matrix.empty()) {
        std::ifstream file(FLAGS_nic_priority_matrix);
        if (file.is_open()) {
            std::string content((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
            file.close();
            return content;
        }
    }
    // Build JSON Data
    auto device_names = formatDeviceNames(FLAGS_device_name);
    return "{\"cpu:0\": [[" + device_names +
           "], []], "
           " \"cpu:1\": [[" +
           device_names +
           "], []], "
           " \"cuda:0\": [[" +
           device_names + "], []]}";
}

int initiator() {
    const size_t ram_buffer_size = 1ull << 30;
    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    Transport *xport = nullptr;
    if (FLAGS_protocol == "rdma") {
        auto nic_priority_matrix = loadNicPriorityMatrix();
        void **args = (void **)malloc(2 * sizeof(void *));
        args[0] = (void *)nic_priority_matrix.c_str();
        args[1] = nullptr;
        xport = engine->installTransport("rdma", args);
    } else if (FLAGS_protocol == "tcp") {
        xport = engine->installTransport("tcp", nullptr);
    } else if (FLAGS_protocol == "nvmeof") {
        xport = engine->installTransport("nvmeof", nullptr);
    } else {
        LOG(ERROR) << "Unsupported protocol";
    }

    LOG_ASSERT(xport);

    void *addr = nullptr;
#ifdef USE_CUDA
    addr = allocateMemoryPool(ram_buffer_size, 0, FLAGS_use_vram);
    std::string name_prefix = FLAGS_use_vram ? "cuda:" : "cpu:";
    int name_suffix = FLAGS_use_vram ? FLAGS_gpu_id : 0;
    int rc = engine->registerLocalMemory(
        addr, ram_buffer_size, name_prefix + std::to_string(name_suffix));
    LOG_ASSERT(!rc);
#else
    addr = allocateMemoryPool(ram_buffer_size, 0, false);
    int rc =
        engine->registerLocalMemory(addr, ram_buffer_size, kWildcardLocation);
    LOG_ASSERT(!rc);
#endif

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());
    std::thread workers(initiatorWorker, engine.get(), segment_id, 0, addr);
    workers.join();
    engine->unregisterLocalMemory(addr);
    freeMemoryPool(addr, ram_buffer_size);
    return 0;
}

int target() {
    const size_t ram_buffer_size = 1ull << 30;
    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    if (FLAGS_protocol == "rdma") {
        auto nic_priority_matrix = loadNicPriorityMatrix();
        void **args = (void **)malloc(2 * sizeof(void *));
        args[0] = (void *)nic_priority_matrix.c_str();
        args[1] = nullptr;
        engine->installTransport("rdma", args);
    } else if (FLAGS_protocol == "tcp") {
        engine->installTransport("tcp", nullptr);
    } else if (FLAGS_protocol == "nvmeof") {
        engine->installTransport("nvmeof", nullptr);
    } else {
        LOG(ERROR) << "Unsupported protocol";
    }

    void *addr = nullptr;
    addr = allocateMemoryPool(ram_buffer_size, 0);
    int rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
    LOG_ASSERT(!rc);

    while (true) sleep(1);

    engine->unregisterLocalMemory(addr);
    freeMemoryPool(addr, ram_buffer_size);
    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_mode == "initiator")
        return initiator();
    else if (FLAGS_mode == "target")
        return target();

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    exit(EXIT_FAILURE);
}
