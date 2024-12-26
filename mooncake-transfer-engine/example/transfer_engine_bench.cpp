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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>
#include <cufile.h>

#include <cassert>

static void checkCudaError(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        exit(EXIT_FAILURE);
    }
}

#endif

#define NR_SOCKETS (2)

static std::string getHostname();

DEFINE_string(local_server_name, getHostname(),
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
DEFINE_int32(batch_size, 128, "Batch size");
DEFINE_int32(block_size, 4096, "Block size for each transfer request");
DEFINE_int32(duration, 10, "Test duration in seconds");
DEFINE_int32(threads, 4, "Task submission threads");

#ifdef USE_CUDA
DEFINE_bool(use_vram, true, "Allocate memory from GPU VRAM");
DEFINE_int32(gpu_id, 0, "GPU ID to use");
#endif

using namespace mooncake;

static std::string getHostname() {
    char hostname[256];
    if (gethostname(hostname, 256)) {
        PLOG(ERROR) << "Failed to get hostname";
        return "";
    }
    return hostname;
}

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
    } else if (attributes.type == cudaMemoryTypeHost || attributes.type == cudaMemoryTypeUnregistered) {
        numa_free(addr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << addr << " " << attributes.type;
    }
#else
    numa_free(addr, size);
#endif
}

volatile bool running = true;
std::atomic<size_t> total_batch_count(0);

int initiatorWorker(TransferEngine *xport, SegmentID segment_id, int thread_id,
                    void *addr) {
    bindToSocket(thread_id % NR_SOCKETS);
    TransferRequest::OpCode opcode;
    if (FLAGS_operation == "read")
        opcode = TransferRequest::READ;
    else if (FLAGS_operation == "write")
        opcode = TransferRequest::WRITE;
    else {
        LOG(ERROR) << "Unsupported operation: must be 'read' or 'write'";
        exit(EXIT_FAILURE);
    }

    auto segment_desc = xport->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc) {
        LOG(ERROR) << "Unable to get target segment ID, please recheck";
        exit(EXIT_FAILURE);
    }
    uint64_t remote_base =
        (uint64_t)segment_desc->buffers[thread_id % NR_SOCKETS].addr;

    size_t batch_count = 0;
    while (running) {
        auto batch_id = xport->allocateBatchID(FLAGS_batch_size);
        int ret = 0;
        std::vector<TransferRequest> requests;
        for (int i = 0; i < FLAGS_batch_size; ++i) {
            TransferRequest entry;
            entry.opcode = opcode;
            entry.length = FLAGS_block_size;
            entry.source = (uint8_t *)(addr) +
                           FLAGS_block_size * (i * FLAGS_threads + thread_id);
            entry.target_id = segment_id;
            entry.target_offset =
                remote_base +
                FLAGS_block_size * (i * FLAGS_threads + thread_id);
            requests.emplace_back(entry);
        }

        ret = xport->submitTransfer(batch_id, requests);
        LOG_ASSERT(!ret);
        for (int task_id = 0; task_id < FLAGS_batch_size; ++task_id) {
            bool completed = false;
            TransferStatus status;
            while (!completed) {
                int ret = xport->getTransferStatus(batch_id, task_id, status);
                LOG_ASSERT(!ret);
                if (status.s == TransferStatusEnum::COMPLETED)
                    completed = true;
                else if (status.s == TransferStatusEnum::FAILED) {
                    LOG(INFO) << "FAILED";
                    completed = true;
                    exit(EXIT_FAILURE);
                }
            }
        }

        ret = xport->freeBatchID(batch_id);
        LOG_ASSERT(!ret);
        batch_count++;
    }
    LOG(INFO) << "Worker " << thread_id << " stopped!";
    total_batch_count.fetch_add(batch_count);
    return 0;
}

std::string formatDeviceNames(const std::string& device_names) {
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
    return "{\"cpu:0\": [[" + device_names + "], []], "
           " \"cpu:1\": [[" + device_names + "], []], "
           " \"gpu:0\": [[" + device_names + "], []]}";
}

int initiator() {
    auto metadata_client =
        std::make_shared<TransferMetadata>(FLAGS_metadata_server);
    LOG_ASSERT(metadata_client);

    const size_t ram_buffer_size = 1ull << 30;
    auto engine = std::make_unique<TransferEngine>(metadata_client);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_local_server_name.c_str(), hostname_port.first.c_str(),
                 hostname_port.second);

    Transport *xport = nullptr;
    if (FLAGS_protocol == "rdma") {
        auto nic_priority_matrix = loadNicPriorityMatrix();
        void **args = (void **)malloc(2 * sizeof(void *));
        args[0] = (void *)nic_priority_matrix.c_str();
        args[1] = nullptr;
        xport = engine->installOrGetTransport("rdma", args);
    } else if (FLAGS_protocol == "tcp") {
        xport = engine->installOrGetTransport("tcp", nullptr);
    } else {
        LOG(ERROR) << "Unsupported protocol";
    }

    LOG_ASSERT(xport);

    void *addr[NR_SOCKETS] = {nullptr};
    int buffer_num = NR_SOCKETS;

#ifdef USE_CUDA
    buffer_num = FLAGS_use_vram ? 1 : NR_SOCKETS;
    if (FLAGS_use_vram)
        LOG(INFO) << "VRAM is used";
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(ram_buffer_size, i, FLAGS_use_vram);
        std::string name_prefix = FLAGS_use_vram ? "gpu:" : "cpu:";
        int rc = engine->registerLocalMemory(addr[i], ram_buffer_size,
                                             name_prefix + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#else
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(ram_buffer_size, i, false);
        int rc = engine->registerLocalMemory(addr[i], ram_buffer_size,
                                             "cpu:" + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#endif

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());

    std::thread workers[FLAGS_threads];

    struct timeval start_tv, stop_tv;
    gettimeofday(&start_tv, nullptr);

    for (int i = 0; i < FLAGS_threads; ++i)
        workers[i] = std::thread(initiatorWorker, engine.get(), segment_id, i,
                                 addr[i % buffer_num]);

    sleep(FLAGS_duration);
    running = false;

    for (int i = 0; i < FLAGS_threads; ++i) workers[i].join();

    gettimeofday(&stop_tv, nullptr);
    auto duration = (stop_tv.tv_sec - start_tv.tv_sec) +
                    (stop_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    auto batch_count = total_batch_count.load();

    LOG(INFO) << "Test completed: duration " << std::fixed
              << std::setprecision(2) << duration << ", batch count "
              << batch_count << ", throughput "
              << (batch_count * FLAGS_batch_size * FLAGS_block_size) /
                     duration / 1000000000.0;

    for (int i = 0; i < buffer_num; ++i) {
        engine->unregisterLocalMemory(addr[i]);
        freeMemoryPool(addr[i], ram_buffer_size);
    }

    return 0;
}

int target() {
    auto metadata_client =
        std::make_shared<TransferMetadata>(FLAGS_metadata_server);
    LOG_ASSERT(metadata_client);

    const size_t ram_buffer_size = 1ull << 30;
    auto engine = std::make_unique<TransferEngine>(metadata_client);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_local_server_name.c_str(), hostname_port.first.c_str(),
                 hostname_port.second);

    if (FLAGS_protocol == "rdma") {
        auto nic_priority_matrix = loadNicPriorityMatrix();
        void **args = (void **)malloc(2 * sizeof(void *));
        args[0] = (void *)nic_priority_matrix.c_str();
        args[1] = nullptr;
        engine->installOrGetTransport("rdma", args);
    } else if (FLAGS_protocol == "tcp") {
        engine->installOrGetTransport("tcp", nullptr);
    } else {
        LOG(ERROR) << "Unsupported protocol";
    }

    void *addr[NR_SOCKETS] = {nullptr};
    for (int i = 0; i < NR_SOCKETS; ++i) {
        addr[i] = allocateMemoryPool(ram_buffer_size, i);
        memset(addr[i], 'x', ram_buffer_size);
        int rc = engine->registerLocalMemory(addr[i], ram_buffer_size,
                                             "cpu:" + std::to_string(i));
        LOG_ASSERT(!rc);
    }

    while (true) sleep(1);

    for (int i = 0; i < NR_SOCKETS; ++i) {
        engine->unregisterLocalMemory(addr[i]);
        freeMemoryPool(addr[i], ram_buffer_size);
    }

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