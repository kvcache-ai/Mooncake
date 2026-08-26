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

// Dedicated large correctness test for manual RDMA chaos runs.
//
// How to run:
// etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls
// http://10.0.0.1:2379
// ./rdma_transport_chaos_test --mode=target  --metadata_server=127.0.0.1:2379
//   --local_server_name=127.0.0.2:12345 --device_name=erdma_0
// ./rdma_transport_chaos_test --metadata_server=127.0.0.1:2379
//   --segment_id=127.0.0.2:12345 --local_server_name=127.0.0.3:12346
//   --device_name=erdma_1

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/time.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

#include "cuda_alike.h"
#if defined(USE_CUDA) && defined(USE_NVMEOF)
#include <cufile.h>
#endif

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)

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
DEFINE_uint64(total_buffer_size, 1ull << 30,
              "Total local memory size to register on each node");
DEFINE_uint64(data_length, 4096000,
              "Bytes transferred by each worker per iteration");
DEFINE_uint64(iterations, 1, "WRITE/READ/compare iterations per worker");
DEFINE_uint64(num_threads, 1, "Number of concurrent initiator workers");

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
DEFINE_bool(use_vram, true, "Allocate memory from GPU VRAM");
DEFINE_int32(gpu_id, 0, "GPU ID to use");
#endif

using namespace mooncake;

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
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
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
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
    const size_t data_length = FLAGS_data_length;
    const size_t local_stride = data_length * 2;
    const size_t local_base = static_cast<size_t>(thread_id) * local_stride;
    const size_t remote_offset = static_cast<size_t>(thread_id) * data_length;
    uint8_t *write_addr = static_cast<uint8_t *>(addr) + local_base;
    uint8_t *read_addr = write_addr + data_length;

    for (uint64_t iteration = 0; iteration < FLAGS_iterations; ++iteration) {
        LOG(INFO) << "Stage 1: Write Data, thread=" << thread_id
                  << ", iteration=" << iteration << ", bytes=" << data_length;
        for (size_t offset = 0; offset < data_length; ++offset) {
            write_addr[offset] = static_cast<uint8_t>(
                (offset + thread_id * 131 + iteration * 17) % 251);
        }
        std::memset(read_addr, 0, data_length);

        LOG(INFO) << "Write Data: "
                  << std::string(reinterpret_cast<char *>(write_addr),
                                 std::min<size_t>(16, data_length))
                  << "...";

        Status s;

        {
            auto batch_id = engine->allocateBatchID(1);
            TransferRequest entry;
            entry.opcode = TransferRequest::WRITE;
            entry.length = data_length;
            entry.source = write_addr;
            entry.target_id = segment_id;
            entry.target_offset = remote_base + remote_offset;
            s = engine->submitTransfer(batch_id, {entry});
            if (!s.ok()) {
                LOG(ERROR) << "submit WRITE failed, thread=" << thread_id
                           << ", iteration=" << iteration;
                engine->freeBatchID(batch_id);
                return 1;
            }
            bool completed = false;
            TransferStatus status;
            while (!completed) {
                Status s = engine->getTransferStatus(batch_id, 0, status);
                if (!s.ok()) {
                    LOG(ERROR)
                        << "get WRITE status failed, thread=" << thread_id
                        << ", iteration=" << iteration;
                    engine->freeBatchID(batch_id);
                    return 1;
                }
                if (status.s == TransferStatusEnum::COMPLETED) {
                    completed = true;
                } else if (status.s == TransferStatusEnum::FAILED) {
                    LOG(ERROR) << "WRITE FAILED, thread=" << thread_id
                               << ", iteration=" << iteration;
                    engine->freeBatchID(batch_id);
                    return 1;
                }
            }
            s = engine->freeBatchID(batch_id);
            if (!s.ok()) return 1;
        }

        LOG(INFO) << "Stage 2: Read Data, thread=" << thread_id
                  << ", iteration=" << iteration << ", bytes=" << data_length;
        {
            auto batch_id = engine->allocateBatchID(1);
            TransferRequest entry;
            entry.opcode = TransferRequest::READ;
            entry.length = data_length;
            entry.source = read_addr;
            entry.target_id = segment_id;
            entry.target_offset = remote_base + remote_offset;
            s = engine->submitTransfer(batch_id, {entry});
            if (!s.ok()) {
                LOG(ERROR) << "submit READ failed, thread=" << thread_id
                           << ", iteration=" << iteration;
                engine->freeBatchID(batch_id);
                return 1;
            }
            bool completed = false;
            TransferStatus status;
            while (!completed) {
                Status s = engine->getTransferStatus(batch_id, 0, status);
                if (!s.ok()) {
                    LOG(ERROR) << "get READ status failed, thread=" << thread_id
                               << ", iteration=" << iteration;
                    engine->freeBatchID(batch_id);
                    return 1;
                }
                if (status.s == TransferStatusEnum::COMPLETED) {
                    completed = true;
                } else if (status.s == TransferStatusEnum::FAILED) {
                    LOG(ERROR) << "READ FAILED, thread=" << thread_id
                               << ", iteration=" << iteration;
                    engine->freeBatchID(batch_id);
                    return 1;
                }
            }
            s = engine->freeBatchID(batch_id);
            if (!s.ok()) return 1;
        }

        int ret = std::memcmp(write_addr, read_addr, data_length);
        LOG(INFO) << "Read Data: "
                  << std::string(reinterpret_cast<char *>(read_addr),
                                 std::min<size_t>(16, data_length))
                  << "...";
        LOG(INFO) << "Compare: " << (ret == 0 ? "OK" : "FAILED")
                  << ", thread=" << thread_id << ", iteration=" << iteration;
        if (ret != 0) return 1;
    }

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
           device_names +
           "], []], "
           " \"musa:0\": [[" +
           device_names + "], []]}";
}

int initiator() {
    const size_t ram_buffer_size = FLAGS_total_buffer_size;
    const size_t required_size = FLAGS_num_threads * FLAGS_data_length * 2;
    if (required_size > ram_buffer_size) {
        LOG(ERROR) << "total_buffer_size too small: required=" << required_size
                   << ", configured=" << ram_buffer_size;
        return 1;
    }
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
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    addr = allocateMemoryPool(ram_buffer_size, 0, FLAGS_use_vram);
    std::string name_prefix = FLAGS_use_vram ? GPU_PREFIX : "cpu:";
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
    std::vector<std::thread> workers;
    std::vector<int> worker_rc(FLAGS_num_threads, 1);
    for (uint64_t i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back([&, i]() {
            worker_rc[i] = initiatorWorker(engine.get(), segment_id,
                                           static_cast<int>(i), addr);
        });
    }
    for (auto &worker : workers) worker.join();
    engine->unregisterLocalMemory(addr);
    freeMemoryPool(addr, ram_buffer_size);
    for (auto rc : worker_rc) {
        if (rc != 0) return rc;
    }
    return 0;
}

int target() {
    const size_t ram_buffer_size = FLAGS_total_buffer_size;
    const size_t required_size = FLAGS_num_threads * FLAGS_data_length;
    if (required_size > ram_buffer_size) {
        LOG(ERROR) << "total_buffer_size too small: required=" << required_size
                   << ", configured=" << ram_buffer_size;
        return 1;
    }
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
