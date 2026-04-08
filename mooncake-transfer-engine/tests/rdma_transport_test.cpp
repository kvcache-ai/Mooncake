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
// ./rdma_transport_test --mode=target --metadata_server=127.0.0.1:2379
//   --local_server_name=127.0.0.2:12345 --device_name=erdma_0
//   --mem_backend=mlu --device_id=0
// ./rdma_transport_test --metadata_server=127.0.0.1:2379
//   --segment_id=127.0.0.2:12345 --local_server_name=127.0.0.3:12346
//   --device_name=erdma_1 --mem_backend=mlu --device_id=0
//   --expect_remote_location=mlu:0
// MACA builds use mem_backend=gpu (same cuda_alike path); example:
//   --expect_remote_location=maca:0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/time.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "memory_location.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

#include "cuda_alike.h"
#if defined(USE_CUDA) && defined(USE_NVMEOF)
#include <cufile.h>
#endif

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MLU) || defined(USE_MACA)

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
DEFINE_string(operation, "read", "Legacy operation selector");
DEFINE_string(protocol, "rdma", "Legacy transport selector");
DEFINE_string(device_name, "mlx5_0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(nic_priority_matrix, "",
              "Path to RDMA NIC priority matrix file (Advanced)");
DEFINE_string(segment_id, "192.168.3.76", "Segment ID to access data");
DEFINE_string(mem_backend, "auto", "Memory backend: auto|cpu|gpu|mlu");
DEFINE_int32(device_id, -1, "Backend device ID");
DEFINE_bool(use_wildcard_location, false,
            "Register memory with wildcard location");
DEFINE_string(expect_remote_location, "", "Expected remote buffer location");
DEFINE_uint64(buffer_size, 64ull << 20, "Registered buffer size");
DEFINE_uint64(data_length, 4ull << 20, "Transfer size");

// Legacy flags kept for backwards compatibility with the old GPU-only test.
DEFINE_bool(use_vram, true, "Legacy alias for mem_backend=gpu");
DEFINE_int32(gpu_id, 0, "Legacy alias for device_id");

using namespace mooncake;

namespace {

std::string pickBackend() {
    if (FLAGS_mem_backend != "auto") {
        return FLAGS_mem_backend;
    }
#if defined(USE_MLU)
    return "mlu";
#elif defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MACA)
    return FLAGS_use_vram ? "gpu" : "cpu";
#else
    return "cpu";
#endif
}

int pickDevId(const std::string &backend) {
    if (FLAGS_device_id >= 0) {
        return FLAGS_device_id;
    }
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MACA)
    if (backend == "gpu") {
        return FLAGS_gpu_id;
    }
#endif
    return 0;
}

bool usesDeviceMemory(const std::string &backend) { return backend != "cpu"; }

void validateBackend(const std::string &backend) {
    if (backend == "cpu") {
        return;
    }
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MACA)
    if (backend == "gpu") {
        return;
    }
#endif
#if defined(USE_MLU)
    if (backend == "mlu") {
        return;
    }
#endif
    LOG(ERROR) << "Unsupported mem_backend=" << backend;
    std::exit(EXIT_FAILURE);
}

std::string explicitLocation(const std::string &backend) {
    if (backend == "cpu") {
        return "cpu:0";
    }
    return GPU_PREFIX + std::to_string(pickDevId(backend));
}

std::string registrationLocation(const std::string &backend) {
    if (FLAGS_protocol != "rdma") {
        return "cpu:0";
    }
    if (FLAGS_use_wildcard_location) {
        return kWildcardLocation;
    }
    return explicitLocation(backend);
}

bool validateTransferSizes() {
    if (FLAGS_data_length == 0) {
        LOG(ERROR) << "data_length must be greater than 0";
        return false;
    }
    if (FLAGS_buffer_size < FLAGS_data_length * 2) {
        LOG(ERROR) << "buffer_size must be at least 2 * data_length";
        return false;
    }
    return true;
}

void setBackendDevice(const std::string &backend) {
    if (!usesDeviceMemory(backend)) {
        return;
    }
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MLU) || defined(USE_MACA)
    checkCudaError(cudaSetDevice(pickDevId(backend)), "Failed to set device");
#else
    LOG(FATAL) << "Device memory backend is not available in this build";
#endif
}

void *allocateMemoryPool(size_t size, int socket_id,
                         const std::string &backend) {
    if (usesDeviceMemory(backend)) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MLU) || defined(USE_MACA)
        setBackendDevice(backend);
        void *d_buf = nullptr;
        checkCudaError(cudaMalloc(&d_buf, size),
                       "Failed to allocate device memory");
        return d_buf;
#else
        LOG(FATAL) << "Device memory backend is not available in this build";
        return nullptr;
#endif
    }
    return numa_alloc_onnode(size, socket_id);
}

void freeMemoryPool(void *addr, size_t size, const std::string &backend) {
    if (usesDeviceMemory(backend)) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MLU) || defined(USE_MACA)
        cudaFree(addr);
        return;
#else
        LOG(FATAL) << "Device memory backend is not available in this build";
#endif
    }
    numa_free(addr, size);
}

void copyFromHost(void *dst, const void *src, size_t size,
                  const std::string &backend) {
    if (usesDeviceMemory(backend)) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MLU) || defined(USE_MACA)
        checkCudaError(cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice),
                       "Failed to copy host data to device");
        return;
#else
        LOG(FATAL) << "Device memory backend is not available in this build";
#endif
    }
    std::memcpy(dst, src, size);
}

void copyToHost(void *dst, const void *src, size_t size,
                const std::string &backend) {
    if (usesDeviceMemory(backend)) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) || \
    defined(USE_MLU) || defined(USE_MACA)
        checkCudaError(cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost),
                       "Failed to copy device data to host");
        return;
#else
        LOG(FATAL) << "Device memory backend is not available in this build";
#endif
    }
    std::memcpy(dst, src, size);
}

bool waitForTransfer(TransferEngine *engine, BatchID batch_id) {
    bool completed = false;
    TransferStatus status;
    while (!completed) {
        Status s = engine->getTransferStatus(batch_id, 0, status);
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            completed = true;
        } else if (status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "Transfer failed";
            return false;
        }
    }
    return true;
}

int initiatorWorker(TransferEngine *engine, SegmentID segment_id, int thread_id,
                    void *addr, const std::string &backend) {
    (void)thread_id;
    bindToSocket(0);
    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    LOG_ASSERT(segment_desc);
    LOG_ASSERT(!segment_desc->buffers.empty());
    LOG(INFO) << "Remote segment protocol: " << segment_desc->protocol;
    LOG(INFO) << "Remote buffer location: " << segment_desc->buffers[0].name;
    if (!FLAGS_expect_remote_location.empty()) {
        LOG_ASSERT(segment_desc->buffers[0].name ==
                   FLAGS_expect_remote_location);
    }

    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;
    const size_t kDataLength = FLAGS_data_length;
    auto write_buf = std::make_unique<char[]>(kDataLength);
    auto read_buf = std::make_unique<char[]>(kDataLength);
    {
        LOG(INFO) << "Stage 1: Write Data";
        for (size_t offset = 0; offset < kDataLength; ++offset)
            write_buf[offset] = 'a' + lrand48() % 26;

        LOG(INFO) << "Write Data: " << std::string(write_buf.get(), 16)
                  << "...";
        copyFromHost(addr, write_buf.get(), kDataLength, backend);

        auto batch_id = engine->allocateBatchID(1);
        Status s;

        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        s = engine->submitTransfer(batch_id, {entry});
        if (!s.ok()) {
            LOG(ERROR) << "WRITE submit failed: " << s.ToString();
            engine->freeBatchID(batch_id);
            return EXIT_FAILURE;
        }
        bool ok = waitForTransfer(engine, batch_id);
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
        if (!ok) {
            return EXIT_FAILURE;
        }
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
        if (!s.ok()) {
            LOG(ERROR) << "READ submit failed: " << s.ToString();
            engine->freeBatchID(batch_id);
            return EXIT_FAILURE;
        }
        bool ok = waitForTransfer(engine, batch_id);
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
        if (!ok) {
            return EXIT_FAILURE;
        }
    }

    copyToHost(read_buf.get(), (uint8_t *)(addr) + kDataLength, kDataLength,
               backend);
    int ret = memcmp(write_buf.get(), read_buf.get(), kDataLength);
    LOG(INFO) << "Read Data: " << std::string(read_buf.get(), 16) << "...";
    LOG(INFO) << "RDMA compare: " << (ret == 0 ? "OK" : "FAILED");

    return ret == 0 ? 0 : EXIT_FAILURE;
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

std::string loadNicPriorityMatrix(const std::string &backend) {
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
    std::string matrix = "{\"cpu:0\": [[" + device_names +
                         "], []], "
                         " \"cpu:1\": [[" +
                         device_names + "], []]";
    if (usesDeviceMemory(backend)) {
        matrix += ", \"" + explicitLocation(backend) + "\": [[" + device_names +
                  "], []]";
    }
    matrix += "}";
    return matrix;
}

Transport *installTransport(TransferEngine *engine,
                            const std::string &backend) {
    if (FLAGS_protocol == "rdma") {
        auto nic_priority_matrix = loadNicPriorityMatrix(backend);
        void *args[2] = {const_cast<char *>(nic_priority_matrix.c_str()),
                         nullptr};
        return engine->installTransport("rdma", args);
    }
    if (FLAGS_protocol == "tcp") {
        return engine->installTransport("tcp", nullptr);
    }
    if (FLAGS_protocol == "nvmeof") {
        return engine->installTransport("nvmeof", nullptr);
    }
    LOG(ERROR) << "Unsupported protocol: must be rdma, tcp, or nvmeof";
    return nullptr;
}

int initiator() {
    (void)FLAGS_operation;
    auto backend = pickBackend();
    validateBackend(backend);
    if (!validateTransferSizes()) {
        return EXIT_FAILURE;
    }
    if (FLAGS_protocol != "rdma" && usesDeviceMemory(backend)) {
        LOG(ERROR) << "protocol=" << FLAGS_protocol
                   << " currently supports only mem_backend=cpu in this test";
        return EXIT_FAILURE;
    }

    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    Transport *xport = installTransport(engine.get(), backend);
    LOG_ASSERT(xport);

    void *addr = allocateMemoryPool(FLAGS_buffer_size, 0, backend);
    int rc = engine->registerLocalMemory(addr, FLAGS_buffer_size,
                                         registrationLocation(backend));
    LOG_ASSERT(!rc);

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());
    int worker_rc = 0;
    std::thread workers([&]() {
        worker_rc = initiatorWorker(engine.get(), segment_id, 0, addr, backend);
    });
    workers.join();
    engine->unregisterLocalMemory(addr);
    freeMemoryPool(addr, FLAGS_buffer_size, backend);
    return worker_rc;
}

int target() {
    (void)FLAGS_operation;
    auto backend = pickBackend();
    validateBackend(backend);
    if (!validateTransferSizes()) {
        return EXIT_FAILURE;
    }
    if (FLAGS_protocol != "rdma" && usesDeviceMemory(backend)) {
        LOG(ERROR) << "protocol=" << FLAGS_protocol
                   << " currently supports only mem_backend=cpu in this test";
        return EXIT_FAILURE;
    }

    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    auto *xport = installTransport(engine.get(), backend);
    LOG_ASSERT(xport);

    void *addr = allocateMemoryPool(FLAGS_buffer_size, 0, backend);
    int rc = engine->registerLocalMemory(addr, FLAGS_buffer_size,
                                         registrationLocation(backend));
    LOG_ASSERT(!rc);

    while (true) sleep(1);

    engine->unregisterLocalMemory(addr);
    freeMemoryPool(addr, FLAGS_buffer_size, backend);
    return 0;
}

}  // namespace

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_mode == "initiator")
        return initiator();
    else if (FLAGS_mode == "target")
        return target();

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    return EXIT_FAILURE;
}
