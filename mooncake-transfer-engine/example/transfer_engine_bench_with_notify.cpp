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
#include <signal.h>
#include <sys/time.h>

#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <unordered_map>

#include "common.h"
#include "common/base/status.h"
#include "transfer_engine.h"
#include "transport/transport.h"

#include "cuda_alike.h"
#ifdef USE_CUDA
#ifdef USE_NVMEOF
#include <cufile.h>
#endif
#endif

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
#include <cassert>

#ifdef USE_MNNVL
#include "gpu_vendor/mnnvl.h"
#endif

#ifdef USE_INTRA_NODE_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif

static void checkCudaError(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        exit(EXIT_FAILURE);
    }
}
#endif

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
const static int NR_SOCKETS = 4;
#else
const static int NR_SOCKETS =
    numa_available() == 0 ? numa_num_configured_nodes() : 1;
#endif

DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "192.168.3.77:2379", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "read", "Operation type: read or write");

DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp|nvlink|hip");

DEFINE_string(device_name, "mlx5_2",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(nic_priority_matrix, "",
              "Path to RDMA NIC priority matrix file (Advanced)");

DEFINE_string(segment_id, "192.168.3.76", "Segment ID to access data");
DEFINE_uint64(buffer_size, 1ull << 30, "total size of data buffer");
DEFINE_int32(batch_size, 128, "Batch size");
DEFINE_uint64(block_size, 4096, "Block size for each transfer request");
DEFINE_int32(duration, 10, "Test duration in seconds");
DEFINE_int32(threads, 4, "Task submission threads");
DEFINE_bool(auto_discovery, false, "Enable auto discovery");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
DEFINE_bool(use_vram, true, "Allocate memory from GPU VRAM");
DEFINE_bool(init_mem, true, "Initialize allocated memory");
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
#ifdef USE_MNNVL
        d_buf = allocateFabricMemory(size);
#else
        checkCudaError(cudaMalloc(&d_buf, size),
                       "Failed to allocate device memory");
#endif
        if (FLAGS_init_mem) {
            checkCudaError(cudaMemset(d_buf, 0xCC, size),
                           "Failed to initialize device memory");
            // Ensure memory initialization is done from CPU standpoint
            checkCudaError(cudaStreamSynchronize(0), "Failed to synchronize");
        }

        return d_buf;
    }
#endif
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
#ifdef USE_MNNVL
    if (FLAGS_use_vram) {
        freeFabricMemory(addr);
        return;
    }
#endif  // USE_MNNVL

    // check pointer on GPU
    cudaPointerAttributes attributes;
    checkCudaError(cudaPointerGetAttributes(&attributes, addr),
                   "Failed to get pointer attributes");

    if (attributes.type == cudaMemoryTypeDevice) {
        cudaFree(addr);
    } else if (attributes.type == cudaMemoryTypeHost ||
               attributes.type == cudaMemoryTypeUnregistered) {
        numa_free(addr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << addr << " " << attributes.type;
    }
#else
    numa_free(addr, size);
#endif
}

const static std::unordered_map<std::string, uint64_t> RATE_UNIT_MP = {
    {"GB", 1000ull * 1000ull * 1000ull},
    {"GiB", 1ull << 30},
    {"Gb", 1000ull * 1000ull * 1000ull / 8},
    {"MB", 1000ull * 1000ull},
    {"MiB", 1ull << 20},
    {"Mb", 1000ull * 1000ull / 8},
    {"KB", 1000ull},
    {"KiB", 1ull << 10},
    {"Kb", 1000ull / 8}};

static inline std::string calculateRate(uint64_t data_bytes, double duration) {
    if (std::fabs(duration) < 1e-10) {
        LOG(ERROR) << "Invalid args: duration shouldn't be 0";
        return "";
    }
    if (!RATE_UNIT_MP.count(FLAGS_report_unit)) {
        LOG(WARNING) << "Invalid flag: report_unit only support "
                        "GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb, not support "
                     << FLAGS_report_unit
                     << " . Now use GB(default) as report_unit";
        FLAGS_report_unit = "GB";
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(FLAGS_report_precision)
        << 1.0 * data_bytes / duration / RATE_UNIT_MP.at(FLAGS_report_unit)
        << " " << FLAGS_report_unit << "/s";
    return oss.str();
}

volatile bool running = true;
std::atomic<size_t> total_batch_count(0);

Status initiatorWorker(TransferEngine *engine, SegmentID segment_id,
                       int thread_id, void *addr) {
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

    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc) {
        LOG(ERROR) << "Unable to get target segment ID, please recheck";
        exit(EXIT_FAILURE);
    }
    uint64_t remote_base =
        (uint64_t)segment_desc->buffers[thread_id % NR_SOCKETS].addr;

    size_t batch_count = 0;
    auto transfer_count = 0;
    while (running) {
        auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
        Status s;
        transfer_count++;
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
        TransferMetadata::NotifyDesc notify;
        notify.name = "agent1";
        notify.notify_msg = "notification" + std::to_string(transfer_count);
        s = engine->submitTransferWithNotify(batch_id, requests, notify);
        if (!s.ok()) LOG(ERROR) << s.ToString();
        LOG_ASSERT(s.ok());
        for (int task_id = 0; task_id < FLAGS_batch_size; ++task_id) {
            bool completed = false;
            TransferStatus status;
            while (!completed) {
                Status s = engine->getTransferStatus(batch_id, task_id, status);
                LOG_ASSERT(s.ok());
                if (status.s == TransferStatusEnum::COMPLETED)
                    completed = true;
                else if (status.s == TransferStatusEnum::FAILED) {
                    LOG(INFO) << "FAILED";
                    completed = true;
                    exit(EXIT_FAILURE);
                }
            }
        }

        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
        batch_count++;
        sleep(1);
    }
    LOG(INFO) << "Worker " << thread_id << " stopped!";
    total_batch_count.fetch_add(batch_count);
    return Status::OK();
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
    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    if (!FLAGS_auto_discovery) {
        Transport *xport = nullptr;
        if (FLAGS_protocol == "rdma") {
            auto nic_priority_matrix = loadNicPriorityMatrix();
            void **args = (void **)malloc(2 * sizeof(void *));
            args[0] = (void *)nic_priority_matrix.c_str();
            args[1] = nullptr;
            xport = engine->installTransport("rdma", args);
        } else if (FLAGS_protocol == "tcp") {
            xport = engine->installTransport("tcp", nullptr);
        } else if (FLAGS_protocol == "nvlink") {
            xport = engine->installTransport("nvlink", nullptr);
        } else if (FLAGS_protocol == "nvlink_intra") {
            xport = engine->installTransport("nvlink_intra", nullptr);
        } else if (FLAGS_protocol == "hip") {
            xport = engine->installTransport("hip", nullptr);
        } else {
            LOG(ERROR) << "Unsupported protocol";
        }
        LOG_ASSERT(xport);
    }

    std::vector<void *> addr(NR_SOCKETS, nullptr);
    int buffer_num = NR_SOCKETS;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    if (FLAGS_use_vram) LOG(INFO) << "VRAM is used";
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, FLAGS_use_vram);
        std::string name_prefix = FLAGS_use_vram ? GPU_PREFIX : "cpu:";
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             name_prefix + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#else
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, false);
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             "cpu:" + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#endif

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());

    std::thread workers[FLAGS_threads];

    struct timeval start_tv, stop_tv;
    gettimeofday(&start_tv, nullptr);

    // for (int i = 0; i < FLAGS_threads; ++i)
    //     workers[i] = std::thread(initiatorWorker, engine.get(), segment_id,
    //     i,
    //                              addr[i % buffer_num]);
    initiatorWorker(engine.get(), segment_id, 1, addr[1 % buffer_num]);
    sleep(FLAGS_duration);
    running = false;

    // for (int i = 0; i < FLAGS_threads; ++i) workers[i].join();

    gettimeofday(&stop_tv, nullptr);
    auto duration = (stop_tv.tv_sec - start_tv.tv_sec) +
                    (stop_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    auto batch_count = total_batch_count.load();

    LOG(INFO) << "numa node num: " << NR_SOCKETS;

    LOG(INFO) << "Test completed: duration " << std::fixed
              << std::setprecision(2) << duration << ", batch count "
              << batch_count << ", throughput "
              << calculateRate(
                     batch_count * FLAGS_batch_size * FLAGS_block_size,
                     duration);

    for (int i = 0; i < buffer_num; ++i) {
        engine->unregisterLocalMemory(addr[i]);
        freeMemoryPool(addr[i], FLAGS_buffer_size);
    }

    return 0;
}

volatile bool target_running = true;

void signalHandler(int signum) {
    LOG(INFO) << "Received signal " << signum << ", stopping target server...";
    target_running = false;
}

int target() {
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    if (!FLAGS_auto_discovery) {
        if (FLAGS_protocol == "rdma") {
            auto nic_priority_matrix = loadNicPriorityMatrix();
            void **args = (void **)malloc(2 * sizeof(void *));
            args[0] = (void *)nic_priority_matrix.c_str();
            args[1] = nullptr;
            engine->installTransport("rdma", args);
        } else if (FLAGS_protocol == "tcp") {
            engine->installTransport("tcp", nullptr);
        } else if (FLAGS_protocol == "nvlink") {
            engine->installTransport("nvlink", nullptr);
        } else if (FLAGS_protocol == "hip") {
            engine->installTransport("hip", nullptr);
        } else {
            LOG(ERROR) << "Unsupported protocol";
        }
    }

    std::vector<void *> addr(NR_SOCKETS, nullptr);
    int buffer_num = NR_SOCKETS;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    if (FLAGS_use_vram) LOG(INFO) << "VRAM is used";
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, FLAGS_use_vram);
        std::string name_prefix = FLAGS_use_vram ? GPU_PREFIX : "cpu:";
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             name_prefix + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#else
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, false);
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             "cpu:" + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#endif

    LOG(INFO) << "numa node num: " << NR_SOCKETS;

    std::vector<TransferMetadata::NotifyDesc> notifies;
    while (target_running) {
        sleep(1);
        engine->getNotifies(notifies);
        for (auto notify : notifies) {
            LOG(INFO) << notify.name << notify.notify_msg;
        }
        notifies.clear();
    }
    for (int i = 0; i < buffer_num; ++i) {
        engine->unregisterLocalMemory(addr[i]);
        freeMemoryPool(addr[i], FLAGS_buffer_size);
    }

    return 0;
}

void check_total_buffer_size() {
    uint64_t require_size = FLAGS_block_size * FLAGS_batch_size * FLAGS_threads;
    if (FLAGS_buffer_size < require_size) {
        FLAGS_buffer_size = require_size;
        LOG(WARNING) << "Invalid flag: buffer size is smaller than "
                        "require_size, adjust to "
                     << require_size;
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    check_total_buffer_size();

    if (FLAGS_mode == "initiator")
        return initiator();
    else if (FLAGS_mode == "target")
        return target();

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    exit(EXIT_FAILURE);
}
