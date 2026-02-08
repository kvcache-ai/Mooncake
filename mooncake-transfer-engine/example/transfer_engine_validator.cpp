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

const static int NR_SOCKETS =
    numa_available() == 0 ? numa_num_configured_nodes() : 1;

static int buffer_num = NR_SOCKETS;

DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "192.168.3.77:2379", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");

DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp|nvlink|hip");

DEFINE_string(device_name, "mlx5_2",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(nic_priority_matrix, "",
              "Path to RDMA NIC priority matrix file (Advanced)");

DEFINE_string(segment_id, "192.168.3.76", "Segment ID to access data");
DEFINE_uint64(buffer_size, 1ull << 30, "total size of data buffer");
DEFINE_int32(batch_size, 128, "Batch size");
DEFINE_uint64(block_size, 65536, "Block size for each transfer request");
DEFINE_int32(duration, 10, "Test duration in seconds");
DEFINE_int32(threads, 12, "Task submission threads");
DEFINE_bool(auto_discovery, false, "Enable auto discovery");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
DEFINE_bool(use_vram, true, "Allocate memory from GPU VRAM");
DEFINE_int32(gpu_id, 0, "GPU ID to use, -1 for all GPUs");
#endif

using namespace mooncake;

static void *allocateMemoryPool(size_t size, int buffer_id,
                                bool from_vram = false) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    if (from_vram) {
        int gpu_id;
        if (FLAGS_gpu_id == -1) {
            gpu_id = buffer_id;
        } else {
            gpu_id = FLAGS_gpu_id;
        }
        void *d_buf;
        LOG(INFO) << "Allocating memory on GPU " << gpu_id;
        checkCudaError(cudaSetDevice(gpu_id), "Failed to set device");
#ifdef USE_MNNVL
        d_buf = allocateFabricMemory(size);
#else
        checkCudaError(cudaMalloc(&d_buf, size),
                       "Failed to allocate device memory");
#endif
        return d_buf;
    }
#endif
    return numa_alloc_onnode(size, buffer_id);
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

Status submitRequestSync(TransferEngine *engine, SegmentID handle,
                         int thread_id, void *addr, uint64_t remote_base,
                         TransferRequest::OpCode opcode) {
    auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
    Status s;
    std::vector<TransferRequest> requests;
    for (int i = 0; i < FLAGS_batch_size; ++i) {
        TransferRequest entry;
        entry.opcode = opcode;
        entry.length = FLAGS_block_size;
        entry.source = (uint8_t *)(addr) +
                       FLAGS_block_size * (i * FLAGS_threads + thread_id);
        entry.target_id = handle;
        entry.target_offset =
            remote_base + FLAGS_block_size * (i * FLAGS_threads + thread_id);
        requests.emplace_back(entry);
    }

    s = engine->submitTransfer(batch_id, requests);
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
    return Status::OK();
}

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
class PinnedBuffer {
   public:
    explicit PinnedBuffer(size_t size) : size_(size), ptr_(nullptr) {
        cudaError_t err = cudaMallocHost(&ptr_, size_);
        if (err != cudaSuccess) {
            throw std::runtime_error("cudaMallocHost failed");
        }
    }

    ~PinnedBuffer() {
        if (ptr_) {
            cudaFreeHost(ptr_);
            ptr_ = nullptr;
        }
    }

    void *data() { return ptr_; }
    const void *data() const { return ptr_; }

    size_t size() const { return size_; }

   private:
    size_t size_;
    void *ptr_;
};

thread_local PinnedBuffer ref_buf(FLAGS_block_size);
thread_local PinnedBuffer user_buf(FLAGS_block_size);
#else
thread_local std::vector<uint8_t> ref_buf(FLAGS_block_size);
thread_local std::vector<uint8_t> user_buf(FLAGS_block_size);
#endif

void fillData(int thread_id, void *addr, uint8_t seed) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    memset(ref_buf.data(), seed, FLAGS_block_size);
    cudaStream_t s;
    cudaStreamCreate(&s);
    for (int i = 0; i < FLAGS_batch_size; ++i) {
        uint8_t *local_addr =
            (uint8_t *)(addr) +
            FLAGS_block_size * (i * FLAGS_threads + thread_id);
        cudaMemcpyAsync(local_addr, ref_buf.data(), FLAGS_block_size,
                        cudaMemcpyDefault, s);
    }
    cudaStreamSynchronize(s);
    cudaStreamDestroy(s);
#else
    for (int i = 0; i < FLAGS_batch_size; ++i) {
        uint8_t *local_addr =
            (uint8_t *)(addr) +
            FLAGS_block_size * (i * FLAGS_threads + thread_id);
        memset(local_addr, seed, FLAGS_block_size);
    }
#endif
}

void checkData(int thread_id, void *addr, uint8_t seed) {
    memset(ref_buf.data(), seed, FLAGS_block_size);
    for (int i = 0; i < FLAGS_batch_size; ++i) {
        uint8_t *local_addr =
            (uint8_t *)(addr) +
            FLAGS_block_size * (i * FLAGS_threads + thread_id);
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
        cudaStream_t s;
        cudaStreamCreate(&s);
        cudaMemcpyAsync(user_buf.data(), local_addr, FLAGS_block_size,
                        cudaMemcpyDefault, s);
        cudaStreamSynchronize(s);
        cudaStreamDestroy(s);
        if (memcmp(user_buf.data(), ref_buf.data(), FLAGS_block_size) != 0) {
            LOG(ERROR) << "Detect data integrity problem";
            exit(EXIT_FAILURE);
        }
#else
        if (memcmp(local_addr, ref_buf.data(), FLAGS_block_size) != 0) {
            LOG(ERROR) << "Detect data integrity problem";
            exit(EXIT_FAILURE);
        }
#endif
    }
}

Status initiatorWorker(TransferEngine *engine, SegmentID segment_id,
                       int thread_id, void *addr) {
    bindToSocket(thread_id % NR_SOCKETS);
    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc) {
        LOG(ERROR) << "Unable to get target segment ID, please recheck";
        exit(EXIT_FAILURE);
    }
    uint64_t remote_base =
        (uint64_t)segment_desc->buffers[thread_id % buffer_num].addr;

    size_t batch_count = 0;
    while (running) {
        uint8_t seed = 0;
        seed = SimpleRandom::Get().next(UINT8_MAX);
        if (batch_count == 0 || SimpleRandom::Get().next(64) == 31) {
            fillData(thread_id, addr, seed);
            submitRequestSync(engine, segment_id, thread_id, addr, remote_base,
                              TransferRequest::WRITE);
            fillData(thread_id, addr, 0);
            submitRequestSync(engine, segment_id, thread_id, addr, remote_base,
                              TransferRequest::READ);
            checkData(thread_id, addr, seed);
        } else {
            submitRequestSync(engine, segment_id, thread_id, addr, remote_base,
                              TransferRequest::WRITE);
            submitRequestSync(engine, segment_id, thread_id, addr, remote_base,
                              TransferRequest::READ);
        }
        batch_count++;
    }
    LOG(INFO) << "Worker " << thread_id << " stopped! Data validation passed";
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

    std::vector<void *> addr;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    if (FLAGS_use_vram) {
        int gpu_num;
        LOG(INFO) << "VRAM is used";
        if (FLAGS_gpu_id == -1 && cudaGetDeviceCount(&gpu_num) == cudaSuccess) {
            LOG(INFO) << "GPU ID is not specified, found " << gpu_num
                      << " GPUs to use";
            buffer_num = gpu_num;
        } else {
            LOG(INFO) << "GPU ID is specified or failed to get GPU count, use "
                      << FLAGS_gpu_id << " GPU";
            buffer_num = 1;
        }
    } else {
        LOG(INFO) << "DRAM is used, numa node num: " << NR_SOCKETS;
    }
    addr.resize(buffer_num);
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, FLAGS_use_vram);
        std::string name_prefix;
        int name_suffix;
        if (FLAGS_use_vram) {
            name_prefix = GPU_PREFIX;
            if (FLAGS_gpu_id == -1) {
                name_suffix = i;
            } else {
                name_suffix = FLAGS_gpu_id;
            }
        } else {
            name_prefix = "cpu:";
            name_suffix = i;
        }
        int rc = engine->registerLocalMemory(
            addr[i], FLAGS_buffer_size,
            name_prefix + std::to_string(name_suffix));
        LOG_ASSERT(!rc);
    }
#else
    LOG(INFO) << "DRAM is used, numa node num: " << NR_SOCKETS;
    addr.resize(buffer_num);
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, false);
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             "cpu:" + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#endif

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());

    std::vector<std::thread> workers(FLAGS_threads);

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

    std::vector<void *> addr;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    if (FLAGS_use_vram) {
        int gpu_num;
        LOG(INFO) << "VRAM is used";
        if (FLAGS_gpu_id == -1 && cudaGetDeviceCount(&gpu_num) == cudaSuccess) {
            LOG(INFO) << "GPU ID is not specified, found " << gpu_num
                      << " GPUs to use";
            buffer_num = gpu_num;
        } else {
            LOG(INFO) << "GPU ID is specified or failed to get GPU count, use "
                      << FLAGS_gpu_id << " GPU";
            buffer_num = 1;
        }
    } else {
        LOG(INFO) << "DRAM is used, numa node num: " << NR_SOCKETS;
    }
    addr.resize(buffer_num);
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, FLAGS_use_vram);
        std::string name_prefix;
        int name_suffix;
        if (FLAGS_use_vram) {
            name_prefix = GPU_PREFIX;
            if (FLAGS_gpu_id == -1) {
                name_suffix = i;
            } else {
                name_suffix = FLAGS_gpu_id;
            }
        } else {
            name_prefix = "cpu:";
            name_suffix = i;
        }
        int rc = engine->registerLocalMemory(
            addr[i], FLAGS_buffer_size,
            name_prefix + std::to_string(name_suffix));
        LOG_ASSERT(!rc);
    }
#else
    LOG(INFO) << "DRAM is used, numa node num: " << NR_SOCKETS;
    addr.resize(buffer_num);
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, false);
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             "cpu:" + std::to_string(i));
        LOG_ASSERT(!rc);
    }
#endif

    while (target_running) sleep(1);
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
