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

#include <bits/stdc++.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/time.h>

#include "v1/transfer_engine.h"
#include "v1/utility/random.h"
#include "v1/utility/system.h"

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif
#include <numa.h>

DEFINE_string(metadata_type, "p2p", "Metadata type: p2p|etcd|redis|http");
DEFINE_string(metadata_servers, "",
              "Metadata servers (required unless type is p2p)");
DEFINE_string(workload, "read", "Test workload: read|write|mix");
DEFINE_string(local_segment, "",
              "Set the custom local segment name (optional)");
DEFINE_string(cluster_topology, "", "Set the cluster topology file (optional)");
DEFINE_string(remote_segment, "", "Set the remote segment name (required)");
DEFINE_bool(check, false, "Check data integrity if workload is mix");
DEFINE_bool(shmfs, false, "Enable shmfs");
DEFINE_bool(mnnvl, false, "Enable multi-node NVLink");

DEFINE_int32(batch, 16, "Number of requests per batch");
DEFINE_uint64(size, 65536, "Block size for each request");
DEFINE_uint64(slice_size, 65536, "Slice size that Mooncake TE split");
DEFINE_int32(duration, 10, "Test duration in seconds");
DEFINE_int32(threads, 16, "Test threads to submit requests");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

#ifdef USE_CUDA
DEFINE_bool(use_dram, true, "Allocate memory from CPU DRAM");
DEFINE_bool(use_vram, false, "Allocate memory from GPU VRAM");
#endif

#define CHECK_FAIL(call)                                        \
    do {                                                        \
        auto status_ = call;                                    \
        if (!status_.ok()) {                                    \
            LOG(INFO) << "Found error: " << status_.ToString(); \
            exit(EXIT_FAILURE);                                 \
        }                                                       \
    } while (0)

using namespace mooncake::v1;

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
                     << ". Now use GB(default) as report_unit";
        FLAGS_report_unit = "GB";
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(FLAGS_report_precision)
        << 1.0 * data_bytes / duration / RATE_UNIT_MP.at(FLAGS_report_unit)
        << " " << FLAGS_report_unit << "/s";
    return oss.str();
}

int num_sockets = 1;
int num_buffers = 1;
int cuda_device_count = 0;
size_t buffer_capacity = 1024 * 1024 * 1024;
volatile bool running = true;
std::atomic<size_t> total_batch_count(0);

uint64_t getStartAddress(TransferEngine *engine, SegmentID handle,
                         int thread_id) {
    SegmentInfo info;
    auto status = engine->getSegmentInfo(handle, info);
    if (!status.ok() || info.buffers.empty()) {
        LOG(ERROR) << "Invalid args: cannot find buffers in "
                   << FLAGS_remote_segment << ", please recheck";
        exit(EXIT_FAILURE);
    }
    int buffer_id = thread_id % info.buffers.size();
    std::string location;
#ifdef USE_CUDA
    if (FLAGS_use_dram && buffer_id < num_sockets) {
        location = "cpu:" + std::to_string(buffer_id);
    } else if (FLAGS_use_vram) {
        if (FLAGS_use_dram) buffer_id -= num_sockets;
        location = "cuda:" + std::to_string(buffer_id);
    }
#else
    location = "cpu:" + std::to_string(buffer_id);
#endif
    for (auto &entry : info.buffers)
        if (entry.location == location) return (uint64_t)entry.base;
    return (uint64_t)info.buffers[0].base;
}

Status submitRequestSync(TransferEngine *engine, SegmentID handle,
                         int thread_id, void *addr, uint64_t remote_base,
                         Request::OpCode opcode) {
    auto batch_id = engine->allocateBatch(FLAGS_batch);
    std::vector<Request> requests;
    for (int i = 0; i < FLAGS_batch; ++i) {
        Request entry;
        entry.opcode = opcode;
        entry.length = FLAGS_size;
        entry.source =
            (uint8_t *)(addr) + FLAGS_size * (i * FLAGS_threads + thread_id);
        entry.target_id = handle;
        entry.target_offset =
            remote_base + FLAGS_size * (i * FLAGS_threads + thread_id);
        requests.emplace_back(entry);
    }
    CHECK_FAIL(engine->submitTransfer(batch_id, requests));
    while (true) {
        TransferStatus overall_status;
        CHECK_FAIL(engine->getTransferStatus(batch_id, overall_status));
        if (overall_status.s == TransferStatusEnum::COMPLETED) {
            break;
        } else if (overall_status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "Failed transfer detected";
            exit(EXIT_FAILURE);
        }
    }
    CHECK_FAIL(engine->freeBatch(batch_id));
    return Status::OK();
}

#ifdef USE_CUDA
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

thread_local PinnedBuffer ref_buf(FLAGS_size);
thread_local PinnedBuffer user_buf(FLAGS_size);
#else
thread_local std::vector<uint8_t> ref_buf(FLAGS_size);
thread_local std::vector<uint8_t> user_buf(FLAGS_size);
#endif

void fillData(int thread_id, void *addr, uint8_t seed) {
#ifdef USE_CUDA
    memset(ref_buf.data(), seed, FLAGS_size);
    cudaStream_t s;
    cudaStreamCreate(&s);
    for (int i = 0; i < FLAGS_batch; ++i) {
        uint8_t *local_addr =
            (uint8_t *)(addr) + FLAGS_size * (i * FLAGS_threads + thread_id);
        cudaMemcpyAsync(local_addr, ref_buf.data(), FLAGS_size,
                        cudaMemcpyDefault, s);
    }
    cudaStreamSynchronize(s);
    cudaStreamDestroy(s);
#else
    for (int i = 0; i < FLAGS_batch; ++i) {
        uint8_t *local_addr =
            (uint8_t *)(addr) + FLAGS_size * (i * FLAGS_threads + thread_id);
        memset(local_addr, seed, FLAGS_size);
    }
#endif
}

void checkData(int thread_id, void *addr, uint8_t seed) {
    for (int i = 0; i < FLAGS_batch; ++i) {
        uint8_t *local_addr =
            (uint8_t *)(addr) + FLAGS_size * (i * FLAGS_threads + thread_id);
#ifdef USE_CUDA
        memset(ref_buf.data(), seed, FLAGS_size);
        cudaStream_t s;
        cudaStreamCreate(&s);
        cudaMemcpyAsync(user_buf.data(), local_addr, FLAGS_size,
                        cudaMemcpyDefault, s);
        cudaStreamSynchronize(s);
        cudaStreamDestroy(s);
        if (memcmp(user_buf.data(), ref_buf.data(), FLAGS_size) != 0) {
            LOG(ERROR) << "Detect data integrity problem";
            exit(EXIT_FAILURE);
        }
#else
        if (memcmp(local_addr, ref_buf.data(), FLAGS_size) != 0) {
            LOG(ERROR) << "Detect data integrity problem";
            exit(EXIT_FAILURE);
        }
#endif
    }
}

Status initiatorWorker(TransferEngine *engine, SegmentID handle, int thread_id,
                       void *addr) {
    bindToSocket(thread_id % num_sockets);
    uint64_t remote_base = getStartAddress(engine, handle, thread_id);
    bool mixture = false;
    Request::OpCode opcode;
    if (FLAGS_check || FLAGS_workload == "mix")
        mixture = true;
    else if (FLAGS_workload == "read")
        opcode = Request::READ;
    else if (FLAGS_workload == "write")
        opcode = Request::WRITE;
    else {
        LOG(ERROR) << "Invalid args: workload only support read|write|mix";
        exit(EXIT_FAILURE);
    }
    size_t batch_count = 0;
    while (running) {
        if (!mixture) {
            CHECK_FAIL(submitRequestSync(engine, handle, thread_id, addr,
                                         remote_base, opcode));
            batch_count++;
        } else {
            uint8_t seed = 0;
            seed = SimpleRandom::Get().next(UINT8_MAX);
            if (FLAGS_check && SimpleRandom::Get().next(64) == 31) {
                fillData(thread_id, addr, seed);
                CHECK_FAIL(submitRequestSync(engine, handle, thread_id, addr,
                                             remote_base, Request::WRITE));
                fillData(thread_id, addr, 0);
                CHECK_FAIL(submitRequestSync(engine, handle, thread_id, addr,
                                             remote_base, Request::READ));
                checkData(thread_id, addr, seed);
            } else {
                CHECK_FAIL(submitRequestSync(engine, handle, thread_id, addr,
                                             remote_base, Request::WRITE));
                CHECK_FAIL(submitRequestSync(engine, handle, thread_id, addr,
                                             remote_base, Request::READ));
            }
            batch_count += 2;
        }
    }
    LOG(INFO) << "Worker " << thread_id << " stopped!";
    total_batch_count.fetch_add(batch_count);
    return Status::OK();
}

void allocateAllLocalMemory(const std::unique_ptr<TransferEngine> &engine,
                            std::vector<void *> &addr) {
    for (int i = 0; i < num_buffers; ++i) {
        MemoryOptions options;
        if (FLAGS_shmfs) options.type = SHM;
#ifdef USE_CUDA
        if (FLAGS_use_dram && i < num_sockets) {
            options.location = "cpu:" + std::to_string(i);
        } else if (FLAGS_use_vram) {
            int cuda_id = i;
            if (FLAGS_use_dram) cuda_id -= num_sockets;
            options.location = "cuda:" + std::to_string(cuda_id);
            if (FLAGS_mnnvl) options.type = MNNVL;
        }
#else
        options.location = "cpu:" + std::to_string(i);
#endif
        CHECK_FAIL(
            engine->allocateLocalMemory(&addr[i], buffer_capacity, options));
        CHECK_FAIL(
            engine->registerLocalMemory(addr[i], buffer_capacity, options));
    }
}

void deallocateAllLocalMemory(const std::unique_ptr<TransferEngine> &engine,
                              std::vector<void *> &addr) {
    for (int i = 0; i < num_buffers; ++i) {
        CHECK_FAIL(engine->unregisterLocalMemory(addr[i], buffer_capacity));
        CHECK_FAIL(engine->freeLocalMemory(addr[i]));
    }
}

std::shared_ptr<ConfigManager> loadConfig() {
    auto config = std::make_shared<ConfigManager>();
    config->set("local_segment_name", FLAGS_local_segment);
    config->set("metadata_type", FLAGS_metadata_type);
    config->set("metadata_servers", FLAGS_metadata_servers);
    config->set("cluster_topology", FLAGS_cluster_topology);
    config->set("transports/rdma/workers/block_size",
                std::to_string(FLAGS_slice_size));
    return config;
}

int initiator() {
    auto engine = std::make_unique<TransferEngine>(loadConfig());
    std::vector<void *> addr(num_buffers, nullptr);
    allocateAllLocalMemory(engine, addr);

    SegmentID handle;
    assert(!FLAGS_remote_segment.empty());
    CHECK_FAIL(engine->openSegment(handle, FLAGS_remote_segment));
    assert(handle);

    std::thread workers[FLAGS_threads];
    struct timeval start_tv, stop_tv;
    gettimeofday(&start_tv, nullptr);
    for (int i = 0; i < FLAGS_threads; ++i)
        workers[i] = std::thread(initiatorWorker, engine.get(), handle, i,
                                 addr[i % num_buffers]);

    sleep(FLAGS_duration);

    running = false;
    for (int i = 0; i < FLAGS_threads; ++i) workers[i].join();

    gettimeofday(&stop_tv, nullptr);
    auto duration = (stop_tv.tv_sec - start_tv.tv_sec) +
                    (stop_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    auto batch_count = total_batch_count.load();

    LOG(INFO) << "Test completed. duration " << std::fixed
              << std::setprecision(2) << duration << ", batch count "
              << batch_count << ", throughput "
              << calculateRate(batch_count * FLAGS_batch * FLAGS_size,
                               duration);

    deallocateAllLocalMemory(engine, addr);
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
    auto engine = std::make_unique<TransferEngine>(loadConfig());
    std::vector<void *> addr(num_buffers, nullptr);
    allocateAllLocalMemory(engine, addr);
    std::cout << "\033[33mTarget server has been started. "
                 "You can spawn another terminal and run: "
              << std::endl
              << std::endl
              << "  ./transfer_engine_bench_v1 --remote_segment="
              << engine->getSegmentName() << std::endl
              << std::endl
              << "Press Ctrl+C to terminate.\033[0m" << std::endl;
    while (target_running) sleep(1);
    deallocateAllLocalMemory(engine, addr);
    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    uint64_t min_capacity = FLAGS_size * FLAGS_batch * FLAGS_threads;
    buffer_capacity = std::max(buffer_capacity, min_capacity);
    num_sockets = numa_num_configured_nodes();

#ifdef USE_CUDA
    num_buffers = 0;
    cudaGetDeviceCount(&cuda_device_count);
    if (FLAGS_use_dram) num_buffers += num_sockets;
    if (FLAGS_use_vram) num_buffers += cuda_device_count;
#else
    num_buffers = num_sockets;
#endif
    if (FLAGS_remote_segment.empty())
        return target();
    else
        return initiator();
}
