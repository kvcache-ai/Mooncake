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

#include "v1/common.h"
#include "v1/transfer_engine.h"
#include "v1/transport/transport.h"

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

DEFINE_string(metadata_type, "p2p", "Metadata type: p2p|etcd|redis|http");
DEFINE_string(metadata_servers, "",
              "Metadata servers (required unless type is `p2p')");
DEFINE_string(mode, "initiator", "Running mode: initiator|target");
DEFINE_string(workload, "read", "Test workload: read|write");
DEFINE_string(local_segment, "",
              "Set the custom local segment name (optional)");
DEFINE_string(remote_segment, "", "Set the remote segment name (required)");

DEFINE_int32(batch, 128, "Number of requests per batch");
DEFINE_uint64(size, 65536, "Block size for each request");
DEFINE_int32(duration, 10, "Test duration in seconds");
DEFINE_int32(threads, 4, "Test threads to submit requests");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

#ifdef USE_CUDA
DEFINE_bool(use_vram, true, "Allocate memory from GPU VRAM");
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

int num_buffers = 1;
int cuda_device_count = 0;
size_t buffer_capacity = 32 * 1024 * 1024;
volatile bool running = true;
std::atomic<size_t> total_batch_count(0);

uint64_t getStartAddress(TransferEngine *engine, SegmentID handle,
                         int thread_id) {
    auto segment_desc = engine->getSegmentDesc(handle);
    if (!segment_desc || segment_desc->type != SegmentType::Memory) {
        LOG(ERROR) << "Invalid args: cannot find memory segment "
                   << FLAGS_remote_segment << ", please recheck";
        exit(EXIT_FAILURE);
    }
    auto &detail = std::get<MemorySegmentDesc>(segment_desc->detail);
    if (detail.buffers.empty()) {
        LOG(ERROR) << "Invalid args: remote segment " << FLAGS_remote_segment
                   << " no registered memory, please recheck";
        exit(EXIT_FAILURE);
    }
    return (uint64_t)detail.buffers[thread_id % detail.buffers.size()].addr;
}

Status initiatorWorker(TransferEngine *engine, SegmentID handle, int thread_id,
                       void *addr) {
    bindToSocket(thread_id % num_buffers);
    uint64_t remote_base = getStartAddress(engine, handle, thread_id);

    Request::OpCode opcode;
    if (FLAGS_workload == "read")
        opcode = Request::READ;
    else if (FLAGS_workload == "write")
        opcode = Request::WRITE;
    else {
        LOG(ERROR) << "Invalid args: workload only support read|write";
        exit(EXIT_FAILURE);
    }

    size_t batch_count = 0;
    while (running) {
        auto batch_id = engine->allocateBatch(FLAGS_batch);
        std::vector<Request> requests;
        for (int i = 0; i < FLAGS_batch; ++i) {
            Request entry;
            entry.opcode = opcode;
            entry.length = FLAGS_size;
            entry.source = (uint8_t *)(addr) +
                           FLAGS_size * (i * FLAGS_threads + thread_id);
            entry.target_id = handle;
            entry.target_offset =
                remote_base + FLAGS_size * (i * FLAGS_threads + thread_id);
            requests.emplace_back(entry);
        }
        CHECK_FAIL(engine->submitTransfer(batch_id, requests));
        for (int task_id = 0; task_id < FLAGS_batch; ++task_id) {
            bool completed = false;
            TransferStatus status;
            while (!completed) {
                CHECK_FAIL(
                    engine->getTransferStatus(batch_id, task_id, status));
                if (status.s == TransferStatusEnum::COMPLETED)
                    completed = true;
                else if (status.s == TransferStatusEnum::FAILED) {
                    LOG(ERROR) << "Failed transfer detected";
                    exit(EXIT_FAILURE);
                }
            }
        }
        CHECK_FAIL(engine->freeBatch(batch_id));
        batch_count++;
    }
    LOG(INFO) << "Worker " << thread_id << " stopped!";
    total_batch_count.fetch_add(batch_count);
    return Status::OK();
}

void allocateAllLocalMemory(const std::unique_ptr<TransferEngine> &engine,
                            std::vector<void *> &addr) {
    for (int i = 0; i < num_buffers; ++i) {
        MemoryOptions options;
#ifdef USE_CUDA
        if (i < cuda_device_count) {
            options.location = "cuda:" + std::to_string(i);
        } else {
            options.location = "cpu:" + std::to_string(i - cuda_device_count);
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
        CHECK_FAIL(engine->freeLocalMemory(addr[i], buffer_capacity));
    }
}

std::shared_ptr<ConfigManager> loadConfig() {
    auto config = std::make_shared<ConfigManager>();
    std::string context;
    context = "{ \"local_segment_name\": \"" + FLAGS_local_segment +
              "\",\n\"metadata_type\": \"" + FLAGS_metadata_type +
              "\",\"metadata_servers\": \"" + FLAGS_metadata_servers + "\"}";
    CHECK_FAIL(config->loadConfigContent(context));
    return config;
}

int initiator() {
    auto engine = std::make_unique<TransferEngine>(loadConfig());
    std::vector<void *> addr(num_buffers, nullptr);
    allocateAllLocalMemory(engine, addr);

    SegmentID handle;
    if (FLAGS_remote_segment.empty()) {
        LOG(ERROR) << "Invalid remote segment name from --remote_segment";
        exit(EXIT_FAILURE);
    }

    CHECK_FAIL(engine->openRemoteSegment(handle, FLAGS_remote_segment.c_str()));

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
    while (target_running) sleep(1);
    deallocateAllLocalMemory(engine, addr);
    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    uint64_t min_capacity = FLAGS_size * FLAGS_batch * FLAGS_threads;
    buffer_capacity = std::max(buffer_capacity, min_capacity);
    num_buffers = numa_num_configured_nodes();
#ifdef USE_CUDA
    cudaGetDeviceCount(&cuda_device_count);
    if (FLAGS_use_vram) num_buffers += cuda_device_count;
#endif

    if (FLAGS_mode == "initiator")
        return initiator();
    else if (FLAGS_mode == "target")
        return target();

    LOG(ERROR) << "Invalid args: mode should be initiator|target";
    exit(EXIT_FAILURE);
}
