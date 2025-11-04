// Copyright 2025 Alibaba Cloud and its affiliates
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

#include <signal.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include <cmath>
#include <memory>
#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "transfer_engine.h"

// Common arguments.
DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "192.168.3.77:2379", "etcd server host address");
DEFINE_string(
    mode, "loopback",
    "Running mode: initiator, target, or loopback. Initiator node read/write "
    "data blocks from target node");

// Initiator arguments.
DEFINE_string(operation, "read", "Operation type: read or write");
DEFINE_string(segment_id, "192.168.3.76", "Segment ID to access data");
DEFINE_int32(batch_size, 4096, "Batch size");
DEFINE_uint64(block_size, 65536, "Block size for each transfer request");
DEFINE_int32(duration, 30, "Test duration in seconds");
DEFINE_int32(threads, 1, "Task submission threads");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

// Target arguments.
DEFINE_string(trtype, "tcp", "TRTYPE of NVMeoF: tcp|rdma");
DEFINE_string(adrfam, "ipv4", "ADRFAM of NVMeoF: ipv4|ipv6");
DEFINE_string(traddr, "127.0.0.1",
              "TRADDR of NVMeoF, i.e. service listen address");
DEFINE_string(trsvcid, "4420", "TRSVCID of NVMeoF, i.e. service listen port");
DEFINE_string(files, "",
              "Files to register as buffers, separated by space, e.g.: "
              "\"/dev/nvme0n1 /dev/nvme1n1\"");

using namespace mooncake;

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

static std::unique_ptr<TransferEngine> initTransferEngine() {
    // Disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>();
    if (engine == nullptr) {
        LOG(ERROR) << "Failed to create transfer engine";
        exit(EXIT_FAILURE);
    }

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    int rc =
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(), hostname_port.second);
    if (rc != 0) {
        LOG(ERROR) << "Failed to init transfer engine, rc=" << rc;
        exit(EXIT_FAILURE);
    }

    const std::string trStr =
        "trtype=" + FLAGS_trtype + " adrfam=" + FLAGS_adrfam +
        " traddr=" + FLAGS_traddr + " trsvcid=" + FLAGS_trsvcid;
    LOG(INFO) << "Using Trid: " << trStr;

    Transport *xport = nullptr;
    void *args[2] = {(void *)trStr.c_str(), nullptr};
    xport = engine->installTransport("nvmeof_generic", args);
    if (xport == nullptr) {
        LOG(ERROR) << "Failed to install nvmeof_generic transport";
        exit(EXIT_FAILURE);
    }

    return engine;
}

static volatile bool initiator_running = true;
static std::atomic<size_t> total_batch_count(0);

static Status initiatorWorker(TransferEngine *engine, SegmentID segment_id,
                              int thread_id, void *addr) {
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

    auto &file_buffers = segment_desc->file_buffers;
    if (file_buffers.size() <= 0) {
        LOG(ERROR) << "No file buffer registered in segment, please check";
        exit(EXIT_FAILURE);
    }

    size_t batch_count = 0;
    while (initiator_running) {
        std::vector<TransferRequest> requests;
        for (int i = 0; i < FLAGS_batch_size; ++i) {
            auto buffer_offset =
                FLAGS_block_size * (i * FLAGS_threads + thread_id);
            // Randomly pick a file.
            auto file_index = std::rand() % file_buffers.size();
            // Randomly pick a file offset.
            auto file_unit_cnt = file_buffers[file_index].size /
                                 FLAGS_block_size / FLAGS_threads;
            auto target_offset =
                FLAGS_block_size *
                ((std::rand() % file_unit_cnt) * FLAGS_threads + thread_id);

            TransferRequest entry;
            entry.opcode = opcode;
            entry.length = FLAGS_block_size;
            entry.source = (void *)((uintptr_t)(addr) + buffer_offset);
            entry.target_id = segment_id;
            entry.file_id = file_buffers[file_index].id;
            entry.target_offset = target_offset;
            requests.emplace_back(entry);
        }

        auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
        Status s = engine->submitTransfer(batch_id, requests);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to submit request: " << s.ToString();
        }

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
    }

    LOG(INFO) << "Worker " << thread_id << " stopped!";
    total_batch_count.fetch_add(batch_count);
    return Status::OK();
}

static void startInitiator(TransferEngine *engine) {
    auto buffer_size = FLAGS_block_size * FLAGS_batch_size * FLAGS_threads;
    void *addr = std::aligned_alloc(4096, buffer_size);
    if (addr == nullptr) {
        LOG(ERROR) << "Failed to allocate buffer";
        exit(EXIT_FAILURE);
    }

    int rc = engine->registerLocalMemory(addr, buffer_size);
    if (rc != 0) {
        LOG(ERROR) << "Failed to register buffer, rc=" << rc;
        exit(EXIT_FAILURE);
    }

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());

    struct timeval start_tv;
    gettimeofday(&start_tv, nullptr);

    std::vector<std::thread> workers(FLAGS_threads);
    for (int i = 0; i < FLAGS_threads; ++i) {
        workers[i] = std::thread(initiatorWorker, engine, segment_id, i, addr);
    }

    sleep(FLAGS_duration);
    initiator_running = false;

    for (int i = 0; i < FLAGS_threads; ++i) {
        workers[i].join();
    }

    struct timeval stop_tv;
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

    engine->unregisterLocalMemory(addr);
    std::free(addr);
}

static volatile bool target_started = false;
static volatile bool target_running = true;

static size_t getFileSize(const std::string &file) {
    size_t size = 0;
    struct stat st;

    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return 0;
    }

    if (fstat(fd, &st) != 0) {
        close(fd);
        return 0;
    }

    if (S_ISLNK(st.st_mode)) {
        close(fd);
        return 0;
    }

    if (S_ISBLK(st.st_mode) || S_ISCHR(st.st_mode)) {
        ioctl(fd, BLKGETSIZE64, &size);
    } else if (S_ISREG(st.st_mode)) {
        size = st.st_size;
    }

    close(fd);
    return size;
}

static void startTarget(TransferEngine *engine) {
    std::vector<std::string> files;
    std::istringstream s(FLAGS_files);
    std::string file;
    while (s >> file) {
        if (file.size() <= 0) {
            LOG(ERROR) << "Invalid file path " << file;
            exit(EXIT_FAILURE);
        }

        auto size = getFileSize(file);
        if (size == 0) {
            LOG(ERROR) << "Invalid file " << file;
            exit(EXIT_FAILURE);
        }

        FileBufferID id;
        int rc = engine->registerLocalFile(file, size, id);
        if (rc != 0) {
            LOG(ERROR) << "Failed to register file " << file << ", rc=" << rc;
            exit(EXIT_FAILURE);
        }

        files.push_back(file);
    }

    if (files.size() <= 0) {
        LOG(ERROR) << "No valid file in \"" << FLAGS_files << "\"";
        exit(EXIT_FAILURE);
    }

    target_started = true;
    while (target_running) sleep(1);

    for (auto &file : files) {
        engine->unregisterLocalFile(file);
    }
}

static int initiator() {
    auto engine = initTransferEngine();
    startInitiator(engine.get());
    return 0;
}

static void signalHandler(int signum) {
    LOG(INFO) << "Received signal " << signum << ", stopping target server...";
    target_running = false;
}

static int target() {
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    auto engine = initTransferEngine();
    startTarget(engine.get());

    return 0;
}

static int loopback() {
    auto engine = initTransferEngine();

    // Start target thread.
    auto target_thread = std::thread(startTarget, engine.get());
    size_t wait_cnt = 0;
    while (!target_started && wait_cnt < 60) {
        sleep(1);
        wait_cnt++;
    }

    if (!target_started) {
        LOG(ERROR) << "Target initialization timedout";
        exit(EXIT_FAILURE);
    }

    // Start initiator thread.
    auto initiator_thread = std::thread(startInitiator, engine.get());

    // Wait initiator to complete.
    initiator_thread.join();

    // Terminate target.
    target_running = false;
    target_thread.join();

    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_mode == "initiator")
        return initiator();
    else if (FLAGS_mode == "target")
        return target();
    else if (FLAGS_mode == "loopback")
        return loopback();

    LOG(ERROR)
        << "Unsupported mode: must be 'initiator', 'target', or 'loopback'";
    exit(EXIT_FAILURE);
}
