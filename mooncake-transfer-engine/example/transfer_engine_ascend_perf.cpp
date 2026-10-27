// Copyright 2025 Huawei Technologies Co., Ltd
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
#include <signal.h>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <unordered_map>
#include "common/base/status.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "acl/acl.h"
#include "hccl.h"

DEFINE_string(local_server_name, "10.20.130.154:12345",
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "P2PHANDSHAKE", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "write", "Operation type: read or write");
DEFINE_string(protocol, "hccl", "Transfer protocol: rdma|tcp|hccl");
DEFINE_string(segment_id, "10.20.130.154:12346", "Segment ID to access data");
DEFINE_int32(batch_size, 32, "Batch size");
DEFINE_uint64(block_size, 32768, "Block size for each transfer request");
DEFINE_uint64(block_iteration, 10, "number of iterations of the block");
DEFINE_bool(auto_discovery, false, "Enable auto discovery");
DEFINE_uint64(device_id, 65536, "The device logic and phy ID of this machine");
DEFINE_uint64(device_logicid, 0, "The device logic ID of this machine");
DEFINE_uint64(device_phyid, 0, "The device phy ID of this machine");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

using namespace mooncake;

int g_deviceLogicId = 0;
int g_devicePhyId = 0;
#define HOST_BUFFER_SIZE 0x1000

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

static inline std::string calculateRate(uint64_t data_bytes,
                                        uint64_t duration) {
    if (!RATE_UNIT_MP.count(FLAGS_report_unit)) {
        LOG(WARNING) << "Invalid flag: report_unit only support "
                        "GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb, not support "
                     << FLAGS_report_unit
                     << " . Now use GB(default) as report_unit";
        FLAGS_report_unit = "GB";
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(FLAGS_report_precision)
        << 1.0 * data_bytes * 1000000 / duration /
               RATE_UNIT_MP.at(FLAGS_report_unit)
        << " " << FLAGS_report_unit << "/s";
    return oss.str();
}

int allocateDevMem(void *&devAddr, size_t size) {
    // malloc device mem
    aclError ret = aclrtMalloc(&devAddr, size, ACL_MEM_MALLOC_NORMAL_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    // malloc host mem
    void *host_addr = nullptr;
    ret = aclrtMallocHost(&host_addr, size);
    if (ret != ACL_ERROR_NONE || host_addr == nullptr) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    for (size_t i = 0; i < size; i += sizeof(uint32_t)) {
        *(uint32_t *)((char *)host_addr + i) = 0x12345678;
    }

    // copy data from host mem to device mem
    ret =
        aclrtMemcpy(devAddr, size, host_addr, size, ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to copy data from host to device, ret: " << ret;
        aclrtFreeHost(host_addr);
        aclrtFree(devAddr);
        return ret;
    }

    // release resource
    ret = aclrtFreeHost(host_addr);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to aclrtFreeHost, ret: " << ret;
        return ret;
    }

    return 0;
}

int initiator() {
    aclrtContext context = NULL;
    aclError ret = aclrtCreateContext(&context, g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to create context, ret: " << ret;
        return ret;
    }

    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    std::string FLAGS_local_server_name_new =
        hostname_port.first + ":" + std::to_string(hostname_port.second) +
        ":npu_" + std::to_string(g_devicePhyId);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name_new.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    // Warm-up transmission
    void *tmp_devAddr = NULL;
    ret = allocateDevMem(tmp_devAddr, FLAGS_block_size);
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "tmp_devAddr_target: " << tmp_devAddr
              << ", len: " << FLAGS_block_size;
    ret = engine->registerLocalMemory(tmp_devAddr, FLAGS_block_size,
                                      "npu:" + std::to_string(g_devicePhyId));

    void *devAddr = NULL;
    std::vector<void *> g_addr;
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        uint64_t block_size = FLAGS_block_size * (1 << i);
        ret = allocateDevMem(devAddr, FLAGS_batch_size * block_size * 2);
        if (ret) {
            LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
            return -1;
        }
        LOG(INFO) << "dev_addr_initiator: " << devAddr
                  << " len:" << FLAGS_batch_size * block_size * 2;
        ret = engine->registerLocalMemory(
            devAddr, FLAGS_batch_size * block_size * 2,
            "npu:" + std::to_string(g_devicePhyId));
        if (ret) {
            LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
            return ret;
        }

        g_addr.push_back(devAddr);
    }

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());

    TransferRequest::OpCode opcode;
    if (FLAGS_operation == "read")
        opcode = TransferRequest::READ;
    else if (FLAGS_operation == "write")
        opcode = TransferRequest::WRITE;
    else {
        LOG(ERROR) << "Unsupported operation: must be 'read' or 'write'";
        return -1;
    }

    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc) {
        LOG(ERROR) << "Unable to get target segment ID, please recheck";
        return -1;
    }

    Status s;
    uint64_t remote_base = 0;
    remote_base = (uint64_t)segment_desc->buffers[0].addr;
    auto tmp_batch_id = engine->allocateBatchID(1);
    std::vector<TransferRequest> tmp_requests;
    TransferRequest entry;
    entry.opcode = opcode;
    entry.length = FLAGS_block_size;
    entry.source = (uint8_t *)tmp_devAddr;
    entry.target_id = segment_id;
    entry.target_offset = remote_base;
    tmp_requests.emplace_back(entry);

    s = engine->submitTransfer(tmp_batch_id, tmp_requests);
    LOG_ASSERT(s.ok());

    bool completed = false;
    TransferStatus status;
    while (!completed) {
        Status s = engine->getBatchTransferStatus(tmp_batch_id, status);
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            completed = true;
        } else if (status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "getTransferStatus FAILED";
            completed = true;
        } else if (status.s == TransferStatusEnum::TIMEOUT) {
            LOG(INFO) << "Sync data transfer timeout";
            completed = true;
        }
    }
    s = engine->freeBatchID(tmp_batch_id);
    LOG_ASSERT(s.ok());

    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        uint64_t block_size = FLAGS_block_size * (1 << i);
        struct timeval start_tv, stop_tv;
        gettimeofday(&start_tv, nullptr);
        remote_base = (uint64_t)segment_desc->buffers[i + 1].addr;
        auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
        std::vector<TransferRequest> requests;
        // Send every other block to ensure that all the sent memory is
        // non-contiguous
        for (int j = 0; j < FLAGS_batch_size; ++j) {
            TransferRequest entry;
            entry.opcode = opcode;
            entry.length = block_size;
            entry.source = (uint8_t *)(g_addr[i]) + block_size * 2 * j;
            entry.target_id = segment_id;
            entry.target_offset = remote_base + block_size * 2 * j;
            requests.emplace_back(entry);
        }
        s = engine->submitTransfer(batch_id, requests);
        LOG_ASSERT(s.ok());
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = engine->getBatchTransferStatus(batch_id, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                completed = true;
            } else if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "getTransferStatus FAILED";
                completed = true;
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
                completed = true;
            }
        }
        gettimeofday(&stop_tv, nullptr);
        uint64_t duration = (stop_tv.tv_sec - start_tv.tv_sec) * 1000000.0 +
                            (stop_tv.tv_usec - start_tv.tv_usec);

        LOG(INFO) << "Test completed: duration " << duration
                  << "us, block size " << block_size / 1024 << "KB, total size "
                  << FLAGS_batch_size * block_size / 1024 << "KB , throughput "
                  << calculateRate(FLAGS_batch_size * block_size, duration);
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    // release resource
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        aclrtFree(g_addr[i]);
    }
    return 0;
}

volatile bool target_running = true;

int target() {
    aclrtContext context = nullptr;
    aclError ret = aclrtCreateContext(&context, g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to create context, ret: " << ret;
        return -1;
    }

    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    std::string FLAGS_local_server_name_new =
        hostname_port.first + ":" + std::to_string(hostname_port.second) +
        ":npu_" + std::to_string(g_devicePhyId);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name_new.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    // Warm-up transmission
    void *tmp_devAddr = NULL;
    ret = allocateDevMem(tmp_devAddr, FLAGS_block_size);
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "tmp_devAddr_target: " << tmp_devAddr
              << ", len: " << FLAGS_block_size;
    ret = engine->registerLocalMemory(tmp_devAddr, FLAGS_block_size,
                                      "npu:" + std::to_string(g_devicePhyId));

    void *devAddr = NULL;
    std::vector<void *> g_addr;
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        uint64_t block_size = FLAGS_block_size * (1 << i);
        ret = allocateDevMem(devAddr, FLAGS_batch_size * block_size * 2);
        if (ret) {
            LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
            return ret;
        }

        LOG(INFO) << "devAddr_target: " << devAddr
                  << ", len: " << FLAGS_batch_size * block_size * 2;
        ret = engine->registerLocalMemory(
            devAddr, FLAGS_batch_size * block_size * 2,
            "npu:" + std::to_string(g_devicePhyId));
        if (ret) {
            LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
            return ret;
        }

        g_addr.push_back(devAddr);
    }

    while (target_running) sleep(1);

    // release resource
    aclrtFree(tmp_devAddr);
    for (uint32_t i = 0; i < FLAGS_block_iteration; i++) {
        aclrtFree(g_addr[i]);
    }

    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_device_id != 65536) {
        g_deviceLogicId = FLAGS_device_id;
        g_devicePhyId = FLAGS_device_id;
    } else {
        g_deviceLogicId = FLAGS_device_logicid;
        g_devicePhyId = FLAGS_device_phyid;
    }
    const char *aclConfigPath = NULL;
    aclError ret = aclInit(aclConfigPath);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to initialize ACL";
        return ret;
    }

    ret = aclrtSetDevice(g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to set device ACL";
        return ret;
    }

    if (FLAGS_mode == "initiator") {
        return initiator();
    } else if (FLAGS_mode == "target") {
        return target();
    }

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    exit(EXIT_FAILURE);
}