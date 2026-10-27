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
DEFINE_uint64(block_size, 8388608, "Block size for each transfer request");
DEFINE_bool(auto_discovery, false, "Enable auto discovery");
DEFINE_uint64(device_id, 65536, "The device logic and phy ID of this machine");
DEFINE_uint64(device_logicid, 0, "The device logic ID of this machine");
DEFINE_uint64(device_phyid, 0, "The device phy ID of this machine");
DEFINE_string(segment_id_2, "NA",
              "Segment ID that a initiators to access another target data");
DEFINE_uint64(
    target_recv_count, 1,
    "Number of initiators connected to this target in 2-to-1 scenario");
DEFINE_uint64(initiator_id, 0,
              "Unique identifier for the initiator sending to the target in "
              "2-to-1 scenario");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

using namespace mooncake;

int g_deviceLogicId = 0;
int g_devicePhyId = 0;
uint64_t g_TotalSize = 0;

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
    aclrtContext context = nullptr;
    aclError ret = aclrtCreateContext(&context, g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to create context, ret: " << ret;
        return ret;
    }

    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    std::string FLAGS_local_server_name_npu =
        hostname_port.first + ":" + std::to_string(hostname_port.second) +
        ":npu_" + std::to_string(g_devicePhyId);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name_npu.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    void *devAddr = nullptr;
    ret = allocateDevMem(devAddr, FLAGS_block_size * FLAGS_batch_size);
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "devAddr_initiator: " << devAddr;

    ret = engine->registerLocalMemory(devAddr, g_TotalSize,
                                      "npu:" + std::to_string(g_devicePhyId));
    if (ret) {
        LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
        return ret;
    }

    void *devAddr2 = nullptr;
    ret = allocateDevMem(devAddr2, FLAGS_block_size * FLAGS_batch_size);
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "devAddr_initiator2: " << devAddr2;

    ret = engine->registerLocalMemory(devAddr2, g_TotalSize,
                                      "npu:" + std::to_string(g_devicePhyId));
    if (ret) {
        LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
        return ret;
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
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
    Status s;
    std::vector<TransferRequest> requests;
    for (int i = 0; i < FLAGS_batch_size; ++i) {
        TransferRequest entry;
        entry.opcode = opcode;
        entry.length = FLAGS_block_size;
        entry.source = (uint8_t *)(devAddr) + FLAGS_block_size * i;
        entry.target_id = segment_id;
        entry.target_offset = remote_base + FLAGS_block_size * i +
                              g_TotalSize * FLAGS_initiator_id;
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

    s = engine->freeBatchID(batch_id);
    LOG_ASSERT(s.ok());

    LOG(INFO) << "The First Time Send OK";

    struct timeval start_tv, stop_tv;
    gettimeofday(&start_tv, nullptr);
    uint64_t remote_base2 = (uint64_t)segment_desc->buffers[1].addr;

    auto batch_id_2 = engine->allocateBatchID(FLAGS_batch_size);
    std::vector<TransferRequest> requests2;
    for (int i = 0; i < FLAGS_batch_size; ++i) {
        TransferRequest entry;
        entry.opcode = opcode;
        entry.length = FLAGS_block_size;
        entry.source = (uint8_t *)(devAddr2) + FLAGS_block_size * i;
        entry.target_id = segment_id;
        entry.target_offset = remote_base2 + FLAGS_block_size * i +
                              g_TotalSize * FLAGS_initiator_id;
        requests2.emplace_back(entry);
    }
    completed = false;
    s = engine->submitTransfer(batch_id_2, requests2);
    LOG_ASSERT(s.ok());
    while (!completed) {
        Status s = engine->getBatchTransferStatus(batch_id_2, status);
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

    s = engine->freeBatchID(batch_id_2);
    LOG_ASSERT(s.ok());

    LOG(INFO) << "The Second Time Send OK";

    gettimeofday(&stop_tv, nullptr);
    uint64_t duration = (stop_tv.tv_sec - start_tv.tv_sec) * 1000000.0 +
                        (stop_tv.tv_usec - start_tv.tv_usec);

    LOG(INFO) << "Test completed: duration " << duration << "us, batch count "
              << FLAGS_batch_size * FLAGS_block_size << ", throughput "
              << calculateRate(FLAGS_batch_size * FLAGS_block_size, duration);

    // When testing 1-to-2 transmission (1 initiator to 2 targets), fill in the
    // segment_id of the second receiver. If not filled, it defaults to "NA" and
    // 1-to-2 transmission is not enabled, only 1-to-1 transmission is
    // performed.
    if (FLAGS_segment_id_2 != "NA") {
        sleep(10);
        auto segment_id_2 = engine->openSegment(FLAGS_segment_id_2.c_str());

        TransferRequest::OpCode opcode;
        if (FLAGS_operation == "read")
            opcode = TransferRequest::READ;
        else if (FLAGS_operation == "write")
            opcode = TransferRequest::WRITE;
        else {
            LOG(ERROR) << "Unsupported operation: must be 'read' or 'write'";
            return -1;
        }

        auto segment_desc_2 =
            engine->getMetadata()->getSegmentDescByID(segment_id_2);
        if (!segment_desc_2) {
            LOG(ERROR) << "Unable to get target segment ID, please recheck";
            return -1;
        }
        uint64_t remote_base_desc_2 = (uint64_t)segment_desc_2->buffers[0].addr;

        auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
        std::vector<TransferRequest> requests;
        for (int i = 0; i < FLAGS_batch_size; ++i) {
            TransferRequest entry;
            entry.opcode = opcode;
            entry.length = FLAGS_block_size;
            entry.source = (uint8_t *)(devAddr) + FLAGS_block_size * i;
            entry.target_id = segment_id_2;
            entry.target_offset = remote_base_desc_2 + FLAGS_block_size * i +
                                  g_TotalSize * FLAGS_initiator_id;
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

        LOG(INFO) << "Send OK 2rd device";
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    // release resource
    aclrtFree(devAddr);
    aclrtFree(devAddr2);

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
    std::string FLAGS_local_server_name_npu =
        hostname_port.first + ":" + std::to_string(hostname_port.second) +
        ":npu_" + std::to_string(g_devicePhyId);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name_npu.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    void *devAddr = nullptr;
    ret = allocateDevMem(devAddr, FLAGS_block_size * FLAGS_batch_size);
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "devAddr_target: " << devAddr;

    ret = engine->registerLocalMemory(devAddr,
                                      g_TotalSize * FLAGS_target_recv_count,
                                      "npu:" + std::to_string(g_devicePhyId));
    if (ret) {
        LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
        return ret;
    }

    void *devAddr2 = nullptr;
    ret = allocateDevMem(devAddr2, FLAGS_block_size * FLAGS_batch_size);
    if (ret) {
        LOG(ERROR) << "Failed to allocateDevMem, ret: " << ret;
        return ret;
    }

    LOG(INFO) << "devAddr_target_2: " << devAddr2;

    ret = engine->registerLocalMemory(devAddr2,
                                      g_TotalSize * FLAGS_target_recv_count,
                                      "npu:" + std::to_string(g_devicePhyId));
    if (ret) {
        LOG(ERROR) << "Failed to registerLocalMemory, ret: " << ret;
        return ret;
    }

    while (target_running) sleep(1);

    // release resource
    aclrtFree(devAddr);
    aclrtFree(devAddr2);

    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    g_TotalSize = (uint64_t)(FLAGS_batch_size * FLAGS_block_size);

    if (FLAGS_device_id != 65536) {
        g_deviceLogicId = FLAGS_device_id;
        g_devicePhyId = FLAGS_device_id;
    } else {
        g_deviceLogicId = FLAGS_device_logicid;
        g_devicePhyId = FLAGS_device_phyid;
    }

    const char *aclConfigPath = nullptr;
    aclError ret = aclInit(aclConfigPath);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to initialize ACL, ret: " << ret;
        return -1;
    }

    ret = aclrtSetDevice(g_deviceLogicId);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to set device, ret: " << ret;
        return -1;
    }

    if (FLAGS_mode == "initiator") {
        return initiator();
    } else if (FLAGS_mode == "target") {
        return target();
    }

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    exit(EXIT_FAILURE);
}