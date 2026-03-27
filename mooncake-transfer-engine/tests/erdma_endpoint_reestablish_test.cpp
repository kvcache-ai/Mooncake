// Copyright 2026 KVCache.AI
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

/*
 * eRDMA Endpoint Re-establishment Test
 *
 * Background:
 * When using eRDMA with TE, re-establishing a connection requires a
 * full destruction and recreation of QPs rather than a simple reset.
 * This logic is implemented in `disconnectForReestablish`
 * (see rdma_endpoint.cpp).
 *
 * Purpose:
 * This test verifies that TE correctly handles endpoint reconstruction
 * during simulated Initiator restarts.
 *
 * How to run:
 * 1. Start etcd:
 *    etcd --listen-client-urls http://127.0.0.1:18222 \
 *      --advertise-client-urls http://127.0.0.1:18222
 *
 * 2. Run Target first (requires root):
 *    ./erdma_endpoint_reestablish_test --mode=target \
 *      --metadata_server=127.0.0.1:18222 \
 *      --local_server_name=127.0.0.1:12345 --device_name=erdma_0
 *
 * 3. Run Initiator (requires root):
 *    ./erdma_endpoint_reestablish_test --metadata_server=127.0.0.1:18222 \
 *      --segment_id=127.0.0.1:12345 --local_server_name=127.0.0.1:12346 \
 *      --device_name=erdma_1
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <numa.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <vector>
#include <sstream>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");

DEFINE_string(metadata_server, "127.0.0.1:18222", "etcd server host address");

DEFINE_string(mode, "initiator", "Running mode: initiator or target.");

DEFINE_string(nic_priority_matrix, "",
              "Path to RDMA NIC priority matrix file (Advanced)");

DEFINE_string(device_name, "erdma_0", "Device name to use for RDMA");

DEFINE_string(segment_id, "127.0.0.1:12345", "Segment ID to access data");

using namespace mooncake;

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
    // Pure CPU JSON Data
    auto device_names = formatDeviceNames(FLAGS_device_name);
    return "{\"cpu:0\": [[" + device_names +
           "], []], "
           " \"cpu:1\": [[" +
           device_names + "],[]]}";
}

void wait_for_transfer(TransferEngine *engine, BatchID batch_id,
                       const std::string &op_name) {
    bool completed = false;
    TransferStatus status;
    while (!completed) {
        Status s = engine->getTransferStatus(batch_id, 0, status);
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            completed = true;
        } else if (status.s == TransferStatusEnum::FAILED) {
            LOG(FATAL) << op_name << " FAILED";
        }
    }
    Status s = engine->freeBatchID(batch_id);
    LOG_ASSERT(s.ok());
}

// Size of the pre-registered memory.
constexpr size_t kRAMBufSize = 256ull << 20;  // 256 MB

// Actual data payload size for RDMA Read/Write.
constexpr size_t kDataLength = 16ull << 20;  // 16MB

int initiator() {
    LOG(INFO) << "========== Phase 1: Start, Connect & Write ==========";
    {
        auto engine = std::make_unique<TransferEngine>(false);
        auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name,
                     hostname_port.first, hostname_port.second);

        // Install RDMA transport with NIC settings
        auto nic_priority_matrix = loadNicPriorityMatrix();
        void *args[2] = {const_cast<char *>(nic_priority_matrix.c_str()),
                         nullptr};
        Transport *xport = engine->installTransport("rdma", args);
        LOG_ASSERT(xport);

        // Register local memory
        void *addr = numa_alloc_onnode(kRAMBufSize, 0);
        auto addr_u8 = static_cast<uint8_t *>(addr);
        int rc = engine->registerLocalMemory(addr, kRAMBufSize, "cpu:0");
        LOG_ASSERT(!rc);

        // Open target segment and get remote address
        auto segment_id = engine->openSegment(FLAGS_segment_id);
        auto segment_desc =
            engine->getMetadata()->getSegmentDescByID(segment_id);
        auto remote_base = segment_desc->buffers[0].addr;

        // Fill buffer with test pattern
        for (size_t i = 0; i < kDataLength; ++i) {
            addr_u8[i] = static_cast<uint8_t>(i % 256);
        }

        LOG(INFO) << "Writing " << kDataLength << " bytes to Target...";
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = addr_u8;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;

        // Submit WRITE request
        Status s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        wait_for_transfer(engine.get(), batch_id, "WRITE");
        LOG(INFO) << "Phase 1: Write Completed. Tearing down connection...";

        // Resource cleanup for Phase 1
        engine->unregisterLocalMemory(addr);
        numa_free(addr, kRAMBufSize);
    }

    LOG(INFO) << "Simulating Initiator Crash/Restart... Waiting 2 seconds.";
    sleep(2);

    LOG(INFO) << "========== Phase 2: Restart, Re-establish Endpoint & Read "
                 "==========";
    {
        // Brand new TransferEngine
        auto engine = std::make_unique<TransferEngine>(false);
        auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name,
                     hostname_port.first, hostname_port.second);

        auto nic_priority_matrix = loadNicPriorityMatrix();
        void *args[2] = {const_cast<char *>(nic_priority_matrix.c_str()),
                         nullptr};
        Transport *xport = engine->installTransport("rdma", args);
        LOG_ASSERT(xport);

        void *addr = numa_alloc_onnode(kRAMBufSize, 0);
        auto addr_u8 = static_cast<uint8_t *>(addr);
        memset(addr, 0, kDataLength);

        int rc = engine->registerLocalMemory(addr, kRAMBufSize, "cpu:0");
        LOG_ASSERT(!rc);

        LOG(INFO) << "Re-opening segment to reconstruct RDMA Endpoint...";
        auto segment_id = engine->openSegment(FLAGS_segment_id);
        auto segment_desc =
            engine->getMetadata()->getSegmentDescByID(segment_id);
        auto remote_base = segment_desc->buffers[0].addr;

        LOG(INFO) << "Reading data back over new Endpoint...";
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = addr_u8;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;

        Status s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        wait_for_transfer(engine.get(), batch_id, "READ");

        bool ok = true;
        for (size_t i = 0; i < kDataLength; ++i) {
            if (addr_u8[i] != static_cast<uint8_t>(i % 256)) {
                ok = false;
                LOG(ERROR) << "Data mismatch at offset " << i << ", expected "
                           << (i % 256) << ", got " << addr_u8[i];
                break;
            }
        }

        if (ok) {
            LOG(INFO) << ">>> ENDPOINT RECONSTRUCTION VERIFICATION: "
                         "\033[32mSUCCESS\033[0m <<<";
        } else {
            LOG(ERROR)
                << ">>> ENDPOINT RECONSTRUCTION VERIFICATION: FAILED <<<";
        }

        engine->unregisterLocalMemory(addr);
        numa_free(addr, kRAMBufSize);
    }

    return 0;
}

int target() {
    auto engine = std::make_unique<TransferEngine>(false);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name,
                 hostname_port.first, hostname_port.second);

    auto nic_priority_matrix = loadNicPriorityMatrix();
    void *args[2] = {const_cast<char *>(nic_priority_matrix.c_str()), nullptr};
    Transport *xport = engine->installTransport("rdma", args);
    LOG_ASSERT(xport);

    void *addr = numa_alloc_onnode(kRAMBufSize, 0);
    memset(addr, 0, kRAMBufSize);

    int rc = engine->registerLocalMemory(addr, kRAMBufSize, "cpu:0");
    LOG_ASSERT(!rc);

    LOG(INFO) << "Target is up. Waiting for RDMA connections and operations...";

    // Never return
    while (true) sleep(1);

    engine->unregisterLocalMemory(addr);
    numa_free(addr, kRAMBufSize);
    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_mode == "initiator")
        return initiator();
    else if (FLAGS_mode == "target")
        return target();

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    return -1;
}