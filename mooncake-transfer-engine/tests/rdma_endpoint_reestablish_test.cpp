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
 * RDMA Endpoint Re-establishment Test
 *
 * Purpose:
 * This test verifies that TE correctly handles endpoint re-establish during
 * simulated initiator restarts, and that classic RDMA can recover from a first
 * RTR/EINVAL by reprobeing the next auto-selected local GID.
 *
 * How to run:
 *   sudo env MC_METADATA_SERVER=P2PHANDSHAKE \
 *     MC_TARGET_SERVER_NAME=127.0.0.1:12345 \
 *     MC_INITIATOR_SERVER_NAME=127.0.0.1:12346 \
 *     MC_TARGET_DEVICE_NAME=erdma_0 MC_INITIATOR_DEVICE_NAME=erdma_1 \
 *     ./build/mooncake-transfer-engine/tests/rdma_endpoint_reestablish_test
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <numa.h>
#include <sys/time.h>
#include <unistd.h>

#include "common.h"
#include "transfer_engine.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

class RdmaTransportTestPeer {
   public:
    static bool setContextActive(RdmaTransport* transport,
                                 const std::string& device_name, bool active) {
        if (!transport) return false;
        for (auto& context : transport->context_list_) {
            if (context && context->deviceName() == device_name) {
                context->set_active(active);
                return true;
            }
        }
        return false;
    }

    static bool contextActive(RdmaTransport* transport,
                              const std::string& device_name) {
        if (!transport) return false;
        for (auto& context : transport->context_list_) {
            if (context && context->deviceName() == device_name) {
                return context->active();
            }
        }
        return false;
    }

    static bool injectContextEvent(RdmaTransport* transport,
                                   const std::string& device_name,
                                   ibv_event_type event_type);
};

class WorkerPoolTestPeer {
   public:
    static void processContextEvent(WorkerPool& worker_pool,
                                    ibv_event_type event_type) {
        worker_pool.processContextEventForTest(event_type);
    }
};

class RdmaContextTestPeer {
   public:
    static bool injectContextEvent(RdmaContext* context,
                                   ibv_event_type event_type) {
        if (context && context->worker_pool_) {
            WorkerPoolTestPeer::processContextEvent(*context->worker_pool_,
                                                    event_type);
            return true;
        }
        return false;
    }
};

bool RdmaTransportTestPeer::injectContextEvent(RdmaTransport* transport,
                                               const std::string& device_name,
                                               ibv_event_type event_type) {
    if (!transport) return false;
    for (auto& context : transport->context_list_) {
        if (context && context->deviceName() == device_name) {
            return RdmaContextTestPeer::injectContextEvent(context.get(),
                                                           event_type);
        }
    }
    return false;
}

}  // namespace mooncake

namespace {

constexpr size_t kRAMBufSize = 256ull << 24;
constexpr size_t kDataLength = 16ull << 24;

bool usesP2PHandshake(const std::string& metadata_server) {
    return metadata_server == P2PHANDSHAKE;
}

std::vector<std::string> getAvailableRdmaDevices() {
    int num_devices = 0;
    ibv_device** device_list = ibv_get_device_list(&num_devices);
    std::vector<std::string> devices;
    if (device_list == nullptr) {
        return devices;
    }
    devices.reserve(num_devices);
    for (int i = 0; i < num_devices; ++i) {
        devices.emplace_back(ibv_get_device_name(device_list[i]));
    }
    ibv_free_device_list(device_list);
    return devices;
}

struct RtrFaultInjectionState {
    std::mutex mu;
    bool synthetic_gid_swap_enabled = false;
    std::string synthetic_gid_device;
    int synthetic_gid_a = 0;
    int synthetic_gid_b = 1;
    bool fail_first_rtr_einval = false;
    std::string fail_rtr_device;
    bool first_rtr_attempted = false;
    int injected_failures = 0;
    std::unordered_map<std::string, std::vector<int>> rtr_sgid_history;
    std::unordered_map<std::string, std::vector<std::string>> rtr_gid_history;
} g_rtr_fault_injection_state;

std::string formatGidBytes(const uint8_t* raw) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < 16; ++i) {
        if (i != 0) {
            oss << ":";
        }
        oss << std::setw(2) << static_cast<int>(raw[i]);
    }
    return oss.str();
}

void resetRtrFaultInjectionState() {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    g_rtr_fault_injection_state.synthetic_gid_swap_enabled = false;
    g_rtr_fault_injection_state.synthetic_gid_device.clear();
    g_rtr_fault_injection_state.synthetic_gid_a = 0;
    g_rtr_fault_injection_state.synthetic_gid_b = 1;
    g_rtr_fault_injection_state.fail_first_rtr_einval = false;
    g_rtr_fault_injection_state.fail_rtr_device.clear();
    g_rtr_fault_injection_state.first_rtr_attempted = false;
    g_rtr_fault_injection_state.injected_failures = 0;
    g_rtr_fault_injection_state.rtr_sgid_history.clear();
    g_rtr_fault_injection_state.rtr_gid_history.clear();
}

void configureRtrFaultInjection(const std::string& device_name) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    g_rtr_fault_injection_state.synthetic_gid_swap_enabled = true;
    g_rtr_fault_injection_state.synthetic_gid_device = device_name;
    g_rtr_fault_injection_state.synthetic_gid_a = 0;
    g_rtr_fault_injection_state.synthetic_gid_b = 1;
    g_rtr_fault_injection_state.fail_first_rtr_einval = true;
    g_rtr_fault_injection_state.fail_rtr_device = device_name;
    g_rtr_fault_injection_state.first_rtr_attempted = false;
    g_rtr_fault_injection_state.injected_failures = 0;
    g_rtr_fault_injection_state.rtr_sgid_history.clear();
    g_rtr_fault_injection_state.rtr_gid_history.clear();
}

std::vector<int> getRtrSgidHistory(const std::string& device_name) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    auto iter = g_rtr_fault_injection_state.rtr_sgid_history.find(device_name);
    if (iter == g_rtr_fault_injection_state.rtr_sgid_history.end()) {
        return {};
    }
    return iter->second;
}

int getInjectedFailureCount() {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    return g_rtr_fault_injection_state.injected_failures;
}

std::vector<std::string> getRtrGidHistory(const std::string& device_name) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    auto iter = g_rtr_fault_injection_state.rtr_gid_history.find(device_name);
    if (iter == g_rtr_fault_injection_state.rtr_gid_history.end()) {
        return {};
    }
    return iter->second;
}

void recordRtrAttempt(const std::string& device_name, int sgid_index,
                      const std::string& gid) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    g_rtr_fault_injection_state.rtr_sgid_history[device_name].push_back(
        sgid_index);
    g_rtr_fault_injection_state.rtr_gid_history[device_name].push_back(gid);
}

int maybeSwapSyntheticGidIndex(const std::string& device_name, int gid_index) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    if (!g_rtr_fault_injection_state.synthetic_gid_swap_enabled ||
        g_rtr_fault_injection_state.synthetic_gid_device != device_name) {
        return gid_index;
    }
    if (gid_index == g_rtr_fault_injection_state.synthetic_gid_a)
        return g_rtr_fault_injection_state.synthetic_gid_b;
    if (gid_index == g_rtr_fault_injection_state.synthetic_gid_b)
        return g_rtr_fault_injection_state.synthetic_gid_a;
    return gid_index;
}

int injectedRtrErrnoOrZero(const std::string& device_name, int sgid_index) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    if (!g_rtr_fault_injection_state.fail_first_rtr_einval ||
        g_rtr_fault_injection_state.fail_rtr_device != device_name ||
        sgid_index != g_rtr_fault_injection_state.synthetic_gid_a ||
        g_rtr_fault_injection_state.first_rtr_attempted) {
        return 0;
    }
    g_rtr_fault_injection_state.fail_first_rtr_einval = false;
    g_rtr_fault_injection_state.synthetic_gid_swap_enabled = false;
    g_rtr_fault_injection_state.first_rtr_attempted = true;
    ++g_rtr_fault_injection_state.injected_failures;
    return EINVAL;
}

void expectRtrEinvalRecoveredWithRetry(const std::string& device_name) {
    EXPECT_EQ(getInjectedFailureCount(), 1);
    auto sgid_history = getRtrSgidHistory(device_name);
    auto gid_history = getRtrGidHistory(device_name);
    ASSERT_EQ(sgid_history.size(), gid_history.size());
    ASSERT_GE(sgid_history.size(), 2u)
        << "RTR/EINVAL was injected, but the endpoint did not retry RTR";
    EXPECT_FALSE(gid_history.front().empty());

    EXPECT_TRUE(std::any_of(
        gid_history.begin() + 1, gid_history.end(),
        [&](const std::string& gid) { return gid != gid_history.front(); }));
}

std::string formatDeviceNames(const std::string& device_names) {
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

std::string makeNicPriorityMatrix(const std::string& device_name) {
    auto formatted_devices = formatDeviceNames(device_name);
    return "{\"cpu:0\": [[" + formatted_devices +
           "],[]], "
           " \"cpu:1\": [[" +
           formatted_devices + "],[]]}";
}

std::string makeNicPriorityMatrix(const std::string& preferred_devices,
                                  const std::string& fallback_devices) {
    auto formatted_preferred = formatDeviceNames(preferred_devices);
    auto formatted_fallback = formatDeviceNames(fallback_devices);
    return "{\"cpu:0\": [[" + formatted_preferred + "],[" + formatted_fallback +
           "]], "
           " \"cpu:1\": [[" +
           formatted_preferred + "],[" + formatted_fallback + "]]}";
}

void waitForTransfer(TransferEngine* engine, BatchID batch_id,
                     const std::string& op_name) {
    bool completed = false;
    TransferStatus status;
    while (!completed) {
        Status s = engine->getTransferStatus(batch_id, 0, status);
        EXPECT_EQ(s, Status::OK());
        if (status.s == TransferStatusEnum::COMPLETED) {
            completed = true;
        } else if (status.s == TransferStatusEnum::FAILED) {
            FAIL() << op_name << " FAILED";
        }
    }
    Status s = engine->freeBatchID(batch_id);
    EXPECT_EQ(s, Status::OK());
}

bool waitForTransferWithTimeout(TransferEngine* engine, BatchID batch_id,
                                const std::string& op_name,
                                std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    TransferStatus status;
    while (std::chrono::steady_clock::now() < deadline) {
        Status s = engine->getTransferStatus(batch_id, 0, status);
        if (s != Status::OK()) {
            ADD_FAILURE() << op_name
                          << " getTransferStatus failed: " << s.ToString();
            return false;
        }
        if (status.s == TransferStatusEnum::COMPLETED) {
            s = engine->freeBatchID(batch_id);
            if (s != Status::OK()) {
                ADD_FAILURE()
                    << op_name << " freeBatchID failed: " << s.ToString();
                return false;
            }
            return true;
        }
        if (status.s == TransferStatusEnum::FAILED) {
            ADD_FAILURE() << op_name << " FAILED";
            engine->freeBatchID(batch_id);
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    ADD_FAILURE() << op_name << " timed out after " << timeout.count()
                  << " seconds";
    engine->freeBatchID(batch_id);
    return false;
}

struct RawNicPriorityMatrix {};

struct TEContext {
    std::unique_ptr<TransferEngine> engine_{};
    RdmaTransport* rdma_transport_{};
    uint8_t* local_addr_{};
    bool segment_opened_{false};
    SegmentHandle segment_handle_{};
    uint64_t remote_base_{};

    TEContext(const std::string& local_server_name,
              const std::string& metadata_server, const std::string& segment_id,
              const std::string& device_name)
        : TEContext(local_server_name, metadata_server, segment_id,
                    makeNicPriorityMatrix(device_name),
                    RawNicPriorityMatrix{}) {}

    TEContext(const std::string& local_server_name,
              const std::string& metadata_server, const std::string& segment_id,
              const std::string& nic_priority_matrix, RawNicPriorityMatrix) {
        engine_ = std::make_unique<TransferEngine>(false);
        auto hostname_port = parseHostNameWithPort(local_server_name);
        engine_->init(metadata_server, local_server_name, hostname_port.first,
                      hostname_port.second);

        void* args[2] = {const_cast<char*>(nic_priority_matrix.c_str()),
                         nullptr};
        Transport* xport = engine_->installTransport("rdma", args);
        LOG_ASSERT(xport);
        rdma_transport_ = dynamic_cast<RdmaTransport*>(xport);
        LOG_ASSERT(rdma_transport_);

        local_addr_ = static_cast<uint8_t*>(numa_alloc_onnode(kRAMBufSize, 0));
        memset(local_addr_, 0, kDataLength);

        int rc =
            engine_->registerLocalMemory(local_addr_, kRAMBufSize, "cpu:0");
        LOG_ASSERT(!rc);

        if (!segment_id.empty()) {
            segment_opened_ = true;
            LOG(INFO) << "Opening segment " << segment_id << "...";
            segment_handle_ = engine_->openSegment(segment_id);
            auto segment_desc =
                engine_->getMetadata()->getSegmentDescByID(segment_handle_);
            remote_base_ = (uint64_t)segment_desc->buffers[0].addr;
        }
    }

    ~TEContext() {
        engine_->unregisterLocalMemory(local_addr_);
        numa_free(local_addr_, kRAMBufSize);
        if (segment_opened_) engine_->closeSegment(segment_handle_);
    }

    std::string localSegmentName() const {
        return engine_->getLocalIpAndPort();
    }
};

class RDMAEndpointReestablishTest : public ::testing::Test {
   protected:
    void SetUp() override {
        resetRtrFaultInjectionState();
        google::InitGoogleLogging("RDMAEndpointReestablishTest");
        FLAGS_logtostderr = true;

        const char* env = std::getenv("MC_METADATA_SERVER");
        metadata_server = env ? env : P2PHANDSHAKE;
        LOG(INFO) << "metadata_server: " << metadata_server;

        env = std::getenv("MC_TARGET_SERVER_NAME");
        target_server_name = env ? env : "127.0.0.1:12345";
        LOG(INFO) << "target_server_name: " << target_server_name;

        env = std::getenv("MC_INITIATOR_SERVER_NAME");
        initiator_server_name = env ? env : "127.0.0.1:12346";
        LOG(INFO) << "initiator_server_name: " << initiator_server_name;

        auto devices = getAvailableRdmaDevices();
        if (devices.size() < 2) {
            GTEST_SKIP() << "Need at least two RDMA devices, found "
                         << devices.size();
        }

        env = std::getenv("MC_TARGET_DEVICE_NAME");
        target_device_name = env ? env : devices[0];
        LOG(INFO) << "target_device_name: " << target_device_name;

        env = std::getenv("MC_INITIATOR_DEVICE_NAME");
        initiator_device_name = env ? env : devices[1];
        LOG(INFO) << "initiator_device_name: " << initiator_device_name;
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        resetRtrFaultInjectionState();
    }

    void runEndpointReestablishScenario(const std::string& target_device,
                                        const std::string& initiator_device) {
        LOG(INFO) << "========== Setting up Target ==========";
        TEContext target_ctx(target_server_name, metadata_server, "",
                             target_device);
        const std::string target_segment_name =
            usesP2PHandshake(metadata_server) ? target_ctx.localSegmentName()
                                              : target_server_name;
        LOG(INFO) << "Resolved target segment name: " << target_segment_name;
        LOG(INFO)
            << "Target is up. Waiting for RDMA connections and operations...";

        LOG(INFO) << "========== Phase 1: Start, Connect & Write ==========";
        {
            TEContext init_ctx(initiator_server_name, metadata_server,
                               target_segment_name, initiator_device);
            for (size_t i = 0; i < kDataLength; ++i) {
                init_ctx.local_addr_[i] = static_cast<uint8_t>(i % 256);
            }

            LOG(INFO) << "Writing " << kDataLength << " bytes to Target...";
            auto batch_id = init_ctx.engine_->allocateBatchID(1);
            TransferRequest entry;
            entry.opcode = TransferRequest::WRITE;
            entry.length = kDataLength;
            entry.source = init_ctx.local_addr_;
            entry.target_id = init_ctx.segment_handle_;
            entry.target_offset = init_ctx.remote_base_;

            Status s = init_ctx.engine_->submitTransfer(batch_id, {entry});
            ASSERT_EQ(s, Status::OK());
            waitForTransfer(init_ctx.engine_.get(), batch_id, "WRITE");
            LOG(INFO) << "Phase 1: Write Completed. Tearing down connection...";
        }

        LOG(INFO) << "Simulating Initiator Crash/Restart... Waiting 2 seconds.";
        sleep(2);

        LOG(INFO)
            << "========== Phase 2: Restart, Re-establish Endpoint & Read "
               "==========";
        {
            TEContext init_ctx(initiator_server_name, metadata_server,
                               target_segment_name, initiator_device);
            LOG(INFO) << "Reading data back over new Endpoint...";
            auto batch_id = init_ctx.engine_->allocateBatchID(1);
            TransferRequest entry;
            entry.opcode = TransferRequest::READ;
            entry.length = kDataLength;
            entry.source = init_ctx.local_addr_;
            entry.target_id = init_ctx.segment_handle_;
            entry.target_offset = init_ctx.remote_base_;

            Status s = init_ctx.engine_->submitTransfer(batch_id, {entry});
            ASSERT_EQ(s, Status::OK());
            waitForTransfer(init_ctx.engine_.get(), batch_id, "READ");

            bool ok = true;
            for (size_t i = 0; i < kDataLength; ++i) {
                if (init_ctx.local_addr_[i] != static_cast<uint8_t>(i % 256)) {
                    ok = false;
                    LOG(ERROR) << "Data mismatch at offset " << i
                               << ", expected " << (i % 256) << ", got "
                               << (int)init_ctx.local_addr_[i];
                    break;
                }
            }

            ASSERT_TRUE(ok) << "Endpoint Reconstruction Verification Failed!";
            LOG(INFO) << ">>> ENDPOINT RECONSTRUCTION VERIFICATION: "
                         "\033[32mSUCCESS\033";
        }
    }

    std::string metadata_server;
    std::string target_server_name;
    std::string initiator_server_name;
    std::string target_device_name;
    std::string initiator_device_name;
};

TEST_F(RDMAEndpointReestablishTest, EndpointReestablish) {
    runEndpointReestablishScenario(target_device_name, initiator_device_name);
}

TEST_F(RDMAEndpointReestablishTest, EndpointReestablishReverseDevices) {
    runEndpointReestablishScenario(initiator_device_name, target_device_name);
}

TEST_F(RDMAEndpointReestablishTest, ActiveHandshakeRetriesAfterAutoGidReprobe) {
    configureRtrFaultInjection(initiator_device_name);
    runEndpointReestablishScenario(target_device_name, initiator_device_name);
    expectRtrEinvalRecoveredWithRetry(initiator_device_name);
}

TEST_F(RDMAEndpointReestablishTest,
       PassiveHandshakeRetriesAfterAutoGidReprobe) {
    configureRtrFaultInjection(target_device_name);
    runEndpointReestablishScenario(target_device_name, initiator_device_name);
    expectRtrEinvalRecoveredWithRetry(target_device_name);
}

TEST_F(RDMAEndpointReestablishTest, SenderSingleRnicDownUsesFallbackLocalRnic) {
    if (target_device_name == initiator_device_name) {
        GTEST_SKIP() << "Need two distinct RDMA devices for sender failover";
    }

    const std::string both_devices =
        target_device_name + "," + initiator_device_name;
    const std::string target_matrix = makeNicPriorityMatrix(both_devices);
    LOG(INFO) << "========== Setting up dual-RNIC Target ==========";
    TEContext target_ctx(target_server_name, metadata_server, "", target_matrix,
                         RawNicPriorityMatrix{});
    const std::string target_segment_name = usesP2PHandshake(metadata_server)
                                                ? target_ctx.localSegmentName()
                                                : target_server_name;

    // Put the soon-to-be-down sender RNIC in the preferred tier and the
    // surviving sender RNIC in the fallback tier. This proves TE can skip the
    // inactive preferred local context and still complete the transfer through
    // another local RNIC.
    const std::string sender_matrix =
        makeNicPriorityMatrix(initiator_device_name, target_device_name);
    TEContext init_ctx(initiator_server_name, metadata_server,
                       target_segment_name, sender_matrix,
                       RawNicPriorityMatrix{});
    ASSERT_TRUE(RdmaTransportTestPeer::setContextActive(
        init_ctx.rdma_transport_, initiator_device_name, false));
    ASSERT_FALSE(RdmaTransportTestPeer::contextActive(init_ctx.rdma_transport_,
                                                      initiator_device_name));
    ASSERT_TRUE(RdmaTransportTestPeer::contextActive(init_ctx.rdma_transport_,
                                                     target_device_name));

    for (size_t i = 0; i < kDataLength; ++i) {
        init_ctx.local_addr_[i] = static_cast<uint8_t>((i * 17) % 251);
    }

    auto batch_id = init_ctx.engine_->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = init_ctx.local_addr_;
    entry.target_id = init_ctx.segment_handle_;
    entry.target_offset = init_ctx.remote_base_;

    Status s = init_ctx.engine_->submitTransfer(batch_id, {entry});
    ASSERT_EQ(s, Status::OK());
    waitForTransfer(init_ctx.engine_.get(), batch_id,
                    "WRITE with one sender RNIC down");

    for (size_t i = 0; i < kDataLength; ++i) {
        ASSERT_EQ(target_ctx.local_addr_[i],
                  static_cast<uint8_t>((i * 17) % 251))
            << "Data mismatch at offset " << i;
    }
}

TEST_F(RDMAEndpointReestablishTest,
       InjectedSenderRnicDownKeepsFallbackTransfersSmoothForTwentySeconds) {
    auto devices = getAvailableRdmaDevices();
    for (const auto* device : {"mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4"}) {
        if (std::find(devices.begin(), devices.end(), device) ==
            devices.end()) {
            GTEST_SKIP() << "Need " << device << " for this 4-RNIC test";
        }
    }

    const std::string target_matrix = makeNicPriorityMatrix("mlx5_3,mlx5_4");
    TEContext target_ctx(target_server_name, metadata_server, "", target_matrix,
                         RawNicPriorityMatrix{});
    const std::string target_segment_name = usesP2PHandshake(metadata_server)
                                                ? target_ctx.localSegmentName()
                                                : target_server_name;

    const std::string sender_matrix = makeNicPriorityMatrix("mlx5_2", "mlx5_1");
    TEContext init_ctx(initiator_server_name, metadata_server,
                       target_segment_name, sender_matrix,
                       RawNicPriorityMatrix{});

    std::atomic<bool> injected_down{false};
    std::atomic<bool> injected_up{false};
    std::atomic<bool> inject_down_ok{false};
    std::atomic<bool> inject_up_ok{false};
    std::thread injector([&] {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        inject_down_ok.store(
            RdmaTransportTestPeer::injectContextEvent(
                init_ctx.rdma_transport_, "mlx5_2", IBV_EVENT_PORT_ERR),
            std::memory_order_release);
        injected_down.store(true, std::memory_order_release);

        std::this_thread::sleep_for(std::chrono::seconds(7));
        inject_up_ok.store(
            RdmaTransportTestPeer::injectContextEvent(
                init_ctx.rdma_transport_, "mlx5_2", IBV_EVENT_PORT_ACTIVE),
            std::memory_order_release);
        injected_up.store(true, std::memory_order_release);
    });

    constexpr size_t kFaultLoopLength = 4ull << 20;
    auto start = std::chrono::steady_clock::now();
    auto deadline = start + std::chrono::seconds(20);
    size_t completed_transfers = 0;
    uint8_t last_pattern = 0;
    bool transfer_ok = true;

    while (std::chrono::steady_clock::now() < deadline) {
        last_pattern = static_cast<uint8_t>((completed_transfers * 37) % 251);
        memset(init_ctx.local_addr_, last_pattern, kFaultLoopLength);

        auto batch_id = init_ctx.engine_->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kFaultLoopLength;
        entry.source = init_ctx.local_addr_;
        entry.target_id = init_ctx.segment_handle_;
        entry.target_offset = init_ctx.remote_base_;

        Status s = init_ctx.engine_->submitTransfer(batch_id, {entry});
        if (s != Status::OK()) {
            ADD_FAILURE() << "submitTransfer failed during injected sender "
                             "RNIC down/fallback: "
                          << s.ToString();
            transfer_ok = false;
            break;
        }
        if (!waitForTransferWithTimeout(
                init_ctx.engine_.get(), batch_id,
                "20s WRITE during injected sender RNIC down/fallback",
                std::chrono::seconds(5))) {
            transfer_ok = false;
            break;
        }
        ++completed_transfers;
    }

    injector.join();
    ASSERT_TRUE(injected_down.load(std::memory_order_acquire));
    ASSERT_TRUE(injected_up.load(std::memory_order_acquire));
    ASSERT_TRUE(inject_down_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(inject_up_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(transfer_ok);
    ASSERT_GT(completed_transfers, 0u);

    for (size_t i = 0; i < kFaultLoopLength; ++i) {
        ASSERT_EQ(target_ctx.local_addr_[i], last_pattern)
            << "Data mismatch at offset " << i;
    }
    LOG(INFO) << "Completed " << completed_transfers
              << " transfers during injected mlx5_2 down/fallback";
}

TEST_F(RDMAEndpointReestablishTest,
       InjectedReceiverRnicDownKeepsFallbackTransfersSmoothForTwentySeconds) {
    auto devices = getAvailableRdmaDevices();
    for (const auto* device : {"mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4"}) {
        if (std::find(devices.begin(), devices.end(), device) ==
            devices.end()) {
            GTEST_SKIP() << "Need " << device << " for this 4-RNIC test";
        }
    }

    const std::string target_matrix = makeNicPriorityMatrix("mlx5_3", "mlx5_4");
    TEContext target_ctx(target_server_name, metadata_server, "", target_matrix,
                         RawNicPriorityMatrix{});
    const std::string target_segment_name = usesP2PHandshake(metadata_server)
                                                ? target_ctx.localSegmentName()
                                                : target_server_name;

    const std::string sender_matrix = makeNicPriorityMatrix("mlx5_1,mlx5_2");
    TEContext init_ctx(initiator_server_name, metadata_server,
                       target_segment_name, sender_matrix,
                       RawNicPriorityMatrix{});

    std::atomic<bool> injected_down{false};
    std::atomic<bool> injected_up{false};
    std::atomic<bool> inject_down_ok{false};
    std::atomic<bool> inject_up_ok{false};
    std::thread injector([&] {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        inject_down_ok.store(
            RdmaTransportTestPeer::injectContextEvent(
                target_ctx.rdma_transport_, "mlx5_3", IBV_EVENT_PORT_ERR),
            std::memory_order_release);
        injected_down.store(true, std::memory_order_release);

        std::this_thread::sleep_for(std::chrono::seconds(7));
        inject_up_ok.store(
            RdmaTransportTestPeer::injectContextEvent(
                target_ctx.rdma_transport_, "mlx5_3", IBV_EVENT_PORT_ACTIVE),
            std::memory_order_release);
        injected_up.store(true, std::memory_order_release);
    });

    constexpr size_t kFaultLoopLength = 4ull << 20;
    auto start = std::chrono::steady_clock::now();
    auto deadline = start + std::chrono::seconds(20);
    size_t completed_transfers = 0;
    uint8_t last_pattern = 0;
    bool transfer_ok = true;

    while (std::chrono::steady_clock::now() < deadline) {
        last_pattern = static_cast<uint8_t>((completed_transfers * 41) % 251);
        memset(init_ctx.local_addr_, last_pattern, kFaultLoopLength);

        auto batch_id = init_ctx.engine_->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kFaultLoopLength;
        entry.source = init_ctx.local_addr_;
        entry.target_id = init_ctx.segment_handle_;
        entry.target_offset = init_ctx.remote_base_;

        Status s = init_ctx.engine_->submitTransfer(batch_id, {entry});
        if (s != Status::OK()) {
            ADD_FAILURE() << "submitTransfer failed during injected receiver "
                             "RNIC down/fallback: "
                          << s.ToString();
            transfer_ok = false;
            break;
        }
        if (!waitForTransferWithTimeout(
                init_ctx.engine_.get(), batch_id,
                "20s WRITE during injected receiver RNIC down/fallback",
                std::chrono::seconds(5))) {
            transfer_ok = false;
            break;
        }
        ++completed_transfers;
    }

    injector.join();
    ASSERT_TRUE(injected_down.load(std::memory_order_acquire));
    ASSERT_TRUE(injected_up.load(std::memory_order_acquire));
    ASSERT_TRUE(inject_down_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(inject_up_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(transfer_ok);
    ASSERT_GT(completed_transfers, 0u);

    for (size_t i = 0; i < kFaultLoopLength; ++i) {
        ASSERT_EQ(target_ctx.local_addr_[i], last_pattern)
            << "Data mismatch at offset " << i;
    }
    LOG(INFO) << "Completed " << completed_transfers
              << " transfers during injected receiver mlx5_3 down/fallback";
}

}  // namespace

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

namespace mooncake {
class RdmaEndPointTestPeer {
   public:
    static void addDummyQp(RdmaEndPoint &endpoint, ibv_qp *qp) {
        endpoint.qp_list_.push_back(qp);
    }

    static void clearDummyQp(RdmaEndPoint &endpoint) {
        endpoint.qp_list_.clear();
    }

    static int doSetupConnection(RdmaEndPoint &endpoint, int qp_index,
                                 const ibv_gid &peer_gid, uint16_t peer_lid,
                                 uint32_t peer_qp_num, int local_gid_index,
                                 std::string *reply_msg,
                                 int &out_stage, int &out_sys_errno) {
        RdmaEndPoint::SetupConnectionFailureInfo failure_info = {};
        int rc = endpoint.doSetupConnection(qp_index, peer_gid, peer_lid,
                                            peer_qp_num, local_gid_index,
                                            reply_msg, &failure_info);
        out_stage = static_cast<int>(failure_info.stage);
        out_sys_errno = failure_info.sys_errno;
        return rc;
    }

    static int getResetStageValue() {
        return static_cast<int>(RdmaEndPoint::SetupConnectionFailureStage::kReset);
    }
};
}

extern std::atomic<int> g_inject_modify_qp_error;

namespace {

TEST(RDMAEndpointSetupErrorTest, VerifyVerbsErrorCorrectlyCaptured) {
    auto* transport = new RdmaTransport();
    // Ignore memory leak of transport during destruction since it's a test
    MC_LSAN_IGNORE_OBJECT(transport);
    auto context = std::make_unique<RdmaContext>(*transport, "unused");
    auto endpoint = std::make_unique<RdmaEndPoint>(*context);

    // Add a dummy non-null QP pointer to trigger doSetupConnection
    ibv_qp* dummy_qp = reinterpret_cast<ibv_qp*>(0xdeadbeef);
    mooncake::RdmaEndPointTestPeer::addDummyQp(*endpoint, dummy_qp);

    // Inject EINVAL error for ibv_modify_qp
    g_inject_modify_qp_error.store(EINVAL);

    ibv_gid dummy_gid = {};
    std::string reply_msg;
    int stage = 0;
    int sys_errno = 0;
    int rc = mooncake::RdmaEndPointTestPeer::doSetupConnection(
        *endpoint, 0, dummy_gid, 0, 0, 0, &reply_msg, stage, sys_errno);

    // Clear injection immediately
    g_inject_modify_qp_error.store(0);

    // Clear dummy qp so destructor doesn't try to destroy 0xdeadbeef and crash
    mooncake::RdmaEndPointTestPeer::clearDummyQp(*endpoint);

    EXPECT_EQ(rc, ERR_ENDPOINT);
    EXPECT_EQ(stage, mooncake::RdmaEndPointTestPeer::getResetStageValue());
    EXPECT_EQ(sys_errno, EINVAL);
    EXPECT_TRUE(reply_msg.find("EINVAL") != std::string::npos || reply_msg.find("Invalid argument") != std::string::npos || reply_msg.find("22") != std::string::npos);
}

}  // namespace

extern "C" int __real__ibv_query_gid_ex(ibv_context* context, uint8_t port_num,
                                        int gid_index,
                                        struct ibv_gid_entry* entry,
                                        uint32_t flags, size_t entry_size);

extern "C" int __wrap__ibv_query_gid_ex(ibv_context* context, uint8_t port_num,
                                        int gid_index,
                                        struct ibv_gid_entry* entry,
                                        uint32_t flags, size_t entry_size) {
    const std::string device_name = ibv_get_device_name(context->device);
    int wrapped_gid_index = maybeSwapSyntheticGidIndex(device_name, gid_index);
    return __real__ibv_query_gid_ex(context, port_num, wrapped_gid_index, entry,
                                    flags, entry_size);
}

extern "C" int __real_ibv_query_gid(ibv_context* context, uint8_t port_num,
                                    int gid_index, union ibv_gid* gid);

extern "C" int __wrap_ibv_query_gid(ibv_context* context, uint8_t port_num,
                                    int gid_index, union ibv_gid* gid) {
    const std::string device_name = ibv_get_device_name(context->device);
    int wrapped_gid_index = maybeSwapSyntheticGidIndex(device_name, gid_index);
    return __real_ibv_query_gid(context, port_num, wrapped_gid_index, gid);
}

extern "C" int __real_ibv_modify_qp(ibv_qp* qp, ibv_qp_attr* attr,
                                    int attr_mask);

std::atomic<int> g_inject_modify_qp_error{0};

extern "C" int __wrap_ibv_modify_qp(ibv_qp* qp, ibv_qp_attr* attr,
                                    int attr_mask) {
    int error_to_inject = g_inject_modify_qp_error.load();
    if (error_to_inject != 0) {
        return error_to_inject;
    }
    if (qp != nullptr && attr != nullptr && attr->qp_state == IBV_QPS_RTR &&
        (attr_mask & IBV_QP_AV)) {
        const std::string device_name =
            ibv_get_device_name(qp->context->device);
        int sgid_index = attr->ah_attr.grh.sgid_index;
        union ibv_gid actual_gid = {};
        std::string gid_string;
        if (__real_ibv_query_gid(qp->context, attr->ah_attr.port_num,
                                 sgid_index, &actual_gid) == 0) {
            gid_string = formatGidBytes(actual_gid.raw);
        }
        recordRtrAttempt(device_name, sgid_index, gid_string);
        int injected_errno = injectedRtrErrnoOrZero(device_name, sgid_index);
        if (injected_errno != 0) {
            return injected_errno;
        }
    }
    return __real_ibv_modify_qp(qp, attr, attr_mask);
}
