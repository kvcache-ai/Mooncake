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
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
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
#include "transport/transport.h"

using namespace mooncake;

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
    bool fail_first_rtr_einval = false;
    std::string fail_rtr_device;
    int injected_failures = 0;
    std::unordered_map<std::string, std::vector<int>> rtr_sgid_history;
    std::unordered_map<std::string, std::vector<std::string>> rtr_gid_history;
} g_rtr_fault_injection_state;

// QP-state tracking and the DisconnectIssuesErrBeforeResetOnReconnect test
// below are compiled only for non-eRDMA builds (USE_ERDMA=OFF). When
// CONFIG_ERDMA is defined, RdmaEndPoint::resetConnection() reconnects via
// reconstruct() (destroy+recreate QPs) rather than disconnectUnlocked(), so the
// ERR-before-RESET state transition this asserts never occurs. See
// RdmaEndPoint::resetConnection() and mooncake-common/common.cmake (USE_ERDMA).
#ifndef CONFIG_ERDMA

// Records QP state transitions to verify the disconnect ordering on reconnect:
// a re-established endpoint must move its QP to IBV_QPS_ERR (flushing inflight
// WRs to the CQ) BEFORE the reconnect drives the same QP back through
// IBV_QPS_RESET. Resetting a connected QP directly, without first transitioning
// to ERR, silently discards outstanding WRs (no CQE is generated for them).
//
// QPs are keyed by (pointer, generation). The generation is bumped each time a
// pointer is handed out by ibv_create_qp, so a destroyed-then-reallocated QP
// reusing the same address gets a fresh generation and is never conflated with
// the QP that previously lived there. That makes the ERR-then-RESET signature
// unambiguous: within a single generation it can only come from the
// disconnect->reconnect path, never from beginDestroy() (which moves to ERR and
// then destroys the QP, never RESET) nor from a fresh endpoint bring-up (which
// starts at RESET and never sees ERR).
struct QpStateTrackingState {
    std::mutex mu;
    bool enabled = false;
    std::unordered_map<ibv_qp*, int> generation;
    struct Event {
        ibv_qp* qp;
        int generation;
        ibv_qp_state state;
    };
    std::vector<Event> events;
} g_qp_state_tracking;

// When set, the wrapped ibv_query_qp reports IBV_QPS_RESET, exercising the
// disconnectUnlocked() guard that skips the QP->ERR flush for an already-RESET
// QP (RESET->ERR is illegal and returns EINVAL). Drives
// SkipsErrFlushWhenQpReportedReset.
std::atomic<bool> g_force_qp_state_reset{false};

void resetQpStateTracking() {
    std::lock_guard<std::mutex> guard(g_qp_state_tracking.mu);
    g_qp_state_tracking.enabled = false;
    g_qp_state_tracking.generation.clear();
    g_qp_state_tracking.events.clear();
}

void enableQpStateTracking() {
    std::lock_guard<std::mutex> guard(g_qp_state_tracking.mu);
    g_qp_state_tracking.enabled = true;
}

void recordQpCreate(ibv_qp* qp) {
    std::lock_guard<std::mutex> guard(g_qp_state_tracking.mu);
    ++g_qp_state_tracking.generation[qp];
}

void recordQpStateTransition(ibv_qp* qp, ibv_qp_state state) {
    if (state != IBV_QPS_ERR && state != IBV_QPS_RESET) return;
    std::lock_guard<std::mutex> guard(g_qp_state_tracking.mu);
    if (!g_qp_state_tracking.enabled) return;
    int gen = g_qp_state_tracking.generation[qp];
    g_qp_state_tracking.events.push_back({qp, gen, state});
}

bool sawErrBeforeResetWithinGeneration() {
    std::lock_guard<std::mutex> guard(g_qp_state_tracking.mu);
    const auto& events = g_qp_state_tracking.events;
    for (size_t i = 0; i < events.size(); ++i) {
        if (events[i].state != IBV_QPS_ERR) continue;
        for (size_t j = i + 1; j < events.size(); ++j) {
            if (events[j].qp == events[i].qp &&
                events[j].generation == events[i].generation &&
                events[j].state == IBV_QPS_RESET) {
                return true;
            }
        }
    }
    return false;
}

int countQpTransitions(ibv_qp_state state) {
    std::lock_guard<std::mutex> guard(g_qp_state_tracking.mu);
    int count = 0;
    for (const auto& event : g_qp_state_tracking.events) {
        if (event.state == state) ++count;
    }
    return count;
}

#endif  // !CONFIG_ERDMA

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
    g_rtr_fault_injection_state.fail_first_rtr_einval = false;
    g_rtr_fault_injection_state.fail_rtr_device.clear();
    g_rtr_fault_injection_state.injected_failures = 0;
    g_rtr_fault_injection_state.rtr_sgid_history.clear();
    g_rtr_fault_injection_state.rtr_gid_history.clear();
}

void configureRtrFaultInjection(const std::string& device_name) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    g_rtr_fault_injection_state.synthetic_gid_swap_enabled = true;
    g_rtr_fault_injection_state.synthetic_gid_device = device_name;
    g_rtr_fault_injection_state.fail_first_rtr_einval = true;
    g_rtr_fault_injection_state.fail_rtr_device = device_name;
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
    if (gid_index == 0) return 1;
    if (gid_index == 1) return 0;
    return gid_index;
}

bool shouldInjectRtrEinval(const std::string& device_name, int sgid_index) {
    std::lock_guard<std::mutex> guard(g_rtr_fault_injection_state.mu);
    if (!g_rtr_fault_injection_state.fail_first_rtr_einval ||
        g_rtr_fault_injection_state.fail_rtr_device != device_name ||
        sgid_index != 0) {
        return false;
    }
    g_rtr_fault_injection_state.fail_first_rtr_einval = false;
    g_rtr_fault_injection_state.synthetic_gid_swap_enabled = false;
    ++g_rtr_fault_injection_state.injected_failures;
    return true;
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

struct TEContext {
    std::unique_ptr<TransferEngine> engine_{};
    uint8_t* local_addr_{};
    bool segment_opened_{false};
    SegmentHandle segment_handle_{};
    uint64_t remote_base_{};

    TEContext(const std::string& local_server_name,
              const std::string& metadata_server, const std::string& segment_id,
              const std::string& device_name) {
        engine_ = std::make_unique<TransferEngine>(false);
        auto hostname_port = parseHostNameWithPort(local_server_name);
        engine_->init(metadata_server, local_server_name, hostname_port.first,
                      hostname_port.second);

        auto nic_priority_matrix = makeNicPriorityMatrix(device_name);
        void* args[2] = {const_cast<char*>(nic_priority_matrix.c_str()),
                         nullptr};
        Transport* xport = engine_->installTransport("rdma", args);
        LOG_ASSERT(xport);

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
#ifndef CONFIG_ERDMA
        resetQpStateTracking();
#endif
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
#ifndef CONFIG_ERDMA
        resetQpStateTracking();
#endif
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

    EXPECT_EQ(getInjectedFailureCount(), 1);
    auto sgid_history = getRtrSgidHistory(initiator_device_name);
    auto gid_history = getRtrGidHistory(initiator_device_name);
    ASSERT_EQ(sgid_history.size(), gid_history.size());
    ASSERT_GE(sgid_history.size(), 2u);
    EXPECT_EQ(sgid_history.front(), 0);
    EXPECT_FALSE(gid_history.front().empty());
    EXPECT_TRUE(std::any_of(
        gid_history.begin() + 1, gid_history.end(),
        [&](const std::string& gid) { return gid != gid_history.front(); }));
}

TEST_F(RDMAEndpointReestablishTest,
       PassiveHandshakeRetriesAfterAutoGidReprobe) {
    configureRtrFaultInjection(target_device_name);
    runEndpointReestablishScenario(target_device_name, initiator_device_name);

    EXPECT_EQ(getInjectedFailureCount(), 1);
    auto sgid_history = getRtrSgidHistory(target_device_name);
    auto gid_history = getRtrGidHistory(target_device_name);
    ASSERT_EQ(sgid_history.size(), gid_history.size());
    ASSERT_GE(sgid_history.size(), 2u);
    EXPECT_EQ(sgid_history.front(), 0);
    EXPECT_FALSE(gid_history.front().empty());
    EXPECT_TRUE(std::any_of(
        gid_history.begin() + 1, gid_history.end(),
        [&](const std::string& gid) { return gid != gid_history.front(); }));
}

// Covers the disconnect ordering on reconnect for non-eRDMA builds. When an
// already-connected endpoint is re-established, disconnectUnlocked() must
// transition its QP to IBV_QPS_ERR (so the HCA flushes outstanding WRs as
// completions, which performPollCq drains through the normal failure path)
// before the reconnect drives the same QP back through IBV_QPS_RESET. Resetting
// the QP straight from a connected state drops inflight WRs without ever
// generating a CQE. See disconnectUnlocked() in rdma_endpoint.cpp.
//
// Triggering the re-establish requires the restarted peer to reconnect under
// the SAME nic_path with new QPs, which only happens when peer names are stable
// across the restart -- i.e. a real metadata server. Under P2PHANDSHAKE every
// restart gets a fresh random RPC port, so the endpoint is never reused and the
// disconnect path is not exercised; skip in that case.
#ifndef CONFIG_ERDMA
TEST_F(RDMAEndpointReestablishTest, DisconnectIssuesErrBeforeResetOnReconnect) {
    if (usesP2PHandshake(metadata_server)) {
        GTEST_SKIP()
            << "Re-establish (and thus the ERR disconnect path) is not "
               "triggered under P2PHANDSHAKE; run with a metadata "
               "server (e.g. MC_METADATA_SERVER=http://host:port/"
               "metadata) and stable MC_*_SERVER_NAME to exercise it";
    }

    enableQpStateTracking();
    runEndpointReestablishScenario(target_device_name, initiator_device_name);

    // The reconnect must have flushed at least one QP through ERR. If the
    // disconnect path reset QPs directly (no ERR transition) this would be
    // zero.
    EXPECT_GT(countQpTransitions(IBV_QPS_ERR), 0)
        << "Expected the reconnect to flush QPs via IBV_QPS_ERR, but no ERR "
           "transition was observed";

    // The core invariant: within a single QP generation, ERR precedes RESET.
    EXPECT_TRUE(sawErrBeforeResetWithinGeneration())
        << "Expected a QP to be moved to IBV_QPS_ERR before being reset on "
           "reconnect; the disconnect path must not RESET a connected QP "
           "directly (that silently discards inflight WRs without CQEs)";
}

// Covers the disconnectUnlocked() guard that skips the QP->ERR flush when the
// QP is already in RESET (RESET->ERR is illegal and returns EINVAL[22], the
// log-flood the guard removes). With ibv_query_qp forced to report RESET, the
// reconnect's disconnect must NOT attempt the ERR flush, so the reused QP
// generation goes to RESET with no preceding ERR -- the ERR-before-RESET
// signature that DisconnectIssuesErrBeforeResetOnReconnect asserts is absent.
// (beginDestroy's ERR-then-destroy never produces ERR-before-RESET within a
// generation, so it can't make this assertion pass spuriously.)
TEST_F(RDMAEndpointReestablishTest, SkipsErrFlushWhenQpReportedReset) {
    if (usesP2PHandshake(metadata_server)) {
        GTEST_SKIP()
            << "Re-establish (and thus the disconnect path) is not triggered "
               "under P2PHANDSHAKE; run with a metadata server and stable "
               "MC_*_SERVER_NAME to exercise it";
    }

    enableQpStateTracking();
    // Make disconnectUnlocked()'s state query see RESET for the duration of the
    // scenario so its guard skips the ERR flush.
    g_force_qp_state_reset.store(true, std::memory_order_relaxed);
    runEndpointReestablishScenario(target_device_name, initiator_device_name);
    g_force_qp_state_reset.store(false, std::memory_order_relaxed);

    EXPECT_FALSE(sawErrBeforeResetWithinGeneration())
        << "Expected disconnectUnlocked to SKIP the QP->ERR flush when the QP "
           "is reported RESET (the guard), so no ERR-before-RESET transition "
           "should be recorded for the reused QP generation";
}
#endif  // !CONFIG_ERDMA

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

// Only wrapped for non-eRDMA builds; the linker --wrap flag is added in
// CMakeLists.txt under the same USE_ERDMA guard.
#ifndef CONFIG_ERDMA
extern "C" struct ibv_qp* __real_ibv_create_qp(struct ibv_pd* pd,
                                               struct ibv_qp_init_attr* attr);

extern "C" struct ibv_qp* __wrap_ibv_create_qp(struct ibv_pd* pd,
                                               struct ibv_qp_init_attr* attr) {
    struct ibv_qp* qp = __real_ibv_create_qp(pd, attr);
    if (qp != nullptr) {
        recordQpCreate(qp);
    }
    return qp;
}

// Wrapped (non-eRDMA only) so SkipsErrFlushWhenQpReportedReset can make
// disconnectUnlocked()'s state query report RESET on demand. Delegates to the
// real call and only overrides the reported state when the toggle is set, so
// it is a no-op for every other caller and when the toggle is off.
extern "C" int __real_ibv_query_qp(ibv_qp* qp, ibv_qp_attr* attr, int attr_mask,
                                   ibv_qp_init_attr* init_attr);

extern "C" int __wrap_ibv_query_qp(ibv_qp* qp, ibv_qp_attr* attr, int attr_mask,
                                   ibv_qp_init_attr* init_attr) {
    int rc = __real_ibv_query_qp(qp, attr, attr_mask, init_attr);
    if (rc == 0 && attr != nullptr &&
        g_force_qp_state_reset.load(std::memory_order_relaxed)) {
        attr->qp_state = IBV_QPS_RESET;
    }
    return rc;
}
#endif  // !CONFIG_ERDMA

extern "C" int __real_ibv_modify_qp(ibv_qp* qp, ibv_qp_attr* attr,
                                    int attr_mask);

extern "C" int __wrap_ibv_modify_qp(ibv_qp* qp, ibv_qp_attr* attr,
                                    int attr_mask) {
#ifndef CONFIG_ERDMA
    if (qp != nullptr && attr != nullptr && (attr_mask & IBV_QP_STATE)) {
        recordQpStateTransition(qp, attr->qp_state);
    }
#endif  // !CONFIG_ERDMA
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
        if (shouldInjectRtrEinval(device_name, sgid_index)) {
            errno = EINVAL;
            return -1;
        }
    }
    return __real_ibv_modify_qp(qp, attr, attr_mask);
}
