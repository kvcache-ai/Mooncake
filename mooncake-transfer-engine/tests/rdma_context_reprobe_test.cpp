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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <string>

#ifdef __linux__
#include <limits>
#endif

#include "common.h"
#include "error.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#ifdef USE_SHCA
#include <infiniband/shca_17b_types.h>
#endif

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

using namespace mooncake;

#ifdef __linux__
namespace {

struct FakeVerbsDevice {
    bool enabled = false;
    ibv_device device = {};
    ibv_device *device_list[2] = {&device, nullptr};
    ibv_context context = {};
    size_t alloc_pd_calls = 0;
};

FakeVerbsDevice fake_verbs;

class FakeVerbsDeviceScope {
   public:
    explicit FakeVerbsDeviceScope(int num_comp_vectors) {
        fake_verbs.enabled = true;
        fake_verbs.context = {};
        fake_verbs.context.num_comp_vectors = num_comp_vectors;
        fake_verbs.alloc_pd_calls = 0;
    }

    ~FakeVerbsDeviceScope() { fake_verbs.enabled = false; }

    size_t allocPdCalls() const { return fake_verbs.alloc_pd_calls; }
};

}  // namespace

#undef ibv_query_port

// Interpose the libibverbs boundary for this test binary so construct() can
// exercise device validation without RDMA hardware.
extern "C" {

ibv_device **ibv_get_device_list(int *num_devices) {
    if (!fake_verbs.enabled) {
        *num_devices = 0;
        return nullptr;
    }
    *num_devices = 1;
    return fake_verbs.device_list;
}

void ibv_free_device_list(ibv_device **) {}

const char *ibv_get_device_name(ibv_device *device) {
    if (fake_verbs.enabled && device == &fake_verbs.device)
        return "nonexistent-device";
    return "";
}

ibv_context *ibv_open_device(ibv_device *device) {
    if (fake_verbs.enabled && device == &fake_verbs.device)
        return &fake_verbs.context;
    return nullptr;
}

int ibv_query_port(ibv_context *context, uint8_t,
                   _compat_ibv_port_attr *compat_port_attr) {
    if (!fake_verbs.enabled || context != &fake_verbs.context) return EINVAL;
    auto *port_attr = reinterpret_cast<ibv_port_attr *>(compat_port_attr);
    *port_attr = {};
    port_attr->state = IBV_PORT_ACTIVE;
#ifdef USE_SHCA
    port_attr->lid = u32_to_17(1);
#else
    port_attr->lid = 1;
#endif
    port_attr->active_mtu = IBV_MTU_4096;
    return 0;
}

int ibv_query_device(ibv_context *context, ibv_device_attr *device_attr) {
    if (!fake_verbs.enabled || context != &fake_verbs.context) return EINVAL;
    *device_attr = {};
    device_attr->max_qp = std::numeric_limits<int>::max();
    device_attr->max_cq = std::numeric_limits<int>::max();
    device_attr->max_qp_wr = std::numeric_limits<int>::max();
    device_attr->max_sge = std::numeric_limits<int>::max();
    device_attr->max_cqe = std::numeric_limits<int>::max();
    device_attr->max_mr_size = std::numeric_limits<uint64_t>::max();
    return 0;
}

int ibv_query_gid(ibv_context *context, uint8_t, int, ibv_gid *gid) {
    if (!fake_verbs.enabled || context != &fake_verbs.context) return EINVAL;
    *gid = {};
    gid->raw[15] = 1;
    return 0;
}

ibv_pd *ibv_alloc_pd(ibv_context *context) {
    if (!fake_verbs.enabled || context != &fake_verbs.context) return nullptr;
    ++fake_verbs.alloc_pd_calls;
    return nullptr;
}

int ibv_close_device(ibv_context *) { return 0; }

}  // extern "C"
#endif  // __linux__

namespace {

class RdmaContextConstructionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // RdmaTransport teardown requires metadata initialized by install().
        // These tests only need the constructor reference, so match the
        // existing uninstalled-transport test setup below.
        transport_ = new RdmaTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);
        context_ =
            std::make_unique<RdmaContext>(*transport_, "nonexistent-device");
    }

    RdmaTransport *transport_ = nullptr;
    std::unique_ptr<RdmaContext> context_;
};

TEST_F(RdmaContextConstructionTest, RejectsZeroCompletionQueuesBeforeSetup) {
    EXPECT_EQ(context_->construct(/*num_cq_list=*/0,
                                  /*num_comp_channels=*/1),
              ERR_INVALID_ARGUMENT);
    EXPECT_FALSE(RdmaContextTestPeer::hasEndpointStore(*context_));
}

TEST_F(RdmaContextConstructionTest, RejectsZeroCompletionChannelsBeforeSetup) {
    EXPECT_EQ(context_->construct(/*num_cq_list=*/1,
                                  /*num_comp_channels=*/0),
              ERR_INVALID_ARGUMENT);
    EXPECT_FALSE(RdmaContextTestPeer::hasEndpointStore(*context_));
}

TEST_F(RdmaContextConstructionTest, LogsPortNumberAsIntegerNotCharacter) {
    // `port` is a uint8_t; streaming it raw into glog renders the byte as a
    // control character, so the operator sees "on port \x01" instead of the
    // number they configured. The failure path for a missing device is enough
    // to exercise the log line.
    const bool saved_logtostderr = FLAGS_logtostderr;
    FLAGS_logtostderr = true;
    ::testing::internal::CaptureStderr();
    EXPECT_EQ(context_->construct(/*num_cq_list=*/1,
                                  /*num_comp_channels=*/1,
                                  /*port=*/1,
                                  /*gid_index=*/-1),
              ERR_CONTEXT);
    const std::string log = ::testing::internal::GetCapturedStderr();
    FLAGS_logtostderr = saved_logtostderr;

    EXPECT_NE(log.find("on port 1 with GID"), std::string::npos) << log;
    EXPECT_EQ(log.find(std::string("on port \x01")), std::string::npos) << log;
}

TEST_F(RdmaContextConstructionTest,
       RejectsDeviceWithoutCompletionVectorsBeforeAllocatingResources) {
#ifdef __linux__
    FakeVerbsDeviceScope fake_device(/*num_comp_vectors=*/0);

    EXPECT_EQ(context_->construct(/*num_cq_list=*/1,
                                  /*num_comp_channels=*/1,
                                  /*port=*/1,
                                  /*gid_index=*/0),
              ERR_CONTEXT);
    EXPECT_EQ(fake_device.allocPdCalls(), 0);
    RdmaContextTestPeer::disableContextForTeardown(*context_);
#else
    GTEST_SKIP() << "Requires Linux libibverbs symbol interposition";
#endif
}

TEST(RdmaMemoryRegistrationPolicyTest, LocalOnlyBufferHasNoPublishedRkey) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    auto local_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    local_desc->name = "local-rdma-segment";
    local_desc->protocol = "rdma";
    ASSERT_EQ(metadata->addLocalSegment(LOCAL_SEGMENT_ID, "local-rdma-segment",
                                        std::move(local_desc)),
              0);

    RdmaTransport transport;
    RdmaTransportTestPeer::bindMetadata(transport, metadata,
                                        "local-rdma-segment");
    std::array<char, 1> buffer{};
    ASSERT_EQ(transport.registerLocalMemory(buffer.data(), buffer.size(),
                                            "cpu:0", false, false),
              0);

    auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(desc, nullptr);
    ASSERT_EQ(desc->buffers.size(), 1);
    EXPECT_TRUE(desc->buffers[0].rkey.empty());
}

ibv_gid makeGid(const std::array<uint8_t, 16> &bytes) {
    ibv_gid gid = {};
    std::memcpy(gid.raw, bytes.data(), bytes.size());
    return gid;
}

std::string formatGid(const std::array<uint8_t, 16> &bytes) {
    std::string gid;
    char buf[4] = {0};
    for (size_t i = 0; i < bytes.size(); ++i) {
        std::snprintf(buf, sizeof(buf), "%02x", bytes[i]);
        gid += i == 0 ? buf : std::string(":") + buf;
    }
    return gid;
}

constexpr std::array<uint8_t, 16> kCurrentGid = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11};
class RdmaContextReprobeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        transport_ = new RdmaTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);
        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "local-rdma-segment");

        auto local_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        local_desc->name = "local-rdma-segment";
        local_desc->protocol = "rdma";
        local_desc->devices.push_back(
            {"synthetic0", 23, formatGid(kCurrentGid), ""});
        ASSERT_EQ(
            metadata_->addLocalSegment(LOCAL_SEGMENT_ID, "local-rdma-segment",
                                       std::move(local_desc)),
            0);

        context_ = new RdmaContext(*transport_, "synthetic0");
        MC_LSAN_IGNORE_OBJECT(context_);
        RdmaContextTestPeer::seedAutoGidState(
            *context_, reinterpret_cast<ibv_context *>(0x1), /*port=*/1,
            /*lid=*/23, makeGid(kCurrentGid), /*gid_index=*/0);
    }

    std::shared_ptr<TransferMetadata::SegmentDesc> localDesc() const {
        return metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    }

    RdmaTransport *transport_ = nullptr;
    std::shared_ptr<TransferMetadata> metadata_;
    RdmaContext *context_ = nullptr;
};

TEST_F(RdmaContextReprobeTest,
       ReprobeStopsWhenExpectedSelectionDoesNotMatchCurrentState) {
    auto before_desc = localDesc();
    ASSERT_TRUE(before_desc);

    bool changed = context_->reprobeAutoGid({formatGid(kCurrentGid), 9}, {});

    EXPECT_FALSE(changed);
    EXPECT_EQ(context_->gidIndex(), 0);
    EXPECT_EQ(context_->gid(), formatGid(kCurrentGid));
    auto after_desc = localDesc();
    EXPECT_EQ(after_desc.get(), before_desc.get());
}

// Regression tests for the HCA-index / context_list_ alignment invariant.
// An HCA's id is its position in getHcaList(), and disableDevice() keeps the
// surviving ids there. initializeRdmaResources() used to append a context only
// for devices that came up, compacting context_list_ so a later device_id
// named the wrong RNIC or ran past its end.

constexpr const char *kAlignmentTopologyJson =
    R"({"cpu:0": [["mlx5_0", "mlx5_1", "mlx5_2"], []]})";

TEST(RdmaHcaIndexAlignmentTest, ContextListKeepsOneSlotPerHcaWhenInitFails) {
#ifndef __linux__
    GTEST_SKIP() << "Requires Linux libibverbs symbol interposition";
#else
    // No FakeVerbsDeviceScope: the interposed ibv_get_device_list reports zero
    // devices, so every construct() fails regardless of the host's hardware.
    auto topology = std::make_shared<Topology>();
    ASSERT_EQ(topology->parse(kAlignmentTopologyJson), 0);
    const auto hca_list = topology->getHcaList();
    ASSERT_EQ(hca_list.size(), static_cast<size_t>(3));

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    RdmaTransport transport;
    RdmaTransportTestPeer::bindMetadata(transport, metadata,
                                        "local-rdma-segment");
    RdmaTransportTestPeer::bindTopology(transport, topology);

    // Every device fails, so the topology ends up empty -- the slot layout
    // must line up with getHcaList() anyway.
    EXPECT_EQ(RdmaTransportTestPeer::initializeResources(transport),
              ERR_DEVICE_NOT_FOUND);

    const auto &contexts = transport.getContextList();
    ASSERT_EQ(contexts.size(), hca_list.size());
    for (size_t i = 0; i < contexts.size(); ++i) {
        ASSERT_NE(contexts[i], nullptr) << "slot " << i << " must be occupied";
        EXPECT_EQ(contexts[i]->deviceName(), hca_list[i])
            << "slot " << i << " names the wrong RNIC";
        EXPECT_FALSE(contexts[i]->active())
            << "placeholder for a failed RNIC must not report itself active";
    }
#endif  // __linux__
}

// Characterizes the contract the fix above relies on.
TEST(RdmaHcaIndexAlignmentTest, DisabledDeviceKeepsRemainingHcaIndexesStable) {
    Topology topology;
    ASSERT_EQ(topology.parse(kAlignmentTopologyJson), 0);
    const auto hca_list = topology.getHcaList();
    ASSERT_EQ(hca_list.size(), static_cast<size_t>(3));
    ASSERT_NE(std::find(hca_list.begin(), hca_list.end(), "mlx5_1"),
              hca_list.end());

    ASSERT_EQ(topology.disableDevice("mlx5_1"), 0);
    // getHcaList() must not shrink: ids are positions in it.
    ASSERT_EQ(topology.getHcaList().size(), hca_list.size());

    for (int retry_count = 0; retry_count < 16; ++retry_count) {
        const int device_id = topology.selectDevice("cpu:0", retry_count);
        ASSERT_GE(device_id, 0);
        ASSERT_LT(static_cast<size_t>(device_id), hca_list.size())
            << "device_id must index an hca_list-sized context array";
        EXPECT_NE(hca_list[device_id], "mlx5_1")
            << "a disabled device must never be selected";
    }
}

}  // namespace
