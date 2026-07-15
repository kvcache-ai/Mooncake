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

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <memory>
#include <string>

#ifdef __linux__
#include <limits>
#endif

#include "common.h"
#include "error.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

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
    port_attr->lid = 1;
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

namespace mooncake {

class RdmaTransportTestPeer {
   public:
    static void bindMetadata(RdmaTransport &transport,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::string local_server_name) {
        transport.metadata_ = std::move(metadata);
        transport.local_server_name_ = std::move(local_server_name);
    }
};

class RdmaContextTestPeer {
   public:
    static bool hasEndpointStore(const RdmaContext &context) {
        return context.endpoint_store_ != nullptr;
    }

    static void seedAutoGidState(RdmaContext &context, ibv_context *verbs_ctx,
                                 uint8_t port, uint16_t lid, const ibv_gid &gid,
                                 int gid_index) {
        context.context_ = verbs_ctx;
        context.port_ = port;
        context.lid_ = lid;
        context.gid_ = gid;
        context.gid_index_ = gid_index;
        context.auto_gid_selection_enabled_ = true;
    }

    static void disableContextForTeardown(RdmaContext &context) {
        context.context_ = nullptr;
    }
};

}  // namespace mooncake

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

}  // namespace
