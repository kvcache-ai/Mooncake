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

// MC_RDMA_NIC_SELECTION registration policy. Only verbs and NUMA are
// simulated; the production transport, chunking, MR maps, metadata and
// cleanup run. A buffer labeled "cuda:0" stands in for device memory,
// since an explicit location is trusted without querying the pointer.

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "memory_location.h"
#include "rdma_test_peers.h"
#include "transport/rdma_twosided/rdma_twosided_transport.h"

namespace {
std::atomic<int> calls{0}, live_mrs{0};
int fail_call = 0;
ibv_pd domains[3]{};
}  // namespace

#undef ibv_reg_mr
extern "C" ibv_mr *ibv_reg_mr(ibv_pd *pd, void *addr, size_t length, int) {
    const int call_number = ++calls;
    if (call_number == fail_call) {
        errno = ENOMEM;
        return nullptr;
    }
    auto *mr = new ibv_mr{};
    mr->addr = addr;
    mr->length = length;
    mr->lkey = 1 + static_cast<uint32_t>(pd - domains);
    mr->rkey = mr->lkey + 10;
    ++live_mrs;
    return mr;
}
extern "C" ibv_mr *ibv_reg_mr_iova2(ibv_pd *pd, void *addr, size_t length,
                                    uint64_t, unsigned int access) {
    return ibv_reg_mr(pd, addr, length, access);
}
extern "C" int ibv_dereg_mr(ibv_mr *mr) {
    --live_mrs;
    delete mr;
    return 0;
}
// Pages report NUMA node 1 once registered and no node before, so a "*" host
// buffer resolves to "cpu:1" only after pinning.
extern "C" int numa_move_pages(int, unsigned long count, void **, const int *,
                               int *status, int) {
    for (unsigned long i = 0; i < count; ++i)
        status[i] = live_mrs > 0 ? 1 : -ENOENT;
    return 0;
}

namespace mooncake {
namespace {

const size_t kPageSize = static_cast<size_t>(sysconf(_SC_PAGESIZE));
constexpr const char *kTopology = R"({"cuda:0":[["nic0"],["nic1","nic2"]],)"
                                  R"("cuda:1":[["nic1","nic2"],["nic0"]],)"
                                  R"("cpu:1":[["nic0","nic1","nic2"],[]]})";

// The parameter selects RdmaTwoSidedTransport, which inherits registration.
class RdmaRegistrationPolicyTest : public ::testing::TestWithParam<bool> {
   protected:
    void SetUp() override {
        calls = live_mrs = 0;
        fail_call = 0;
        old_limit_ = globalConfig().max_mr_size;
        old_parallel_ = globalConfig().parallel_reg_mr;
        old_selection_ = globalConfig().rdma_nic_selection;
        globalConfig().max_mr_size = kPageSize;
        globalConfig().parallel_reg_mr = 0;
        for (int i = 0; i < 2; ++i) {
            void *addr = mmap(nullptr, 2 * kPageSize, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            ASSERT_NE(addr, MAP_FAILED);
            buffers_.push_back(addr);
        }
    }

    void TearDown() override {
        if (transport_) {
            for (void *addr : buffers_)
                transport_->unregisterLocalMemory(addr, false);
        }
        EXPECT_EQ(live_mrs, 0);
        transport_.reset();
        for (void *addr : buffers_) munmap(addr, 2 * kPageSize);
        globalConfig().max_mr_size = old_limit_;
        globalConfig().parallel_reg_mr = old_parallel_;
        globalConfig().rdma_nic_selection = old_selection_;
    }

    // Builds a transport with one context per HCA of `topology`, in HCA index
    // order as initializeRdmaResources() does, which builds the local NIC map
    // only when `selection` is LOCAL.
    void install(NicSelection selection,
                 const std::string &topology = kTopology) {
        topology_ = std::make_shared<Topology>();
        ASSERT_EQ(topology_->parse(topology), 0);
        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
        desc->name = "registration-policy-test";
        desc->protocol = "rdma";
        desc->topology = *topology_;
        for (const auto &name : topology_->getHcaList())
            desc->devices.push_back({name, 0, "", ""});
        ASSERT_EQ(metadata_->addLocalSegment(LOCAL_SEGMENT_ID, desc->name,
                                             std::move(desc)),
                  0);
        if (GetParam())
            transport_ = std::make_unique<RdmaTwoSidedTransport>(selection);
        else
            transport_ = std::make_unique<RdmaTransport>(selection);
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "registration-policy-test");
        RdmaTransportTestPeer::bindTopology(*transport_, topology_);
        size_t i = 0;
        for (const auto &name : topology_->getHcaList()) {
            auto context = std::make_shared<RdmaContext>(*transport_, name);
            RdmaContextTestPeer::bindProtectionDomain(*context, &domains[i++]);
            RdmaTransportTestPeer::addContext(*transport_, context);
        }
        RdmaTransportTestPeer::initializeLocalNicMap(*transport_);
    }

    int reg(size_t buffer, const std::string &location,
            size_t size = kPageSize) {
        return transport_->registerLocalMemory(buffers_[buffer], size, location,
                                               true, false);
    }

    // Key slot of `hca`, i.e. its context index.
    int index(const std::string &hca) {
        const auto &contexts = transport_->getContextList();
        for (size_t i = 0; i < contexts.size(); ++i)
            if (contexts[i]->deviceName() == hca) return static_cast<int>(i);
        return -1;
    }

    std::shared_ptr<TransferMetadata::SegmentDesc> desc() {
        return metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    }

    // Expects every BufferDesc to hold a nonzero lkey and rkey exactly at the
    // indices of `hcas`, with one slot per context.
    void expectKeyedOn(const std::vector<std::string> &hcas) {
        const auto segment = desc();
        ASSERT_FALSE(segment->buffers.empty());
        for (const auto &buffer : segment->buffers) {
            ASSERT_EQ(buffer.lkey.size(), 3u);
            ASSERT_EQ(buffer.rkey.size(), 3u);
            for (int i = 0; i < 3; ++i) {
                bool expected = false;
                for (const auto &hca : hcas) expected |= index(hca) == i;
                EXPECT_EQ(buffer.lkey[i] != 0, expected) << i;
                EXPECT_EQ(buffer.rkey[i] != 0, expected) << i;
            }
        }
    }

    std::vector<void *> buffers_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::shared_ptr<Topology> topology_;
    uint64_t old_limit_ = 0;
    int old_parallel_ = 0;
    NicSelection old_selection_ = NicSelection::ALL;
};

TEST_P(RdmaRegistrationPolicyTest, AllRegistersGpuBufferOnEveryHca) {
    install(NicSelection::ALL);
    ASSERT_EQ(reg(0, "cuda:0"), 0);
    EXPECT_EQ(calls, 3);
    expectKeyedOn({"nic0", "nic1", "nic2"});
}

TEST_P(RdmaRegistrationPolicyTest, LocalRegistersGpuBufferOnPreferredHcas) {
    install(NicSelection::LOCAL);
    ASSERT_EQ(reg(0, "cuda:0"), 0);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(desc()->buffers[0].name, "cuda:0");
    expectKeyedOn({"nic0"});
}

// The selection passed at construction wins over MC_RDMA_NIC_SELECTION, so
// engines in one process can differ. The LOCAL tests cover the other direction.
TEST_P(RdmaRegistrationPolicyTest, ExplicitAllOverridesGlobalLocal) {
    globalConfig().rdma_nic_selection = NicSelection::LOCAL;
    install(NicSelection::ALL);
    ASSERT_EQ(reg(0, "cuda:0"), 0);
    EXPECT_EQ(calls, 3);
    expectKeyedOn({"nic0", "nic1", "nic2"});
}

TEST_P(RdmaRegistrationPolicyTest, LocalKeepsWildcardHostBufferOnEveryHca) {
    install(NicSelection::LOCAL);
    ASSERT_EQ(reg(0, kWildcardLocation), 0);
    EXPECT_EQ(calls, 3);
    EXPECT_EQ(desc()->buffers[0].name, "cpu:1");
    expectKeyedOn({"nic0", "nic1", "nic2"});
}

// Only GPU entries are narrowed, even if the topology prefers a subset of the
// HCAs for a NUMA node.
TEST_P(RdmaRegistrationPolicyTest, LocalIgnoresHostPreferredSubset) {
    install(NicSelection::LOCAL, R"({"cpu:0":[["nic0"],["nic1","nic2"]]})");
    ASSERT_EQ(reg(0, "cpu:0"), 0);
    EXPECT_EQ(calls, 3);
}

TEST_P(RdmaRegistrationPolicyTest, UnknownGpuLocationRegistersOnEveryHca) {
    install(NicSelection::LOCAL);
    ASSERT_EQ(reg(0, "cuda:99"), 0);
    EXPECT_EQ(calls, 3);
}

TEST_P(RdmaRegistrationPolicyTest, AllPreferredInactiveStillNarrows) {
    install(NicSelection::LOCAL);
    transport_->getContextList()[index("nic0")]->set_active(false);
    ASSERT_EQ(reg(0, "cuda:0"), 0);
    EXPECT_EQ(calls, 1);
    expectKeyedOn({"nic0"});
}

TEST_P(RdmaRegistrationPolicyTest, SequentialAndParallelChunksShareLocalNics) {
    install(NicSelection::LOCAL);
    for (int parallel : {0, 1}) {
        globalConfig().parallel_reg_mr = parallel;
        calls = 0;
        ASSERT_EQ(reg(0, "cuda:0", 2 * kPageSize), 0);
        EXPECT_EQ(calls, 2) << parallel;
        ASSERT_EQ(desc()->buffers.size(), 2u);
        expectKeyedOn({"nic0"});
        ASSERT_EQ(transport_->unregisterLocalMemory(buffers_[0], false), 0);
        EXPECT_EQ(live_mrs, 0);
    }
}

TEST_P(RdmaRegistrationPolicyTest, FailedChunkRollsBackNarrowedRegistration) {
    install(NicSelection::LOCAL);
    fail_call = 2;
    EXPECT_NE(reg(0, "cuda:0", 2 * kPageSize), 0);
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(live_mrs, 0);
    EXPECT_TRUE(desc()->buffers.empty());
}

TEST_P(RdmaRegistrationPolicyTest, BatchNarrowsEveryBuffer) {
    install(NicSelection::LOCAL);
    ASSERT_EQ(
        transport_->registerLocalMemoryBatch(
            {{buffers_[0], kPageSize}, {buffers_[1], kPageSize}}, "cuda:1"),
        0);
    EXPECT_EQ(calls, 4);
    ASSERT_EQ(desc()->buffers.size(), 2u);
    expectKeyedOn({"nic1", "nic2"});
}

// A preferred HCA that failed to open is a placeholder without a PD; its
// registration succeeds without an MR, which must not publish a keyless buffer.
TEST_P(RdmaRegistrationPolicyTest, NarrowedSetWithoutMrFails) {
    install(NicSelection::LOCAL);
    RdmaContextTestPeer::bindProtectionDomain(
        *transport_->getContextList()[index("nic0")], nullptr);
    EXPECT_NE(reg(0, "cuda:0", 2 * kPageSize), 0);
    EXPECT_EQ(calls, 0);
    EXPECT_EQ(live_mrs, 0);
    EXPECT_TRUE(desc()->buffers.empty());
}

INSTANTIATE_TEST_SUITE_P(RdmaAndTwoSided, RdmaRegistrationPolicyTest,
                         ::testing::Bool());

}  // namespace
}  // namespace mooncake
