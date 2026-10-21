#include <gtest/gtest.h>
#include <numa.h>
#include <sys/mman.h>

#include <cerrno>
#include <memory>

#include "config.h"
#include "memory_location.h"
#include "rdma_test_peers.h"

namespace {
int registered_regions = 0;
int location_queries = 0;
int registration_calls = 0;
int fail_registration = 0;
ibv_pd test_pd{};
}  // namespace

// Model the verbs/NUMA boundary: an untouched page has no NUMA node until
// MR registration pins it. The real transport, MR map and metadata paths run.
#undef ibv_reg_mr
extern "C" ibv_mr *ibv_reg_mr(ibv_pd *, void *addr, size_t length, int) {
    if (++registration_calls == fail_registration) {
        errno = ENOMEM;
        return nullptr;
    }
    auto *mr = new ibv_mr{};
    mr->addr = addr;
    mr->length = length;
    mr->lkey = 1;
    mr->rkey = 2;
    ++registered_regions;
    return mr;
}

extern "C" ibv_mr *ibv_reg_mr_iova2(ibv_pd *pd, void *addr, size_t length,
                                    uint64_t, unsigned int access) {
    return ibv_reg_mr(pd, addr, length, access);
}

extern "C" int ibv_dereg_mr(ibv_mr *mr) {
    --registered_regions;
    delete mr;
    return 0;
}

extern "C" int numa_move_pages(int, unsigned long count, void **, const int *,
                               int *status, int) {
    ++location_queries;
    for (unsigned long i = 0; i < count; ++i)
        status[i] = registered_regions > 0 ? 1 : -ENOENT;
    return 0;
}

namespace mooncake {
namespace {

class RdmaMemoryLocationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        registered_regions = location_queries = registration_calls = 0;
        fail_registration = 0;
        old_chunk_limit_ = globalConfig().max_mr_size;
        old_parallel_ = globalConfig().parallel_reg_mr;
        globalConfig().max_mr_size = pagesize;
        globalConfig().parallel_reg_mr = 0;

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
        desc->name = "memory-location-test";
        desc->protocol = "rdma";
        ASSERT_EQ(
            metadata_->addLocalSegment(LOCAL_SEGMENT_ID, "memory-location-test",
                                       std::move(desc)),
            0);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "memory-location-test");
        auto context = std::make_shared<RdmaContext>(*transport_, "test-nic");
        RdmaContextTestPeer::bindProtectionDomain(*context, &test_pd);
        RdmaTransportTestPeer::addContext(*transport_, std::move(context));
        buffer_ = mmap(nullptr, 2 * pagesize, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(buffer_, MAP_FAILED);
    }

    void TearDown() override {
        if (buffer_ != MAP_FAILED) {
            transport_->unregisterLocalMemory(buffer_, false);
            munmap(buffer_, 2 * pagesize);
        }
        EXPECT_EQ(registered_regions, 0);
        transport_.reset();
        globalConfig().max_mr_size = old_chunk_limit_;
        globalConfig().parallel_reg_mr = old_parallel_;
    }

    void *buffer_ = MAP_FAILED;
    uint64_t old_chunk_limit_ = 0;
    int old_parallel_ = 0;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
};

TEST_F(RdmaMemoryLocationTest, UntouchedBufferResolvesAfterRegistration) {
    ASSERT_EQ(transport_->registerLocalMemory(buffer_, pagesize,
                                              kWildcardLocation, true, false),
              0);
    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(desc->buffers.size(), 1);
    EXPECT_EQ(desc->buffers[0].name, "cpu:1");
    EXPECT_EQ(location_queries, 1);
}

TEST_F(RdmaMemoryLocationTest, ChunksReuseResolvedOriginalLocation) {
    ASSERT_EQ(transport_->registerLocalMemory(buffer_, 2 * pagesize,
                                              kWildcardLocation, true, false),
              0);
    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(desc->buffers.size(), 2);
    for (const auto &buffer : desc->buffers) EXPECT_EQ(buffer.name, "cpu:1");
    EXPECT_EQ(location_queries, 1);
    EXPECT_EQ(registered_regions, 2);
}

TEST_F(RdmaMemoryLocationTest, ExplicitLocationIsPreserved) {
    ASSERT_EQ(transport_->registerLocalMemory(buffer_, pagesize, "cpu:7", true,
                                              false),
              0);
    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(desc->buffers.size(), 1);
    EXPECT_EQ(desc->buffers[0].name, "cpu:7");
    EXPECT_EQ(location_queries, 0);
}

TEST_F(RdmaMemoryLocationTest, LaterRegistrationFailureStillRollsBack) {
    fail_registration = 2;
    EXPECT_NE(transport_->registerLocalMemory(buffer_, 2 * pagesize,
                                              kWildcardLocation, true, false),
              0);
    EXPECT_TRUE(
        metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
    EXPECT_EQ(registered_regions, 0);
}

}  // namespace
}  // namespace mooncake
