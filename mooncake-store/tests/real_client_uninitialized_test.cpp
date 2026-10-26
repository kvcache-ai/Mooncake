// real_client_uninitialized_test.cpp
//
// Regression test for batch entry points on a RealClient whose setup never
// completed (e.g. setup_real exhausted its retries and left client_ null).
//
// The batch wrappers time their internal call with execute_timed_operation and
// report the observed bytes via client_->ObserveTransferOperation. They used an
// always-true success predicate, so after the internal call had already logged
// "Client is not initialized" and returned errors, the observer still
// dereferenced the null client_ and crashed the host process. Each call below
// must instead return per-key errors without crashing.

#include "real_client.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace mooncake {
namespace testing {

class RealClientUninitializedTest : public ::testing::Test {
   protected:
    void SetUp() override {
        client_ = RealClient::create();
        buffers_.assign(keys_.size(), data_);
        sizes_.assign(keys_.size(), sizeof(data_));
        multi_buffers_.assign(keys_.size(), std::vector<void *>{data_});
        multi_sizes_.assign(keys_.size(), std::vector<size_t>{sizeof(data_)});
    }

    std::shared_ptr<RealClient> client_;
    const std::vector<std::string> keys_{"key_a", "key_b"};
    char data_[64] = {};
    std::vector<void *> buffers_;
    std::vector<size_t> sizes_;
    std::vector<std::vector<void *>> multi_buffers_;
    std::vector<std::vector<size_t>> multi_sizes_;
};

template <typename T>
void ExpectAllFailed(const std::vector<T> &results, size_t expected_size) {
    ASSERT_EQ(results.size(), expected_size);
    for (const auto &result : results) {
        EXPECT_LT(result, 0);
    }
}

TEST_F(RealClientUninitializedTest, BatchPutFromReturnsErrors) {
    ExpectAllFailed(client_->batch_put_from(keys_, buffers_, sizes_),
                    keys_.size());
}

TEST_F(RealClientUninitializedTest, BatchUpsertFromReturnsErrors) {
    ExpectAllFailed(client_->batch_upsert_from(keys_, buffers_, sizes_),
                    keys_.size());
}

TEST_F(RealClientUninitializedTest, BatchGetIntoReturnsErrors) {
    ExpectAllFailed(client_->batch_get_into(keys_, buffers_, sizes_),
                    keys_.size());
}

TEST_F(RealClientUninitializedTest, BatchPutFromMultiBuffersReturnsErrors) {
    ExpectAllFailed(client_->batch_put_from_multi_buffers(keys_, multi_buffers_,
                                                          multi_sizes_),
                    keys_.size());
}

TEST_F(RealClientUninitializedTest, BatchUpsertFromMultiBuffersReturnsErrors) {
    ExpectAllFailed(client_->batch_upsert_from_multi_buffers(
                        keys_, multi_buffers_, multi_sizes_),
                    keys_.size());
}

TEST_F(RealClientUninitializedTest, BatchGetIntoMultiBuffersReturnsErrors) {
    ExpectAllFailed(client_->batch_get_into_multi_buffers(keys_, multi_buffers_,
                                                          multi_sizes_, false),
                    keys_.size());
}

TEST_F(RealClientUninitializedTest, BatchGetBufferReturnsNullHandles) {
    auto results = client_->batch_get_buffer(keys_);
    ASSERT_EQ(results.size(), keys_.size());
    for (const auto &handle : results) {
        EXPECT_EQ(handle, nullptr);
    }
}

TEST_F(RealClientUninitializedTest, GetIntoRangesDoesNotCrash) {
    std::vector<void *> range_buffers{data_};
    std::vector<std::vector<std::string>> all_keys{keys_};
    std::vector<std::vector<std::vector<size_t>>> offsets{
        {std::vector<size_t>{0}, std::vector<size_t>{0}}};
    std::vector<std::vector<std::vector<size_t>>> range_sizes{
        {std::vector<size_t>{8}, std::vector<size_t>{8}}};
    auto results = client_->get_into_ranges(range_buffers, all_keys, offsets,
                                            offsets, range_sizes);
    ASSERT_EQ(results.size(), 1u);
}

}  // namespace testing
}  // namespace mooncake
