#include "get_buffer_fixture.h"

namespace mooncake {
namespace testing {
TEST_F(DummyClientGetBufferTest,
       StableHotCacheEntryShouldBeInvalidatedByRemove) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "stable_hot_cache_remove_invalidation";

    const char fill = static_cast<char>(7);
    const std::string data(kPayloadSize, fill);

    PutData(key, data);
    WarmHotCache(key);

    auto hot_buf_1 = dummy_client_->get_buffer(key);
    ASSERT_NE(hot_buf_1, nullptr)
        << "dummy get_buffer should succeed after WarmHotCache";
    ASSERT_EQ(hot_buf_1->size(), data.size())
        << "hot cache buffer size mismatch before remove";
    ASSERT_TRUE(dummy_client_->is_hot_cache_ptr(hot_buf_1->ptr()))
        << "buffer should be in hot cache shm before remove";

    {
        std::string got(static_cast<char *>(hot_buf_1->ptr()),
                        hot_buf_1->size());
        ASSERT_EQ(got.size(), data.size());
        ASSERT_TRUE(std::equal(got.begin(), got.end(), data.begin()))
            << "hot cache data mismatch before remove";
    }

    auto hot_buf_2 = dummy_client_->get_buffer(key);
    ASSERT_NE(hot_buf_2, nullptr)
        << "second dummy get_buffer should still succeed before remove";
    ASSERT_TRUE(dummy_client_->is_hot_cache_ptr(hot_buf_2->ptr()))
        << "second buffer should still be in hot cache shm before remove";
    ASSERT_EQ(hot_buf_2->size(), data.size())
        << "second hot cache buffer size mismatch before remove";

    {
        std::string got(static_cast<char *>(hot_buf_2->ptr()),
                        hot_buf_2->size());
        ASSERT_EQ(got.size(), data.size());
        ASSERT_TRUE(std::equal(got.begin(), got.end(), data.begin()))
            << "second hot cache data mismatch before remove";
    }

    int remove_rc = real_client_->remove(key, true);
    ASSERT_EQ(remove_rc, 0) << "remove failed";

    auto after_remove_buf = dummy_client_->get_buffer(key);
    if (after_remove_buf != nullptr) {
        const bool after_remove_is_hot =
            dummy_client_->is_hot_cache_ptr(after_remove_buf->ptr());
        std::cerr << "[StableHotCacheEntryShouldBeInvalidatedByRemove] "
                  << "after remove get_buffer returned non-null; "
                  << "size=" << after_remove_buf->size()
                  << ", is_hot_cache_ptr=" << after_remove_is_hot << std::endl;
        FAIL() << "remove(key, true) succeeded, but dummy get_buffer(key) "
               << "still returned a non-null buffer";
    }
}

// ---- Diagnosis: removed key must not be resurrected by async hot-cache fill
// ----
//
// Compare against pre-fix behavior:
//   mooncake-store/tests/scripts/compare_hot_cache_fix.sh all
TEST_F(DummyClientGetBufferTest,
       RemoveShouldNotBeResurrectedByAsyncHotCacheFill) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "async_fill_resurrect_removed_key";
    constexpr int kRounds = 8;

    for (int round = 0; round < kRounds; ++round) {
        SCOPED_TRACE("round=" + std::to_string(round));

        const char fill = static_cast<char>(1 + (round % 250));
        const std::string data(kPayloadSize, fill);

        PutData(key, data);

        // Two fallback reads reach CMS admission threshold (default 2) and
        // submit async fill, but we do not wait for worker PutHotKey.
        TriggerAdmissionWithoutWaiting(key);

        int remove_rc = real_client_->remove(key, true);
        ASSERT_EQ(remove_rc, 0) << "remove failed at round=" << round;

        auto immediately_after_remove = dummy_client_->get_buffer(key);
        const bool immediate_non_null = (immediately_after_remove != nullptr);
        bool immediate_is_hot = false;
        size_t immediate_size = 0;
        if (immediate_non_null) {
            immediate_is_hot = dummy_client_->is_hot_cache_ptr(
                immediately_after_remove->ptr());
            immediate_size = immediately_after_remove->size();
            std::cerr << "[round=" << round
                      << "] immediately after remove: NON_NULL"
                      << ", size=" << immediate_size
                      << ", is_hot_cache_ptr=" << immediate_is_hot << std::endl;
        } else {
            std::cerr << "[round=" << round
                      << "] immediately after remove: nullptr" << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(2000));

        auto delayed_after_remove = dummy_client_->get_buffer(key);
        const bool delayed_non_null = (delayed_after_remove != nullptr);
        bool delayed_is_hot = false;
        size_t delayed_size = 0;
        if (delayed_non_null) {
            delayed_is_hot =
                dummy_client_->is_hot_cache_ptr(delayed_after_remove->ptr());
            delayed_size = delayed_after_remove->size();
            std::cerr << "[round=" << round
                      << "] delayed after remove: NON_NULL"
                      << ", size=" << delayed_size
                      << ", is_hot_cache_ptr=" << delayed_is_hot << std::endl;
        } else {
            std::cerr << "[round=" << round << "] delayed after remove: nullptr"
                      << std::endl;
        }

        if (immediate_non_null || delayed_non_null) {
            if (!immediate_non_null && delayed_non_null) {
                FAIL() << "FAILED at round=" << round
                       << ": key was nullptr immediately after remove, "
                       << "but became non-null after waiting. "
                       << "This strongly suggests async hot-cache fill "
                       << "resurrected a removed key. "
                       << "delayed_is_hot_cache_ptr=" << delayed_is_hot
                       << ", delayed_size=" << delayed_size;
            }
            if (immediate_non_null && delayed_non_null) {
                FAIL() << "FAILED at round=" << round
                       << ": key was non-null immediately after remove "
                       << "and remained non-null after waiting. "
                       << "immediate_is_hot_cache_ptr=" << immediate_is_hot
                       << ", delayed_is_hot_cache_ptr=" << delayed_is_hot;
            }
            if (immediate_non_null && !delayed_non_null) {
                FAIL() << "FAILED at round=" << round
                       << ": key was non-null immediately after remove, "
                       << "but became nullptr after waiting. "
                       << "immediate_is_hot_cache_ptr=" << immediate_is_hot;
            }
        }
    }
}

// ---- Test: get_buffer via allocator fallback (no hot cache hit) ----
}  // namespace testing
}  // namespace mooncake
