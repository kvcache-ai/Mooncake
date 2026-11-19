#include <gtest/gtest.h>

#include <async_simple/Try.h>
#include <async_simple/coro/SyncAwait.h>
#include <chrono>
#include <future>
#include <thread>

#include "real_client.h"

namespace mooncake {
class RealClientDummyReadTestPeer {
   public:
    static bool Drain(RealClient& client) {
        return client.dummy_read_drain_.drain_for(std::chrono::milliseconds(0));
    }
};

TEST(RealClientDummyReadTest, SubmissionDoesNotWaitForTheShmLock) {
    using namespace std::chrono_literals;
    using Results = std::vector<tl::expected<int64_t, ErrorCode>>;
    RealClient client;
    std::unique_lock shm_lock(client.dummy_client_mutex_);
    const std::vector<std::string> keys{"missing"};
    const std::vector<uint64_t> buffers{0};
    const std::vector<size_t> sizes{1};
    const UUID id{1, 1};
    std::promise<void> submitted;
    auto submitted_future = submitted.get_future();
    std::promise<Results> completed;
    auto result_future = completed.get_future();
    asio::io_context caller_context;
    coro_io::ExecutorWrapper<> executor(caller_context.get_executor());
    asio::post(caller_context, [&] {
        auto read =
            client.batch_get_into_dummy_helper(keys, buffers, sizes, 0, id);
        read.directlyStart(
            [&](async_simple::Try<Results> result) {
                if (result.hasError()) {
                    completed.set_exception(result.getException());
                } else {
                    completed.set_value(std::move(result).value());
                }
            },
            &executor);
        submitted.set_value();
    });
    std::thread caller([&] { caller_context.run(); });

    // The worker can wait for SHM, but the RPC/coroutine caller must be free.
    const auto submission = submitted_future.wait_for(2s);
    const auto before_unlock = result_future.wait_for(0s);
    // Preserve a non-hanging negative control for the old caller-side lock.
    if (submission != std::future_status::ready) {
        shm_lock.unlock();
        caller.join();
        FAIL() << "Submission waited for the SHM lock";
    }
    caller_context.stop();
    caller.join();
    EXPECT_FALSE(RealClientDummyReadTestPeer::Drain(client));
    auto close =
        std::async(std::launch::async, [&] { return client.tearDownAll(); });
    const auto before_release = close.wait_for(100ms);
    shm_lock.unlock();
    const auto completed_without_executor = close.wait_for(5s);
    // If a future dependency changes post's inline completion semantics,
    // restore the executor for cleanup while preserving the failed verdict.
    if (completed_without_executor != std::future_status::ready) {
        caller_context.restart();
        caller_context.run();
    }
    EXPECT_EQ(close.get(), 0);
    EXPECT_EQ(before_release, std::future_status::timeout);
    EXPECT_EQ(completed_without_executor, std::future_status::ready);
    EXPECT_TRUE(RealClientDummyReadTestPeer::Drain(client));
    EXPECT_EQ(before_unlock, std::future_status::timeout);
    ASSERT_EQ(result_future.wait_for(5s), std::future_status::ready);
    auto result = result_future.get();
    ASSERT_EQ(result.size(), 1);
    ASSERT_FALSE(result[0].has_value());
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(RealClientDummyReadTest, MappedReadReleasesTheWorkerOwnedLock) {
    RealClient client;
    const UUID id{2, 1};
    char storage[16] = {};
    // The context has a real translation, but no initialized backend client.
    // This must enter the posted read path before returning INVALID_PARAMS.
    RealClient::MappedShm mapping{};
    mapping.dummy_base_addr = 4096;
    mapping.shm_addr_offset = reinterpret_cast<uintptr_t>(storage) - 4096;
    mapping.shm_buffer = storage;
    mapping.shm_size = sizeof(storage);
    client.shm_contexts_[id].mapped_shms.push_back(mapping);
    const std::vector<std::string> keys{"mapped"};
    const std::vector<uint64_t> buffers{4096};
    const std::vector<size_t> sizes{1};
    auto result = async_simple::coro::syncAwait(
        client.batch_get_into_dummy_helper(keys, buffers, sizes, 0, id));
    EXPECT_EQ(result.size(), 1);
    EXPECT_FALSE(result[0].has_value());
    // Avoid asking teardown to munmap stack storage.
    std::unique_lock lock(client.dummy_client_mutex_);
    client.shm_contexts_.clear();
}

}  // namespace mooncake
