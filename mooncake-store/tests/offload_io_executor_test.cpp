#include "offload_io_executor.h"
#include <gtest/gtest.h>
#include <atomic>
#include <barrier>
#include <future>
#include <mutex>
#include <set>
#include <vector>
#include <asio/post.hpp>

namespace mooncake {
TEST(OffloadIoExecutorTest, ValidatesWorkerCount) {
    const char* original = std::getenv("MOONCAKE_OFFLOAD_IO_WORKERS");
    const std::string saved = original ? original : "";
    for (const char* value :
         {"", "0", "-1", "257", "4junk", "999999999999999"}) {
        setenv("MOONCAKE_OFFLOAD_IO_WORKERS", value, 1);
        EXPECT_EQ(OffloadIoWorkers(), 4U);
    }
    setenv("MOONCAKE_OFFLOAD_IO_WORKERS", "2", 1);
    EXPECT_EQ(OffloadIoWorkers(), 2U);
    if (original)
        setenv("MOONCAKE_OFFLOAD_IO_WORKERS", saved.c_str(), 1);
    else
        unsetenv("MOONCAKE_OFFLOAD_IO_WORKERS");
}
TEST(OffloadIoExecutorTest, BoundsWorkersAndDrainsOnStop) {
    const char* original = std::getenv("MOONCAKE_OFFLOAD_IO_WORKERS");
    const std::string saved = original ? original : "";
    setenv("MOONCAKE_OFFLOAD_IO_WORKERS", "2", 1);
    std::atomic<unsigned> done{0};
    std::mutex mutex;
    std::set<std::thread::id> threads;
    std::barrier started(2);
    std::promise<void> ready;
    auto running = ready.get_future();
    {
        OffloadIoExecutor executor;
        for (int i = 0; i < 100; ++i) {
            asio::post(executor.GetExecutor()->get_asio_executor(), [&, i] {
                if (i < 2) started.arrive_and_wait();
                if (i == 1) ready.set_value();
                std::lock_guard lock(mutex);
                threads.insert(std::this_thread::get_id());
                ++done;
            });
        }
        ASSERT_EQ(running.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        executor.Stop();
        EXPECT_EQ(done, 100U);
        EXPECT_EQ(threads.size(), 2U);
        executor.Stop();
    }
    if (original)
        setenv("MOONCAKE_OFFLOAD_IO_WORKERS", saved.c_str(), 1);
    else
        unsetenv("MOONCAKE_OFFLOAD_IO_WORKERS");
}
}  // namespace mooncake
