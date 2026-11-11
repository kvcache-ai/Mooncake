#pragma once

#include <charconv>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <thread>
#include <glog/logging.h>
#include <ylt/coro_io/io_context_pool.hpp>

namespace mooncake {

inline unsigned OffloadIoWorkers() {
    constexpr unsigned kDefaultWorkers = 4;
    const char* raw = std::getenv("MOONCAKE_OFFLOAD_IO_WORKERS");
    if (!raw) return kDefaultWorkers;
    std::string_view text(raw);
    unsigned workers = 0;
    auto [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), workers);
    if (error != std::errc{} || end != text.data() + text.size() ||
        workers == 0 || workers > 256) {
        LOG(WARNING) << "Invalid MOONCAKE_OFFLOAD_IO_WORKERS; using "
                     << kDefaultWorkers;
        return kDefaultWorkers;
    }
    return workers;
}

// A dedicated, bounded executor. Only SSD owner requests instantiate it.
// Drain jobs and destroy thread-local rings before releasing FileStorage.
class OffloadIoExecutor {
   public:
    OffloadIoExecutor()
        : pool_(OffloadIoWorkers()), runner_([this] { pool_.run(); }) {}
    ~OffloadIoExecutor() { Stop(); }
    void Stop() {
        if (runner_.joinable()) {
            pool_.stop();
            runner_.join();
        }
    }
    auto* GetExecutor() { return pool_.get_executor(); }

   private:
    coro_io::io_context_pool pool_;
    std::thread runner_;
};
}  // namespace mooncake
