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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/shm/shm_transport.h"

namespace mooncake {
namespace tent {

class ShmTransportTestPeer {
   public:
    static Status relocate(ShmTransport& transport, uint64_t& address,
                           uint64_t length, SegmentID target_id) {
        return transport.relocateSharedMemoryAddress(address, length,
                                                     target_id);
    }

    static size_t mappingCount(ShmTransport& transport, SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        return it == transport.relocate_map_.end() ? 0 : it->second.size();
    }
};

namespace {

class ScopedShmFile {
   public:
    explicit ScopedShmFile(size_t length)
        : name_("/mooncake_tent_shm_test_" + std::to_string(getpid())),
          length_(length) {
        shm_unlink(name_.c_str());
        fd_ = shm_open(name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
        EXPECT_GE(fd_, 0);
        if (fd_ >= 0) EXPECT_EQ(ftruncate(fd_, length_), 0);
    }

    ~ScopedShmFile() {
        if (fd_ >= 0) close(fd_);
        shm_unlink(name_.c_str());
    }

    const std::string& name() const { return name_; }

   private:
    std::string name_;
    size_t length_;
    int fd_{-1};
};

TEST(ShmTransportTest, SharesAndReleasesRelocationAcrossThreads) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    constexpr uint64_t kRemoteAddress = 0x10000000;
    constexpr size_t kThreadCount = 8;
    ScopedShmFile shm_file(page_size);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) -> Status {
                        segment.name = "shm_test_segment";
                        segment.machine_id = "shm_test_machine";
                        segment.type = SegmentType::Memory;
                        auto& memory =
                            std::get<MemorySegmentDesc>(segment.detail);
                        BufferDesc buffer;
                        buffer.addr = kRemoteAddress;
                        buffer.length = page_size;
                        buffer.location = "cpu:0";
                        buffer.shm_path = shm_file.name();
                        memory.buffers.push_back(std::move(buffer));
                        return Status::OK();
                    })
                    .ok());

    ShmTransport transport;
    std::string local_segment_name = "shm_test_segment";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());

    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<uint64_t> relocated(kThreadCount, kRemoteAddress);
    std::vector<uint8_t> succeeded(kThreadCount, 0);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            succeeded[i] =
                ShmTransportTestPeer::relocate(transport, relocated[i],
                                               page_size, LOCAL_SEGMENT_ID)
                    .ok();
        });
    }
    while (ready.load(std::memory_order_acquire) != kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();

    for (uint8_t success : succeeded) EXPECT_TRUE(success);
    for (uint64_t address : relocated) EXPECT_EQ(address, relocated.front());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, LOCAL_SEGMENT_ID),
              1u);

    auto* mapped = reinterpret_cast<void*>(relocated.front());
    ASSERT_TRUE(transport.uninstall().ok());
    unsigned char residency = 0;
    errno = 0;
    EXPECT_EQ(mincore(mapped, page_size, &residency), -1);
    EXPECT_EQ(errno, ENOMEM);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
