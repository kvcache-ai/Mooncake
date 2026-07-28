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
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
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

    static bool hasTarget(ShmTransport& transport, SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        return transport.relocate_map_.find(target_id) !=
               transport.relocate_map_.end();
    }

    static void* createSharedMemory(ShmTransport& transport,
                                    const std::string& path, size_t size) {
        return transport.createSharedMemory(path, size);
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
        if (fd_ >= 0) {
            EXPECT_EQ(ftruncate(fd_, length_), 0);
        }
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

Status installLocalSegmentWithShm(ControlService& metadata,
                                  const std::string& shm_path,
                                  uint64_t remote_addr, size_t length) {
    return metadata.segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            segment.name = "shm_test_segment";
            segment.machine_id = "shm_test_machine";
            segment.type = SegmentType::Memory;
            auto& memory = std::get<MemorySegmentDesc>(segment.detail);
            memory.buffers.clear();
            BufferDesc buffer;
            buffer.addr = remote_addr;
            buffer.length = length;
            buffer.location = "cpu:0";
            buffer.shm_path = shm_path;
            memory.buffers.push_back(std::move(buffer));
            return Status::OK();
        });
}

TEST(ShmTransportTest, SharesAndReleasesRelocationAcrossThreads) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    constexpr uint64_t kRemoteAddress = 0x10000000;
    constexpr size_t kThreadCount = 8;
    ScopedShmFile shm_file(page_size);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithShm(*metadata, shm_file.name(),
                                           kRemoteAddress, page_size)
                    .ok());

    ShmTransport transport;
    std::string local_segment_name = "shm_test_segment";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());

    uint64_t missing_address = kRemoteAddress + page_size;
    EXPECT_TRUE(ShmTransportTestPeer::relocate(transport, missing_address,
                                               page_size, LOCAL_SEGMENT_ID)
                    .IsNeedsRefreshCache());
    EXPECT_FALSE(ShmTransportTestPeer::hasTarget(transport, LOCAL_SEGMENT_ID));

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

    uint64_t address_after_uninstall = kRemoteAddress;
    EXPECT_TRUE(ShmTransportTestPeer::relocate(transport,
                                               address_after_uninstall,
                                               page_size, LOCAL_SEGMENT_ID)
                    .IsInvalidArgument());
}

// B1: consumer CXL path must not create a missing file (would SIGBUS later).
TEST(ShmTransportTest, CxlConsumerDoesNotCreateMissingFile) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    constexpr uint64_t kRemoteAddress = 0x20000000;

    char tmpl[] = "/tmp/mooncake_shm_cxl_XXXXXX";
    ASSERT_NE(mkdtemp(tmpl), nullptr);
    const std::string cxl_dir(tmpl);
    const std::string shm_name =
        "mooncake_cxl_missing_" + std::to_string(getpid());
    const std::string full_path = cxl_dir + "/" + shm_name;

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithShm(*metadata, shm_name, kRemoteAddress,
                                           page_size)
                    .ok());

    auto conf = std::make_shared<Config>();
    conf->set("transports/shm/cxl_mount_path", cxl_dir);

    ShmTransport transport;
    std::string local_segment_name = "shm_test_segment";
    ASSERT_TRUE(
        transport.install(local_segment_name, metadata, nullptr, conf).ok());

    uint64_t address = kRemoteAddress;
    auto status = ShmTransportTestPeer::relocate(transport, address, page_size,
                                                 LOCAL_SEGMENT_ID);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInternalError());

    // The consumer must not have created the missing backing file.
    struct stat st;
    EXPECT_EQ(stat(full_path.c_str(), &st), -1);
    EXPECT_EQ(errno, ENOENT);

    ASSERT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(rmdir(cxl_dir.c_str()), 0);
}

// B2: creating with an existing name must not truncate the live object.
TEST(ShmTransportTest, CreateSharedMemoryDoesNotTruncateExisting) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const std::string name = "mooncake_excl_test_" + std::to_string(getpid());
    shm_unlink(name.c_str());

    int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, page_size), 0);
    void* existing =
        mmap(nullptr, page_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(existing, MAP_FAILED);
    close(fd);
    std::memset(existing, 0xAB, page_size);

    ShmTransport transport;
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    std::string local_segment_name = "shm_test_segment";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());

    errno = 0;
    void* created =
        ShmTransportTestPeer::createSharedMemory(transport, name, page_size);
    EXPECT_EQ(created, nullptr);
    EXPECT_EQ(errno, EEXIST);

    // Existing mapping content must be intact (not truncated to zero).
    auto* bytes = static_cast<unsigned char*>(existing);
    EXPECT_EQ(bytes[0], 0xAB);
    EXPECT_EQ(bytes[page_size - 1], 0xAB);

    // allocateLocalMemory should still succeed by picking a different name.
    MemoryOptions options;
    options.location = "cpu:0";
    void* allocated = nullptr;
    ASSERT_TRUE(
        transport.allocateLocalMemory(&allocated, page_size, options).ok());
    ASSERT_NE(allocated, nullptr);
    EXPECT_NE(options.shm_path, name);
    ASSERT_TRUE(transport.freeLocalMemory(allocated, page_size).ok());

    munmap(existing, page_size);
    shm_unlink(name.c_str());
    ASSERT_TRUE(transport.uninstall().ok());
}

// NeedsRefreshCache must not munmap cached mappings: memcpy may still be
// in flight on a previously resolved address after the relocate lock is
// released (no transfer-level refcount/quiesce yet).
TEST(ShmTransportTest, NeedsRefreshCacheKeepsExistingMappings) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    constexpr uint64_t kRemoteAddress = 0x30000000;
    ScopedShmFile shm_file(page_size);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithShm(*metadata, shm_file.name(),
                                           kRemoteAddress, page_size)
                    .ok());

    ShmTransport transport;
    std::string local_segment_name = "shm_test_segment";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());

    uint64_t address = kRemoteAddress;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, address, page_size,
                                               LOCAL_SEGMENT_ID)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, LOCAL_SEGMENT_ID),
              1u);
    auto* mapped = reinterpret_cast<void*>(address);

    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) -> Status {
                        auto& memory =
                            std::get<MemorySegmentDesc>(segment.detail);
                        memory.buffers.clear();
                        return Status::OK();
                    })
                    .ok());

    uint64_t missing_address = kRemoteAddress + page_size;
    auto status = ShmTransportTestPeer::relocate(transport, missing_address,
                                                 page_size, LOCAL_SEGMENT_ID);
    EXPECT_TRUE(status.IsNeedsRefreshCache());
    // Mapping must remain alive for any in-flight reader.
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, LOCAL_SEGMENT_ID),
              1u);
    EXPECT_TRUE(ShmTransportTestPeer::hasTarget(transport, LOCAL_SEGMENT_ID));

    unsigned char residency = 0;
    errno = 0;
    EXPECT_EQ(mincore(mapped, page_size, &residency), 0);

    ASSERT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
