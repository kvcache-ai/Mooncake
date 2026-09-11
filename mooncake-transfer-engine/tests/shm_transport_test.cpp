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

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common.h"
#include "multi_transport.h"
#include "transfer_metadata.h"
#include "transport/shm_transport/shm_transport.h"

namespace mooncake {

class MultiTransportTestPeer {
   public:
    static Status selectTransport(MultiTransport& multi,
                                  const Transport::TransferRequest& entry,
                                  Transport*& transport) {
        return multi.selectTransport(entry, transport);
    }
};

class ShmTransportTestPeer {
   public:
    static int install(ShmTransport& transport, std::string& local_server_name,
                       std::shared_ptr<TransferMetadata> metadata) {
        std::shared_ptr<Topology> topo;
        return transport.install(local_server_name, metadata, topo);
    }

    static Status relocate(ShmTransport& transport, uint64_t& address,
                           uint64_t length, Transport::SegmentID target_id) {
        return transport.relocateSharedMemoryAddress(address, length,
                                                     target_id);
    }

    static Status relocatePinned(
        ShmTransport& transport, uint64_t& address, uint64_t length,
        Transport::SegmentID target_id,
        std::unique_ptr<ShmTransport::MappingPin>& pin) {
        pin = std::make_unique<ShmTransport::MappingPin>();
        return transport.relocateSharedMemoryAddress(address, length, target_id,
                                                     pin.get());
    }

    static uint32_t pinCount(ShmTransport& transport,
                             Transport::SegmentID target_id, uint64_t addr) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        if (it == transport.relocate_map_.end()) return 0;
        auto mapping = it->second.find(addr);
        if (mapping == it->second.end() || !mapping->second) return 0;
        return mapping->second->pin_count.load();
    }

    static size_t mappingCount(ShmTransport& transport,
                               Transport::SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        return it == transport.relocate_map_.end() ? 0 : it->second.size();
    }

    static size_t maxMappingsPerTarget() {
        return ShmTransport::kMaxMappingsPerTarget;
    }

    static void* createSharedMemory(ShmTransport& transport,
                                    const std::string& path, size_t size,
                                    int* error = nullptr) {
        return transport.createSharedMemory(path, size, error);
    }

    static int registerLocalMemory(ShmTransport& transport, void* addr,
                                   size_t length,
                                   const std::string& location = "cpu:0") {
        return transport.registerLocalMemory(addr, length, location, true,
                                             true);
    }

    static int registerLocalMemoryBatch(
        ShmTransport& transport,
        const std::vector<Transport::BufferEntry>& buffer_list,
        const std::string& location = "cpu:0") {
        return transport.registerLocalMemoryBatch(buffer_list, location);
    }
};

namespace {

constexpr Transport::SegmentID kPeerSegmentId = 1;
constexpr uint64_t kRemoteAddress = 0x10000000;

class ScopedShmFile {
   public:
    explicit ScopedShmFile(size_t length, const std::string& tag = "test")
        : name_(std::string(kPosixShmNamePrefix) + tag + "_" +
                std::to_string(getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(this))),
          length_(length) {
        shm_unlink(name_.c_str());
        fd_ = shm_open(name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
        EXPECT_GE(fd_, 0);
        if (fd_ >= 0) {
            EXPECT_EQ(ftruncate(fd_, static_cast<off_t>(length_)), 0);
            addr_ = mmap(nullptr, length_, PROT_READ | PROT_WRITE, MAP_SHARED,
                         fd_, 0);
            EXPECT_NE(addr_, MAP_FAILED);
        }
    }

    ~ScopedShmFile() {
        if (addr_ && addr_ != MAP_FAILED) munmap(addr_, length_);
        if (fd_ >= 0) close(fd_);
        shm_unlink(name_.c_str());
    }

    const std::string& name() const { return name_; }
    void* addr() const { return addr_; }

   private:
    std::string name_;
    size_t length_;
    int fd_{-1};
    void* addr_{nullptr};
};

void addPeerBuffers(
    TransferMetadata& metadata,
    const std::vector<std::pair<std::string, uint64_t>>& buffers,
    size_t length) {
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "127.0.0.1:19001";
    desc->protocol = "tcp";
    for (const auto& buffer_info : buffers) {
        TransferMetadata::BufferDesc buffer;
        buffer.name = "cpu:0";
        buffer.addr = buffer_info.second;
        buffer.length = length;
        buffer.shm_name = buffer_info.first;
        desc->buffers.push_back(buffer);
    }
    metadata.addLocalSegment(kPeerSegmentId, desc->name, std::move(desc));
}

void addPeerSegment(TransferMetadata& metadata, const std::string& shm_name,
                    uint64_t remote_addr, size_t length) {
    addPeerBuffers(metadata, {{shm_name, remote_addr}}, length);
}

}  // namespace

TEST(ShmNameTest, PrefixDetectsPosixNames) {
    EXPECT_TRUE(isPosixShmName("/mooncake_1234_abcdefgh"));
    EXPECT_TRUE(isPosixShmName("mooncake_1234_abcdefgh"));
    EXPECT_FALSE(isPosixShmName(""));
    EXPECT_FALSE(isPosixShmName("/mooncake_"));
    EXPECT_FALSE(isPosixShmName("mooncake_"));
    EXPECT_FALSE(isPosixShmName("cuda-ipc-handle"));
}

TEST(ShmTransportTest, SharesRelocationAcrossThreads) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile shm_file(page_size);
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerSegment(*metadata, shm_file.name(), kRemoteAddress, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    constexpr size_t kThreadCount = 8;
    std::vector<uint64_t> relocated(kThreadCount, kRemoteAddress);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&transport, &relocated, i, page_size]() {
            auto status = ShmTransportTestPeer::relocate(
                transport, relocated[i], page_size, kPeerSegmentId);
            EXPECT_TRUE(status.ok()) << status.ToString();
        });
    }
    for (auto& thread : threads) thread.join();

    for (uint64_t address : relocated) EXPECT_EQ(address, relocated.front());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);

    auto* mapped = reinterpret_cast<char*>(relocated.front());
    mapped[0] = 0x5a;
    EXPECT_EQ(static_cast<unsigned char*>(shm_file.addr())[0], 0x5a);
}

TEST(ShmTransportTest, ConsumerDoesNotCreateMissingFile) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const std::string shm_name = std::string(kPosixShmNamePrefix) + "missing_" +
                                 std::to_string(getpid());
    shm_unlink(shm_name.c_str());

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerSegment(*metadata, shm_name, kRemoteAddress, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t address = kRemoteAddress;
    auto status = ShmTransportTestPeer::relocate(transport, address, page_size,
                                                 kPeerSegmentId);
    EXPECT_FALSE(status.ok());

    std::string_view key = shm_name;
    if (!key.empty() && key.front() == '/') key.remove_prefix(1);
    std::string path = std::string("/dev/shm/") + std::string(key);
    EXPECT_EQ(access(path.c_str(), F_OK), -1);
    EXPECT_EQ(errno, ENOENT);
}

TEST(ShmTransportTest, RejectsBackingFileShorterThanBuffer) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const std::string shm_name =
        std::string(kPosixShmNamePrefix) + "short_" + std::to_string(getpid());
    shm_unlink(shm_name.c_str());
    int fd = shm_open(shm_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, static_cast<off_t>(page_size / 2)), 0);
    close(fd);

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerSegment(*metadata, shm_name, kRemoteAddress, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t address = kRemoteAddress;
    auto status = ShmTransportTestPeer::relocate(transport, address, page_size,
                                                 kPeerSegmentId);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 0);

    shm_unlink(shm_name.c_str());
}

TEST(ShmTransportTest, CreateSharedMemoryDoesNotTruncateExisting) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile shm_file(page_size);
    std::memset(shm_file.addr(), 0x3c, page_size);

    ShmTransport transport;
    int error = 0;
    void* mapped = ShmTransportTestPeer::createSharedMemory(
        transport, shm_file.name(), page_size, &error);
    EXPECT_EQ(mapped, nullptr);
    EXPECT_EQ(error, EEXIST);
    EXPECT_EQ(static_cast<unsigned char*>(shm_file.addr())[0], 0x3c);
}

TEST(ShmTransportTest, RelocateMissKeepsExistingMappings) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile shm_file(page_size);
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerSegment(*metadata, shm_file.name(), kRemoteAddress, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t address = kRemoteAddress;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, address, page_size,
                                               kPeerSegmentId)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);

    uint64_t missing = kRemoteAddress + page_size * 4;
    auto status = ShmTransportTestPeer::relocate(transport, missing, page_size,
                                                 kPeerSegmentId);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);
}

TEST(ShmTransportTest, RelocateInvalidatesStaleShmName) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile first(page_size, "first");
    ScopedShmFile second(page_size, "second");
    std::memset(first.addr(), 0x11, page_size);
    std::memset(second.addr(), 0x22, page_size);

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerSegment(*metadata, first.name(), kRemoteAddress, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t address = kRemoteAddress;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, address, page_size,
                                               kPeerSegmentId)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);
    std::memset(reinterpret_cast<void*>(address), 0xaa, page_size);
    EXPECT_EQ(static_cast<unsigned char*>(first.addr())[0], 0xaa);
    EXPECT_EQ(static_cast<unsigned char*>(second.addr())[0], 0x22);

    addPeerSegment(*metadata, second.name(), kRemoteAddress, page_size);
    address = kRemoteAddress;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, address, page_size,
                                               kPeerSegmentId)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);
    std::memset(reinterpret_cast<void*>(address), 0xbb, page_size);
    EXPECT_EQ(static_cast<unsigned char*>(first.addr())[0], 0xaa);
    EXPECT_EQ(static_cast<unsigned char*>(second.addr())[0], 0xbb);
}

TEST(ShmTransportTest, RelocateUnlinkedObjectDropsMapping) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile shm_file(page_size);
    std::memset(shm_file.addr(), 0x11, page_size);

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerSegment(*metadata, shm_file.name(), kRemoteAddress, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t address = kRemoteAddress;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, address, page_size,
                                               kPeerSegmentId)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);
    std::memset(reinterpret_cast<void*>(address), 0xaa, page_size);
    EXPECT_EQ(static_cast<unsigned char*>(shm_file.addr())[0], 0xaa);

    ASSERT_EQ(shm_unlink(shm_file.name().c_str()), 0);
    address = kRemoteAddress;
    auto status = ShmTransportTestPeer::relocate(transport, address, page_size,
                                                 kPeerSegmentId);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 0);
    EXPECT_EQ(static_cast<unsigned char*>(shm_file.addr())[0], 0xaa);
}

TEST(ShmTransportTest, RelocatePrunesAddrsNotInDescriptor) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile first(page_size, "keep");
    ScopedShmFile second(page_size, "drop");
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerBuffers(*metadata,
                   {{first.name(), kRemoteAddress},
                    {second.name(), kRemoteAddress + page_size}},
                   page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t first_addr = kRemoteAddress;
    uint64_t second_addr = kRemoteAddress + page_size;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, first_addr, page_size,
                                               kPeerSegmentId)
                    .ok());
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, second_addr,
                                               page_size, kPeerSegmentId)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 2);

    addPeerBuffers(*metadata, {{first.name(), kRemoteAddress}}, page_size);
    first_addr = kRemoteAddress;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, first_addr, page_size,
                                               kPeerSegmentId)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);
}

TEST(ShmTransportTest, RelocateCapsMappingsPerTarget) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const size_t cap = ShmTransportTestPeer::maxMappingsPerTarget();
    std::vector<std::unique_ptr<ScopedShmFile>> files;
    std::vector<std::pair<std::string, uint64_t>> buffers;
    files.reserve(cap + 1);
    buffers.reserve(cap + 1);
    for (size_t i = 0; i < cap + 1; ++i) {
        files.push_back(std::make_unique<ScopedShmFile>(
            page_size, "cap" + std::to_string(i)));
        buffers.emplace_back(files.back()->name(),
                             kRemoteAddress + i * page_size);
    }

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerBuffers(*metadata, buffers, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    for (const auto& buffer : buffers) {
        uint64_t address = buffer.second;
        ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, address,
                                                   page_size, kPeerSegmentId)
                        .ok());
    }
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId),
              cap);
}

TEST(ShmTransportTest, RelocatePinSurvivesCap) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const size_t cap = ShmTransportTestPeer::maxMappingsPerTarget();
    std::vector<std::unique_ptr<ScopedShmFile>> files;
    std::vector<std::pair<std::string, uint64_t>> buffers;
    files.reserve(cap + 1);
    buffers.reserve(cap + 1);
    for (size_t i = 0; i < cap + 1; ++i) {
        files.push_back(std::make_unique<ScopedShmFile>(
            page_size, "pin-cap" + std::to_string(i)));
        buffers.emplace_back(files.back()->name(),
                             kRemoteAddress + i * page_size);
    }

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerBuffers(*metadata, buffers, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t pinned_addr = buffers.front().second;
    std::unique_ptr<ShmTransport::MappingPin> pin;
    ASSERT_TRUE(ShmTransportTestPeer::relocatePinned(
                    transport, pinned_addr, page_size, kPeerSegmentId, pin)
                    .ok());
    ASSERT_TRUE(pin && *pin);
    EXPECT_EQ(ShmTransportTestPeer::pinCount(transport, kPeerSegmentId,
                                             buffers.front().second),
              1u);

    for (size_t i = 1; i < buffers.size(); ++i) {
        uint64_t address = buffers[i].second;
        ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, address,
                                                   page_size, kPeerSegmentId)
                        .ok());
    }
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId),
              cap);
    EXPECT_EQ(ShmTransportTestPeer::pinCount(transport, kPeerSegmentId,
                                             buffers.front().second),
              1u);

    std::memset(reinterpret_cast<void*>(pinned_addr), 0xab, page_size);
    EXPECT_EQ(static_cast<unsigned char*>(files.front()->addr())[0], 0xab);

    pin.reset();
    EXPECT_EQ(ShmTransportTestPeer::pinCount(transport, kPeerSegmentId,
                                             buffers.front().second),
              0u);
}

TEST(ShmTransportTest, RelocatePinSurvivesPrune) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile first(page_size, "keep");
    ScopedShmFile second(page_size, "drop");
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerBuffers(*metadata,
                   {{first.name(), kRemoteAddress},
                    {second.name(), kRemoteAddress + page_size}},
                   page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t first_addr = kRemoteAddress;
    uint64_t second_addr = kRemoteAddress + page_size;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, first_addr, page_size,
                                               kPeerSegmentId)
                    .ok());
    std::unique_ptr<ShmTransport::MappingPin> pin;
    ASSERT_TRUE(ShmTransportTestPeer::relocatePinned(
                    transport, second_addr, page_size, kPeerSegmentId, pin)
                    .ok());
    ASSERT_TRUE(pin && *pin);
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 2);

    addPeerBuffers(*metadata, {{first.name(), kRemoteAddress}}, page_size);
    first_addr = kRemoteAddress;
    ASSERT_TRUE(ShmTransportTestPeer::relocate(transport, first_addr, page_size,
                                               kPeerSegmentId)
                    .ok());
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(transport, kPeerSegmentId), 1);

    std::memset(reinterpret_cast<void*>(second_addr), 0xcd, page_size);
    EXPECT_EQ(static_cast<unsigned char*>(second.addr())[0], 0xcd);

    pin.reset();
    std::memset(reinterpret_cast<void*>(first_addr), 0xef, page_size);
    EXPECT_EQ(static_cast<unsigned char*>(first.addr())[0], 0xef);
}

TEST(ShmTransportTest, RelocateCopySurvivesConcurrentCap) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const size_t cap = ShmTransportTestPeer::maxMappingsPerTarget();
    std::vector<std::unique_ptr<ScopedShmFile>> files;
    std::vector<std::pair<std::string, uint64_t>> buffers;
    files.reserve(cap + 1);
    buffers.reserve(cap + 1);
    for (size_t i = 0; i < cap + 1; ++i) {
        files.push_back(std::make_unique<ScopedShmFile>(
            page_size, "race" + std::to_string(i)));
        buffers.emplace_back(files.back()->name(),
                             kRemoteAddress + i * page_size);
    }

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerBuffers(*metadata, buffers, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    uint64_t pinned_addr = buffers.front().second;
    std::unique_ptr<ShmTransport::MappingPin> pin;
    ASSERT_TRUE(ShmTransportTestPeer::relocatePinned(
                    transport, pinned_addr, page_size, kPeerSegmentId, pin)
                    .ok());
    ASSERT_TRUE(pin && *pin);

    std::atomic<bool> stop{false};
    std::thread copier([&]() {
        std::vector<char> src(page_size, 0x5a);
        while (!stop.load()) {
            std::memcpy(reinterpret_cast<void*>(pinned_addr), src.data(),
                        page_size);
        }
    });
    for (int round = 0; round < 4; ++round) {
        for (const auto& buffer : buffers) {
            uint64_t address = buffer.second;
            auto status = ShmTransportTestPeer::relocate(
                transport, address, page_size, kPeerSegmentId);
            EXPECT_TRUE(status.ok()) << status.ToString();
        }
    }
    stop.store(true);
    copier.join();
    pin.reset();
    EXPECT_EQ(static_cast<unsigned char*>(files.front()->addr())[0], 0x5a);
}

TEST(ShmTransportTest, SubmitIsAllOrNothingWhenRelocateFailsMidBatch) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    ScopedShmFile shm_file(page_size);
    std::memset(shm_file.addr(), 0x11, page_size);

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    addPeerSegment(*metadata, shm_file.name(), kRemoteAddress, page_size);

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    std::vector<char> src(page_size, 0x22);
    Transport::TransferRequest ok;
    ok.opcode = Transport::TransferRequest::WRITE;
    ok.source = src.data();
    ok.target_id = kPeerSegmentId;
    ok.target_offset = kRemoteAddress;
    ok.length = page_size;

    Transport::TransferRequest bad = ok;
    bad.target_offset = kRemoteAddress + page_size * 8;

    auto batch = transport.allocateBatchID(2);
    ASSERT_NE(batch, static_cast<Transport::BatchID>(ERR_MEMORY));
    auto status = transport.submitTransfer(batch, {ok, bad});
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(static_cast<unsigned char*>(shm_file.addr())[0], 0x11);
    ASSERT_TRUE(transport.freeBatchID(batch).ok());
}

TEST(ShmTransportTest, LocalSegmentCopiesWithoutShmOpen) {
    const size_t length = 4096;
    std::vector<char> src(length, 0x44);
    std::vector<char> dst(length, 0);

    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = src.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(dst.data());
    req.length = length;

    auto batch = transport.allocateBatchID(1);
    ASSERT_TRUE(transport.submitTransfer(batch, {req}).ok());
    Transport::TransferStatus st;
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, st).ok());
    EXPECT_EQ(st.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(dst, src);
    ASSERT_TRUE(transport.freeBatchID(batch).ok());
}

TEST(ShmTransportTest, RegisterRejectsSubRangeAndOverflow) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    void* base = transport.allocateSharedMemory(page_size * 2);
    ASSERT_NE(base, nullptr);
    auto* mid = static_cast<char*>(base) + page_size;
    EXPECT_EQ(
        ShmTransportTestPeer::registerLocalMemory(transport, mid, page_size),
        ERR_INVALID_ARGUMENT);
    EXPECT_EQ(ShmTransportTestPeer::registerLocalMemory(transport, base,
                                                        page_size * 2 + 1),
              ERR_INVALID_ARGUMENT);

    std::vector<char> heap(page_size);
    EXPECT_EQ(ShmTransportTestPeer::registerLocalMemory(transport, heap.data(),
                                                        page_size),
              0);

    ASSERT_EQ(
        ShmTransportTestPeer::registerLocalMemory(transport, base, page_size),
        0);
    auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(desc);
    ASSERT_EQ(desc->buffers.size(), 1u);
    EXPECT_EQ(desc->buffers[0].addr, reinterpret_cast<uint64_t>(base));
    EXPECT_EQ(desc->buffers[0].length, page_size);
    EXPECT_TRUE(isPosixShmName(desc->buffers[0].shm_name));

    ASSERT_EQ(transport.freeSharedMemory(base), 0);
}

TEST(ShmTransportTest, RegisterBatchSkipsMallocAndExportsShm) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);

    std::vector<char> heap(page_size);
    Transport::BufferEntry heap_entry{heap.data(), page_size};
    EXPECT_EQ(
        ShmTransportTestPeer::registerLocalMemoryBatch(transport, {heap_entry}),
        0);
    auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(desc);
    for (const auto& buffer : desc->buffers) {
        EXPECT_FALSE(isPosixShmName(buffer.shm_name));
    }

    void* base = transport.allocateSharedMemory(page_size);
    ASSERT_NE(base, nullptr);
    std::string shm_name;
    ASSERT_TRUE(transport.getShmName(base, &shm_name));
    ASSERT_FALSE(shm_name.empty());
    EXPECT_EQ(shm_name.front(), '/');
    EXPECT_TRUE(isPosixShmName(shm_name));

    Transport::BufferEntry shm_entry{base, page_size};
    EXPECT_EQ(ShmTransportTestPeer::registerLocalMemoryBatch(
                  transport, {heap_entry, shm_entry}),
              0);
    desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(desc);
    int posix_buffers = 0;
    for (const auto& buffer : desc->buffers) {
        if (isPosixShmName(buffer.shm_name)) ++posix_buffers;
    }
    EXPECT_EQ(posix_buffers, 1);
    ASSERT_EQ(transport.freeSharedMemory(base), 0);
}

#ifndef ENABLE_MULTI_PROTOCOL
TEST(ShmTransportTest, InstallReplacesNonShmProtocol) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "127.0.0.1:19000";
    desc->protocol = "tcp";
    metadata->addLocalSegment(LOCAL_SEGMENT_ID, desc->name, std::move(desc));

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);
    auto installed = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(installed);
    EXPECT_EQ(installed->protocol, "shm");
}
#endif

#ifdef ENABLE_MULTI_PROTOCOL
TEST(ShmTransportTest, InstallDoesNotTreatUbshmemAsShm) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "127.0.0.1:19000";
    desc->protocol = "ubshmem";
    metadata->addLocalSegment(LOCAL_SEGMENT_ID, desc->name, std::move(desc));

    ShmTransport transport;
    std::string local = "127.0.0.1:19000";
    ASSERT_EQ(ShmTransportTestPeer::install(transport, local, metadata), 0);
    auto installed = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(installed);
    EXPECT_EQ(installed->protocol, "ubshmem,shm");
}
#endif

class ShmRoutingTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kRemoteAddr = 0x10000000;

    void SetUp() override {
        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        local_name_ = "127.0.0.1:19000";
        multi_ = std::make_unique<MultiTransport>(metadata_, local_name_);
        ASSERT_NE(multi_->installTransport("shm", nullptr), nullptr);
        shm_ = multi_->getTransport("shm");
        ASSERT_NE(shm_, nullptr);
    }

    void AddBuffer(TransferMetadata::SegmentDesc& desc,
                   const std::string& shm_name,
                   const std::string& buffer_protocol) {
        TransferMetadata::BufferDesc buffer;
        buffer.name = "cpu:0";
        buffer.addr = kRemoteAddr;
        buffer.length = 4096;
        buffer.shm_name = shm_name;
#ifdef ENABLE_MULTI_PROTOCOL
        buffer.protocol = buffer_protocol;
#else
        (void)buffer_protocol;
#endif
        desc.buffers.push_back(buffer);
    }

    Transport::SegmentID AddPeer(const std::string& segment_name,
                                 const std::string& shm_name,
                                 const std::string& protocol = "shm",
                                 const std::string& buffer_protocol = "") {
        auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
        desc->name = segment_name;
        desc->protocol = protocol;
        AddBuffer(*desc, shm_name,
                  buffer_protocol.empty() ? protocol : buffer_protocol);
        const Transport::SegmentID id = next_segment_id_++;
        metadata_->addLocalSegment(id, segment_name, std::move(desc));
        return id;
    }

    Transport::TransferRequest MakeWrite(Transport::SegmentID id) const {
        Transport::TransferRequest request;
        request.opcode = Transport::TransferRequest::WRITE;
        request.source = nullptr;
        request.target_id = id;
        request.target_offset = kRemoteAddr;
        request.length = 64;
        return request;
    }

    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_name_;
    std::unique_ptr<MultiTransport> multi_;
    Transport* shm_{nullptr};
    Transport::SegmentID next_segment_id_{10};
};

TEST_F(ShmRoutingTest, SameHostSelectsShm) {
    auto id = AddPeer("127.0.0.1:19001", "mooncake_1_abcdefgh");
    auto request = MakeWrite(id);
    Transport* transport = nullptr;
    auto status =
        MultiTransportTestPeer::selectTransport(*multi_, request, transport);
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(transport, shm_);
}

#ifndef ENABLE_MULTI_PROTOCOL
TEST_F(ShmRoutingTest, CrossHostStillSelectsShmWithoutMultiProtocol) {
    auto id = AddPeer("10.0.0.2:19001", "mooncake_1_abcdefgh");
    auto request = MakeWrite(id);
    Transport* transport = nullptr;
    auto status =
        MultiTransportTestPeer::selectTransport(*multi_, request, transport);
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(transport, shm_);
}
#endif

#ifdef ENABLE_MULTI_PROTOCOL
TEST_F(ShmRoutingTest, SameHostShmBeatsTcp) {
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "127.0.0.1:19001";
    desc->protocol = "tcp,shm";
    AddBuffer(*desc, "", "tcp");
    AddBuffer(*desc, "mooncake_1_abcdefgh", "shm");
    const auto id = next_segment_id_++;
    metadata_->addLocalSegment(id, desc->name, std::move(desc));

    auto request = MakeWrite(id);
    Transport* transport = nullptr;
    auto status =
        MultiTransportTestPeer::selectTransport(*multi_, request, transport);
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(transport, shm_);
}

TEST_F(ShmRoutingTest, CrossHostSkipsShm) {
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "10.0.0.2:19001";
    desc->protocol = "tcp,shm";
    AddBuffer(*desc, "", "tcp");
    AddBuffer(*desc, "mooncake_1_abcdefgh", "shm");
    const auto id = next_segment_id_++;
    metadata_->addLocalSegment(id, desc->name, std::move(desc));

    auto request = MakeWrite(id);
    Transport* transport = nullptr;
    auto status =
        MultiTransportTestPeer::selectTransport(*multi_, request, transport);
    EXPECT_TRUE(status.IsNotSupportedTransport()) << status.ToString();
    EXPECT_NE(transport, shm_);
}

TEST_F(ShmRoutingTest, TcpOnlyBufferDoesNotSelectShm) {
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "127.0.0.1:19001";
    desc->protocol = "tcp,shm";
    AddBuffer(*desc, "", "tcp");
    const auto id = next_segment_id_++;
    metadata_->addLocalSegment(id, desc->name, std::move(desc));

    auto request = MakeWrite(id);
    Transport* transport = nullptr;
    auto status =
        MultiTransportTestPeer::selectTransport(*multi_, request, transport);
    EXPECT_TRUE(status.IsNotSupportedTransport()) << status.ToString();
    EXPECT_NE(transport, shm_);
}

TEST_F(ShmRoutingTest, HipBufferDoesNotSelectShm) {
    auto id =
        AddPeer("127.0.0.1:19001", "mooncake_1_abcdefgh", "tcp,hip", "hip");
    auto request = MakeWrite(id);
    Transport* transport = nullptr;
    auto status =
        MultiTransportTestPeer::selectTransport(*multi_, request, transport);
    EXPECT_TRUE(status.IsNotSupportedTransport()) << status.ToString();
    EXPECT_NE(transport, shm_);
}
#endif

}  // namespace mooncake
