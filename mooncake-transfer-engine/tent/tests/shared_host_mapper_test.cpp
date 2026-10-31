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

#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <string>

#include "shared_segment/host_memfd.h"
#include "tent/transport/ascend/shared_host_mapper.h"

namespace mooncake {
namespace tent {
namespace {

SegmentDesc MakeSharedSegment(void* base, uint64_t length,
                              const std::string& handle) {
    SegmentDesc segment;
    segment.name = "shared-host-peer";
    segment.type = SegmentType::Memory;
    MemorySegmentDesc detail;
    BufferDesc buffer{};
    buffer.addr = reinterpret_cast<uint64_t>(base);
    buffer.length = length;
    buffer.location = "cpu:0";
    buffer.shm_path = handle;
    detail.buffers.push_back(buffer);
    segment.detail = std::move(detail);
    return segment;
}

class SharedHostMapping {
   public:
    SharedHostMapping() = default;
    ~SharedHostMapping() {
        if (base_ != nullptr) {
            munmap(base_, length_);
        }
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    bool Create() {
        length_ = host_memfd::Granularity();
        std::string error;
        if (!host_memfd::Create(length_, fd_, base_, error)) {
            ADD_FAILURE() << "host_memfd::Create failed: " << error;
            return false;
        }
        handle_ = host_memfd::ExportHandle(fd_);
        return true;
    }

    void* base() const { return base_; }
    uint64_t length() const { return length_; }
    const std::string& handle() const { return handle_; }

   private:
    int fd_ = -1;
    void* base_ = nullptr;
    uint64_t length_ = 0;
    std::string handle_;
};

TEST(SharedHostMapperTest, SameProcessBufferIsUsedInPlace) {
    SharedHostMapping mapping;
    ASSERT_TRUE(mapping.Create());
    const auto segment =
        MakeSharedSegment(mapping.base(), mapping.length(), mapping.handle());

    SharedHostMapper mapper;
    void* local = nullptr;
    const auto target = reinterpret_cast<uint64_t>(mapping.base()) + 128;
    EXPECT_TRUE(mapper.Translate(segment, target, 64, &local));
    EXPECT_EQ(local, static_cast<char*>(mapping.base()) + 128);
}

TEST(SharedHostMapperTest, InvalidHandleFallsBack) {
    SharedHostMapping mapping;
    ASSERT_TRUE(mapping.Create());
    auto segment = MakeSharedSegment(mapping.base(), mapping.length(),
                                     "this-is-not-a-memfd-handle");

    SharedHostMapper mapper;
    void* local = nullptr;
    EXPECT_FALSE(mapper.Translate(
        segment, reinterpret_cast<uint64_t>(mapping.base()), 64, &local));
    EXPECT_EQ(local, nullptr);
    // The negative result is cached, so a second try still falls back.
    EXPECT_FALSE(mapper.Translate(
        segment, reinterpret_cast<uint64_t>(mapping.base()), 64, &local));
}

TEST(SharedHostMapperTest, RangeEscapingTheBufferFallsBack) {
    SharedHostMapping mapping;
    ASSERT_TRUE(mapping.Create());
    const auto segment =
        MakeSharedSegment(mapping.base(), mapping.length(), mapping.handle());

    SharedHostMapper mapper;
    void* local = nullptr;
    // Last byte plus two bytes crosses the buffer end.
    EXPECT_FALSE(mapper.Translate(
        segment,
        reinterpret_cast<uint64_t>(mapping.base()) + mapping.length() - 1, 2,
        &local));
    // Beyond the buffer.
    EXPECT_FALSE(mapper.Translate(
        segment, reinterpret_cast<uint64_t>(mapping.base()) + mapping.length(),
        1, &local));
    // Zero-length is never translated.
    EXPECT_FALSE(mapper.Translate(
        segment, reinterpret_cast<uint64_t>(mapping.base()), 0, &local));
}

TEST(SharedHostMapperTest, SegmentWithoutHandleFallsBack) {
    SharedHostMapping mapping;
    ASSERT_TRUE(mapping.Create());
    auto segment = MakeSharedSegment(mapping.base(), mapping.length(), "");

    SharedHostMapper mapper;
    void* local = nullptr;
    EXPECT_FALSE(mapper.Translate(
        segment, reinterpret_cast<uint64_t>(mapping.base()), 64, &local));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
