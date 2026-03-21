#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {

// Mirror the internal merge declarations from transfer_engine_impl.cpp
// without exporting them in a public header.
struct BufferKey {
    uint64_t addr{0};
    uint64_t length{0};

    bool operator==(const BufferKey&) const = default;
};

struct RequestBoundaryInfo {
    std::optional<BufferKey> source_key;
    std::optional<BufferKey> target_key;
};

struct MergeResult {
    std::vector<Request> request_list;
    std::map<size_t, size_t> task_lookup;
};

MergeResult mergeRequests(const std::vector<Request>& requests,
                          const std::vector<RequestBoundaryInfo>& boundaries,
                          bool do_merge);

namespace {

Request makeWriteRequest(char* base, size_t source_offset,
                         uint64_t target_offset, size_t length) {
    return Request{
        Request::WRITE, base + source_offset, 7, target_offset, length,
    };
}

BufferKey makeBufferKey(uint64_t addr, uint64_t length) {
    return BufferKey{addr, length};
}

TEST(RequestMergeTest, KeepsRequestsSplitAcrossRegisteredBufferBoundaries) {
    std::array<char, 2048> source{};
    const auto source_addr =
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(source.data()));

    std::vector<Request> requests = {
        makeWriteRequest(source.data(), 0, 0, 1024),
        makeWriteRequest(source.data(), 1024, 1024, 1024),
    };
    std::vector<RequestBoundaryInfo> boundaries = {
        {makeBufferKey(source_addr, 1024), makeBufferKey(0, 1024)},
        {makeBufferKey(source_addr + 1024, 1024), makeBufferKey(1024, 1024)},
    };

    auto merged = mergeRequests(requests, boundaries, true);

    ASSERT_EQ(merged.request_list.size(), 2u);
    EXPECT_EQ(merged.request_list[0].length, 1024u);
    EXPECT_EQ(merged.request_list[1].length, 1024u);
    EXPECT_EQ(merged.task_lookup.at(0), 0u);
    EXPECT_EQ(merged.task_lookup.at(1), 1u);
}

TEST(RequestMergeTest, MergesAdjacentRequestsInsideSameRegisteredBuffers) {
    std::array<char, 2048> source{};
    const auto source_addr =
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(source.data()));

    std::vector<Request> requests = {
        makeWriteRequest(source.data(), 0, 4096, 1024),
        makeWriteRequest(source.data(), 1024, 5120, 1024),
    };
    std::vector<RequestBoundaryInfo> boundaries = {
        {makeBufferKey(source_addr, 2048), makeBufferKey(4096, 2048)},
        {makeBufferKey(source_addr, 2048), makeBufferKey(4096, 2048)},
    };

    auto merged = mergeRequests(requests, boundaries, true);

    ASSERT_EQ(merged.request_list.size(), 1u);
    EXPECT_EQ(merged.request_list[0].length, 2048u);
    EXPECT_EQ(merged.task_lookup.at(0), 0u);
    EXPECT_EQ(merged.task_lookup.at(1), 0u);
}

TEST(RequestMergeTest, KeepsNonContiguousRequestsSplit) {
    std::array<char, 3072> source{};
    const auto source_addr =
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(source.data()));

    std::vector<Request> requests = {
        makeWriteRequest(source.data(), 0, 8192, 1024),
        makeWriteRequest(source.data(), 1536, 9728, 1024),
    };
    std::vector<RequestBoundaryInfo> boundaries = {
        {makeBufferKey(source_addr, 3072), makeBufferKey(8192, 3072)},
        {makeBufferKey(source_addr, 3072), makeBufferKey(8192, 3072)},
    };

    auto merged = mergeRequests(requests, boundaries, true);

    ASSERT_EQ(merged.request_list.size(), 2u);
    EXPECT_EQ(merged.task_lookup.at(0), 0u);
    EXPECT_EQ(merged.task_lookup.at(1), 1u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
