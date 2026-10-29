#include "storage/distributed/object_storage_backend.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <future>
#include <limits>
#include <map>
#include <stdexcept>
#include <utility>

namespace mooncake {
namespace {

class MemoryObjectAdapter : public ObjectStorageAdapter {
   public:
    std::map<std::string, std::string> objects;
    std::vector<std::pair<off_t, size_t>> ranges;
    size_t put_calls{0};
    bool short_read{false};
    ErrorCode put_error{ErrorCode::OK};
    ErrorCode read_error{ErrorCode::OK};
    std::function<void()> before_put;
    std::function<void()> before_read;

    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      std::span<const char> data) override {
        ++put_calls;
        if (before_put) before_put();
        if (put_error != ErrorCode::OK) return tl::make_unexpected(put_error);
        objects[key] = std::string(data.begin(), data.end());
        return {};
    }
    tl::expected<void, ErrorCode> PutV(const std::string&, const iovec*,
                                       int) override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<size_t, ErrorCode> Get(const std::string&, void*,
                                        size_t) override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<size_t, ErrorCode> GetRange(const std::string& key,
                                             void* buffer, size_t size,
                                             off_t offset) override {
        ranges.emplace_back(offset, size);
        if (before_read) before_read();
        if (read_error != ErrorCode::OK) return tl::make_unexpected(read_error);
        const auto it = objects.find(key);
        if (it == objects.end())
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        if (offset < 0 || static_cast<uint64_t>(offset) > it->second.size() ||
            size > it->second.size() - offset)
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        const auto count = short_read && size ? size - 1 : size;
        std::memcpy(buffer, it->second.data() + offset, count);
        return count;
    }
    tl::expected<bool, ErrorCode> Exists(const std::string& key) override {
        return objects.contains(key);
    }
    tl::expected<void, ErrorCode> Delete(const std::string& key) override {
        objects.erase(key);
        return {};
    }
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<void, ErrorCode> Init() override { return {}; }
    const char* GetName() const override { return "memory"; }
    bool SupportsSinglePutAttempt() const override { return true; }
};

bool HostCopy(void* dst, const void* src, size_t size) {
    std::memcpy(dst, src, size);
    return true;
}

class ObjectStorageBackendTest : public ::testing::Test {
   protected:
    std::shared_ptr<MemoryObjectAdapter> adapter =
        std::make_shared<MemoryObjectAdapter>();
};

TEST_F(ObjectStorageBackendTest, GathersFullObjectAndScattersExactRange) {
    ObjectStorageBackend backend({32, 32}, adapter);
    std::string first = "abc", second = "defgh";
    const std::string key("physical\0name", 13);
    bool started = false;
    EXPECT_EQ(backend.Write(
                  key, 8, {{first.data(), 3}, {nullptr, 0}, {second.data(), 5}},
                  HostCopy, &started),
              ErrorCode::OK);
    EXPECT_TRUE(started);
    EXPECT_EQ(adapter->put_calls, 1U);
    EXPECT_EQ(adapter->objects.at(key), "abcdefgh");
    std::array<char, 8> output;
    output.fill('!');
    EXPECT_EQ(
        backend.Read(
            key, 8,
            {{output.data() + 1, 2}, {nullptr, 0}, {output.data() + 3, 4}}, 1,
            HostCopy),
        ErrorCode::OK);
    EXPECT_EQ(std::string(output.data(), output.size()), "!bcdefg!");
    ASSERT_EQ(adapter->ranges.size(), 1U);
    EXPECT_EQ(adapter->ranges[0], std::make_pair(off_t{1}, size_t{6}));
    ASSERT_TRUE(adapter->Delete(key));
    EXPECT_EQ(backend.Read(key, 8, {{output.data(), 1}}, 0, HostCopy),
              ErrorCode::FILE_NOT_FOUND);
}

TEST_F(ObjectStorageBackendTest, CoalescesRangesAcrossDestinationBoundaries) {
    constexpr size_t max_chunk = 4 * 1024 * 1024;
    for (const size_t budget : {size_t{3}, max_chunk}) {
        SCOPED_TRACE(budget);
        const size_t size = budget == 3 ? 13 : max_chunk + 11;
        std::string source(size + 13, '\0');
        for (size_t i = 0; i < source.size(); ++i) {
            source[i] = static_cast<char>(i % 251);
        }
        adapter->objects["key"] = source;
        adapter->ranges.clear();
        ObjectStorageBackend backend({source.size(), budget}, adapter);
        std::string last(8, '!');
        std::string middle(size - 6, '!');
        std::string first(4, '!');
        EXPECT_EQ(backend.Read("key", source.size(),
                               {{nullptr, 0},
                                {first.data() + 1, 2},
                                {middle.data() + 1, size - 8},
                                {nullptr, 0},
                                {last.data() + 1, 6},
                                {nullptr, 0}},
                               5, HostCopy),
                  ErrorCode::OK);
        EXPECT_EQ(
            first.substr(1, 2) + middle.substr(1, size - 8) + last.substr(1, 6),
            source.substr(5, size));
        for (const auto* output : {&first, &middle, &last}) {
            EXPECT_EQ(output->front(), '!');
            EXPECT_EQ(output->back(), '!');
        }
        const auto chunk = std::min(max_chunk, budget);
        ASSERT_EQ(adapter->ranges.size(), (size + chunk - 1) / chunk);
        size_t next = 5;
        for (const auto& [offset, length] : adapter->ranges) {
            EXPECT_EQ(offset, next);
            EXPECT_EQ(length, std::min(chunk, size + 5 - next));
            next += length;
        }
        EXPECT_EQ(next, size + 5);
    }
}

TEST_F(ObjectStorageBackendTest,
       LaterChunkFailurePreservesEarlierScatterAndReleasesBudget) {
    ObjectStorageBackend backend({9, 4}, adapter);
    adapter->objects["key"] = "abcdefghi";
    std::array<char, 5> first, last, middle;
    first.fill('!');
    middle.fill('!');
    last.fill('!');
    const std::vector<Slice> slices{{first.data() + 1, 3},
                                    {nullptr, 0},
                                    {middle.data() + 1, 3},
                                    {last.data() + 1, 3}};
    adapter->before_read = [&] {
        adapter->short_read = adapter->ranges.back().first == 4;
    };
    EXPECT_EQ(backend.Read("key", 9, slices, 0, HostCopy),
              ErrorCode::FILE_READ_FAIL);
    EXPECT_EQ(std::string(first.data(), first.size()), "!abc!");
    EXPECT_EQ(std::string(middle.data(), middle.size()), "!d!!!");
    EXPECT_EQ(std::string(last.data(), last.size()), "!!!!!");

    adapter->before_read = {};
    adapter->short_read = false;
    EXPECT_EQ(backend.Read("key", 9, slices, 0, HostCopy), ErrorCode::OK);
    EXPECT_EQ(std::string(first.data(), first.size()), "!abc!");
    EXPECT_EQ(std::string(middle.data(), middle.size()), "!def!");
    EXPECT_EQ(std::string(last.data(), last.size()), "!ghi!");
}

TEST_F(ObjectStorageBackendTest, RejectsInvalidConfiguration) {
    EXPECT_THROW((ObjectStorageBackend({}, nullptr)), std::invalid_argument);
    EXPECT_THROW((ObjectStorageBackend({0, 1}, adapter)),
                 std::invalid_argument);
    EXPECT_THROW((ObjectStorageBackend({1, 0}, adapter)),
                 std::invalid_argument);
}

TEST_F(ObjectStorageBackendTest, RejectsRetryingAdaptersBeforeStagingOrPut) {
    class RetryingAdapter : public MemoryObjectAdapter {
       public:
        bool SupportsSinglePutAttempt() const override { return false; }
    };
    auto retrying = std::make_shared<RetryingAdapter>();
    ObjectStorageBackend backend({}, retrying);
    char byte = 'x';
    bool started = true;
    size_t copies = 0;
    EXPECT_EQ(backend.Write(
                  "key", 1, {{&byte, 1}},
                  [&](void*, const void*, size_t) {
                      ++copies;
                      return true;
                  },
                  &started),
              ErrorCode::NOT_SUPPORTED);
    EXPECT_FALSE(started);
    EXPECT_EQ(copies, 0U);
    EXPECT_EQ(retrying->put_calls, 0U);
}

TEST_F(ObjectStorageBackendTest, ValidatesAllSlicesBeforeIoOrCopy) {
    ObjectStorageBackend backend({8, 8}, adapter);
    char byte = 'x';
    size_t copies = 0;
    const auto copy = [&](void*, const void*, size_t) {
        ++copies;
        return true;
    };
    const std::vector<std::vector<Slice>> invalid{
        {{nullptr, 1}},
        {{&byte, 1}, {&byte, 8}},
        {{&byte, std::numeric_limits<size_t>::max()}},
        {{&byte, 1}, {nullptr, 1}}};
    for (const auto& slices : invalid) {
        bool started = true;
        EXPECT_EQ(backend.Write("key", 8, slices, copy, &started),
                  ErrorCode::INVALID_PARAMS);
        EXPECT_FALSE(started);
        EXPECT_EQ(backend.Read("key", 8, slices, 0, copy),
                  ErrorCode::INVALID_PARAMS);
    }
    EXPECT_EQ(backend.Write("key", 2, {{&byte, 1}}, copy),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(backend.Write("key", 9, {}, copy), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(backend.Write("key", 1, {{&byte, 1}}, {}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(backend.Read("key", 8, {{&byte, 1}}, 8, copy),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(backend.Read("key", 8, {}, 9, copy), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(backend.Read("key", 9, {}, 0, copy), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(backend.Read("key", 1, {{&byte, 1}}, 0, {}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(copies, 0U);
    EXPECT_EQ(adapter->put_calls, 0U);
    EXPECT_TRUE(adapter->ranges.empty());
}

TEST_F(ObjectStorageBackendTest, RejectsUnrepresentableRangeBeforeIo) {
    const auto limit = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    ObjectStorageBackend backend({std::numeric_limits<uint64_t>::max(), 8},
                                 adapter);
    std::array<char, 2> output;
    EXPECT_EQ(backend.Read("key", limit + 2, {{output.data(), 1}}, limit + 1,
                           HostCopy),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(
        backend.Read("key", limit + 2, {{output.data(), 2}}, limit, HostCopy),
        ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(adapter->ranges.empty());
}

TEST_F(ObjectStorageBackendTest, SupportsEmptyObjectsAndEmptyRanges) {
    ObjectStorageBackend backend({8, 8}, adapter);
    bool started = false;
    EXPECT_EQ(backend.Write("empty", 0, {{nullptr, 0}}, {}, &started),
              ErrorCode::OK);
    EXPECT_TRUE(started);
    EXPECT_TRUE(adapter->objects.at("empty").empty());
    EXPECT_EQ(backend.Read("empty", 0, {}, 0, {}), ErrorCode::OK);
    EXPECT_EQ(backend.Read("key", 8, {{nullptr, 0}}, 8, {}), ErrorCode::OK);
    EXPECT_TRUE(adapter->ranges.empty());
}

TEST_F(ObjectStorageBackendTest,
       ChunksLargeReadsAndHonorsSmallerStagingBudget) {
    constexpr size_t chunk = 4 * 1024 * 1024;
    for (const uint64_t budget : {uint64_t{3}, uint64_t{2 * chunk}}) {
        SCOPED_TRACE(budget);
        const size_t size = budget == 3 ? 13 : chunk + 7;
        std::string expected(size, 'a');
        expected.back() = 'z';
        adapter->objects["key"] = expected;
        adapter->ranges.clear();
        ObjectStorageBackend backend({size, budget}, adapter);
        std::string output(size, '!');
        EXPECT_EQ(
            backend.Read("key", size, {{output.data(), size}}, 0, HostCopy),
            ErrorCode::OK);
        EXPECT_EQ(output, expected);
        ASSERT_GT(adapter->ranges.size(), 1U);
        size_t next = 0;
        for (const auto& [offset, length] : adapter->ranges) {
            EXPECT_EQ(offset, next);
            EXPECT_LE(length, std::min<uint64_t>(chunk, budget));
            next += length;
        }
        EXPECT_EQ(next, size);
    }
}

TEST_F(ObjectStorageBackendTest, WriteFailuresReleaseBudgetWithoutRetryingPut) {
    ObjectStorageBackend backend({8, 4}, adapter);
    std::string value = "data";
    const std::vector<Slice> slices{{value.data(), value.size()}};
    bool started = true;
    EXPECT_EQ(
        backend.Write("too-large", 5, {{value.data(), 5}}, HostCopy, &started),
        ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(started);
    const auto fail_copy = [](void*, const void*, size_t) { return false; };
    EXPECT_EQ(backend.Write("key", 4, slices, fail_copy, &started),
              ErrorCode::TRANSFER_FAIL);
    EXPECT_FALSE(started);
    EXPECT_EQ(adapter->put_calls, 0U);
    adapter->put_error = ErrorCode::DFS_SERVICE_UNAVAILABLE;
    EXPECT_EQ(backend.Write("key", 4, slices, HostCopy, &started),
              ErrorCode::DFS_SERVICE_UNAVAILABLE);
    EXPECT_TRUE(started);
    EXPECT_EQ(adapter->put_calls, 1U);
    adapter->put_error = ErrorCode::OK;
    EXPECT_EQ(backend.Write("new-attempt", 4, slices, HostCopy, &started),
              ErrorCode::OK);
    EXPECT_TRUE(started);
    EXPECT_EQ(adapter->put_calls, 2U);
}

TEST_F(ObjectStorageBackendTest,
       ReadFailuresReleaseBudgetAndDoNotCopyShortData) {
    ObjectStorageBackend backend({8, 4}, adapter);
    adapter->objects["key"] = "data";
    std::string output(4, '!');
    const std::vector<Slice> slices{{output.data(), output.size()}};
    adapter->short_read = true;
    EXPECT_EQ(backend.Read("key", 4, slices, 0, HostCopy),
              ErrorCode::FILE_READ_FAIL);
    EXPECT_EQ(output, "!!!!");
    adapter->short_read = false;
    adapter->read_error = ErrorCode::DFS_SERVICE_UNAVAILABLE;
    EXPECT_EQ(backend.Read("key", 4, slices, 0, HostCopy),
              ErrorCode::DFS_SERVICE_UNAVAILABLE);
    EXPECT_EQ(output, "!!!!");
    adapter->read_error = ErrorCode::OK;
    const auto fail_copy = [](void*, const void*, size_t) { return false; };
    EXPECT_EQ(backend.Read("key", 4, slices, 0, fail_copy),
              ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(backend.Read("key", 4, slices, 0, HostCopy), ErrorCode::OK);
    EXPECT_EQ(output, "data");
}

TEST_F(ObjectStorageBackendTest, ExceptionsReleaseStagingReservations) {
    ObjectStorageBackend backend({4, 4}, adapter);
    std::string value = "data";
    const std::vector<Slice> slices{{value.data(), value.size()}};
    const auto throws = [](void*, const void*, size_t) -> bool {
        throw std::runtime_error("copy failed");
    };
    EXPECT_THROW(backend.Write("key", 4, slices, throws), std::runtime_error);
    EXPECT_EQ(backend.Write("key", 4, slices, HostCopy), ErrorCode::OK);
    EXPECT_THROW(backend.Read("key", 4, slices, 0, throws), std::runtime_error);
    EXPECT_EQ(backend.Read("key", 4, slices, 0, HostCopy), ErrorCode::OK);
}

TEST_F(ObjectStorageBackendTest, InFlightReadAndWriteShareTheBudget) {
    using namespace std::chrono_literals;
    for (const bool reading : {false, true}) {
        SCOPED_TRACE(reading);
        ObjectStorageBackend backend({4, 4}, adapter);
        adapter->objects["key"] = "data";
        std::string value = "data", output(4, '!');
        std::promise<void> entered, release;
        auto entry = entered.get_future();
        auto released = release.get_future().share();
        auto block = [&] {
            entered.set_value();
            released.wait();
        };
        if (reading)
            adapter->before_read = block;
        else
            adapter->before_put = block;
        auto work = std::async(std::launch::async, [&] {
            return reading
                       ? backend.Read("key", 4, {{output.data(), 4}}, 0,
                                      HostCopy)
                       : backend.Write("key", 4, {{value.data(), 4}}, HostCopy);
        });
        const bool in_flight = entry.wait_for(5s) == std::future_status::ready;
        EXPECT_TRUE(in_flight);
        if (in_flight) {
            EXPECT_EQ(backend.Write("other", 4, {{value.data(), 4}}, HostCopy),
                      ErrorCode::NO_AVAILABLE_HANDLE);
            EXPECT_EQ(backend.Read("key", 4, {{value.data(), 4}}, 0, HostCopy),
                      ErrorCode::NO_AVAILABLE_HANDLE);
        }
        release.set_value();
        EXPECT_EQ(work.get(), ErrorCode::OK);
        adapter->before_read = {};
        adapter->before_put = {};
        EXPECT_EQ(backend.Write("other", 4, {{value.data(), 4}}, HostCopy),
                  ErrorCode::OK);
        EXPECT_EQ(backend.Read("other", 4, {{output.data(), 4}}, 0, HostCopy),
                  ErrorCode::OK);
    }
}

TEST_F(ObjectStorageBackendTest,
       WholeObjectAdaptersExplicitlyRejectRangeReads) {
    class WholeObjectAdapter : public MemoryObjectAdapter {
       public:
        tl::expected<size_t, ErrorCode> GetRange(const std::string& key,
                                                 void* buf, size_t len,
                                                 off_t offset) override {
            return ObjectStorageAdapter::GetRange(key, buf, len, offset);
        }
    };
    ObjectStorageBackend backend({}, std::make_shared<WholeObjectAdapter>());
    char byte;
    EXPECT_EQ(backend.Read("key", 1, {{&byte, 1}}, 0, HostCopy),
              ErrorCode::NOT_SUPPORTED);
}

}  // namespace
}  // namespace mooncake
