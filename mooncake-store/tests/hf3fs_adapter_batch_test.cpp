#include <gtest/gtest.h>
#include <hf3fs_usrbio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <barrier>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "storage/distributed/hf3fs_adapter.h"

namespace {

struct FakeIo {
    uint8_t* buffer;
    int fd;
    size_t offset;
    size_t length;
    const void* userdata;
    int index;
    std::vector<uint8_t> write_data;
};

struct FakeRing {
    std::thread::id owner = std::this_thread::get_id();
    std::vector<FakeIo> pending;
    int next_index = 0;
};

// Small resources force multi-wave and multi-chunk transfers. Perform the data
// copy only when reaping, in reverse order, to catch early staging-buffer
// reuse.
struct FakeUsrbio {
    size_t buffer_size = 64;
    int entries = 4;
    int capacity = 4;
    int completions_per_wait = 2;
    int prep_error_fd = -1;
    int submit_error = 0;
    bool fail_create = false;
    bool bad_userdata = false;
    std::deque<int> wait_returns;
    std::unordered_map<int, int64_t> io_results;
    std::unordered_set<int> registered;
    std::vector<size_t> submissions;
    std::vector<size_t> prepared_lengths;
    std::unordered_map<int, size_t> prepared_counts;
    size_t outstanding = 0;
    size_t max_outstanding = 0;
    size_t destroyed_outstanding = 0;
    int buffers_created = 0;
    int rings_created = 0;
    int waits = 0;
} fake;

std::mutex fake_mutex;

}  // namespace

// These definitions intentionally replace libhf3fs in this executable while
// compiling against its public header and the production adapter sources.
extern "C" {
int hf3fs_extract_mount_point(char* result, int size, const char*) {
    if (size >= 2) std::memcpy(result, "/", 2);
    return 2;
}

int hf3fs_reg_fd(int fd, uint64_t) {
    std::lock_guard lock(fake_mutex);
    fake.registered.insert(fd);
    return 0;
}

void hf3fs_dereg_fd(int fd) {
    std::lock_guard lock(fake_mutex);
    fake.registered.erase(fd);
}

int hf3fs_iovcreate(hf3fs_iov* iov, const char*, size_t size, size_t, int) {
    std::lock_guard lock(fake_mutex);
    if (fake.fail_create) return -ENOMEM;
    *iov = {};
    iov->size = std::min(size, fake.buffer_size);
    iov->base = new uint8_t[iov->size];
    ++fake.buffers_created;
    return 0;
}

void hf3fs_iovdestroy(hf3fs_iov* iov) {
    delete[] iov->base;
    *iov = {};
}

int hf3fs_iorcreate4(hf3fs_ior* ior, const char*, int, bool read, int, int, int,
                     uint64_t) {
    std::lock_guard lock(fake_mutex);
    *ior = {};
    ior->for_read = read;
    ior->iorh = new FakeRing;
    ++fake.rings_created;
    return 0;
}

void hf3fs_iordestroy(hf3fs_ior* ior) {
    std::lock_guard lock(fake_mutex);
    auto* ring = static_cast<FakeRing*>(ior->iorh);
    fake.destroyed_outstanding += ring->pending.size();
    fake.outstanding -= ring->pending.size();
    delete ring;
    ior->iorh = nullptr;
}

int hf3fs_io_entries(const hf3fs_ior*) { return fake.entries; }

int hf3fs_prep_io(const hf3fs_ior* ior, const hf3fs_iov* iov, bool read,
                  void* ptr, int fd, size_t offset, uint64_t length,
                  const void* userdata) {
    std::lock_guard lock(fake_mutex);
    auto& ring = *static_cast<FakeRing*>(ior->iorh);
    EXPECT_EQ(ring.owner, std::this_thread::get_id());
    EXPECT_EQ(read, ior->for_read);
    auto* buffer = static_cast<uint8_t*>(ptr);
    EXPECT_GE(buffer, iov->base);
    EXPECT_LE(buffer + length, iov->base + iov->size);
    EXPECT_GT(length, 0);
    if (!fake.registered.contains(fd) || fd == fake.prep_error_fd)
        return -EBADF;
    if (ring.pending.size() >= static_cast<size_t>(fake.capacity))
        return -EAGAIN;
    for (const auto& io : ring.pending) {
        EXPECT_TRUE(buffer + length <= io.buffer ||
                    io.buffer + io.length <= buffer);
    }
    const int index = ring.next_index++ % fake.entries;
    FakeIo io{buffer, fd, offset, length, userdata, index, {}};
    if (!read) io.write_data.assign(buffer, buffer + length);
    ring.pending.push_back(std::move(io));
    fake.prepared_lengths.push_back(length);
    ++fake.prepared_counts[fd];
    ++fake.outstanding;
    fake.max_outstanding = std::max(fake.max_outstanding, fake.outstanding);
    return index;
}

int hf3fs_submit_ios(const hf3fs_ior* ior) {
    std::lock_guard lock(fake_mutex);
    const auto& ring = *static_cast<FakeRing*>(ior->iorh);
    fake.submissions.push_back(ring.pending.size());
    return std::exchange(fake.submit_error, 0);
}

int hf3fs_wait_for_ios(const hf3fs_ior* ior, hf3fs_cqe* cqes, int count,
                       int min_results, const timespec*) {
    std::lock_guard lock(fake_mutex);
    auto& ring = *static_cast<FakeRing*>(ior->iorh);
    EXPECT_EQ(ring.owner, std::this_thread::get_id());
    EXPECT_GT(min_results, 0);
    ++fake.waits;
    if (!fake.wait_returns.empty()) {
        const int result = fake.wait_returns.front();
        fake.wait_returns.pop_front();
        return result;
    }
    count = std::min({count, fake.completions_per_wait,
                      static_cast<int>(ring.pending.size())});
    for (int i = 0; i < count; ++i) {
        auto io = std::move(ring.pending.back());
        ring.pending.pop_back();
        --fake.outstanding;
        int64_t result = io.length;
        if (auto it = fake.io_results.find(io.fd);
            it != fake.io_results.end()) {
            result = it->second;
        }
        if (result >= 0 && static_cast<uint64_t>(result) <= io.length) {
            if (ior->for_read) {
                result = ::pread(io.fd, io.buffer, result, io.offset);
            } else {
                EXPECT_EQ(io.write_data, std::vector<uint8_t>(
                                             io.buffer, io.buffer + io.length));
                result = ::pwrite(io.fd, io.buffer, result, io.offset);
            }
            if (result < 0) result = -errno;
        }
        cqes[i] = {io.index, 0, result,
                   fake.bad_userdata ? nullptr : io.userdata};
    }
    return count;
}
}  // extern "C"

namespace mooncake::test {
namespace {

class Hf3fsBatchTest : public ::testing::Test {
   protected:
    void SetUp() override {
        fake = {};
        char path[] = "/tmp/mooncake-hf3fs-batch-XXXXXX";
        ASSERT_NE(::mkdtemp(path), nullptr);
        directory_ = path;
        ASSERT_TRUE(adapter_.Init(directory_));
    }

    void TearDown() override {
        EXPECT_EQ(fake.outstanding, 0);
        for (int fd : fds_) EXPECT_TRUE(adapter_.CloseFile(fd));
        EXPECT_TRUE(adapter_.Shutdown());
        std::filesystem::remove_all(directory_);
    }

    int Open() {
        auto fd =
            adapter_.OpenFile(directory_ + "/" + std::to_string(fds_.size()));
        EXPECT_TRUE(fd);
        if (!fd) return -1;
        fds_.push_back(*fd);
        return *fd;
    }

    void CheckSuccess(
        const std::vector<tl::expected<size_t, ErrorCode>>& results,
        size_t count, size_t length) {
        ASSERT_EQ(results.size(), count);
        for (const auto& result : results) {
            ASSERT_TRUE(result);
            EXPECT_EQ(*result, length);
        }
    }

    Hf3fsAdapter adapter_;
    std::string directory_;
    std::vector<int> fds_;
};

TEST_F(Hf3fsBatchTest,
       BatchesAcrossFilesAndRingCapacityWithReorderedCompletions) {
    constexpr size_t count = 11;
    std::array<std::string, count> data;
    std::array<iovec, count> iovs;
    std::array<FdIoRequest, count> requests;
    const int shared_fd = Open();
    for (size_t i = 0; i < count; ++i) {
        data[i] = std::string(9, 'a' + i);
        iovs[i] = {data[i].data(), data[i].size()};
        requests[i] = {i % 2 ? shared_fd : Open(), &iovs[i], 1,
                       static_cast<int64_t>(i * 19 + 3)};
    }
    CheckSuccess(adapter_.BatchWriteAt(requests), count, 9);
    EXPECT_EQ(fake.submissions, (std::vector<size_t>{4, 4, 3}));
    EXPECT_EQ(fake.max_outstanding, 4);
    EXPECT_EQ(fake.waits, 6);
    for (auto& value : data) std::fill(value.begin(), value.end(), '?');
    fake.submissions.clear();
    CheckSuccess(adapter_.BatchReadAt(requests), count, 9);
    EXPECT_EQ(fake.submissions, (std::vector<size_t>{4, 4, 3}));
    for (size_t i = 0; i < count; ++i)
        EXPECT_EQ(data[i], std::string(9, 'a' + i));
}

TEST_F(Hf3fsBatchTest, ChunksLargeVectorsWithoutMixingStagingSlices) {
    std::array<std::string, 3> data{std::string(137, 'A'), std::string(81, 'B'),
                                    std::string(7, 'C')};
    for (size_t i = 0; i < data.size(); ++i) {
        for (size_t j = 0; j < data[i].size(); ++j) {
            data[i][j] = 'A' + (j * 13 + i * 7) % 26;
        }
    }
    const auto expected = data;
    std::array<std::array<iovec, 4>, 3> iovs;
    std::array<FdIoRequest, 3> requests;
    for (size_t i = 0; i < data.size(); ++i) {
        iovs[i] = {{{nullptr, 0},
                    {data[i].data(), 3},
                    {nullptr, 0},
                    {data[i].data() + 3, data[i].size() - 3}}};
        requests[i] = {Open(), iovs[i].data(), 4, 5};
    }
    auto results = adapter_.BatchWriteAt(requests);
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_TRUE(results[i]);
        EXPECT_EQ(*results[i], data[i].size());
        std::string stored(data[i].size(), '?');
        ASSERT_EQ(::pread(requests[i].fd, stored.data(), stored.size(), 5),
                  static_cast<ssize_t>(stored.size()));
        EXPECT_EQ(stored, expected[i]);
        std::fill(data[i].begin(), data[i].end(), '?');
    }
    EXPECT_GT(fake.submissions.front(), 1);
    EXPECT_LE(*std::max_element(fake.prepared_lengths.begin(),
                                fake.prepared_lengths.end()),
              fake.buffer_size);
    results = adapter_.BatchReadAt(requests);
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_TRUE(results[i]);
        EXPECT_EQ(*results[i], data[i].size());
        EXPECT_EQ(data[i], expected[i]);
    }
}

TEST_F(Hf3fsBatchTest, PreservesPerRequestPrepAndCompletionErrors) {
    std::array<std::string, 4> data;
    std::array<iovec, 4> iovs;
    std::array<FdIoRequest, 4> requests;
    for (size_t i = 0; i < requests.size(); ++i) {
        data[i] = std::string(80, 'a' + i);
        iovs[i] = {data[i].data(), data[i].size()};
        requests[i] = {Open(), &iovs[i], 1, 0};
    }
    fake.prep_error_fd = requests[1].fd;
    fake.io_results[requests[2].fd] = -EIO;
    fake.io_results[requests[3].fd] = 5;
    const auto results = adapter_.BatchWriteAt(requests);
    ASSERT_TRUE(results[0]);
    EXPECT_EQ(*results[0], 80);
    for (size_t i = 1; i < results.size(); ++i) {
        ASSERT_FALSE(results[i]);
        EXPECT_TRUE(results[i].error() == ErrorCode::FILE_WRITE_FAIL);
    }
    EXPECT_EQ(::lseek(requests[3].fd, 0, SEEK_END), 5);
    EXPECT_EQ(fake.destroyed_outstanding, 0);
    fake.prep_error_fd = -1;
    fake.io_results.clear();
    CheckSuccess(adapter_.BatchWriteAt(requests), 4, 80);
}

TEST_F(Hf3fsBatchTest, ShortReadsCopyOnlyAvailableBytesAndStopTheirTail) {
    std::string short_data = "abcde";
    const int short_fd = Open();
    ASSERT_EQ(::pwrite(short_fd, short_data.data(), short_data.size(), 0), 5);
    const int full_fd = Open();
    std::string full_data(90, 'F');
    ASSERT_EQ(::pwrite(full_fd, full_data.data(), full_data.size(), 0), 90);
    std::array<char, 90> short_output;
    short_output.fill('?');
    std::array<iovec, 3> short_iovs{{{short_output.data(), 3},
                                     {nullptr, 0},
                                     {short_output.data() + 3, 87}}};
    iovec full_iov{full_data.data(), full_data.size()};
    std::array<FdIoRequest, 2> requests{
        {{short_fd, short_iovs.data(), 3, 0}, {full_fd, &full_iov, 1, 0}}};
    const auto results = adapter_.BatchReadAt(requests);
    ASSERT_FALSE(results[0]);
    EXPECT_TRUE(results[0].error() == ErrorCode::FILE_READ_FAIL);
    ASSERT_TRUE(results[1]);
    EXPECT_EQ(*results[1], 90);
    EXPECT_EQ(std::string(short_output.data(), 5), short_data);
    EXPECT_EQ(std::string(short_output.data() + 5, 85), std::string(85, '?'));
    EXPECT_EQ(fake.prepared_counts[short_fd], 1);
    EXPECT_GT(fake.prepared_counts[full_fd], 1);
}

TEST_F(Hf3fsBatchTest, HandlesRingPressurePartialWaitsAndInterruptedWaits) {
    fake.capacity = 2;
    fake.completions_per_wait = 1;
    fake.wait_returns = {0, -EINTR, -EAGAIN};
    std::string data(55, 'R');
    iovec iov{data.data(), data.size()};
    std::array<FdIoRequest, 5> requests;
    for (auto& request : requests) request = {Open(), &iov, 1, 0};
    CheckSuccess(adapter_.BatchWriteAt(requests), requests.size(), data.size());
    EXPECT_EQ(fake.max_outstanding, 2);
    EXPECT_EQ(fake.destroyed_outstanding, 0);
    for (const auto& request : requests) {
        std::string output(data.size(), '?');
        ASSERT_EQ(::pread(request.fd, output.data(), output.size(), 0), 55);
        EXPECT_EQ(output, data);
    }
}

TEST_F(Hf3fsBatchTest, SubmitFailureStillDrainsPreparedRequests) {
    fake.submit_error = -EIO;
    char data = 'x';
    iovec iov{&data, 1};
    std::array<FdIoRequest, 3> requests;
    for (auto& request : requests) request = {Open(), &iov, 1, 0};
    auto results = adapter_.BatchWriteAt(requests);
    for (const auto& result : results) {
        ASSERT_FALSE(result);
        EXPECT_TRUE(result.error() == ErrorCode::FILE_WRITE_FAIL);
    }
    EXPECT_EQ(fake.outstanding, 0);
    EXPECT_EQ(fake.destroyed_outstanding, 0);
    EXPECT_EQ(fake.waits, 2);
    CheckSuccess(adapter_.BatchWriteAt(requests), 3, 1);
}

TEST_F(Hf3fsBatchTest, FatalWaitRetiresResourcesBeforeTheNextOperation) {
    fake.wait_returns = {-EIO};
    char data = 'x';
    iovec iov{&data, 1};
    std::array<FdIoRequest, 2> requests{
        {{Open(), &iov, 1, 0}, {Open(), &iov, 1, 0}}};
    const auto results = adapter_.BatchWriteAt(requests);
    for (const auto& result : results) ASSERT_FALSE(result);
    EXPECT_EQ(fake.destroyed_outstanding, 2);
    EXPECT_EQ(fake.rings_created, 2);
    auto written = adapter_.WriteAt(requests[0].fd, &iov, 1, 0);
    ASSERT_TRUE(written);
    EXPECT_EQ(*written, 1);
    EXPECT_EQ(fake.rings_created, 4);
    EXPECT_EQ(fake.buffers_created, 2);
    data = '?';
    ASSERT_TRUE(adapter_.ReadAt(requests[0].fd, &iov, 1, 0));
    EXPECT_EQ(data, 'x');
}

TEST_F(Hf3fsBatchTest, InvalidCompletionsCannotCopyPastTheRequestedBuffer) {
    const int fd = Open();
    std::array<char, 3> output{'?', '?', '?'};
    iovec iov{output.data() + 1, 1};
    const FdIoRequest request{fd, &iov, 1, 0};
    for (const int64_t result : {int64_t{0}, int64_t{-EIO}, int64_t{2}}) {
        fake.io_results[fd] = result;
        const auto results = adapter_.BatchReadAt({&request, 1});
        ASSERT_FALSE(results[0]);
        EXPECT_TRUE(results[0].error() == ErrorCode::FILE_READ_FAIL);
        EXPECT_EQ(output, (std::array<char, 3>{'?', '?', '?'}));
    }
    fake.io_results.clear();
    fake.bad_userdata = true;
    EXPECT_FALSE(adapter_.BatchReadAt({&request, 1})[0]);
    fake.bad_userdata = false;
    ASSERT_TRUE(adapter_.WriteAt(fd, &iov, 1, 0));
    EXPECT_EQ(fake.rings_created, 4);
}

TEST_F(Hf3fsBatchTest, RejectsInvalidRequestsWithoutBlockingValidNeighbors) {
    char data = 'v';
    iovec valid{&data, 1};
    iovec null_buffer{nullptr, 1};
    iovec huge{&data, std::numeric_limits<size_t>::max()};
    std::array<iovec, 2> overflow{
        {{&data, 1},
         {&data, static_cast<size_t>(std::numeric_limits<int64_t>::max())}}};
    const int fd = Open();
    std::vector<FdIoRequest> requests{
        {-1, &valid, 1, 0},
        {fd, &valid, 1, -1},
        {fd, &valid, -1, 0},
        {fd, nullptr, 1, 0},
        {fd, &null_buffer, 1, 0},
        {fd, &huge, 1, 0},
        {fd, overflow.data(), 2, 0},
        {fd, &valid, 1, std::numeric_limits<int64_t>::max()},
        {fd, nullptr, 0, 0},
        {fd, &valid, 1, 0}};
    const auto results = adapter_.BatchWriteAt(requests);
    ASSERT_EQ(results.size(), requests.size());
    for (size_t i = 0; i < 8; ++i) {
        ASSERT_FALSE(results[i]);
        EXPECT_TRUE(results[i].error() == ErrorCode::INVALID_PARAMS);
    }
    ASSERT_TRUE(results[8]);
    EXPECT_EQ(*results[8], 0);
    ASSERT_TRUE(results[9]);
    EXPECT_EQ(*results[9], 1);
    EXPECT_EQ(fake.prepared_lengths.size(), 1);
}

TEST_F(Hf3fsBatchTest, EmptyBatchesAndMissingResources) {
    Hf3fsAdapter uninitialized;
    EXPECT_TRUE(uninitialized.BatchWriteAt({}).empty());
    EXPECT_TRUE(uninitialized.BatchReadAt({}).empty());
    const FdIoRequest empty{1, nullptr, 0, 0};
    CheckSuccess(uninitialized.BatchReadAt({&empty, 1}), 1, 0);
    char data = 'x';
    iovec iov{&data, 1};
    const FdIoRequest request{Open(), &iov, 1, 0};
    for (auto* adapter : {&uninitialized, &adapter_}) {
        fake.fail_create = true;
        const auto results = adapter->BatchReadAt({&request, 1});
        ASSERT_FALSE(results[0]);
        EXPECT_TRUE(results[0].error() == ErrorCode::FILE_OPEN_FAIL);
    }
    fake.fail_create = false;
    CheckSuccess(adapter_.BatchWriteAt({&request, 1}), 1, 1);
}

TEST_F(Hf3fsBatchTest, ConcurrentCallersUseSeparateRingsOnTheSameFd) {
    const int fd = Open();
    std::array<std::thread, 3> threads;
    std::barrier start(3);
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([&, i] {
            std::string data(101, 'a' + i);
            iovec iov{data.data(), data.size()};
            const int64_t offset = i * 512;
            std::array<FdIoRequest, 2> requests{
                {{fd, &iov, 1, offset}, {fd, &iov, 1, offset + 128}}};
            start.arrive_and_wait();
            CheckSuccess(adapter_.BatchWriteAt(requests), 2, data.size());
            std::fill(data.begin(), data.end(), '?');
            // Read one destination at a time to avoid overlapping user buffers.
            ASSERT_TRUE(adapter_.ReadAt(fd, &iov, 1, offset));
            EXPECT_EQ(data, std::string(101, 'a' + i));
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(fake.rings_created, 6);
    EXPECT_EQ(fake.buffers_created, 3);
}

}  // namespace
}  // namespace mooncake::test
