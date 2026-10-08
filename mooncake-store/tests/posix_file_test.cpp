#include <glog/logging.h>
#include <gtest/gtest.h>
#include <array>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <fcntl.h>
#include <functional>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/uio.h>
#include <string_view>
#include <thread>
#include <vector>
#include "file_interface.h"
#include "../src/iovec_cursor.h"
#include "../src/uring_submit.h"

namespace mooncake {

class PosixFileTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PosixFileTest");
        FLAGS_logtostderr = 1;

        // Create and open a test file
        test_filename = "test_file.txt";
        test_fd = open(test_filename.c_str(), O_CREAT | O_RDWR, 0644);
        ASSERT_GE(test_fd, 0) << "Failed to open test file";
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        if (test_fd >= 0) {
            close(test_fd);
        }
        remove(test_filename.c_str());
    }

    std::string test_filename;
    int test_fd = -1;
};

// PosixFile continues a short pwritev/preadv on a copy of the caller's
// iovecs. A kernel cannot be made to stop at an arbitrary byte and then
// succeed, so where the continuation starts is tested on the helpers.
class IovecCursorTest : public ::testing::Test {
   protected:
    // iovecs over consecutive slices of `stream_`, one per length.
    std::vector<iovec> Slices(const std::vector<size_t>& lens) {
        size_t total = 0;
        for (size_t len : lens) total += len;
        stream_.resize(total);
        for (size_t i = 0; i < total; ++i)
            stream_[i] = static_cast<char>('!' + i % 89);
        std::vector<iovec> iovs;
        size_t at = 0;
        for (size_t len : lens) {
            iovs.push_back({stream_.data() + at, len});
            at += len;
        }
        return iovs;
    }

    // The bytes the remaining iovecs still describe, in order.
    static std::string Remaining(const std::vector<iovec>& iovs, size_t first) {
        std::string out;
        for (size_t i = first; i < iovs.size(); ++i)
            out.append(static_cast<const char*>(iovs[i].iov_base),
                       iovs[i].iov_len);
        return out;
    }

    char* At(size_t offset) { return stream_.data() + offset; }

    std::string stream_;
};

TEST_F(IovecCursorTest, StopInsideAnIovecResumesAtThatByte) {
    auto iovs = Slices({3000, 3000});
    size_t first = 0;
    ASSERT_TRUE(detail::SkipEmptyIovecs(iovs, first));
    detail::ConsumeIovecs(iovs, first, 4096);
    ASSERT_TRUE(detail::SkipEmptyIovecs(iovs, first));
    EXPECT_EQ(first, 1u);
    EXPECT_EQ(iovs[1].iov_base, At(4096));
    EXPECT_EQ(iovs[1].iov_len, 1904u);
    EXPECT_EQ(Remaining(iovs, first), stream_.substr(4096));
}

TEST_F(IovecCursorTest, StopAtAnIovecBoundaryResumesAtTheNextIovec) {
    auto iovs = Slices({3000, 3000});
    size_t first = 0;
    detail::ConsumeIovecs(iovs, first, 3000);
    EXPECT_EQ(first, 1u);
    EXPECT_EQ(iovs[0].iov_len, 0u);
    EXPECT_EQ(iovs[1].iov_base, At(3000));
    EXPECT_EQ(iovs[1].iov_len, 3000u);
    EXPECT_EQ(Remaining(iovs, first), stream_.substr(3000));
}

TEST_F(IovecCursorTest, StopAcrossSeveralBoundaries) {
    auto iovs = Slices({100, 200, 300, 400});
    size_t first = 0;
    detail::ConsumeIovecs(iovs, first, 350);
    EXPECT_EQ(first, 2u);
    EXPECT_EQ(iovs[2].iov_base, At(350));
    EXPECT_EQ(iovs[2].iov_len, 250u);
    EXPECT_EQ(iovs[3].iov_base, At(600));
    EXPECT_EQ(iovs[3].iov_len, 400u);
    EXPECT_EQ(Remaining(iovs, first), stream_.substr(350));

    detail::ConsumeIovecs(iovs, first, 650);
    EXPECT_EQ(first, 4u);
    EXPECT_FALSE(detail::SkipEmptyIovecs(iovs, first));
}

TEST_F(IovecCursorTest, ZeroLengthIovecsAreSkipped) {
    // Empty iovecs at the front, in the middle and at the end.
    auto iovs = Slices({0, 100, 0, 0, 200, 0});
    size_t first = 0;
    ASSERT_TRUE(detail::SkipEmptyIovecs(iovs, first));
    EXPECT_EQ(first, 1u);

    detail::ConsumeIovecs(iovs, first, 100);
    EXPECT_EQ(first, 2u);
    ASSERT_TRUE(detail::SkipEmptyIovecs(iovs, first));
    EXPECT_EQ(first, 4u);

    detail::ConsumeIovecs(iovs, first, 50);
    EXPECT_EQ(iovs[4].iov_base, At(150));
    EXPECT_EQ(iovs[4].iov_len, 150u);
    EXPECT_EQ(Remaining(iovs, first), stream_.substr(150));

    detail::ConsumeIovecs(iovs, first, 150);
    EXPECT_FALSE(detail::SkipEmptyIovecs(iovs, first));
    EXPECT_EQ(first, 6u);

    // ConsumeIovecs also steps over empty iovecs on its own.
    iovs = Slices({10, 0, 0, 20});
    first = 0;
    detail::ConsumeIovecs(iovs, first, 15);
    EXPECT_EQ(first, 3u);
    EXPECT_EQ(iovs[3].iov_base, At(15));
    EXPECT_EQ(iovs[3].iov_len, 15u);
}

// The same loop as PosixFile::vector_write, with a fake pwritev that moves
// at most `max_per_call` bytes: the destination must equal the stream for
// every short count, including stops inside, at and across iovec bounds.
TEST_F(IovecCursorTest, EveryShortCountCopiesTheStreamInOrder) {
    const std::vector<size_t> lens = {0, 3000, 0, 1096, 1, 0, 2903, 0};
    for (size_t max_per_call :
         {size_t{1}, size_t{7}, size_t{1095}, size_t{1096}, size_t{2999},
          size_t{3000}, size_t{3001}, size_t{4096}, size_t{7000}}) {
        auto iovs = Slices(lens);
        std::string dest(stream_.size(), '\0');
        size_t first = 0;
        size_t written = 0;
        while (detail::SkipEmptyIovecs(iovs, first)) {
            size_t moved = 0;
            for (size_t i = first; i < iovs.size() && moved < max_per_call;
                 ++i) {
                const size_t n =
                    std::min(iovs[i].iov_len, max_per_call - moved);
                std::memcpy(dest.data() + written + moved, iovs[i].iov_base, n);
                moved += n;
            }
            written += moved;
            detail::ConsumeIovecs(iovs, first, moved);
        }
        EXPECT_EQ(written, stream_.size()) << "max_per_call=" << max_per_call;
        EXPECT_EQ(dest, stream_) << "max_per_call=" << max_per_call;
    }
}

#ifdef USE_URING
TEST(UringSubmitTest, ContinuesAfterPositiveShortSubmit) {
    unsigned pending = 8;
    std::vector<unsigned> requested;
    std::array<int, 2> returns{3, 5};
    size_t call = 0;

    auto result = detail::submit_all_pending(
        [&] { return pending; },
        [&](unsigned requested_count) {
            requested.push_back(requested_count);
            int submitted = returns[call++];
            pending -= static_cast<unsigned>(submitted);
            return submitted;
        },
        [] {});

    EXPECT_EQ(result.error, 0);
    EXPECT_EQ(result.submitted, 8U);
    EXPECT_EQ(result.pending, 0U);
    EXPECT_EQ(requested, (std::vector<unsigned>{8, 5}));
}

TEST(UringSubmitTest, RetriesTransientSubmissionErrors) {
    unsigned pending = 4;
    std::array<int, 3> returns{-EINTR, -EAGAIN, 4};
    size_t call = 0;
    unsigned yields = 0;

    auto result = detail::submit_all_pending(
        [&] { return pending; },
        [&](unsigned) {
            int ret = returns[call++];
            if (ret > 0) pending -= static_cast<unsigned>(ret);
            return ret;
        },
        [&] { ++yields; });

    EXPECT_EQ(result.error, 0);
    EXPECT_EQ(result.submitted, 4U);
    EXPECT_EQ(result.pending, 0U);
    EXPECT_EQ(yields, 2U);
}

TEST(UringSubmitTest, StopsAfterBoundedNoProgress) {
    unsigned pending = 4;
    unsigned calls = 0;
    auto submit = [&](unsigned) {
        ++calls;
        return -ENOMEM;
    };

    auto result =
        detail::submit_all_pending([&] { return pending; }, submit, [] {}, 2);

    EXPECT_EQ(result.error, -ENOMEM);
    EXPECT_EQ(result.submitted, 0U);
    EXPECT_EQ(result.pending, pending);
    EXPECT_EQ(calls, 3U);
}
#endif

// Test basic file lifecycle
TEST_F(PosixFileTest, FileLifecycle) {
    PosixFile posix_file(test_filename, test_fd);
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
    // Destructor will close the file
}

// Test basic write operation
TEST_F(PosixFileTest, BasicWrite) {
    PosixFile posix_file(test_filename, test_fd);

    std::string test_data = "Test write data";
    auto result = posix_file.write(test_data, test_data.size());

    ASSERT_TRUE(result) << "Write failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, test_data.size());
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// Test basic read operation
TEST_F(PosixFileTest, BasicRead) {
    // Clear file content
    ASSERT_EQ(ftruncate(test_fd, 0), 0) << "Failed to truncate file";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    // Write test data
    const char* test_data = "Test read data";
    ssize_t written = write(test_fd, test_data, strlen(test_data));
    ASSERT_EQ(written, static_cast<ssize_t>(strlen(test_data)))
        << "Write failed";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    PosixFile posix_file(test_filename, test_fd);

    std::string buffer;
    auto result = posix_file.read(
        buffer, strlen(test_data));  // Read up to test_data bytes

    ASSERT_TRUE(result) << "Read failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, strlen(test_data));
    EXPECT_EQ(buffer, test_data);
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// Test vectorized write operation
TEST_F(PosixFileTest, VectorizedWrite) {
    PosixFile posix_file(test_filename, test_fd);

    std::string data1 = "First part ";
    std::string data2 = "Second part";

    iovec iov[2];
    iov[0].iov_base = const_cast<char*>(data1.data());
    iov[0].iov_len = data1.size();
    iov[1].iov_base = const_cast<char*>(data2.data());
    iov[1].iov_len = data2.size();

    auto result = posix_file.vector_write(iov, 2, 0);

    ASSERT_TRUE(result) << "Vector write failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, data1.size() + data2.size());
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// A write that cannot complete must fail, so that the PosixFile destructor
// removes the partial object instead of leaving it truncated on disk.
TEST_F(PosixFileTest, ShortVectorizedWriteRemovesPartialFile) {
#if defined(RLIMIT_FSIZE) && defined(SIGXFSZ)
    const std::string partial_filename =
        "short_vector_write_" + std::to_string(getpid()) + ".tmp";

    pid_t child = fork();
    ASSERT_GE(child, 0) << "Failed to fork short-write test";

    if (child == 0) {
        if (signal(SIGXFSZ, SIG_IGN) == SIG_ERR) _exit(10);

        const struct rlimit limit = {4096, 4096};
        if (setrlimit(RLIMIT_FSIZE, &limit) != 0) _exit(11);

        int fd =
            open(partial_filename.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (fd < 0) _exit(12);

        std::string data(8192, 'x');
        {
            PosixFile posix_file(partial_filename, fd);
            iovec iov{data.data(), data.size()};
            auto result = posix_file.vector_write(&iov, 1, 0);
            if (result || result.error() != ErrorCode::FILE_WRITE_FAIL)
                _exit(13);
        }

        if (access(partial_filename.c_str(), F_OK) == 0) _exit(14);
        if (errno != ENOENT) _exit(15);
        _exit(0);
    }

    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    unlink(partial_filename.c_str());
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
#else
    GTEST_SKIP() << "RLIMIT_FSIZE/SIGXFSZ is unavailable on this platform";
#endif
}

// A failed syscall keeps its errno (the first one wins), so the disk fence
// can classify it; the ErrorCode alone cannot.
TEST_F(PosixFileTest, FailedSyscallKeepsFirstErrno) {
    int full_fd = open("/dev/full", O_RDWR);
    if (full_fd < 0) GTEST_SKIP() << "/dev/full is unavailable";
    {
        PosixFile full("/dev/full", full_fd);
        // A failed write would otherwise unlink the path on destruction.
        full.SetDeleteOnWriteFail(false);
        EXPECT_EQ(full.sys_errno(), 0);
        std::string data(4096, 'x');
        auto written = full.write(data, data.size());
        ASSERT_FALSE(written);
        EXPECT_EQ(written.error(), ErrorCode::FILE_WRITE_FAIL);
        EXPECT_EQ(full.sys_errno(), ENOSPC);

        iovec iov{data.data(), data.size()};
        ASSERT_FALSE(full.vector_write(&iov, 1, 0));
        EXPECT_EQ(full.sys_errno(), ENOSPC);
    }

    // preadv on a write-only descriptor fails with EBADF.
    int wronly_fd = open(test_filename.c_str(), O_WRONLY);
    ASSERT_GE(wronly_fd, 0);
    PosixFile wronly(test_filename, wronly_fd);
    char buf[16];
    iovec iov{buf, sizeof(buf)};
    auto read = wronly.vector_read(&iov, 1, 0);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_EQ(wronly.sys_errno(), EBADF);
}

// A short read is a logical failure: no syscall failed, so errno stays 0.
TEST_F(PosixFileTest, ShortReadHasNoErrno) {
    PosixFile posix_file(test_filename, test_fd);
    test_fd = -1;  // owned by posix_file now
    std::string buffer;
    auto read = posix_file.read(buffer, 64);  // the file is empty
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_EQ(posix_file.sys_errno(), 0);
}

#if defined(RLIMIT_FSIZE) && defined(SIGXFSZ)
// Runs `body` in a child whose file size limit is `limit_bytes`. With SIGXFSZ
// ignored, a write that crosses the limit is short, and the next write at the
// limit fails with EFBIG: a short write followed by the real error.
static int RunWithFileSizeLimit(rlim_t limit_bytes,
                                const std::function<int()>& body) {
    pid_t child = fork();
    if (child < 0) return -1;
    if (child == 0) {
        if (signal(SIGXFSZ, SIG_IGN) == SIG_ERR) _exit(100);
        const struct rlimit limit = {limit_bytes, limit_bytes};
        if (setrlimit(RLIMIT_FSIZE, &limit) != 0) _exit(101);
        _exit(body());
    }
    int status = 0;
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status)) return -1;
    return WEXITSTATUS(status);
}
#endif

// A short pwritev is continued, so the caller sees the errno of the call that
// really failed (EFBIG here, EIO on a dying disk) instead of a bare failure.
TEST_F(PosixFileTest, ShortVectorWriteContinuesToTheRealErrno) {
#if defined(RLIMIT_FSIZE) && defined(SIGXFSZ)
    const std::string path =
        "short_write_errno_" + std::to_string(getpid()) + ".tmp";
    int rc = RunWithFileSizeLimit(4096, [&]() -> int {
        int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (fd < 0) return 10;
        PosixFile file(path, fd);
        std::string data(8192, 'x');
        iovec iov{data.data(), data.size()};
        auto result = file.vector_write(&iov, 1, 0);
        if (result) return 11;
        if (result.error() != ErrorCode::FILE_WRITE_FAIL) return 12;
        if (file.sys_errno() != EFBIG) return 13;
        return 0;
    });
    unlink(path.c_str());
    EXPECT_EQ(rc, 0) << "11: write succeeded, 12: wrong ErrorCode, "
                        "13: errno is not EFBIG";
#else
    GTEST_SKIP() << "RLIMIT_FSIZE/SIGXFSZ is unavailable on this platform";
#endif
}

// The first pwritev stops inside the second iovec, at the file size limit,
// and the continuation fails there with EFBIG. The caller sees that errno, its
// iovecs are not modified, and the bytes before the stop are on disk. (The
// continuation writes nothing here; where it resumes is tested by
// IovecCursorTest.)
TEST_F(PosixFileTest, ShortVectorWriteInsideSecondIovecKeepsTheRealErrno) {
#if defined(RLIMIT_FSIZE) && defined(SIGXFSZ)
    const std::string path =
        "short_write_iov_" + std::to_string(getpid()) + ".tmp";
    int rc = RunWithFileSizeLimit(4096, [&]() -> int {
        int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (fd < 0) return 10;
        PosixFile file(path, fd);
        file.SetDeleteOnWriteFail(false);  // keep the bytes for the check
        std::string a(3000, 'a');
        std::string b(3000, 'b');
        iovec iov[2] = {{a.data(), a.size()}, {b.data(), b.size()}};
        auto result = file.vector_write(iov, 2, 0);
        if (result) return 11;
        if (file.sys_errno() != EFBIG) return 12;
        // The caller's iovecs are not modified.
        if (iov[0].iov_base != a.data() || iov[0].iov_len != a.size() ||
            iov[1].iov_base != b.data() || iov[1].iov_len != b.size())
            return 13;
        return 0;
    });
    EXPECT_EQ(rc, 0) << "11: write succeeded, 12: errno is not EFBIG, "
                        "13: caller's iovecs changed";

    // 3000 'a' then the first 1096 bytes of the second iovec.
    std::string on_disk;
    {
        int fd = open(path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        char buf[8192];
        ssize_t n = pread(fd, buf, sizeof(buf), 0);
        close(fd);
        ASSERT_GE(n, 0);
        on_disk.assign(buf, static_cast<size_t>(n));
    }
    unlink(path.c_str());
    EXPECT_EQ(on_disk, std::string(3000, 'a') + std::string(1096, 'b'));
#else
    GTEST_SKIP() << "RLIMIT_FSIZE/SIGXFSZ is unavailable on this platform";
#endif
}

// vector_read returns fewer bytes than asked only at EOF. A short preadv
// before EOF is continued, so the caller sees the errno of the call that
// really failed: here the second iovec is an inaccessible page, the kernel
// fills the first iovec and returns that short count, and the continuation
// fails with EFAULT (on a disk, a bad block gives a short read, then EIO).
TEST_F(PosixFileTest, VectorReadIsShortOnlyAtEof) {
    const size_t page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    std::string content(2 * page, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('0' + i % 75);
    ASSERT_EQ(pwrite(test_fd, content.data(), content.size(), 0),
              static_cast<ssize_t>(content.size()));
    PosixFile file(test_filename, test_fd);
    test_fd = -1;  // owned by file now

    // EOF inside the third iovec: the short count, no error, iovecs in order.
    char c1[30], c2[30], c3[30];
    iovec eof_iov[3] = {{c1, 30}, {c2, 30}, {c3, 30}};
    auto at_eof = file.vector_read(eof_iov, 3, content.size() - 50);
    ASSERT_TRUE(at_eof);
    EXPECT_EQ(*at_eof, 50u);
    EXPECT_EQ(std::string(c1, 30) + std::string(c2, 20),
              content.substr(content.size() - 50));
    EXPECT_EQ(file.sys_errno(), 0);

    // Short before EOF: continued, and the continuation's errno is kept.
    void* mem = mmap(nullptr, 2 * page, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(mem, MAP_FAILED);
    char* base = static_cast<char*>(mem);
    ASSERT_EQ(mprotect(base + page, page, PROT_NONE), 0);
    iovec iov[2] = {{base, page}, {base + page, page}};
    auto result = file.vector_read(iov, 2, 0);
    EXPECT_FALSE(result);
    EXPECT_EQ(file.sys_errno(), EFAULT);
    EXPECT_EQ(std::string(base, page), content.substr(0, page));
    munmap(mem, 2 * page);
}

// Test vectorized read operation
TEST_F(PosixFileTest, VectorizedRead) {
    // Clear file content
    ASSERT_EQ(ftruncate(test_fd, 0), 0) << "Failed to truncate file";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    // Write test data
    const char* test_data = "Vectorized read test data";
    ssize_t written = write(test_fd, test_data, strlen(test_data));
    ASSERT_EQ(written, static_cast<ssize_t>(strlen(test_data)))
        << "Write failed";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    PosixFile posix_file(test_filename, test_fd);

    char buf1[11] = {0};  // "Vectorized" + null
    char buf2[16] = {0};  // " read test data" + null

    iovec iov[2];
    iov[0].iov_base = buf1;
    iov[0].iov_len = 10;  // Exact length of "Vectorized"
    iov[1].iov_base = buf2;
    iov[1].iov_len = 15;  // Exact length of " read test data"

    auto result = posix_file.vector_read(iov, 2, 0);

    ASSERT_TRUE(result) << "Vector read failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, strlen(test_data));
    EXPECT_STREQ(buf1, "Vectorized");
    EXPECT_STREQ(buf2, " read test data");
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// Test error cases
TEST_F(PosixFileTest, ErrorCases) {
    // Test invalid file descriptor
    PosixFile posix_file("invalid.txt", -1);
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::FILE_INVALID_HANDLE);

    // Test write to invalid file
    std::string test_data = "test";
    auto write_result = posix_file.write(test_data, test_data.size());
    EXPECT_FALSE(write_result);
    EXPECT_EQ(write_result.error(), ErrorCode::FILE_NOT_FOUND);

    // Test read from invalid file
    std::string buffer;
    auto read_result = posix_file.read(buffer, test_data.size());
    EXPECT_FALSE(read_result);
    EXPECT_EQ(read_result.error(), ErrorCode::FILE_NOT_FOUND);
}

// Test file locking
TEST_F(PosixFileTest, FileLocking) {
    PosixFile posix_file(test_filename, test_fd);

    {
        // Acquire write lock
        auto lock = posix_file.acquire_write_lock();
        EXPECT_TRUE(lock.is_locked());

        // Try to read while locked
        std::string buffer;
        auto result = posix_file.read(buffer, 10);
        EXPECT_FALSE(result);
    }

    {
        // Acquire read lock
        auto lock = posix_file.acquire_read_lock();
        EXPECT_TRUE(lock.is_locked());
    }
}

#ifdef USE_URING
TEST_F(PosixFileTest, UringBatchReadReportsPerRequestResultsAcrossQueueDepth) {
    constexpr size_t kBlockSize = 128;
    constexpr int kRequestCount = 40;
    std::vector<std::array<char, kBlockSize>> expected(kRequestCount);
    for (int i = 0; i < kRequestCount; ++i) {
        expected[i].fill(static_cast<char>('A' + i % 26));
        ASSERT_EQ(pwrite(test_fd, expected[i].data(), expected[i].size(),
                         static_cast<off_t>(i * kBlockSize)),
                  static_cast<ssize_t>(expected[i].size()));
    }

    int uring_fd = dup(test_fd);
    ASSERT_GE(uring_fd, 0);
    UringFile uring_file(test_filename, uring_fd, 32, false);
    std::vector<std::array<char, kBlockSize>> actual(kRequestCount);
    std::vector<UringFile::ReadDesc> descs;
    descs.reserve(kRequestCount);
    for (int i = 0; i < kRequestCount; ++i) {
        descs.push_back(
            UringFile::ReadDesc{actual[i].data(), actual[i].size(),
                                static_cast<off_t>(i * kBlockSize)});
    }

    auto result = uring_file.batch_read(descs.data(), descs.size());
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    for (int i = 0; i < kRequestCount; ++i) {
        EXPECT_TRUE(descs[i].completed);
        EXPECT_EQ(descs[i].error, ErrorCode::OK);
        EXPECT_EQ(descs[i].bytes_read, kBlockSize);
        EXPECT_EQ(actual[i], expected[i]);
    }
}

TEST_F(PosixFileTest, UringBatchReadReportsShortReadPerRequest) {
    constexpr std::string_view data = "abcdef";
    ASSERT_EQ(pwrite(test_fd, data.data(), data.size(), 0),
              static_cast<ssize_t>(data.size()));

    int uring_fd = dup(test_fd);
    ASSERT_GE(uring_fd, 0);
    UringFile uring_file(test_filename, uring_fd, 32, false);
    std::array<char, 3> complete{};
    std::array<char, 8> short_read{};
    std::array<UringFile::ReadDesc, 2> descs{
        UringFile::ReadDesc{complete.data(), complete.size(), 0},
        UringFile::ReadDesc{short_read.data(), short_read.size(), 4}};

    auto result = uring_file.batch_read(descs.data(), descs.size());
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_TRUE(descs[0].completed);
    EXPECT_EQ(descs[0].error, ErrorCode::OK);
    EXPECT_EQ(descs[0].bytes_read, complete.size());
    EXPECT_TRUE(descs[1].completed);
    EXPECT_EQ(descs[1].error, ErrorCode::OK);
    EXPECT_EQ(descs[1].bytes_read, 2U);
}

TEST_F(PosixFileTest, UringBatchReadRejectsMisalignedDirectIoDescriptors) {
    int uring_fd = dup(test_fd);
    ASSERT_GE(uring_fd, 0);
    UringFile uring_file(test_filename, uring_fd, 32, true);

    void* allocation = nullptr;
    ASSERT_EQ(posix_memalign(&allocation, 4096, 8192), 0);
    std::unique_ptr<void, decltype(&std::free)> buffer(allocation, &std::free);
    auto* aligned = static_cast<char*>(buffer.get());

    auto expect_invalid = [&](UringFile::ReadDesc desc) {
        auto result = uring_file.batch_read(&desc, 1);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::FILE_INVALID_BUFFER);
        EXPECT_FALSE(desc.completed);
    };

    expect_invalid(UringFile::ReadDesc{aligned + 1, 4096, 0});
    expect_invalid(UringFile::ReadDesc{aligned, 4095, 0});
    expect_invalid(UringFile::ReadDesc{aligned, 4096, 1});
}

TEST_F(PosixFileTest, UringBatchReadDrainsErrorsBeforeNextOperation) {
    std::array<char, 16> first{};
    std::array<char, 16> second{};
    {
        int invalid_fd = open("/dev/null", O_WRONLY | O_CLOEXEC);
        ASSERT_GE(invalid_fd, 0);
        UringFile invalid_file("/dev/null", invalid_fd, 32, false);
        std::array<UringFile::ReadDesc, 2> invalid_descs{
            UringFile::ReadDesc{first.data(), first.size(), 0},
            UringFile::ReadDesc{second.data(), second.size(), 0}};

        auto invalid_result =
            invalid_file.batch_read(invalid_descs.data(), invalid_descs.size());
        ASSERT_FALSE(invalid_result.has_value());
        EXPECT_EQ(invalid_result.error(), ErrorCode::FILE_READ_FAIL);
        EXPECT_TRUE(invalid_descs[0].completed);
        EXPECT_EQ(invalid_descs[0].error, ErrorCode::FILE_READ_FAIL);
        EXPECT_TRUE(invalid_descs[1].completed);
        EXPECT_EQ(invalid_descs[1].error, ErrorCode::FILE_READ_FAIL);
    }

    constexpr std::string_view data = "ring-remains-usable";
    ASSERT_EQ(pwrite(test_fd, data.data(), data.size(), 0),
              static_cast<ssize_t>(data.size()));
    int valid_fd = dup(test_fd);
    ASSERT_GE(valid_fd, 0);
    UringFile valid_file(test_filename, valid_fd, 32, false);
    std::vector<char> output(data.size());
    UringFile::ReadDesc desc{output.data(), output.size(), 0};

    auto valid_result = valid_file.batch_read(&desc, 1);
    ASSERT_TRUE(valid_result.has_value()) << toString(valid_result.error());
    EXPECT_TRUE(desc.completed);
    EXPECT_EQ(desc.error, ErrorCode::OK);
    EXPECT_EQ(desc.bytes_read, data.size());
    EXPECT_EQ(std::string_view(output.data(), output.size()), data);
}

TEST_F(PosixFileTest, UringVectorIoHandlesUnalignedDirectIo) {
    const std::string direct_filename = "direct_vector_test.bin";
    remove(direct_filename.c_str());

    int direct_fd = open(direct_filename.c_str(),
                         O_CREAT | O_RDWR | O_DIRECT | O_CLOEXEC, 0644);
    ASSERT_GE(direct_fd, 0) << strerror(errno);
    ASSERT_EQ(ftruncate(direct_fd, 16384), 0);

    UringFile uring_file(direct_filename, direct_fd, 32, true);

    // Neighboring data in the same 4KiB block must survive RMW writes.
    const std::string neighbor = "keep-me";
    iovec neighbor_iov;
    neighbor_iov.iov_base = const_cast<char*>(neighbor.data());
    neighbor_iov.iov_len = neighbor.size();
    auto neighbor_write = uring_file.vector_write(&neighbor_iov, 1, 0);
    ASSERT_TRUE(neighbor_write.has_value()) << toString(neighbor_write.error());

    char header[48] = {};
    std::memcpy(header, "HDR", 3);
    const std::string key = "object-key";
    const std::string payload = "payload-bytes-for-direct-io";

    iovec write_iov[3];
    write_iov[0].iov_base = header;
    write_iov[0].iov_len = sizeof(header);
    write_iov[1].iov_base = const_cast<char*>(key.data());
    write_iov[1].iov_len = key.size();
    write_iov[2].iov_base = const_cast<char*>(payload.data());
    write_iov[2].iov_len = payload.size();

    constexpr off_t kOffset = 513;
    auto write_result = uring_file.vector_write(write_iov, 3, kOffset);
    ASSERT_TRUE(write_result.has_value()) << toString(write_result.error());
    const size_t expected = sizeof(header) + key.size() + payload.size();
    EXPECT_EQ(*write_result, expected);

    char out_header[sizeof(header)] = {};
    std::string out_key(key.size(), '\0');
    std::string out_payload(payload.size(), '\0');
    iovec read_iov[3];
    read_iov[0].iov_base = out_header;
    read_iov[0].iov_len = sizeof(out_header);
    read_iov[1].iov_base = out_key.data();
    read_iov[1].iov_len = out_key.size();
    read_iov[2].iov_base = out_payload.data();
    read_iov[2].iov_len = out_payload.size();

    auto read_result = uring_file.vector_read(read_iov, 3, kOffset);
    ASSERT_TRUE(read_result.has_value()) << toString(read_result.error());
    EXPECT_EQ(*read_result, expected);
    EXPECT_EQ(std::string_view(out_header, 3), "HDR");
    EXPECT_EQ(out_key, key);
    EXPECT_EQ(out_payload, payload);

    std::string out_neighbor(neighbor.size(), '\0');
    iovec neighbor_read_iov;
    neighbor_read_iov.iov_base = out_neighbor.data();
    neighbor_read_iov.iov_len = out_neighbor.size();
    auto neighbor_read = uring_file.vector_read(&neighbor_read_iov, 1, 0);
    ASSERT_TRUE(neighbor_read.has_value()) << toString(neighbor_read.error());
    EXPECT_EQ(out_neighbor, neighbor);

    remove(direct_filename.c_str());
}

TEST_F(PosixFileTest, UringVectorWriteAbortsWhenPreservationReadFails) {
    const std::string direct_filename = "direct_vector_preserve_fail.bin";
    remove(direct_filename.c_str());

    {
        int setup_fd = open(direct_filename.c_str(),
                            O_CREAT | O_RDWR | O_DIRECT | O_CLOEXEC, 0644);
        ASSERT_GE(setup_fd, 0) << strerror(errno);
        ASSERT_EQ(ftruncate(setup_fd, 16384), 0);
        UringFile setup_file(direct_filename, setup_fd, 32, true);

        const std::string neighbor = "keep-neighbor-bytes";
        iovec neighbor_iov;
        neighbor_iov.iov_base = const_cast<char*>(neighbor.data());
        neighbor_iov.iov_len = neighbor.size();
        auto neighbor_write = setup_file.vector_write(&neighbor_iov, 1, 0);
        ASSERT_TRUE(neighbor_write.has_value())
            << toString(neighbor_write.error());
    }

    // Re-open write-only so the RMW preservation read must fail. The write
    // must abort without clobbering the already-written neighbor bytes.
    int write_only_fd =
        open(direct_filename.c_str(), O_WRONLY | O_DIRECT | O_CLOEXEC);
    ASSERT_GE(write_only_fd, 0) << strerror(errno);
    {
        UringFile write_only_file(direct_filename, write_only_fd, 32, true);
        char payload[64];
        std::memset(payload, 'X', sizeof(payload));
        iovec write_iov;
        write_iov.iov_base = payload;
        write_iov.iov_len = sizeof(payload);

        constexpr off_t kOffset = 513;
        auto write_result =
            write_only_file.vector_write(&write_iov, 1, kOffset);
        ASSERT_FALSE(write_result.has_value());
        EXPECT_EQ(write_result.error(), ErrorCode::FILE_READ_FAIL);
    }

    int verify_fd =
        open(direct_filename.c_str(), O_RDONLY | O_DIRECT | O_CLOEXEC);
    ASSERT_GE(verify_fd, 0) << strerror(errno);
    UringFile verify_file(direct_filename, verify_fd, 32, true);

    const std::string neighbor = "keep-neighbor-bytes";
    std::string out_neighbor(neighbor.size(), '\0');
    iovec neighbor_read_iov;
    neighbor_read_iov.iov_base = out_neighbor.data();
    neighbor_read_iov.iov_len = out_neighbor.size();
    auto neighbor_read = verify_file.vector_read(&neighbor_read_iov, 1, 0);
    ASSERT_TRUE(neighbor_read.has_value()) << toString(neighbor_read.error());
    EXPECT_EQ(out_neighbor, neighbor);

    remove(direct_filename.c_str());
}

// Some sandboxes block io_uring (seccomp); the tests below skip there.
static bool UringAvailable() {
    struct io_uring ring;
    if (io_uring_queue_init(2, &ring, 0) < 0) return false;
    io_uring_queue_exit(&ring);
    return true;
}

// A failed CQE keeps its errno, as a failed syscall does in PosixFile.
TEST_F(PosixFileTest, UringFailedCqeKeepsErrno) {
    if (!UringAvailable()) GTEST_SKIP() << "io_uring is unavailable";
    // Reads on a write-only descriptor fail with EBADF.
    int wronly_fd = open(test_filename.c_str(), O_WRONLY);
    ASSERT_GE(wronly_fd, 0);
    UringFile wronly(test_filename, wronly_fd, 32, false);
    EXPECT_EQ(wronly.sys_errno(), 0);
    std::array<char, 16> buf{};
    UringFile::ReadDesc desc{buf.data(), buf.size(), 0};
    ASSERT_FALSE(wronly.batch_read(&desc, 1));
    EXPECT_EQ(wronly.sys_errno(), EBADF);
    // A new file: the first errno wins, so `wronly` would keep EBADF anyway.
    int wronly_fd2 = open(test_filename.c_str(), O_WRONLY);
    ASSERT_GE(wronly_fd2, 0);
    UringFile wronly2(test_filename, wronly_fd2, 32, false);
    iovec iov{buf.data(), buf.size()};
    ASSERT_FALSE(wronly2.vector_read(&iov, 1, 0));
    EXPECT_EQ(wronly2.sys_errno(), EBADF);
}

#if defined(RLIMIT_FSIZE) && defined(SIGXFSZ)
// Runs `body` on a new thread so that it gets its own io_uring ring: a forked
// child must not submit to the ring it inherited from the parent's thread.
static int OnFreshRing(const std::function<int()>& body) {
    int rc = -1;
    std::thread([&] { rc = body(); }).join();
    return rc;
}

// Whether this kernel applies the file size limit to io_uring writes: a
// write across the limit completes short, and the next one fails with EFBIG.
// Buffered writes can be punted to io-wq workers, which older kernels did
// not run with the submitter's limits.
static bool UringHonorsFileSizeLimit(const std::string& path) {
    int rc = RunWithFileSizeLimit(4096, [&]() -> int {
        int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (fd < 0) return 10;
        struct io_uring ring;
        if (io_uring_queue_init(2, &ring, 0) < 0) return 11;
        std::string data(8192, 'x');
        const off_t offsets[2] = {0, 4096};
        int results[2] = {0, 0};
        for (int i = 0; i < 2; ++i) {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_write(sqe, fd, data.data(), data.size(), offsets[i]);
            struct io_uring_cqe* cqe = nullptr;
            if (io_uring_submit_and_wait(&ring, 1) < 0) return 12;
            if (io_uring_wait_cqe(&ring, &cqe) < 0) return 12;
            results[i] = cqe->res;
            io_uring_cqe_seen(&ring, cqe);
        }
        return results[0] == 4096 && results[1] == -EFBIG ? 0 : 13;
    });
    unlink(path.c_str());
    return rc == 0;
}
#endif

// A write that completes short (here it crosses the file size limit) is
// continued, so the caller sees the errno of the part that really failed
// (EFBIG here, EIO on a dying disk) instead of a short success.
TEST_F(PosixFileTest, UringShortWriteContinuesToTheRealErrno) {
#if defined(RLIMIT_FSIZE) && defined(SIGXFSZ)
    if (!UringAvailable()) GTEST_SKIP() << "io_uring is unavailable";
    const std::string path =
        "uring_short_write_" + std::to_string(getpid()) + ".tmp";
    if (!UringHonorsFileSizeLimit(path))
        GTEST_SKIP() << "this kernel does not apply RLIMIT_FSIZE to io_uring "
                        "writes";
    int rc = RunWithFileSizeLimit(5000, [&]() -> int {
        return OnFreshRing([&]() -> int {
            int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
            if (fd < 0) return 10;
            UringFile file(path, fd, 32, false);
            std::string data(8192, 'x');
            if (file.write_aligned(data.data(), data.size(), 0)) return 11;
            if (file.sys_errno() != EFBIG) return 12;
            return 0;
        });
    });
    unlink(path.c_str());
    EXPECT_EQ(rc, 0) << "11: write succeeded, 12: errno is not EFBIG";

    // One SQE per iovec: the second iovec stops at the limit, and its rest is
    // resubmitted and fails with EFBIG. The caller's iovecs are not modified.
    rc = RunWithFileSizeLimit(4096, [&]() -> int {
        return OnFreshRing([&]() -> int {
            int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
            if (fd < 0) return 10;
            UringFile file(path, fd, 32, false);
            file.SetDeleteOnWriteFail(false);  // keep the bytes for the check
            std::string a(3000, 'a');
            std::string b(3000, 'b');
            iovec iov[2] = {{a.data(), a.size()}, {b.data(), b.size()}};
            if (file.vector_write(iov, 2, 0)) return 11;
            if (file.sys_errno() != EFBIG) return 12;
            if (iov[0].iov_base != a.data() || iov[0].iov_len != a.size() ||
                iov[1].iov_base != b.data() || iov[1].iov_len != b.size())
                return 13;
            return 0;
        });
    });
    EXPECT_EQ(rc, 0) << "11: write succeeded, 12: errno is not EFBIG, "
                        "13: caller's iovecs changed";
    std::string on_disk;
    {
        int fd = open(path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        char buf[8192];
        ssize_t n = pread(fd, buf, sizeof(buf), 0);
        close(fd);
        ASSERT_GE(n, 0);
        on_disk.assign(buf, static_cast<size_t>(n));
    }
    unlink(path.c_str());
    EXPECT_EQ(on_disk, std::string(3000, 'a') + std::string(1096, 'b'));
#else
    GTEST_SKIP() << "RLIMIT_FSIZE/SIGXFSZ is unavailable on this platform";
#endif
}

// A read completes short only at EOF. A short CQE before EOF is continued,
// so the caller sees the errno of the part that really failed: here the
// buffer runs into an inaccessible page, the kernel fills the accessible
// half and completes short, and the continuation fails with EFAULT.
TEST_F(PosixFileTest, UringReadIsShortOnlyAtEof) {
    if (!UringAvailable()) GTEST_SKIP() << "io_uring is unavailable";
    // A short completion resumes only on a 4 KiB boundary (O_DIRECT), and
    // the inaccessible page below must start on one.
    const size_t page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    if (page != 4096) GTEST_SKIP() << "needs 4 KiB pages, not " << page;
    std::string content(3 * page, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('0' + i % 75);
    ASSERT_EQ(pwrite(test_fd, content.data(), content.size(), 0),
              static_cast<ssize_t>(content.size()));
    int uring_fd = dup(test_fd);
    ASSERT_GE(uring_fd, 0);
    UringFile file(test_filename, uring_fd, 32, false);

    // EOF: the short count, no error.
    std::string tail(page, '\0');
    auto at_eof = file.read_aligned(tail.data(), page, content.size() - 100);
    ASSERT_TRUE(at_eof) << toString(at_eof.error());
    EXPECT_EQ(*at_eof, 100u);
    EXPECT_EQ(tail.substr(0, 100), content.substr(content.size() - 100));
    EXPECT_EQ(file.sys_errno(), 0);

    // Short before EOF: continued, and the continuation's errno is kept.
    void* mem = mmap(nullptr, 2 * page, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(mem, MAP_FAILED);
    char* base = static_cast<char*>(mem);
    ASSERT_EQ(mprotect(base + page, page, PROT_NONE), 0);
    char* buf = base + page / 2;  // half a page, then the inaccessible page
    auto result = file.read_aligned(buf, page, page / 2);
    EXPECT_FALSE(result);
    EXPECT_EQ(file.sys_errno(), EFAULT);
    EXPECT_EQ(std::string(buf, page / 2), content.substr(page / 2, page / 2));

    // The same through batch_read and vector_read, each on a new file (the
    // first errno wins).
    {
        int fd = dup(uring_fd);
        ASSERT_GE(fd, 0);
        UringFile batch_file(test_filename, fd, 32, false);
        std::memset(buf, 0, page / 2);
        UringFile::ReadDesc desc{buf, page, static_cast<off_t>(page / 2)};
        EXPECT_FALSE(batch_file.batch_read(&desc, 1));
        EXPECT_EQ(batch_file.sys_errno(), EFAULT);
        EXPECT_EQ(std::string(buf, page / 2),
                  content.substr(page / 2, page / 2));
    }
    {
        int fd = dup(uring_fd);
        ASSERT_GE(fd, 0);
        UringFile vector_file(test_filename, fd, 32, false);
        std::memset(buf, 0, page / 2);
        iovec iov{buf, page};
        EXPECT_FALSE(vector_file.vector_read(&iov, 1, page / 2));
        EXPECT_EQ(vector_file.sys_errno(), EFAULT);
        EXPECT_EQ(std::string(buf, page / 2),
                  content.substr(page / 2, page / 2));
    }
    munmap(mem, 2 * page);
}
#endif

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
