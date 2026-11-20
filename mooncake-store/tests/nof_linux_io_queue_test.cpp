#include <gtest/gtest.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>
#include <vector>

#include "nof/linux_io_queue.h"

namespace {
extern "C" int __real_io_uring_submit(io_uring*);
decltype(&__real_io_uring_submit) next_submit = nullptr;

int UnexpectedSubmit(io_uring* ring) {
    ADD_FAILURE() << "Unexpected submission";
    return __real_io_uring_submit(ring);
}

extern "C" int __wrap_io_uring_submit(io_uring* ring) {
    if (!next_submit) return __real_io_uring_submit(ring);
    auto submit = std::exchange(next_submit, UnexpectedSubmit);
    return submit(ring);
}

int SubmitFirstOnly(io_uring* ring) {
    // Only the first read reaches the kernel. Leave the second in the SQ.
    auto& sq = ring->sq;
    const unsigned tail = sq.sqe_tail;
    sq.sqe_tail = sq.sqe_head + 1;
    const int ret = __real_io_uring_submit(ring);
    sq.sqe_tail = tail;
    EXPECT_EQ(ret, 1);
    return ret;
}

struct Pipe {
    int fds[2] = {-1, -1};
    ~Pipe() {
        if (fds[1] >= 0) close(fds[1]);
        if (fds[0] >= 0) close(fds[0]);
    }
};
}  // namespace

namespace mooncake {
namespace {
class LinuxIoQueueTest : public ::testing::Test {
    void TearDown() override { next_submit = nullptr; }
};

TEST_F(LinuxIoQueueTest, FailsOnlyUnsubmittedRequestsAfterSubmitError) {
    char accepted_buffer{}, unsubmitted_buffer{};
    using Results = std::vector<NofIOCompletion>;
    Results accepted_read, unsubmitted_read;
    auto record = [](void* ctx, const NofIOCompletion& completion) {
        static_cast<Results*>(ctx)->push_back(completion);
    };
    {
        IoQueue queue;
        const int ret = queue.Init(2);
        if (ret == -EPERM || ret == -ENOSYS)
            GTEST_SKIP() << "io_uring unavailable: " << std::strerror(-ret);
        ASSERT_EQ(ret, 0);
        Pipe pipe;
        ASSERT_EQ(::pipe(pipe.fds), 0);
        ASSERT_EQ(queue.Submit(pipe.fds[0], &accepted_buffer, -1, 1,
                               NofIOOp::kRead, {record, &accepted_read}),
                  0);
        ASSERT_EQ(queue.Submit(pipe.fds[0], &unsubmitted_buffer, -1, 1,
                               NofIOOp::kRead, {record, &unsubmitted_read}),
                  0);

        next_submit = SubmitFirstOnly;
        EXPECT_EQ(queue.Poll(0), 0);  // The first read waits on the empty pipe.
        next_submit = [](io_uring*) { return -EPERM; };
        EXPECT_EQ(queue.Poll(0), 1);
        EXPECT_TRUE(accepted_read.empty());
        ASSERT_EQ(unsubmitted_read.size(), 1u);
        EXPECT_FALSE(unsubmitted_read.front().success);
        EXPECT_NE(
            unsubmitted_read.front().error_string.find(std::strerror(EPERM)),
            std::string::npos);
        EXPECT_EQ(queue.Submit(pipe.fds[0], &unsubmitted_buffer, -1, 1,
                               NofIOOp::kRead, {record, &unsubmitted_read}),
                  -EPERM);

        // Let the accepted read finish and the queue destructor drain its CQE.
        ASSERT_EQ(write(pipe.fds[1], "x", 1), 1);
    }
    ASSERT_EQ(accepted_read.size(), 1u);
    EXPECT_TRUE(accepted_read.front().success);
    EXPECT_EQ(accepted_buffer, 'x');
    EXPECT_EQ(unsubmitted_read.size(), 1u);
}
}  // namespace
}  // namespace mooncake
