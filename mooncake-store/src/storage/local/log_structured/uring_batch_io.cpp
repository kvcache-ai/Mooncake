#include "storage/local/log_structured/uring_batch_io.h"

#ifdef USE_URING
#include <liburing.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <thread>
#include <vector>

#include "uring_submit.h"
#endif

namespace mooncake::logstructured {

#ifdef USE_URING
namespace {

class ThreadLocalUringBatchWriter {
   public:
    static constexpr unsigned kQueueDepth = 32;

    static ThreadLocalUringBatchWriter& Instance() {
        thread_local ThreadLocalUringBatchWriter writer;
        return writer;
    }

    UringBatchWriteResult Write(int fd,
                                std::span<const UringWriteRequest> requests,
                                size_t max_in_flight) {
        if (!initialized_) return UringBatchWriteResult::kUnavailable;
        if (fd < 0 || requests.empty() || max_in_flight == 0) {
            return UringBatchWriteResult::kIoError;
        }

        std::vector<size_t> bytes_written(requests.size(), 0);
        size_t next_request = 0;
        while (next_request < requests.size()) {
            const size_t batch_size =
                std::min({requests.size() - next_request, max_in_flight,
                          static_cast<size_t>(kQueueDepth)});
            if (!WriteBatch(fd, requests.subspan(next_request, batch_size),
                            std::span<size_t>(bytes_written)
                                .subspan(next_request, batch_size))) {
                return UringBatchWriteResult::kIoError;
            }
            next_request += batch_size;
        }
        return UringBatchWriteResult::kSuccess;
    }

   private:
    static constexpr unsigned kBatchIndexBits = 8;
    static constexpr uint64_t kBatchIndexMask =
        (uint64_t{1} << kBatchIndexBits) - 1;
    static_assert(kQueueDepth <= kBatchIndexMask);

    ThreadLocalUringBatchWriter() {
        initialized_ = io_uring_queue_init(kQueueDepth, &ring_, 0) == 0;
    }

    ~ThreadLocalUringBatchWriter() {
        if (initialized_) io_uring_queue_exit(&ring_);
    }

    bool WriteBatch(int fd, std::span<const UringWriteRequest> requests,
                    std::span<size_t> bytes_written) {
        size_t remaining = requests.size();
        while (remaining > 0) {
            const uint64_t operation = (++operation_id_) << kBatchIndexBits;
            unsigned submitted_requests = 0;
            for (size_t index = 0; index < requests.size(); ++index) {
                if (bytes_written[index] == requests[index].length) continue;

                io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (sqe == nullptr) return false;
                const size_t pending =
                    requests[index].length - bytes_written[index];
                const unsigned write_size = static_cast<unsigned>(
                    std::min(pending, static_cast<size_t>(INT_MAX)));
                io_uring_prep_write(sqe, fd,
                                    requests[index].data + bytes_written[index],
                                    write_size,
                                    static_cast<off_t>(requests[index].offset +
                                                       bytes_written[index]));
                sqe->user_data = operation | (index + 1);
                ++submitted_requests;
            }

            const auto submit = detail::submit_all_pending(
                [this] { return io_uring_sq_ready(&ring_); },
                [this](unsigned pending) {
                    return io_uring_submit_and_wait(&ring_, pending);
                },
                [] { std::this_thread::yield(); });
            if (submit.error != 0 || submit.pending != 0 ||
                submit.submitted != submitted_requests) {
                DrainSubmitted(submit.submitted, operation);
                Reset();
                return false;
            }

            unsigned completed = 0;
            bool failed = false;
            while (completed < submitted_requests) {
                io_uring_cqe* cqe = nullptr;
                int result;
                do {
                    result = io_uring_wait_cqe(&ring_, &cqe);
                } while (result == -EINTR);
                if (result < 0) {
                    Reset();
                    return false;
                }

                const uint64_t cqe_operation =
                    cqe->user_data & ~kBatchIndexMask;
                if (cqe_operation == operation) {
                    const size_t encoded_index =
                        cqe->user_data & kBatchIndexMask;
                    if (encoded_index == 0 || encoded_index > requests.size() ||
                        cqe->res <= 0) {
                        failed = true;
                    } else {
                        const size_t index = encoded_index - 1;
                        const size_t result_size =
                            static_cast<size_t>(cqe->res);
                        if (result_size >
                            requests[index].length - bytes_written[index]) {
                            failed = true;
                        } else {
                            bytes_written[index] += result_size;
                        }
                    }
                    ++completed;
                }
                io_uring_cqe_seen(&ring_, cqe);
            }
            if (failed) return false;

            remaining = 0;
            for (size_t index = 0; index < requests.size(); ++index) {
                if (bytes_written[index] != requests[index].length) ++remaining;
            }
        }
        return true;
    }

    void DrainSubmitted(unsigned submitted, uint64_t operation) {
        unsigned completed = 0;
        while (completed < submitted) {
            io_uring_cqe* cqe = nullptr;
            if (io_uring_wait_cqe(&ring_, &cqe) < 0) return;
            if ((cqe->user_data & ~kBatchIndexMask) == operation) ++completed;
            io_uring_cqe_seen(&ring_, cqe);
        }
    }

    void Reset() {
        if (initialized_) io_uring_queue_exit(&ring_);
        ring_ = {};
        initialized_ = io_uring_queue_init(kQueueDepth, &ring_, 0) == 0;
    }

    io_uring ring_{};
    bool initialized_ = false;
    uint64_t operation_id_ = 0;
};

}  // namespace
#endif

UringBatchWriteResult UringBatchWrite(
    int fd, std::span<const UringWriteRequest> requests, size_t max_in_flight) {
#ifdef USE_URING
    return ThreadLocalUringBatchWriter::Instance().Write(fd, requests,
                                                         max_in_flight);
#else
    static_cast<void>(fd);
    static_cast<void>(requests);
    static_cast<void>(max_in_flight);
    return UringBatchWriteResult::kUnavailable;
#endif
}

}  // namespace mooncake::logstructured
