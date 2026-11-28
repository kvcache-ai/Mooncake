#include "linux_io_queue.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

namespace mooncake {

int IoQueue::Init(unsigned depth) {
    requests_.resize(depth);
    unsubmitted_.resize(depth);
    free_.reserve(depth);
    for (auto& request : requests_) free_.push_back(&request);
    const int ret = io_uring_queue_init(depth, &ring_, 0);
    if (ret < 0) return ret;
    initialized_ = true;
    return 0;
}

IoQueue::~IoQueue() {
    if (!initialized_) return;
    // Only Poll may submit SQEs. A timed liburing wait can implicitly submit
    // the rejected suffix on kernels without IORING_FEAT_EXT_ARG.
    while (free_.size() != requests_.size()) {
        if (Poll(0) == 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io_uring_queue_exit(&ring_);
}

int IoQueue::Submit(int fd, void* buffer, uint64_t offset, uint32_t length,
                    NofIOOp op, const NofIOAdaptor& adaptor) {
    if (submit_error_) return submit_error_;
    if (free_.empty()) return -EAGAIN;
    auto* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return -EAGAIN;
    auto* request = free_.back();
    free_.pop_back();
    request->adaptor = adaptor;
    request->length = length;
    if (op == NofIOOp::kRead)
        io_uring_prep_read(sqe, fd, buffer, length, offset);
    else
        io_uring_prep_write(sqe, fd, buffer, length, offset);
    io_uring_sqe_set_data(sqe, request);
    unsubmitted_[(head_ + count_) % unsubmitted_.size()] = request;
    ++count_;
    return 0;
}

int64_t IoQueue::Poll(uint32_t maximum) {
    if (!submit_error_ && count_) {
        const int ret = io_uring_submit(&ring_);
        if (ret > 0) {
            // Without SQPOLL, ret is the prefix consumed by the kernel.
            head_ = (head_ + ret) % unsubmitted_.size();
            count_ -= ret;
        } else if (ret < 0 && ret != -EINTR && ret != -EAGAIN &&
                   ret != -EBUSY) {
            submit_error_ = ret;
        }
    }

    int64_t completed = 0;
    while (maximum == 0 || completed < maximum) {
        Request* request;
        NofIOCompletion completion;
        if (submit_error_ && count_) {
            request = unsubmitted_[head_];
            head_ = (head_ + 1) % unsubmitted_.size();
            --count_;
            completion.error_string = std::string("Linux NoF submit: ") +
                                      std::strerror(-submit_error_);
        } else {
            io_uring_cqe* cqe = nullptr;
            if (io_uring_peek_cqe(&ring_, &cqe) != 0) break;
            request = static_cast<Request*>(io_uring_cqe_get_data(cqe));
            const int result = cqe->res;
            io_uring_cqe_seen(&ring_, cqe);
            completion.success =
                result >= 0 && static_cast<uint32_t>(result) == request->length;
            if (!completion.success) {
                completion.error_string =
                    result < 0
                        ? std::string("Linux NoF I/O: ") +
                              std::strerror(-result)
                        : "Linux NoF short I/O: " + std::to_string(result) +
                              "/" + std::to_string(request->length);
            }
        }
        const auto adaptor = request->adaptor;
        free_.push_back(request);
        ++completed;
        adaptor.cb(adaptor.ctx, completion);
    }
    return completed;
}

}  // namespace mooncake
