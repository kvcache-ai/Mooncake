#pragma once

#include <liburing.h>

#include <vector>

#include "nof/nvmeof_initiator.h"

namespace mooncake {

class IoQueue {
   public:
    IoQueue() = default;
    IoQueue(const IoQueue&) = delete;
    IoQueue& operator=(const IoQueue&) = delete;
    ~IoQueue();

    int Init(unsigned depth);
    int Submit(int fd, void* buffer, uint64_t offset, uint32_t length,
               NofIOOp op, const NofIOAdaptor& adaptor);
    int64_t Poll(uint32_t maximum);

   private:
    struct Request {
        NofIOAdaptor adaptor{};
        uint32_t length = 0;
    };

    io_uring ring_{};
    std::vector<Request> requests_;
    std::vector<Request*> free_;
    // Keep SQ order so a partial submit consumes the correct prefix.
    std::vector<Request*> unsubmitted_;
    size_t head_ = 0;
    size_t count_ = 0;
    int submit_error_ = 0;
    bool initialized_ = false;
};

}  // namespace mooncake
