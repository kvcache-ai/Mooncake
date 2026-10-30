// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <chrono>
#include <thread>

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"

// Test-only ELF interposition: the Python wrapper does not expose cancellation
// or the batch IDs of synchronous calls. Submit through the real TENT runtime,
// then use its native cancellation API before returning to the wrapper. The
// peer's data socket never responds, so completion cannot race cancellation.
// No status is fabricated and no production test hook is needed.
namespace {
int submissions = 0;
int cancellations = 0;
int frees = 0;
}  // namespace

extern "C" int tent_test_submissions() { return submissions; }
extern "C" int tent_test_cancellations() { return cancellations; }
extern "C" int tent_test_frees() { return frees; }
extern "C" void tent_test_reset() {
    submissions = 0;
    cancellations = 0;
    frees = 0;
}

namespace mooncake::tent {

Status TransferEngine::submitTransfer(BatchID batch_id,
                                      const std::vector<Request>& requests) {
    ++submissions;
    CHECK_STATUS(impl_->submitTransfer(batch_id, requests));
    for (size_t i = 0; i < requests.size(); ++i) {
        CHECK_STATUS(cancelTransfer(batch_id, i));
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    for (size_t i = 0; i < requests.size(); ++i) {
        for (;;) {
            TransferStatus task_status;
            CHECK_STATUS(getTransferStatus(batch_id, i, task_status));
            if (task_status.s == CANCELED) {
                ++cancellations;
                break;
            }
            if (task_status.s != PENDING ||
                std::chrono::steady_clock::now() >= deadline) {
                return Status::InternalError("test task did not cancel");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    return Status::OK();
}

Status TransferEngine::freeBatch(BatchID batch_id) {
    ++frees;
    return impl_->freeBatch(batch_id);
}

}  // namespace mooncake::tent
