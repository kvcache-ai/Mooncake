#include "device_comm/device_collective/device_collective_recovery.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace mooncake {
struct DeviceCollectiveRecoveryWorker::MailboxState {
    DeviceCollectiveRecoveryMailbox* mailbox = nullptr;
    Handler handler;
};

void DeviceCollectiveRecoveryWorker::runLoop() {
    while (true) {
        MailboxState* pending = nullptr;
        uint64_t generation = 0;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (shutdown_requested_) break;
            for (const auto& state : mailboxes_) {
                const uint64_t observed =
                    std::atomic_ref(state->mailbox->failure_generation)
                        .load(std::memory_order_acquire);
                const uint64_t ready =
                    std::atomic_ref(state->mailbox->ready_generation)
                        .load(std::memory_order_acquire);
                if (observed <= ready) continue;
                pending = state.get();
                generation = observed;
                active_mailbox_ = state->mailbox;
                break;
            }
            if (!pending) {
                state_changed_.wait_for(lock, kRecoveryCheckInterval);
                continue;
            }
        }

        auto recovered = pending->handler();
        if (!recovered.has_value()) {
            LOG(ERROR) << "Device collective recovery failed; the kernel "
                          "remains parked: "
                       << recovered.error().message;
            std::lock_guard<std::mutex> lock(mutex_);
            active_mailbox_ = nullptr;
            terminated_with_error_ = true;
            state_changed_.notify_all();
            return;
        }

        // A successful handler means the runtime has published a Plan,
        // so release the kernel waiting on this exact failure generation.
        std::atomic_ref(pending->mailbox->ready_generation)
            .store(generation, std::memory_order_release);

        std::lock_guard<std::mutex> lock(mutex_);
        active_mailbox_ = nullptr;
        state_changed_.notify_all();
    }
}

void DeviceCollectiveRecoveryWorker::run() noexcept {
    try {
        runLoop();
    } catch (const std::exception& error) {
        LOG(ERROR) << "DeviceCollectiveRecoveryWorker stopped after an "
                      "exception: "
                   << error.what();
        std::lock_guard<std::mutex> lock(mutex_);
        active_mailbox_ = nullptr;
        terminated_with_error_ = true;
        state_changed_.notify_all();
    } catch (...) {
        LOG(ERROR) << "DeviceCollectiveRecoveryWorker stopped after an "
                      "unknown exception";
        std::lock_guard<std::mutex> lock(mutex_);
        active_mailbox_ = nullptr;
        terminated_with_error_ = true;
        state_changed_.notify_all();
    }
}

DeviceCollectiveRecoveryWorker::DeviceCollectiveRecoveryWorker() = default;

DeviceCollectiveRecoveryWorker::~DeviceCollectiveRecoveryWorker() noexcept {
    shutdown();
}

PGResult<void> DeviceCollectiveRecoveryWorker::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "DeviceCollectiveRecoveryWorker is shut down");
    PG_VALIDATE_STATE(
        !terminated_with_error_,
        "DeviceCollectiveRecoveryWorker terminated with an error");
    if (started_) return {};
    try {
        worker_ = std::thread([this] { run(); });
    } catch (const std::exception& error) {
        return makePGError(
            PGErrorCode::SystemError,
            std::string("failed to start DeviceCollectiveRecoveryWorker: ") +
                error.what());
    }
    started_ = true;
    return {};
}

PGResult<void> DeviceCollectiveRecoveryWorker::addMailbox(
    DeviceCollectiveRecoveryMailbox* mailbox, Handler handler) {
    auto state = std::make_unique<MailboxState>();
    state->mailbox = mailbox;
    state->handler = std::move(handler);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        PG_VALIDATE_STATE(
            started_ && !shutdown_requested_ && !terminated_with_error_,
            "DeviceCollectiveRecoveryWorker is not running");
        mailboxes_.push_back(std::move(state));
    }
    state_changed_.notify_all();
    return {};
}

void DeviceCollectiveRecoveryWorker::removeMailbox(
    DeviceCollectiveRecoveryMailbox* mailbox) noexcept {
    if (!mailbox) return;
    std::unique_lock<std::mutex> lock(mutex_);
    const auto selected = std::find_if(
        mailboxes_.begin(), mailboxes_.end(),
        [mailbox](const auto& current) { return current->mailbox == mailbox; });
    if (selected == mailboxes_.end()) return;
    // Stop future scans immediately, but keep the state alive until a handler
    // selected before the erase has finished using it.
    auto removed_state = std::move(*selected);
    auto* const removed_mailbox = removed_state->mailbox;
    mailboxes_.erase(selected);
    state_changed_.notify_all();
    state_changed_.wait(lock, [this, removed_mailbox] {
        return active_mailbox_ != removed_mailbox;
    });
}

void DeviceCollectiveRecoveryWorker::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_requested_) return;
        if (!mailboxes_.empty() || active_mailbox_) {
            LOG(ERROR)
                << "DeviceCollectiveRecoveryWorker is shutting down with "
                   "mailboxes still added";
        }
        shutdown_requested_ = true;
    }
    state_changed_.notify_all();
    if (worker_.joinable()) worker_.join();
    std::lock_guard<std::mutex> lock(mutex_);
    mailboxes_.clear();
}

}  // namespace mooncake
