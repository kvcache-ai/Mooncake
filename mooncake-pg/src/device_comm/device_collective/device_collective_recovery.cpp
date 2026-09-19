#include "device_comm/device_collective/device_collective_recovery.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "device_comm/device_utils/d2h_request_slot.h"

namespace mooncake {

struct DeviceCollectiveRecoveryWorker::MailboxState {
    ControlMailbox* mailbox = nullptr;
    PrepareResumeCallback prepare_resume;
};

void DeviceCollectiveRecoveryWorker::runLoop() {
    while (true) {
        MailboxState* pending = nullptr;
        ControlMailbox::RecoverySlot::ReceivedRequest received;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (shutdown_requested_) break;
            for (const auto& state : mailboxes_) {
                if (!state->mailbox->recovery.tryReceive(received)) continue;
                pending = state.get();
                active_mailbox_ = state->mailbox;
                break;
            }
            if (!pending) {
                state_changed_.wait_for(lock, kRecoveryCheckInterval);
                continue;
            }
        }

        auto prepared_resume = pending->prepare_resume(received.request);
        if (!prepared_resume.has_value()) {
            LOG(ERROR) << "Device collective recovery failed; the last "
                          "channel CTA remains waiting: "
                       << prepared_resume.error().message;
            std::lock_guard<std::mutex> lock(mutex_);
            active_mailbox_ = nullptr;
            terminated_with_error_ = true;
            state_changed_.notify_all();
            return;
        }

        // The callback pinned the update before this acknowledgement. The
        // acquire load in the last channel CTA therefore observes the complete
        // update before applying it and leaving the failed collective.
        received.handle.reply({});

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
    ControlMailbox* mailbox, PrepareResumeCallback prepare_resume) {
    auto state = std::make_unique<MailboxState>();
    state->mailbox = mailbox;
    state->prepare_resume = std::move(prepare_resume);

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
    ControlMailbox* mailbox) noexcept {
    if (!mailbox) return;
    std::unique_lock<std::mutex> lock(mutex_);
    const auto selected = std::find_if(
        mailboxes_.begin(), mailboxes_.end(),
        [mailbox](const auto& current) { return current->mailbox == mailbox; });
    if (selected == mailboxes_.end()) return;
    // Stop future scans immediately, but keep the state alive until a callback
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
