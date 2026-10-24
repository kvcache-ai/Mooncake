#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_RECOVERY_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_RECOVERY_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "error_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

class DeviceCollectiveRuntime;

// One process-wide worker observes the small mapped mailbox owned by each
// device communicator and handles the only outstanding failure directly.
class DeviceCollectiveRecoveryWorker {
    static constexpr auto kRecoveryCheckInterval = std::chrono::milliseconds(1);

   public:
    DeviceCollectiveRecoveryWorker();
    ~DeviceCollectiveRecoveryWorker() noexcept;

    DeviceCollectiveRecoveryWorker(const DeviceCollectiveRecoveryWorker&) =
        delete;
    DeviceCollectiveRecoveryWorker& operator=(
        const DeviceCollectiveRecoveryWorker&) = delete;

    PGResult<void> start();
    void shutdown();

   private:
    friend class DeviceCollectiveRuntime;

    struct MailboxState;
    // Handles a complete device failure request. On success, the callback has
    // published the pinned control update and replied to the request.
    using HandleFailureCallback = std::function<PGResult<void>(
        const ControlMailbox::RecoverySlot::ReceivedRequest&)>;

    PGResult<void> addMailbox(ControlMailbox* mailbox,
                              HandleFailureCallback handle_failure);
    void removeMailbox(ControlMailbox* mailbox) noexcept;
    void runLoop();
    void run() noexcept;

    std::mutex mutex_;
    std::condition_variable state_changed_;
    std::vector<std::unique_ptr<MailboxState>> mailboxes_;
    ControlMailbox* active_mailbox_ = nullptr;
    std::thread worker_;
    bool started_ = false;
    bool shutdown_requested_ = false;
    bool terminated_with_error_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_RECOVERY_H
