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
    // Called after the worker observes a new device failure. The callback must
    // prepare and pin the control update that releases the parked CTA.
    using PrepareResumeCallback = std::function<PGResult<void>()>;

    PGResult<void> addMailbox(DeviceCollectiveRecoveryMailbox* mailbox,
                              PrepareResumeCallback prepare_resume);
    void removeMailbox(DeviceCollectiveRecoveryMailbox* mailbox) noexcept;
    void runLoop();
    void run() noexcept;

    std::mutex mutex_;
    std::condition_variable state_changed_;
    std::vector<std::unique_ptr<MailboxState>> mailboxes_;
    DeviceCollectiveRecoveryMailbox* active_mailbox_ = nullptr;
    std::thread worker_;
    bool started_ = false;
    bool shutdown_requested_ = false;
    bool terminated_with_error_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_RECOVERY_H
