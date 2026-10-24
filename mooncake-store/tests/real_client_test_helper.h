#pragma once

#include "real_client.h"

namespace mooncake {

// Callers retain the locking used for direct member access.
struct RealClientTestHelper {
    using MappedShm = RealClient::MappedShm;
    using ShmContext = RealClient::ShmContext;

    static auto MapShm(RealClient& client, int fd, uint64_t base, size_t size,
                       bool local, const UUID& id) {
        return client.map_shm_internal(fd, base, size, local, id);
    }

    static auto FindShmContext(RealClient& client, const UUID& id) {
        return client.find_shm_context(id);
    }

    static auto& ShmMutex(RealClient& client) {
        return client.dummy_client_mutex_;
    }

    static auto& ShmContexts(RealClient& client) {
        return client.shm_contexts_;
    }

    static bool IpcThreadJoinable(const RealClient& client) {
        return client.ipc_thread_.joinable();
    }

    static bool IpcRunning(const RealClient& client) {
        return client.ipc_running_.load();
    }

    static bool DummyRpcStopped(RealClient& client) {
        return client.dummy_rpc_exit_.hasResult();
    }

    static bool DummyMonitorRunning(const RealClient& client) {
        return client.dummy_client_monitor_running_.load();
    }

    static auto NativeClient(const RealClient& client) {
        return client.client_service_;
    }

    static auto& DummyRpcServer(RealClient& client) {
        return client.dummy_rpc_server_;
    }

    static auto& LifecycleMutex(RealClient& client) {
        return client.lifecycle_mutex_;
    }

    static void CloseAdmission(RealClient& client) {
        client.state_.store(RealClient::State::CLOSING,
                            std::memory_order_release);
    }
};

}  // namespace mooncake
