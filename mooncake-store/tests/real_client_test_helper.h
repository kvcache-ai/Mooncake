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

    static bool ShmMappingsEmpty(const ShmContext& context) {
        SharedMutexLocker lock(&context.mutex_, shared_lock);
        return context.mapped_shms_.empty();
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

    static bool DummyMonitorJoinable(const RealClient& client) {
        return client.dummy_client_monitor_thread_.joinable();
    }

    static auto NativeClient(const RealClient& client) {
        return client.client_service_;
    }

    static uint16_t DummyRpcPort(const RealClient& client) {
        return client.dummy_rpc_server_ ? client.dummy_rpc_server_->port() : 0;
    }

    // Only the two handler-drain cases use this after setup without Dummy RPC.
    // Real teardown owns/stops the server; no owning pointer escapes.
    template <typename Handler>
    static auto StartPausedDummyReadServer(RealClient& client,
                                           Handler& handler) {
        client.dummy_rpc_server_ =
            std::make_unique<coro_rpc::coro_rpc_server>(2, 0, "127.0.0.1");
        client.dummy_rpc_server_->register_handler<&Handler::Read>(&handler);
        return client.dummy_rpc_server_->async_start();
    }
};

}  // namespace mooncake
