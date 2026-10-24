#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "common_types.h"

namespace mooncake {

class AutoPortBinder;
class ClientBufferAllocator;
class TransferEngine;

// Per-service transport resources. Users must stop before destruction.
class ClientResources {
   public:
    ClientResources();
    ~ClientResources();

    ClientResources(const ClientResources&) = delete;
    ClientResources& operator=(const ClientResources&) = delete;

    ErrorCode InitTransferEngine(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& device_names);

    // Adopt an initialized engine before creating the local pool. Do not
    // reconfigure it or explicitly stop an engine shared with another owner.
    void InitTransferEngine(const std::string& local_hostname,
                            const std::shared_ptr<TransferEngine>& engine);

    std::shared_ptr<TransferEngine> GetTransferEngine() const {
        return transfer_engine_;
    }
    const std::string& GetLocalIp() const { return local_ip_; }
    std::string GetLocalEndpoint() const;
    std::shared_ptr<ClientBufferAllocator> GetBufferAllocator() const {
        return local_buffer_allocator_;
    }

    static tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
        const void* addr, size_t length);

    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    ErrorCode InitLocalBufferAllocator(size_t pool_size,
                                       const std::string& protocol,
                                       bool use_hugepage = false);

    // Only the owned pool is released here. Caller-registered buffers and SHM
    // retain their existing owners and explicit registration lifecycle.
    ErrorCode ReleaseLocalBuffer(bool update_metadata);

   private:
    // Extract the network address; port 0 requests automatic selection.
    void ParseLocalHostname(const std::string& local_hostname);

    ErrorCode InnerInitTransferEngine(
        bool auto_discover, const std::string& protocol,
        const std::optional<std::string>& device_names,
        const std::string& metadata_connstring,
        const std::string& local_hostname);

    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<AutoPortBinder> port_binder_;
    std::shared_ptr<ClientBufferAllocator> local_buffer_allocator_;
    std::string local_ip_;
    uint16_t te_port_ = 0;
    bool use_tent_ = false;
};

}  // namespace mooncake
