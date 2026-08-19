// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace mooncake {
namespace device {

enum class NcclGinBackend : uint8_t {
    kNone = 0,
    kProxy,
    kGdaki,
    kGpi,
};

enum class NcclDeviceRoute : uint8_t {
    kUnavailable = 0,
    kLocal,
    kLsa,
    kGin,
};

struct NcclTransportConfig {
    int rank = -1;
    int num_ranks = 0;

    // Mooncake currently supports either full world-team GIN connectivity or
    // no GIN. Rail connectivity is deferred until Mooncake has a rail-team
    // rank contract.
    bool enable_gin = true;
    int gin_context_count = 4;
    bool gin_exclusive_contexts = false;

    // LSA barriers synchronize only the local LSA team. Cross-LSA/world
    // synchronization remains the caller's responsibility.
    int lsa_barrier_count = 0;
    bool require_lsa_multimem = false;
};

struct NcclTransportProperties {
    int runtime_version = 0;
    int rank = -1;
    int num_ranks = 0;
    int cuda_device = -1;
    bool device_api_supported = false;
    bool multimem_supported = false;
    bool lsa_multimem_enabled = false;
    int lsa_team_count = 0;
    int lsa_barrier_count = 0;
    bool gin_enabled = false;
    NcclGinBackend gin_backend = NcclGinBackend::kNone;
    int gin_connection_count = 0;
    int gin_context_count = 0;
};

namespace detail {
struct NcclDeviceContextAccess;
}  // namespace detail

class NcclDeviceTransportImpl;

// Opaque token for one collectively registered symmetric buffer. The token
// does not own the allocation; deregister it before freeing the buffer.
class NcclBufferRegistration {
   public:
    bool valid() const { return id_ != 0; }

   private:
    uint64_t id_ = 0;

    friend class NcclDeviceTransportImpl;
};

// Pass this small Mooncake context by value to kernels. Native NCCL
// communicators and windows remain behind opaque pointers.
class NcclDeviceContext {
   public:
    bool valid() const { return native_comm_ != nullptr; }

   private:
    const void* native_comm_ = nullptr;
    const void* native_window_ = nullptr;
    const void* local_base_ = nullptr;
    int rank_ = -1;
    int gin_context_count_ = 0;
    bool gin_enabled_ = false;
    bool lsa_multimem_enabled_ = false;

    friend class NcclDeviceTransportImpl;
    friend struct detail::NcclDeviceContextAccess;
};

// Host calls on one transport instance are not thread-safe and must be
// externally serialized. initialize() binds the transport to the current CUDA
// device; keep that device current for every later host call, including
// shutdown and destruction.
//
class NcclTransport {
   public:
    virtual ~NcclTransport() = default;

    // Generate the NCCL bootstrap ID on one rank. Exchange this int32_t blob
    // through the caller's existing control plane before initialize().
    virtual std::vector<int32_t> createUniqueId() = 0;

    // Create the host and device communicators. Every rank must call this with
    // the same unique ID and compatible config. The NCCL headers used to build
    // Mooncake and device kernels must exactly match the runtime libnccl;
    // initialization rejects a mismatch. Rebuild AOT kernels and regenerate
    // cached JIT kernels after every NCCL upgrade.
    virtual int initialize(const NcclTransportConfig& config,
                           const std::vector<int32_t>& unique_id) = 0;

    // NCCL-compatible VMM allocation. These low-level methods are local; every
    // rank must verify allocation success before entering registerBuffer().
    virtual void* allocateBuffer(size_t bytes) = 0;
    virtual int freeBuffer(void* ptr) = 0;

    // Collectively register a symmetric buffer. Calls must occur in the same
    // order on every rank. Deregistration is local after all device work and
    // remote access have completed. The registration is invalidated on
    // successful deregistration.
    virtual int registerBuffer(void* ptr, size_t bytes,
                               NcclBufferRegistration* registration) = 0;
    virtual int deregisterBuffer(NcclBufferRegistration* registration) = 0;

    // Safe common path: every rank allocates, collectively checks that all
    // allocations succeeded, and only then enters registration. All ranks
    // must call this method in the same order.
    virtual int allocateAndRegisterBuffer(
        size_t bytes, void** ptr, NcclBufferRegistration* registration) = 0;

    // Snapshot passed by value to a CUDA kernel and bound to one registered
    // buffer. It remains valid until registration removal or shutdown.
    virtual NcclDeviceContext deviceContext(
        const NcclBufferRegistration& registration) const = 0;

    virtual NcclTransportProperties properties() const = 0;
    virtual bool initialized() const = 0;

    // The caller must first ensure that no kernel can access the context or
    // any registered buffer.
    virtual int shutdown() = 0;
};

// Create the CUDA-only NCCL LSA/GIN device transport.
std::unique_ptr<NcclTransport> createNcclDeviceTransport();

}  // namespace device
}  // namespace mooncake
