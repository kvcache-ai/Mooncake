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

#ifndef TENT_TRANSPORT_MPCOMM_MPCOMM_ADAPTER_H
#define TENT_TRANSPORT_MPCOMM_MPCOMM_ADAPTER_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

// A transfer issued to the MPComm data plane. Zero is never a valid handle,
// matching MPComm's own convention for an invalid one.
using MpcommTransferHandle = uint64_t;
constexpr MpcommTransferHandle kInvalidMpcommTransferHandle = 0;

// Outcome of a transfer that has reached a terminal state, in adapter-neutral
// terms so that nothing above this boundary needs the provider's headers.
struct MpcommTransferOutcome {
    // Whether the provider reported success. MPComm only reports success once
    // every chunk of the transfer has completed, so this implies the full
    // request length was moved.
    bool ok = false;
    // Bytes the provider accounted for. MPComm counts bytes at post time, so
    // on success this equals the request length.
    size_t bytes_transferred = 0;
    // Provider status, kept for diagnostics only. Zero means success.
    int native_status = 0;
};

// Injectable MPComm boundary.
//
// This interface deliberately knows nothing about TENT Request, SubBatch,
// MpcommTask, SegmentDesc or the peer cache: everything above this line is
// TENT's own logic, and keeping it behind an interface is what lets that logic
// be tested without RDMA hardware or a real libmpcomm - the same reason
// UrmaAdapter and TpuPjrtShim exist for their providers.
//
// It is a thin pass-through rather than a device abstraction. MPComm owns its
// own slicing, NIC and QP selection and worker threads, so there is no
// scheduling here to model; the operations map one to one onto the provider.
//
// Threading: implementations must tolerate concurrent calls, because TENT
// submits and polls transfers from several threads. MPComm itself is
// thread-safe, so the real adapter adds no locking of its own.
class MpcommAdapter {
   public:
    virtual ~MpcommAdapter() = default;

    // False when the provider is not usable at all - for example when this
    // build has no libmpcomm. install() must fail in that case rather than
    // advertise a transport that cannot move data.
    [[nodiscard]] virtual bool available() const noexcept = 0;

    // --- Lifecycle -------------------------------------------------------

    virtual Status init(const std::string &host_id,
                        const std::string &device_names, int tcp_port) = 0;

    // The port actually bound for the metadata handshake, which may differ
    // from the one requested by init().
    [[nodiscard]] virtual int tcpPort() const = 0;

    virtual Status startAcceptThread() = 0;

    // Teardown is best effort and must be safe to call when init() was never
    // called or failed part way, since uninstall() runs unconditionally.
    virtual void stopAcceptThread() = 0;
    virtual void shutdown() = 0;

    // --- Memory ----------------------------------------------------------

    virtual Status registerMemory(void *addr, size_t length) = 0;
    virtual void unregisterMemory(void *addr) = 0;
    virtual Status publishBuffer(void *addr, size_t length, int numa_node) = 0;
    virtual void unpublishBuffer(void *addr) = 0;

    // --- Peers -----------------------------------------------------------

    // Establishes the provider-level connection to a peer. MPComm keys its
    // connections by host id and offers no way to close one, so callers must
    // not call this twice for the same host id: a second call replaces the
    // connection record wholesale, dropping the remote keys it carries and
    // leaking the previous queue pairs.
    virtual Status connect(const std::string &host_id,
                           const std::string &tcp_addr, int tcp_port) = 0;

    // Fetches the peer's memory keys into the provider's own cache. Idempotent
    // and safe to repeat: the provider replaces the whole key set atomically,
    // which is what makes a refresh possible without reconnecting.
    virtual Status queryRemoteBuffer(const std::string &host_id,
                                     const std::string &tcp_addr,
                                     int tcp_port) = 0;

    // --- Data path -------------------------------------------------------

    // Returns kInvalidMpcommTransferHandle when the transfer could not be
    // issued. A valid handle must eventually be passed to releaseTransfer()
    // exactly once.
    virtual MpcommTransferHandle putAsync(uintptr_t local_addr,
                                          const std::string &host_id,
                                          uintptr_t remote_addr,
                                          size_t length) = 0;
    virtual MpcommTransferHandle getAsync(uintptr_t local_addr,
                                          const std::string &host_id,
                                          uintptr_t remote_addr,
                                          size_t length) = 0;

    [[nodiscard]] virtual bool isTransferComplete(
        MpcommTransferHandle handle) = 0;
    virtual MpcommTransferOutcome getTransferResult(
        MpcommTransferHandle handle) = 0;
    virtual void releaseTransfer(MpcommTransferHandle handle) = 0;
};

// Returns the libmpcomm-backed adapter when this build has the provider,
// otherwise an unavailable stub whose operations fail with an explicit
// message. Tests inject their own implementation instead of calling this.
//
// Shared rather than unique because uninstall() drops the transport's
// reference: a test that injected an adapter keeps its own so that it can
// still inspect what the transport did during teardown.
std::shared_ptr<MpcommAdapter> createDefaultMpcommAdapter();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_MPCOMM_MPCOMM_ADAPTER_H
