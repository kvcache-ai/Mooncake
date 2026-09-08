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

#ifndef TENT_TRANSPORT_MPCOMM_MPCOMM_PEER_REGISTRY_H
#define TENT_TRANSPORT_MPCOMM_MPCOMM_PEER_REGISTRY_H

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/mpcomm/mpcomm_adapter.h"

namespace mooncake {
namespace tent {

// Endpoint attribute published in the segment descriptor:
//   "v1:<ipv4>:<port>"
// The version prefix makes a later format change detectable instead of being
// silently misparsed by an older peer. An attribute without a prefix is read
// as v1 so that a peer predating the prefix still interoperates.
inline constexpr char kMpcommEndpointAttrPrefix[] = "v1:";

// Parses a decimal TCP port. Rejects everything atoi() would quietly turn into
// 0 or a truncated value, which would then be used as if it were a real port.
Status parseMpcommTcpPort(const std::string &text, int &port);

// Splits "[v1:]<ipv4>:<port>" into address and port.
Status parseMpcommEndpointAttr(const std::string &attr, std::string &addr,
                               int &port);

// Builds the attribute a peer will read, from the local segment name (which is
// "host:rpc_port") and the port MPComm actually bound. Paired with
// parseMpcommEndpointAttr so that the published and parsed formats cannot drift
// apart unnoticed.
std::string buildMpcommEndpointAttr(const std::string &local_segment_name,
                                    int tcp_port);

// One buffer range as published by a peer.
struct MpcommBufferRange {
    uint64_t addr{0};
    uint64_t length{0};
};

// Lifecycle of a cached peer. The connection and the remote memory keys are
// tracked separately because they are not equally recoverable: MPComm offers no
// way to close a connection, so a live one must be kept and reused, whereas the
// keys can be fetched again at any time.
enum class MpcommPeerState {
    // No connection yet; the owning caller is establishing one.
    CONNECTING,
    // The connection is established but the keys are absent or stale.
    // Recoverable by querying again - never by reconnecting.
    CONNECTED_NO_KEYS,
    // Connection and keys are both valid, the keys covering covered_buffers.
    READY,
};

// Cached state of one MPComm peer, keyed by its host id (the segment name).
struct MpcommPeerInfo {
    // Endpoint of the live connection. Retained so that a key refresh reaches
    // the process we are actually connected to, even if the peer has since
    // started advertising a different endpoint.
    std::string tcp_addr;
    int tcp_port{0};
    MpcommPeerState state{MpcommPeerState::CONNECTING};
    // Fingerprint of the peer's buffer set when its keys were last fetched,
    // used as a cheap equality check on the hot path.
    uint64_t buffers_epoch{0};
    // The ranges those keys are known to cover. A differing fingerprint alone
    // is not a reason to refetch: descriptors are cached per thread and expire
    // independently, so two threads routinely observe different buffer sets for
    // the same peer during a refresh window. Comparing coverage instead of
    // equality keeps the decision monotonic - keys are refetched only when a
    // range appears that they do not cover - which stops threads from
    // alternately invalidating each other's work and issuing a blocking query
    // every time. It also avoids refetching when the peer merely unregistered
    // memory, since keys the peer no longer publishes are harmless.
    std::vector<MpcommBufferRange> covered_buffers;
    // Non-zero while a caller is connecting or refetching keys. Kept separate
    // from the state so that a refresh never has to invalidate keys that are
    // still usable: a request whose ranges the current keys cover proceeds
    // while the refresh runs, instead of blocking behind it. The value also
    // identifies the owner, so that a late cleanup cannot touch the work of a
    // later one.
    uint64_t owner_generation{0};
};

// Owns the MPComm peer cache: which peers are connected, whose keys are
// current, and who is allowed to talk to a peer at any moment.
//
// Split out of MpcommTransport for two reasons. It is the part of the transport
// with non-trivial concurrency, and it depends only on MpcommAdapter - not on
// SegmentDesc, ControlService or the TENT runtime - so it can be constructed
// directly and driven against an injected adapter without RDMA hardware.
//
// Keyed by MPComm host id rather than SegmentID because that is what MPComm
// keys connections by: closing and reopening a segment yields a fresh
// SegmentID for the same peer, and a second connect() to an already connected
// peer replaces its connection record wholesale, discarding the remote keys it
// carries and leaking the queue pairs of the previous one.
class MpcommPeerRegistry {
   public:
    explicit MpcommPeerRegistry(std::shared_ptr<MpcommAdapter> adapter);

    MpcommPeerRegistry(const MpcommPeerRegistry &) = delete;
    MpcommPeerRegistry &operator=(const MpcommPeerRegistry &) = delete;

    // Makes the peer usable for transfers touching `buffers`: connects if it
    // has never been connected, and fetches its keys if they are missing or do
    // not cover those ranges. Concurrent callers for the same peer wait for the
    // one that owns it rather than contacting the peer a second time.
    //
    // `tcp_addr` / `tcp_port` are the endpoint the peer currently advertises.
    // They are used for a first connection only; afterwards the endpoint the
    // connection was actually made to is used, since that is the process the
    // keys must come from.
    Status ensure(const std::string &host_id, const std::string &tcp_addr,
                  int tcp_port, const std::vector<MpcommBufferRange> &buffers);

    // Drops every entry and wakes every waiter. Used by uninstall(): a caller
    // released this way finds its entry gone and fails through the adapter,
    // which by then reports that the provider is no longer initialised.
    void clear();

    // Diagnostics. Also what the hardware-free tests assert on, since the
    // observable effect of this class is which peers it keeps and in what
    // state.
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool contains(const std::string &host_id) const;
    // Returns false when the peer is not cached at all.
    [[nodiscard]] bool stateOf(const std::string &host_id,
                               MpcommPeerState &state) const;

   private:
    std::shared_ptr<MpcommAdapter> adapter_;
    std::unordered_map<std::string, MpcommPeerInfo> peers_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    uint64_t next_generation_{0};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_MPCOMM_MPCOMM_PEER_REGISTRY_H
