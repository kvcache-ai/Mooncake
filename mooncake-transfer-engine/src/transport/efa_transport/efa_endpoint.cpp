// Copyright 2024 KVCache.AI
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

#include "transport/efa_transport/efa_endpoint.h"

#include <glog/logging.h>

#include <cstdlib>
#include <sstream>

#include "config.h"

namespace mooncake {

EfaEndPoint::EfaEndPoint(EfaContext& context)
    : context_(context), status_(INITIALIZING), peer_fi_addr_(FI_ADDR_UNSPEC) {}

EfaEndPoint::~EfaEndPoint() { disconnect(); }

void EfaEndPoint::setPeerNicPath(const std::string& peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (peer_nic_path_ == peer_nic_path) return;  // No change

    // The P2PHANDSHAKE transport assigns a fresh RPC port per bootstrap,
    // so every sglang KV transfer arrives with a different peer_nic_path
    // string ("host:PORT@nicX") even though the EFA peer has not moved.
    // The RPC port is pure metadata — EFA SRD addressing is keyed on the
    // binary efa_addr (GID/QPN) returned by fi_getname(), which only
    // changes on actual peer process restart.  Treating a port change as
    // a reconnect would fi_av_remove + fi_av_insert on every KV transfer,
    // tearing down the provider's AH activation / RNR state and paying a
    // first-packet AH warm-up latency each time.
    //
    // If the normalized nic path (host+NIC, no port) is unchanged, keep
    // the AV slot intact and only refresh the stored string.  A real peer
    // restart still gets caught in setupConnectionsByPassive by comparing
    // peer_desc.efa_addr against cached_peer_addr_.
    if (normalizeNicPath(peer_nic_path_) == normalizeNicPath(peer_nic_path)) {
        VLOG(1) << "Peer RPC port rotated, keeping AV slot: " << peer_nic_path_
                << " -> " << peer_nic_path;
        peer_nic_path_ = peer_nic_path;
        return;
    }

    // Different host or different NIC reached the same endpoint slot — this
    // should not normally happen because EfaContext::endpoint() keys the map
    // by normalized nic path, but handle it defensively.
    if (status_.load(std::memory_order_relaxed) == CONNECTED) {
        LOG(INFO) << "Peer reconnected with new address, re-establishing: "
                  << peer_nic_path_ << " -> " << peer_nic_path;
        disconnectUnlocked();
    }
    peer_nic_path_ = peer_nic_path;
}

int EfaEndPoint::setupConnectionsByActive() {
    RWSpinlock::WriteGuard guard(lock_);
    if (status_.load(std::memory_order_relaxed) == CONNECTED) return 0;

    // Loopback: handshake against ourselves.  Use the binary overload
    // so we avoid hex-encoding the local address just to have
    // insertPeerAddr decode it right back.
    if (context_.nicPath() == peer_nic_path_) {
        const auto& bytes = context_.localEpAddrBytes();
        int ret = context_.insertPeerAddrBytes(bytes.data(), bytes.size(),
                                               peer_fi_addr_);
        if (ret != 0) return ret;
        status_.store(CONNECTED, std::memory_order_release);
        LOG(INFO) << "EFA loopback connection established: " << toString();
        return 0;
    }

    // Exchange addresses via the transfer-metadata handshake RPC.
    TransferMetadata::HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.efa_addr = context_.localEpAddr();

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        LOG(ERROR) << "Parse peer EFA nic path failed: " << peer_nic_path_;
        return ERR_INVALID_ARGUMENT;
    }

    int rc = context_.engine().sendHandshake(peer_server_name, local_desc,
                                             peer_desc);
    if (rc) return rc;

    if (peer_desc.efa_addr.empty()) {
        LOG(ERROR) << "Peer did not provide EFA address in handshake";
        return ERR_REJECT_HANDSHAKE;
    }

    rc = context_.insertPeerAddr(peer_desc.efa_addr, peer_fi_addr_);
    if (rc != 0) return rc;
    cached_peer_addr_ = peer_desc.efa_addr;

    status_.store(CONNECTED, std::memory_order_release);
    VLOG(1) << "EFA connection established: " << toString()
            << " peer_fi_addr=" << peer_fi_addr_;
    return 0;
}

int EfaEndPoint::setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                           HandShakeDesc& local_desc) {
    RWSpinlock::WriteGuard guard(lock_);

    // Compare against the normalized peer path so RPC port rotation (see
    // setPeerNicPath()) does not trip this sanity check.  context_.nicPath()
    // has no port to begin with, so a direct string compare is fine for it.
    if (peer_desc.peer_nic_path != context_.nicPath() ||
        normalizeNicPath(peer_desc.local_nic_path) !=
            normalizeNicPath(peer_nic_path_)) {
        local_desc.reply_msg = "EFA nic path inconsistency";
        LOG(ERROR) << "Invalid argument: peer EFA nic path inconsistency"
                   << " peer_nic_path=" << peer_desc.peer_nic_path
                   << " context_.nicPath()=" << context_.nicPath()
                   << " local_nic_path=" << peer_desc.local_nic_path
                   << " peer_nic_path_=" << peer_nic_path_;
        return ERR_REJECT_HANDSHAKE;
    }

    if (peer_desc.efa_addr.empty()) {
        local_desc.reply_msg = "No EFA address provided";
        LOG(ERROR) << "Peer did not provide EFA address";
        return ERR_REJECT_HANDSHAKE;
    }

    // Classify this passive handshake so we can emit the right log level
    // without changing functional behavior: the AV reinsert below must
    // still happen on every handshake because libfabric's EFA provider
    // tracks per-peer transport state that depends on a fresh
    // fi_av_insert (e.g. provider-internal AH activation and RNR state).
    // Skipping reinsert caused request-level stalls under bilateral
    // sglang P/D load even when the peer EFA address was unchanged.
    //
    // Log semantics:
    //   * Same cached peer address → benign symmetric handshake under
    //     bilateral traffic.  Demote to INFO to keep decode logs readable
    //     without hiding real reconnects.
    //   * Different cached peer address → genuine reconnect (peer
    //     restart, port reshuffle that reached disconnect first, etc.).
    //     Keep at WARNING so it stays visible.
    if (status_.load(std::memory_order_relaxed) == CONNECTED) {
        if (!cached_peer_addr_.empty() &&
            peer_desc.efa_addr == cached_peer_addr_) {
            VLOG(1) << "EFA passive handshake (same peer addr): "
                    << toString();
        } else {
            LOG(WARNING) << "Re-establish EFA connection: " << toString();
        }
        disconnectUnlocked();
    }

    int ret = context_.insertPeerAddr(peer_desc.efa_addr, peer_fi_addr_);
    if (ret != 0) {
        local_desc.reply_msg = "Failed to insert peer address";
        return ret;
    }
    cached_peer_addr_ = peer_desc.efa_addr;

    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.efa_addr = context_.localEpAddr();
    // reply_msg empty on success

    status_.store(CONNECTED, std::memory_order_release);
    VLOG(1) << "EFA connection established (passive): " << toString();
    return 0;
}

void EfaEndPoint::disconnect() {
    RWSpinlock::WriteGuard guard(lock_);
    disconnectUnlocked();
}

void EfaEndPoint::disconnectUnlocked() {
    if (peer_fi_addr_ != FI_ADDR_UNSPEC) {
        context_.removePeerAddr(peer_fi_addr_);
        peer_fi_addr_ = FI_ADDR_UNSPEC;
    }
    cached_peer_addr_.clear();
    status_.store(UNCONNECTED, std::memory_order_release);
}

void EfaEndPoint::markDetachedForTeardown() {
    RWSpinlock::WriteGuard guard(lock_);
    peer_fi_addr_ = FI_ADDR_UNSPEC;
    cached_peer_addr_.clear();
    status_.store(UNCONNECTED, std::memory_order_release);
}

const std::string EfaEndPoint::toString() const {
    return "EfaEndPoint[" + context_.nicPath() + " <-> " + peer_nic_path_ + "]";
}

int EfaEndPoint::submitPostSend(
    std::vector<Transport::Slice*>& slice_list,
    std::vector<Transport::Slice*>& failed_slice_list) {
    if (status_.load(std::memory_order_relaxed) != CONNECTED) {
        int ret = setupConnectionsByActive();
        if (ret != 0) {
            for (auto* slice : slice_list) {
                failed_slice_list.push_back(slice);
            }
            slice_list.clear();
            return ret;
        }
    }

    // Hold a read lock for the entire submit window so setPeerNicPath() /
    // disconnect() (which take the write lock) cannot fi_av_remove() our
    // slot while an fi_write() inside submitSlicesOnPeer is still in
    // flight.  The previous code latched peer_fi_addr_ under the lock and
    // released it before calling into the context, leaving a race: a
    // concurrent peer reconnect could remove the AV entry after the latch,
    // and libfabric would segfault on the stale fi_addr_t.  Read locks
    // stack, so multiple senders to the same peer still submit in parallel.
    RWSpinlock::ReadGuard guard(lock_);

    // Re-check status under the lock — a racing disconnect() could have
    // flipped us to UNCONNECTED between the outer check and acquiring the
    // lock.  Fail the batch so the caller retries with a fresh setup.
    if (status_.load(std::memory_order_acquire) != CONNECTED) {
        for (auto* slice : slice_list) failed_slice_list.push_back(slice);
        slice_list.clear();
        return ERR_ENDPOINT;
    }

    fi_addr_t peer = peer_fi_addr_;
    if (peer == FI_ADDR_UNSPEC) {
        for (auto* slice : slice_list) failed_slice_list.push_back(slice);
        slice_list.clear();
        return ERR_ENDPOINT;
    }

    return context_.submitSlicesOnPeer(peer, slice_list, failed_slice_list);
}

}  // namespace mooncake
