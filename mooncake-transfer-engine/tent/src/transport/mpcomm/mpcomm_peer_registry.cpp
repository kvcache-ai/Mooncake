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

#include "tent/transport/mpcomm/mpcomm_peer_registry.h"

#include <glog/logging.h>

#include <utility>

namespace mooncake {
namespace tent {
namespace {

// Fingerprint of a peer's published buffer set, used only as a cheap equality
// check before the coverage comparison.
uint64_t computeBuffersEpoch(const std::vector<MpcommBufferRange> &buffers) {
    uint64_t hash = 1469598103934665603ULL;  // FNV-1a 64-bit offset basis
    auto mix = [&hash](uint64_t value) {
        hash ^= value;
        hash *= 1099511628211ULL;  // FNV-1a 64-bit prime
    };
    mix(buffers.size());
    for (const auto &buffer : buffers) {
        mix(buffer.addr);
        mix(buffer.length);
    }
    return hash;
}

// Whether the keys cached for a peer already cover every range the peer is
// currently observed to publish. Ranges the peer has dropped are ignored: a key
// for memory that is no longer published is never used.
bool keysCoverBuffers(const MpcommPeerInfo &peer,
                      const std::vector<MpcommBufferRange> &buffers) {
    for (const auto &buffer : buffers) {
        bool covered = false;
        for (const auto &range : peer.covered_buffers) {
            if (range.addr == buffer.addr && range.length == buffer.length) {
                covered = true;
                break;
            }
        }
        if (!covered) return false;
    }
    return true;
}

}  // namespace

Status parseMpcommTcpPort(const std::string &text, int &port) {
    if (text.empty() || text.size() > 5) {
        return Status::InvalidArgument("MpcommTransport: malformed port '" +
                                       text + "'");
    }
    int value = 0;
    for (char c : text) {
        if (c < '0' || c > '9') {
            return Status::InvalidArgument(
                "MpcommTransport: non-numeric port '" + text + "'");
        }
        value = value * 10 + (c - '0');
    }
    if (value <= 0 || value > 65535) {
        return Status::InvalidArgument("MpcommTransport: port out of range '" +
                                       text + "'");
    }
    port = value;
    return Status::OK();
}

Status parseMpcommEndpointAttr(const std::string &attr, std::string &addr,
                               int &port) {
    std::string body = attr;
    // A version prefix is 'v', digits, then a colon. Host names may also begin
    // with 'v', so require that exact shape before treating it as a version.
    if (body.size() > 1 && body[0] == 'v') {
        size_t i = 1;
        while (i < body.size() && body[i] >= '0' && body[i] <= '9') ++i;
        if (i > 1 && i < body.size() && body[i] == ':') {
            if (body.compare(0, i + 1, kMpcommEndpointAttrPrefix) != 0) {
                return Status::InvalidArgument(
                    "MpcommTransport: unsupported endpoint attribute '" + attr +
                    "', this build understands " + kMpcommEndpointAttrPrefix);
            }
            body.erase(0, i + 1);
        }
    }
    auto sep = body.rfind(':');
    if (sep == std::string::npos || sep == 0 || sep + 1 >= body.size()) {
        return Status::InvalidArgument(
            "MpcommTransport: malformed endpoint attribute '" + attr + "'");
    }
    addr = body.substr(0, sep);
    if (addr.find(':') != std::string::npos) {
        // MPComm's handshake sockets are AF_INET, and an IPv6 literal would
        // additionally need bracket syntax to be unambiguous here.
        return Status::InvalidArgument(
            "MpcommTransport: only IPv4 endpoints are supported, got '" + attr +
            "'");
    }
    return parseMpcommTcpPort(body.substr(sep + 1), port);
}

std::string buildMpcommEndpointAttr(const std::string &local_segment_name,
                                    int tcp_port) {
    // The segment name is "host:rpc_port"; only the host part identifies where
    // this process can be reached, and the port is MPComm's own.
    std::string host = local_segment_name;
    auto sep = host.rfind(':');
    if (sep != std::string::npos) host.erase(sep);
    return std::string(kMpcommEndpointAttrPrefix) + host + ":" +
           std::to_string(tcp_port);
}

MpcommPeerRegistry::MpcommPeerRegistry(std::shared_ptr<MpcommAdapter> adapter)
    : adapter_(std::move(adapter)) {}

Status MpcommPeerRegistry::ensure(
    const std::string &host_id, const std::string &tcp_addr, int tcp_port,
    const std::vector<MpcommBufferRange> &buffers) {
    const uint64_t buffers_epoch = computeBuffersEpoch(buffers);

    // Fast path: there is nothing to do while the peer is connected and its
    // keys still describe the buffer set just observed. This runs once per
    // submitted request, so it stays ahead of everything else.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = peers_.find(host_id);
        if (it != peers_.end() && it->second.state == MpcommPeerState::READY &&
            (it->second.buffers_epoch == buffers_epoch ||
             keysCoverBuffers(it->second, buffers))) {
            return Status::OK();
        }
    }

    uint64_t my_generation = 0;
    bool needs_connect = false;
    std::string endpoint_addr;
    int endpoint_port = 0;

    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            auto it = peers_.find(host_id);
            if (it == peers_.end()) {
                // Never connected: claim the entry and run the full handshake.
                MpcommPeerInfo entry;
                entry.tcp_addr = tcp_addr;
                entry.tcp_port = tcp_port;
                entry.state = MpcommPeerState::CONNECTING;
                entry.owner_generation = ++next_generation_;
                my_generation = entry.owner_generation;
                endpoint_addr = tcp_addr;
                endpoint_port = tcp_port;
                needs_connect = true;
                peers_.emplace(host_id, std::move(entry));
                break;
            }
            // Keys that already cover the observed ranges are usable whatever
            // another caller may be doing to this entry: the connection is live
            // and those keys stay valid, so a refresh in progress must not
            // block a request that does not need its result.
            if (it->second.state == MpcommPeerState::READY &&
                (it->second.buffers_epoch == buffers_epoch ||
                 keysCoverBuffers(it->second, buffers))) {
                return Status::OK();
            }
            if (it->second.owner_generation != 0) {
                // Owned by another caller. Re-evaluate once it is done rather
                // than trusting the outcome: it may have failed and left work.
                cv_.wait(lock);
                continue;
            }
            // Unowned, and the keys are missing or do not cover what this
            // request needs. Take ownership of a key refresh over the existing
            // connection - reconnecting would replace MPComm's record for this
            // host, dropping the keys it carries and leaking its queue pairs.
            // Query the endpoint the connection was made to rather than the
            // newly advertised one, so the keys match the live connection.
            needs_connect = it->second.state == MpcommPeerState::CONNECTING;
            endpoint_addr = it->second.tcp_addr;
            endpoint_port = it->second.tcp_port;
            if (!needs_connect &&
                (endpoint_addr != tcp_addr || endpoint_port != tcp_port)) {
                LOG(WARNING)
                    << "MpcommTransport: peer " << host_id << " now advertises "
                    << tcp_addr << ":" << tcp_port
                    << " but the live connection uses " << endpoint_addr << ":"
                    << endpoint_port
                    << "; MPComm cannot replace a connection, so this peer "
                       "stays reachable only at the old endpoint";
            }
            it->second.owner_generation = ++next_generation_;
            my_generation = it->second.owner_generation;
            break;
        }
    }

    // Every exit that does not publish usable keys has to release ownership, or
    // later callers would wait on this entry forever. A guard covers early
    // returns and exceptions alike, which a hand-written cleanup at each return
    // would not. An established connection is kept rather than dropped: MPComm
    // cannot close one, so discarding the entry would only cause a reconnect,
    // which is exactly what has to be avoided.
    struct PeerGuard {
        MpcommPeerRegistry *self;
        const std::string *host;
        uint64_t generation;
        bool committed{false};
        ~PeerGuard() {
            if (committed) return;
            bool changed = false;
            try {
                std::lock_guard<std::mutex> lock(self->mutex_);
                auto it = self->peers_.find(*host);
                // Touch only our own claim: clear() may have emptied the map,
                // and another caller may already own a newer entry.
                if (it != self->peers_.end() &&
                    it->second.owner_generation == generation) {
                    if (it->second.state == MpcommPeerState::CONNECTING) {
                        // No connection was established, nothing to keep.
                        self->peers_.erase(it);
                    } else {
                        it->second.owner_generation = 0;
                    }
                    changed = true;
                }
            } catch (...) {
                return;  // a destructor must not throw
            }
            if (changed) self->cv_.notify_all();
        }
    } guard{this, &host_id, my_generation};

    if (needs_connect) {
        auto status = adapter_->connect(host_id, endpoint_addr, endpoint_port);
        if (!status.ok()) {
            LOG(ERROR) << "MpcommTransport: Failed to connect to " << host_id
                       << " at " << endpoint_addr << ":" << endpoint_port
                       << ": " << status.ToString();
            return status;
        }
        // Record that the connection now exists before the keys are fetched:
        // if the query fails, the guard has to keep the connection rather than
        // erase the entry and let the next caller reconnect.
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = peers_.find(host_id);
            if (it != peers_.end() &&
                it->second.owner_generation == my_generation) {
                it->second.state = MpcommPeerState::CONNECTED_NO_KEYS;
            }
        }
    }

    // Fetch the peer's memory keys. This is the only source of its rkeys, so
    // without them every transfer to this peer fails; report the failure rather
    // than publish an entry that cannot work. The usual cause is a peer that
    // has not finished registering its buffers, which a retry resolves - a
    // retry of the query alone, since the entry stays CONNECTED_NO_KEYS.
    auto status =
        adapter_->queryRemoteBuffer(host_id, endpoint_addr, endpoint_port);
    if (!status.ok()) {
        LOG(ERROR) << "MpcommTransport: Failed to query remote buffers from "
                   << host_id << ": " << status.ToString();
        return status;
    }

    // Publish and release the waiters. Look the entry up rather than inserting
    // it: if it is gone the registry was cleared while we worked, and
    // re-creating it would resurrect a stale entry.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = peers_.find(host_id);
        if (it == peers_.end() ||
            it->second.owner_generation != my_generation) {
            return Status::InternalError(
                "MpcommTransport: peer entry dropped during handshake");
        }
        it->second.state = MpcommPeerState::READY;
        it->second.owner_generation = 0;
        it->second.buffers_epoch = buffers_epoch;
        it->second.covered_buffers = buffers;
    }
    guard.committed = true;
    cv_.notify_all();

    if (needs_connect) {
        LOG(INFO) << "MpcommTransport: Connected to segment " << host_id
                  << " at " << endpoint_addr << ":" << endpoint_port;
    }
    return Status::OK();
}

void MpcommPeerRegistry::clear() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        peers_.clear();
    }
    // Release anyone blocked waiting for work that will never finish.
    cv_.notify_all();
}

size_t MpcommPeerRegistry::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return peers_.size();
}

bool MpcommPeerRegistry::contains(const std::string &host_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return peers_.find(host_id) != peers_.end();
}

bool MpcommPeerRegistry::stateOf(const std::string &host_id,
                                 MpcommPeerState &state) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = peers_.find(host_id);
    if (it == peers_.end()) return false;
    state = it->second.state;
    return true;
}

}  // namespace tent
}  // namespace mooncake
