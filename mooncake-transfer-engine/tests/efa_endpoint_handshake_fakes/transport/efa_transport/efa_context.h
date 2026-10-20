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

// Test-only RPC and address-vector boundary for the real EfaEndPoint code.
// This header shadows "transport/efa_transport/efa_context.h" on the test's
// include path, so efa_endpoint.cpp can be compiled and driven without an
// EFA device or a real provider: the handshake RPC and the AV insert/remove
// calls are recorded and scripted by the test.
#pragma once

#include <functional>
#include <string>
#include <vector>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

struct EfaHandshakeTestTransport {
    using Desc = TransferMetadata::HandShakeDesc;
    std::function<int(const Desc&, Desc&)> rpc;
    int sendHandshake(const std::string&, const Desc& local, Desc& peer) {
        return rpc(local, peer);
    }
};

class EfaContext {
   public:
    std::string name;
    EfaHandshakeTestTransport transport;
    int inserts = 0;
    int removals = 0;
    explicit EfaContext(std::string path) : name(std::move(path)) {}
    const std::string& nicPath() const { return name; }
    std::string localEpAddr() const { return name; }
    const std::vector<char>& localEpAddrBytes() const {
        static const std::vector<char> bytes{1};
        return bytes;
    }
    EfaHandshakeTestTransport& engine() { return transport; }
    int insertPeerAddr(const std::string&, fi_addr_t& addr) {
        addr = ++inserts;
        return 0;
    }
    int insertPeerAddrBytes(const void*, size_t, fi_addr_t& addr) {
        return insertPeerAddr("", addr);
    }
    int removePeerAddr(fi_addr_t) {
        ++removals;
        return 0;
    }
    int submitSlicesOnPeer(fi_addr_t, std::vector<Transport::Slice*>&,
                           std::vector<Transport::Slice*>&) {
        return 0;
    }
};

}  // namespace mooncake
