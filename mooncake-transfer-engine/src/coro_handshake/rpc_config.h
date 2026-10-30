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

#ifndef MOONCAKE_CORO_HANDSHAKE_RPC_CONFIG_H
#define MOONCAKE_CORO_HANDSHAKE_RPC_CONFIG_H

#include <cstdint>

namespace mooncake {

// Extra bytes the RPC body may add on top of MC_HANDSHAKE_MAX_LENGTH.
constexpr uint64_t kHandshakeEnvelopeBytes = 64;

struct HandshakeLimits {
    int threads = 2;
    int max_concurrency = 256;
    int max_pools = 1024;
    int max_idle_per_peer = 128;
    int max_active_per_peer = 128;
    int timeout_seconds = 60;
    int idle_ttl_seconds = 60;
    int connect_timeout_seconds = 5;
    uint64_t max_frame = 1ull << 20;
    uint64_t max_bytes = 512ull << 20;
    bool ipv6 = false;
};

// Uses the common integer parser. Zero and values above 64 GiB return fallback.
uint64_t ParseHandshakeUint64(const char *text, uint64_t fallback);

HandshakeLimits LoadHandshakeLimits();

}  // namespace mooncake

#endif  // MOONCAKE_CORO_HANDSHAKE_RPC_CONFIG_H
