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

#include "rpc_config.h"

#include <glog/logging.h>

#include "common.h"
#include "config.h"
#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variable.h"

namespace mooncake {
namespace {

constexpr uint64_t kMaxInflightBytes = 64ull << 30;

int ReadInt(const char *name, int fallback, int min_value, int max_value) {
    const int parsed =
        Environ::ReadOr(EnvironmentVariable<int>{name}, fallback);
    if (parsed < min_value || parsed > max_value) {
        LOG(WARNING) << "Ignoring out-of-range " << name << "=" << parsed
                     << ", using " << fallback;
        return fallback;
    }
    return parsed;
}

uint64_t ReadInflightBytes(uint64_t fallback) {
    const uint64_t parsed = Environ::ReadOr(
        EnvironmentVariable<uint64_t>{"MC_HANDSHAKE_MAX_INFLIGHT_BYTES"},
        fallback);
    if (parsed == 0 || parsed > kMaxInflightBytes) {
        LOG(WARNING) << "Ignoring out-of-range MC_HANDSHAKE_MAX_INFLIGHT_BYTES="
                     << parsed << ", using " << fallback;
        return fallback;
    }
    return parsed;
}

}  // namespace

uint64_t ParseHandshakeUint64(const char *text, uint64_t fallback) {
    if (text == nullptr) return fallback;
    const auto parsed = TryParseEnvironmentValue<uint64_t>(text);
    if (!parsed.has_value() || *parsed == 0 || *parsed > kMaxInflightBytes) {
        return fallback;
    }
    return *parsed;
}

HandshakeLimits LoadHandshakeLimits() {
    constexpr int kDefaultThreads = 2;
    constexpr int kDefaultConcurrency = 256;
    constexpr int kDefaultPools = 1024;
    constexpr int kDefaultMaxIdlePerPeer = 128;
    constexpr int kDefaultMaxActivePerPeer = 128;
    constexpr int kDefaultTimeoutSeconds = 60;
    constexpr uint64_t kDefaultInflightBytes = 512ull << 20;

    HandshakeLimits limits;
    limits.threads = ReadInt("MC_HANDSHAKE_RPC_THREADS", kDefaultThreads, 1, 8);
    limits.max_concurrency =
        ReadInt("MC_HANDSHAKE_MAX_CONCURRENCY", kDefaultConcurrency, 1, 1024);
    limits.max_pools =
        ReadInt("MC_HANDSHAKE_MAX_POOLS", kDefaultPools, 1, 1000000);
    limits.max_idle_per_peer = ReadInt("MC_HANDSHAKE_MAX_IDLE_PER_PEER",
                                       kDefaultMaxIdlePerPeer, 0, 1024);
    limits.max_active_per_peer = ReadInt("MC_HANDSHAKE_MAX_ACTIVE_PER_PEER",
                                         kDefaultMaxActivePerPeer, 1, 1024);
    limits.timeout_seconds = ReadInt("MC_HANDSHAKE_RPC_TIMEOUT_SECONDS",
                                     kDefaultTimeoutSeconds, 1, 3600);
    limits.idle_ttl_seconds = ReadInt("MC_HANDSHAKE_IDLE_TTL_SECONDS",
                                      kDefaultTimeoutSeconds, 1, 3600);
    limits.max_frame = getHandshakeMaxLength();
    const uint64_t configured = ReadInflightBytes(kDefaultInflightBytes);
    const uint64_t floor = limits.max_frame + kHandshakeEnvelopeBytes;
    limits.max_bytes = configured < floor ? floor : configured;
    limits.ipv6 = globalConfig().use_ipv6;
    limits.connect_timeout_seconds = globalConfig().handshake_connect_timeout;
    if (limits.connect_timeout_seconds <= 0) limits.connect_timeout_seconds = 5;
    return limits;
}

}  // namespace mooncake
