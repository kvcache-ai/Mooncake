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

#ifndef MOONCAKE_HANDSHAKE_RPC_H
#define MOONCAKE_HANDSHAKE_RPC_H

#include <memory>
#include <string>

namespace mooncake {

struct HandShakePlugin;

// MC_HANDSHAKE_TRANSPORT=coro. Never returns null.
std::shared_ptr<HandShakePlugin> CreateCoroHandshakePlugin();

// MC_HANDSHAKE_TRANSPORT set to anything other than "socket" or "coro".
// startDaemon and every send* return ERR_INVALID_ARGUMENT. Never returns null.
std::shared_ptr<HandShakePlugin> CreateInvalidHandshakePlugin(
    const std::string &configured_value);

}  // namespace mooncake

#endif  // MOONCAKE_HANDSHAKE_RPC_H
