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

#ifndef MOONCAKE_GRACEFUL_SHUTDOWN_H_
#define MOONCAKE_GRACEFUL_SHUTDOWN_H_

#include <memory>

namespace mooncake {

class TransferEngineImpl;

class ShutdownToken {
   public:
    virtual ~ShutdownToken() = default;

    virtual void shutdown() = 0;

    virtual void detach() = 0;
};

void registerTokenForShutdown(std::shared_ptr<ShutdownToken> token);

void registerEngineForShutdown(std::shared_ptr<TransferEngineImpl> impl);

void installGracefulShutdownHandlers();

}  // namespace mooncake

#endif  // MOONCAKE_GRACEFUL_SHUTDOWN_H_
