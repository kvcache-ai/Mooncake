// Copyright 2025 KVCache.AI
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

#ifndef TENT_NOTIFICATION_CHANNEL_H
#define TENT_NOTIFICATION_CHANNEL_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace mooncake {
namespace tent {

// Forward declarations to avoid including RDMA headers
class RdmaContext;
class RdmaTransport;

class NotificationChannel {
   public:
    NotificationChannel(RdmaTransport& transport, RdmaContext& context);
    ~NotificationChannel();

    // Disable copy and move
    NotificationChannel(const NotificationChannel&) = delete;
    NotificationChannel& operator=(const NotificationChannel&) = delete;
    NotificationChannel(NotificationChannel&&) = delete;
    NotificationChannel& operator=(NotificationChannel&&) = delete;

    // Connect to peer's notification QP
    bool connect(uint32_t peer_qp_num, uint16_t peer_lid,
                 const std::vector<uint8_t>& peer_gid);

    // Send notification message
    bool send(const std::string& name, const std::string& msg);

    // Receive notification message (non-blocking)
    bool receive(std::string& name, std::string& msg);

    // Process completions from CQ
    int processCompletions();

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_NOTIFICATION_CHANNEL_H
