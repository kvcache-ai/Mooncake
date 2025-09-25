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

#ifndef IP_UTIL_H
#define IP_UTIL_H

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <ifaddrs.h>
#include <jsoncpp/json/value.h>
#include <net/if.h>
#include <netdb.h>
#include <sys/socket.h>

#include <string>

#include "v1/common/status.h"

namespace mooncake {
namespace v1 {

Status discoverLocalIpAddress(std::string &hostname, bool &ipv6);

Status checkLocalIpAddress(std::string &hostname, bool &ipv6);

std::string buildIpAddrWithPort(const std::string &hostname, uint16_t port,
                                bool ipv6);

std::pair<std::string, uint16_t> parseHostNameWithPort(const std::string &url,
                                                       uint16_t default_port);

}  // namespace v1
}  // namespace mooncake
#endif  // IP_UTIL_H