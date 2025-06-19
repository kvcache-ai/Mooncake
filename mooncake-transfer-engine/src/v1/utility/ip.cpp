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

#include "v1/utility/ip.h"

namespace mooncake {
namespace v1 {

Status discoverLocalIpAddress(std::string &hostname, bool &ipv6) {
    struct ifaddrs *ifaddr, *ifa;
    if (getifaddrs(&ifaddr))
        return Status::InternalError("Failed to call getifaddrs");

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) continue;
        if (ifa->ifa_addr->sa_family == AF_INET) {
            if (strcmp(ifa->ifa_name, "lo") == 0) continue;
            char host[NI_MAXHOST];
            if (!getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host,
                             NI_MAXHOST, nullptr, 0, NI_NUMERICHOST)) {
                hostname = host;
                ipv6 = false;
                freeifaddrs(ifaddr);
                return Status::OK();
            }
        }
    }

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) continue;
        if (ifa->ifa_addr->sa_family == AF_INET6) {
            if (strcmp(ifa->ifa_name, "lo") == 0) continue;
            char host[NI_MAXHOST];
            if (!getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in6), host,
                             NI_MAXHOST, nullptr, 0, NI_NUMERICHOST)) {
                hostname = host;
                ipv6 = true;
                freeifaddrs(ifaddr);
                return Status::OK();
            }
        }
    }
    freeifaddrs(ifaddr);
    return Status::InvalidArgument("No available IP address");
}

Status checkLocalIpAddress(std::string &hostname, bool &ipv6) {
    addrinfo hints, *res, *p;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int status = getaddrinfo(hostname.c_str(), NULL, &hints, &res);
    if (status)
        return Status::InternalError(std::string("DNS resolution failed") +
                                     gai_strerror(status));
    char ipstr[INET6_ADDRSTRLEN];
    for (p = res; p != nullptr; p = p->ai_next) {
        if (p->ai_family != AF_INET) continue;
        void *addr = &(((sockaddr_in *)p->ai_addr)->sin_addr);
        inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
        hostname = ipstr;
        ipv6 = false;
        freeaddrinfo(res);
        return Status::OK();
    }
    for (p = res; p != nullptr; p = p->ai_next) {
        if (p->ai_family != AF_INET6) continue;
        void *addr = &(((sockaddr_in6 *)p->ai_addr)->sin6_addr);
        inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
        hostname = ipstr;
        ipv6 = true;
        freeaddrinfo(res);
        return Status::OK();
    }
    freeaddrinfo(res);
    return Status::InvalidArgument("No available IP address");
}

std::string buildIpAddrWithPort(const std::string &hostname, uint16_t port,
                                bool ipv6) {
    if (ipv6)
        return std::string("[") + hostname + "]:" + std::to_string(port);
    else
        return hostname + ":" + std::to_string(port);
}
}  // namespace v1
}  // namespace mooncake