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

#include "tent/common/utils/ip.h"
#include <glog/logging.h>

namespace mooncake {
namespace tent {

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

std::pair<std::string, uint16_t> parseHostNameWithPort(const std::string &url,
                                                       uint16_t default_port) {
    uint16_t port = default_port;

    // Check if url is valid IPv6 address
    in6_addr addr;
    if (inet_pton(AF_INET6, url.c_str(), &addr) == 1) return {url, port};

    // Check if url is valid IPv6 address with port
    size_t start_pos = 0;
    size_t end_pos = url.find_last_of(']');
    size_t port_pos = url.find_last_of(':');
    if (url.front() == '[' && end_pos != std::string::npos &&
        port_pos > end_pos) {
        auto ip = url.substr(start_pos + 1, end_pos - start_pos - 1);
        std::string port_str = url.substr(port_pos + 1);
        int val = std::atoi(port_str.c_str());
        if (val <= 0 || val > 65535) {
            LOG(WARNING) << "Illegal port number in " << url
                         << ". Use default port " << port << " instead";
        } else {
            port = static_cast<uint16_t>(val);
        }
        return std::make_pair(port_str, port);
    }

    // Check if url has port field
    auto pos = url.find(':');
    if (pos == url.npos) return std::make_pair(url, port);
    auto trimmed_server_name = url.substr(0, pos);
    auto port_str = url.substr(pos + 1);
    int val = std::atoi(port_str.c_str());
    if (val <= 0 || val > 65535)
        LOG(WARNING) << "Illegal port number in " << url
                     << ". Use default port " << port << " instead";
    else
        port = (uint16_t)val;

    return std::make_pair(trimmed_server_name, port);
}

std::string buildIpAddrWithPort(const std::string &hostname, uint16_t port,
                                bool ipv6) {
    if (ipv6)
        return std::string("[") + hostname + "]:" + std::to_string(port);
    else
        return hostname + ":" + std::to_string(port);
}
}  // namespace tent
}  // namespace mooncake