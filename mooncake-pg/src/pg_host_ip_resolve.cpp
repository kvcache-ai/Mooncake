#include "pg_host_ip_resolve.h"

#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <glog/logging.h>

#include "common.h"

namespace mooncake {

std::optional<std::string> LocalIpTowardPeer(const std::string& peer_host) {
    if (IsLoopbackHost(peer_host)) return std::nullopt;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* result = nullptr;
    if (getaddrinfo(peer_host.c_str(), "1", &hints, &result) != 0 ||
        result == nullptr) {
        return std::nullopt;
    }

    std::optional<std::string> local_ip;
    for (addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        const int fd = socket(rp->ai_family, SOCK_DGRAM, 0);
        if (fd < 0) continue;
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) != 0) {
            close(fd);
            continue;
        }
        sockaddr_storage local_addr{};
        socklen_t local_len = sizeof(local_addr);
        if (getsockname(fd, reinterpret_cast<sockaddr*>(&local_addr),
                        &local_len) == 0) {
            char buf[INET6_ADDRSTRLEN] = {};
            if (local_addr.ss_family == AF_INET) {
                auto* sin = reinterpret_cast<sockaddr_in*>(&local_addr);
                if (inet_ntop(AF_INET, &sin->sin_addr, buf, sizeof(buf))) {
                    local_ip = buf;
                }
            } else if (local_addr.ss_family == AF_INET6) {
                auto* sin6 = reinterpret_cast<sockaddr_in6*>(&local_addr);
                if (inet_ntop(AF_INET6, &sin6->sin6_addr, buf, sizeof(buf))) {
                    local_ip = buf;
                }
            }
        }
        close(fd);
        if (local_ip && !IsLoopbackHost(*local_ip)) break;
        local_ip.reset();
    }
    freeaddrinfo(result);
    return local_ip;
}

std::optional<std::string> LocalIpFromHostname() {
    char hostname[256] = {};
    if (gethostname(hostname, sizeof(hostname)) != 0) return std::nullopt;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    if (getaddrinfo(hostname, nullptr, &hints, &result) != 0 ||
        result == nullptr) {
        return std::nullopt;
    }

    std::optional<std::string> local_ip;
    for (addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        char buf[INET6_ADDRSTRLEN] = {};
        const void* addr_ptr = nullptr;
        if (rp->ai_family == AF_INET) {
            addr_ptr = &reinterpret_cast<sockaddr_in*>(rp->ai_addr)->sin_addr;
        } else if (rp->ai_family == AF_INET6) {
            addr_ptr = &reinterpret_cast<sockaddr_in6*>(rp->ai_addr)->sin6_addr;
        }
        if (addr_ptr == nullptr) continue;
        if (!inet_ntop(rp->ai_family, addr_ptr, buf, sizeof(buf))) continue;
        if (IsLoopbackHost(buf)) continue;
        local_ip = buf;
        if (rp->ai_family == AF_INET) break;
    }
    freeaddrinfo(result);
    return local_ip;
}

HostIpResolveResult MaybeResolveRoutableHostIpFromEnv(std::string host_ip) {
    const char* master_addr = std::getenv("MASTER_ADDR");
    std::string master_host;
    if (master_addr && *master_addr) {
        master_host = getHostNameWithoutPort(master_addr);
    }

    HostIpResolveHooks hooks;
    hooks.local_ip_toward_peer = [](const std::string& peer) {
        return LocalIpTowardPeer(peer);
    };
    hooks.local_ip_from_hostname = []() { return LocalIpFromHostname(); };

    auto result = ResolveRoutableHostIp(std::move(host_ip), master_host, hooks);
    if (result.outcome == HostIpResolveOutcome::Resolved) {
        LOG(INFO) << "Mooncake PG: derived host_ip=" << result.host_ip
                  << " (MASTER_ADDR host="
                  << (master_host.empty() ? "<unset>" : master_host) << ")";
    }
    return result;
}

}  // namespace mooncake
