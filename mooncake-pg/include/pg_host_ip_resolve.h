#ifndef MOONCAKE_PG_HOST_IP_RESOLVE_H
#define MOONCAKE_PG_HOST_IP_RESOLVE_H

#include <arpa/inet.h>
#include <cstring>
#include <functional>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace mooncake {
namespace detail {

inline bool IsLoopbackIpv4(const in_addr& addr) {
    // 127.0.0.0/8 (covers Debian/Ubuntu 127.0.1.1 hostname mappings).
    return (ntohl(addr.s_addr) >> 24) == 127;
}

inline bool IsLoopbackIpv6(const in6_addr& addr) {
    static const in6_addr kLoopback = IN6ADDR_LOOPBACK_INIT;
    if (std::memcmp(&addr, &kLoopback, sizeof(addr)) == 0) return true;
    // IPv4-mapped ::ffff:127.x.x.x
    if (IN6_IS_ADDR_V4MAPPED(&addr)) {
        in_addr v4{};
        std::memcpy(&v4, &addr.s6_addr[12], sizeof(v4));
        return IsLoopbackIpv4(v4);
    }
    return false;
}

}  // namespace detail

inline bool IsLoopbackHost(std::string_view host) {
    if (host.empty() || host == "localhost") return true;

    const std::string host_str(host);
    in_addr v4{};
    if (inet_pton(AF_INET, host_str.c_str(), &v4) == 1) {
        return detail::IsLoopbackIpv4(v4);
    }
    in6_addr v6{};
    if (inet_pton(AF_INET6, host_str.c_str(), &v6) == 1) {
        return detail::IsLoopbackIpv6(v6);
    }
    // Non-IP hostnames are not treated as loopback.
    return false;
}

struct HostIpResolveHooks {
    // Optional: return a routable local address toward peer_host.
    std::function<std::optional<std::string>(const std::string& peer_host)>
        local_ip_toward_peer;
    // Optional: return a routable address derived from the local hostname.
    std::function<std::optional<std::string>()> local_ip_from_hostname;
};

enum class HostIpResolveOutcome {
    Unchanged,
    Resolved,
    FailFast,
};

struct HostIpResolveResult {
    HostIpResolveOutcome outcome = HostIpResolveOutcome::Unchanged;
    std::string host_ip;
    std::string error_message;
};

// Resolve a routable host_ip when the current value is still loopback.
//
// Hostname fallback and fail-fast only run when master_host is a confirmed
// multi-node signal (non-empty and non-loopback). Single-node torchrun with
// MASTER_ADDR=127.0.0.1 / unset keeps the loopback default.
// Discovery hooks that return loopback (e.g. 127.0.1.1) are ignored so the
// next fallback / fail-fast can run.
inline HostIpResolveResult ResolveRoutableHostIp(
    std::string host_ip, std::string_view master_host,
    const HostIpResolveHooks& hooks) {
    HostIpResolveResult result;
    result.host_ip = std::move(host_ip);
    if (!IsLoopbackHost(result.host_ip)) {
        result.outcome = HostIpResolveOutcome::Unchanged;
        return result;
    }

    const bool multi_node =
        !master_host.empty() && !IsLoopbackHost(master_host);
    if (!multi_node) {
        // Preserve the documented single-node default (127.0.0.1).
        result.outcome = HostIpResolveOutcome::Unchanged;
        return result;
    }

    if (hooks.local_ip_toward_peer) {
        if (auto ip = hooks.local_ip_toward_peer(std::string(master_host))) {
            if (!IsLoopbackHost(*ip)) {
                result.host_ip = std::move(*ip);
                result.outcome = HostIpResolveOutcome::Resolved;
                return result;
            }
        }
    }

    if (hooks.local_ip_from_hostname) {
        if (auto ip = hooks.local_ip_from_hostname()) {
            if (!IsLoopbackHost(*ip)) {
                result.host_ip = std::move(*ip);
                result.outcome = HostIpResolveOutcome::Resolved;
                return result;
            }
        }
    }

    result.outcome = HostIpResolveOutcome::FailFast;
    result.error_message =
        "Mooncake PG host_ip is still loopback while MASTER_ADDR=" +
        std::string(master_host) +
        " looks multi-node. Call mooncake.pg.set_host_ip(<routable "
        "IP>) before init_process_group(), or pass a TransferEngine "
        "via set_transfer_engine().";
    return result;
}

// Production helpers implemented in pg_host_ip_resolve.cpp.
std::optional<std::string> LocalIpTowardPeer(const std::string& peer_host);
std::optional<std::string> LocalIpFromHostname();

// Reads MASTER_ADDR and applies ResolveRoutableHostIp with real socket hooks.
HostIpResolveResult MaybeResolveRoutableHostIpFromEnv(std::string host_ip);

}  // namespace mooncake

#endif  // MOONCAKE_PG_HOST_IP_RESOLVE_H
