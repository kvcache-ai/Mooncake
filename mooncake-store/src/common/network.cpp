#include "common/network.h"

#include "common.h"
#include "ascii_string.h"
#include "random.h"

#include <cerrno>
#include <cstring>
#include <ifaddrs.h>
#include <memory>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake {

bool isPortAvailable(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    bool available = (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    close(sock);
    return available;
}

// AutoPortBinder implementation
AutoPortBinder::AutoPortBinder(int min_port, int max_port)
    : socket_fd_(-1), port_(-1) {
    for (int attempt = 0; attempt < 20; ++attempt) {
        int port = randomUniform(min_port, max_port);

        socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ < 0) continue;

        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (bind(socket_fd_, (sockaddr *)&addr, sizeof(addr)) == 0) {
            port_ = port;
            break;
        } else {
            close(socket_fd_);
            socket_fd_ = -1;
        }
    }
}

AutoPortBinder::~AutoPortBinder() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
    }
}

tl::expected<std::string, int> httpGet(const std::string &url) {
    coro_http::coro_http_client client;
    auto res = client.get(url);
    if (res.status == 200) {
        return std::string(res.resp_body);
    }
    return tl::unexpected(res.status);
}

tl::expected<std::string, std::string> GetInterfaceIPv4Address(
    const std::string &interface_name) {
    if (interface_name.empty()) {
        return tl::unexpected(std::string("network interface name is empty"));
    }

    struct ifaddrs *interfaces = nullptr;
    if (getifaddrs(&interfaces) != 0) {
        return tl::unexpected("getifaddrs failed: " +
                              std::string(strerror(errno)));
    }
    std::unique_ptr<struct ifaddrs, decltype(&freeifaddrs)> interface_guard(
        interfaces, freeifaddrs);

    bool found_interface = false;
    bool interface_is_up = false;
    for (auto *current = interfaces; current != nullptr;
         current = current->ifa_next) {
        if (current->ifa_name == nullptr ||
            interface_name != current->ifa_name) {
            continue;
        }

        found_interface = true;
        if ((current->ifa_flags & IFF_UP) == 0) {
            continue;
        }
        interface_is_up = true;

        if (current->ifa_addr == nullptr ||
            current->ifa_addr->sa_family != AF_INET) {
            continue;
        }

        char host[NI_MAXHOST] = {};
        auto *ipv4_addr = reinterpret_cast<sockaddr_in *>(current->ifa_addr);
        if (getnameinfo(reinterpret_cast<sockaddr *>(ipv4_addr),
                        sizeof(*ipv4_addr), host, sizeof(host), nullptr, 0,
                        NI_NUMERICHOST) == 0) {
            return std::string(host);
        }
    }

    if (!found_interface) {
        return tl::unexpected("network interface '" + interface_name +
                              "' was not found");
    }
    if (!interface_is_up) {
        return tl::unexpected("network interface '" + interface_name +
                              "' is down");
    }
    return tl::unexpected("network interface '" + interface_name +
                          "' has no IPv4 address");
}

int getFreeTcpPort() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);
    if (::bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
        ::close(sock);
        return -1;
    }
    int port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

std::vector<int> getFreeTcpPorts(int count) {
    std::vector<int> ports;
    std::vector<int> sockets;
    ports.reserve(count);
    sockets.reserve(count);

    for (int i = 0; i < count; ++i) {
        int sock = ::socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) break;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(0);
        if (::bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) !=
            0) {
            ::close(sock);
            break;
        }
        socklen_t len = sizeof(addr);
        if (::getsockname(sock, reinterpret_cast<sockaddr *>(&addr), &len) !=
            0) {
            ::close(sock);
            break;
        }
        ports.push_back(ntohs(addr.sin_port));
        sockets.push_back(sock);
    }

    for (int sock : sockets) {
        ::close(sock);
    }
    return ports;
}

std::string ResolveMooncakeHostId(const std::string &local_hostname) {
    const std::string hostname(TrimAsciiWhitespace(local_hostname));
    const std::string host_id = (hostname == "::1" || hostname == "::")
                                    ? hostname
                                    : std::string(TrimAsciiWhitespace(
                                          getHostNameWithoutPort(hostname)));
    if (host_id.empty()) {
        return "";
    }

    if (AsciiCaseInsensitiveEquals(host_id, "localhost") ||
        host_id == "127.0.0.1" || host_id == "0.0.0.0" || host_id == "::1" ||
        host_id == "[::1]" || host_id == "::" || host_id == "[::]") {
        return "";
    }
    return host_id;
}

}  // namespace mooncake
