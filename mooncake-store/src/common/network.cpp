#include "common/network.h"

#include <cstring>
#include <netinet/in.h>
#include <random>
#include <sys/socket.h>
#include <unistd.h>

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

    bool available = (bind(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0);
    close(sock);
    return available;
}

// AutoPortBinder implementation
AutoPortBinder::AutoPortBinder(int min_port, int max_port)
    : socket_fd_(-1), port_(-1), min_port_(min_port), max_port_(max_port) {
    tryBindPort();
}

int AutoPortBinder::tryBindPort() {
    static std::random_device rand_gen;
    std::mt19937 gen(rand_gen());
    std::uniform_int_distribution<> rand_dist(min_port_, max_port_);

    for (int attempt = 0; attempt < 20; ++attempt) {
        int port = rand_dist(gen);

        socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ < 0) continue;

        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (bind(socket_fd_, (sockaddr*)&addr, sizeof(addr)) == 0) {
            port_ = port;
            return port;
        } else {
            close(socket_fd_);
            socket_fd_ = -1;
        }
    }
    return -1;
}

int AutoPortBinder::rebind() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
    port_ = -1;
    return tryBindPort();
}

AutoPortBinder::~AutoPortBinder() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
    }
}

tl::expected<std::string, int> httpGet(const std::string& url) {
    coro_http::coro_http_client client;
    auto res = client.get(url);
    if (res.status == 200) {
        return std::string(res.resp_body);
    }
    return tl::unexpected(res.status);
}

int getFreeTcpPort() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);
    if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(sock);
        return -1;
    }
    int port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

}  // namespace mooncake
