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

#include "metadata/handshake.h"

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <ifaddrs.h>
#include <jsoncpp/json/value.h>
#include <net/if.h>
#include <netdb.h>
#include <sys/socket.h>

#include <cassert>
#include <random>
#include <set>

#include "common/common.h"
#include "common/config.h"
#include "common/error.h"

namespace mooncake {

static inline const std::string getNetworkAddress(struct sockaddr *addr) {
    if (addr->sa_family == AF_INET) {
        struct sockaddr_in *sock_addr = (struct sockaddr_in *)addr;
        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin_addr), ip,
                      INET_ADDRSTRLEN) != NULL)
            return std::string(ip) + ":" +
                   std::to_string(ntohs(sock_addr->sin_port));
    } else if (addr->sa_family == AF_INET6) {
        struct sockaddr_in6 *sock_addr = (struct sockaddr_in6 *)addr;
        char ip[INET6_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin6_addr), ip,
                      INET6_ADDRSTRLEN) != NULL)
            return std::string(ip) + ":" +
                   std::to_string(ntohs(sock_addr->sin6_port));
    }
    PLOG(ERROR) << "Failed to parse socket address";
    return "";
}

struct SocketHandShakePlugin : public HandShakePlugin {
    SocketHandShakePlugin() : listener_running_(false), listen_fd_(-1) {}

    void closeListen() {
        if (listen_fd_ >= 0) {
            // LOG(INFO) << "SocketHandShakePlugin: closing listen socket";
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    virtual ~SocketHandShakePlugin() {
        closeListen();
        if (listener_running_) {
            listener_running_ = false;
            listener_.join();
        }
    }

    virtual void registerOnConnectionCallBack(OnReceiveCallBack callback) {
        on_connection_callback_ = callback;
    }

    virtual void registerOnMetadataCallBack(OnReceiveCallBack callback) {
        on_metadata_callback_ = callback;
    }

    virtual int startDaemon(uint16_t listen_port, int sockfd) {
        if (listener_running_) {
            // LOG(INFO) << "SocketHandShakePlugin: listener already running";
            return 0;
        }

        sockaddr_in bind_address;
        int on = 1;
        memset(&bind_address, 0, sizeof(sockaddr_in));
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(listen_port);
        bind_address.sin_addr.s_addr = INADDR_ANY;

        if (sockfd >= 0) {
            listen_fd_ = sockfd;
        } else {
            listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
            if (listen_fd_ < 0) {
                PLOG(ERROR) << "SocketHandShakePlugin: socket()";
                return ERR_SOCKET;
            }

            struct timeval timeout;
            timeout.tv_sec = 1;
            timeout.tv_usec = 0;
            if (setsockopt(listen_fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                           sizeof(timeout))) {
                PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_RCVTIMEO)";
                closeListen();
                return ERR_SOCKET;
            }

            if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on,
                           sizeof(on))) {
                PLOG(ERROR)
                    << "SocketHandShakePlugin: setsockopt(SO_REUSEADDR)";
                closeListen();
                return ERR_SOCKET;
            }

            if (bind(listen_fd_, (sockaddr *)&bind_address,
                     sizeof(sockaddr_in)) < 0) {
                PLOG(ERROR) << "SocketHandShakePlugin: bind (port "
                            << listen_port << ")";
                closeListen();
                return ERR_SOCKET;
            }
        }

        if (listen(listen_fd_, 5)) {
            PLOG(ERROR) << "SocketHandShakePlugin: listen()";
            closeListen();
            return ERR_SOCKET;
        }

        listener_running_ = true;
        listener_ = std::thread([this]() {
            while (listener_running_) {
                sockaddr_in addr;
                socklen_t addr_len = sizeof(sockaddr_in);
                int conn_fd = accept(listen_fd_, (sockaddr *)&addr, &addr_len);
                if (conn_fd < 0) {
                    if (errno != EWOULDBLOCK)
                        PLOG(ERROR) << "SocketHandShakePlugin: accept()";
                    continue;
                }

                if (addr.sin_family != AF_INET && addr.sin_family != AF_INET6) {
                    LOG(ERROR) << "SocketHandShakePlugin: unsupported socket "
                                  "type, should be AF_INET or AF_INET6";
                    close(conn_fd);
                    continue;
                }

                struct timeval timeout;
                timeout.tv_sec = 60;
                timeout.tv_usec = 0;
                if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                               sizeof(timeout))) {
                    PLOG(ERROR)
                        << "SocketHandShakePlugin: setsockopt(SO_RCVTIMEO)";
                    close(conn_fd);
                    continue;
                }

                auto peer_hostname =
                    getNetworkAddress((struct sockaddr *)&addr);

                Json::Value local, peer;
                Json::Reader reader;

                auto [type, json_str] = readString(conn_fd);
                if (!reader.parse(json_str, peer)) {
                    LOG(ERROR) << "SocketHandShakePlugin: failed to receive "
                                  "handshake message, "
                                  "malformed json format:"
                               << reader.getFormattedErrorMessages()
                               << ", json string length: " << json_str.size()
                               << ", json string content: " << json_str;
                    close(conn_fd);
                    continue;
                }

                // old protocol equals Connection type
                if (type == HandShakeRequestType::Connection ||
                    type == HandShakeRequestType::OldProtocol) {
                    on_connection_callback_(peer, local);
                } else if (type == HandShakeRequestType::Metadata) {
                    on_metadata_callback_(peer, local);
                } else {
                    LOG(ERROR) << "SocketHandShakePlugin: unexpected handshake "
                                  "message type";
                    close(conn_fd);
                    continue;
                }

                int ret =
                    writeString(conn_fd, type, Json::FastWriter{}.write(local));
                if (ret) {
                    LOG(ERROR) << "SocketHandShakePlugin: failed to send "
                                  "message: "
                                  "malformed json format, check tcp connection";
                    close(conn_fd);
                    continue;
                }

                ret = shutdown(conn_fd, SHUT_WR);
                if (ret) {
                    PLOG(ERROR) << "SocketHandShakePlugin: shutdown() failed, "
                                   "connection may be incomplete";
                    close(conn_fd);
                    continue;
                }

                // Wait for the client to close the connection
                char byte;
                ssize_t rc = read(conn_fd, &byte, sizeof(byte));
                if (rc > 0) {
                    LOG(ERROR) << "Unexpected socket read result: " << rc
                               << ", byte: " << int(byte);
                } else if (rc < 0) {
                    PLOG(ERROR)
                        << "Socket read failed while waiting client to close";
                }
                // else rc == 0, client close the connection, safe to close.

                close(conn_fd);
            }
            return;
        });

        return 0;
    }

    virtual int send(std::string ip_or_host_name, uint16_t rpc_port,
                     const Json::Value &local, Json::Value &peer) {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        char service[16];
        sprintf(service, "%u", rpc_port);
        if (getaddrinfo(ip_or_host_name.c_str(), service, &hints, &result)) {
            PLOG(ERROR)
                << "SocketHandShakePlugin: failed to get IP address of peer "
                   "server "
                << ip_or_host_name << ":" << rpc_port
                << ", check DNS and /etc/hosts, or use IPv4 address instead";
            return ERR_DNS;
        }

        int ret = 0;
        for (rp = result; rp; rp = rp->ai_next) {
            ret = doSend(rp, local, peer);
            if (ret == 0) {
                freeaddrinfo(result);
                return 0;
            }
            if (ret == ERR_MALFORMED_JSON) {
                return ret;
            }
        }

        freeaddrinfo(result);
        return ret;
    }

    int doConnect(struct addrinfo *addr, int &conn_fd) {
        int on = 1;
        conn_fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (conn_fd == -1) {
            PLOG(ERROR) << "SocketHandShakePlugin: socket()";
            return ERR_SOCKET;
        }
        if (setsockopt(conn_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_REUSEADDR)";
            close(conn_fd);
            return ERR_SOCKET;
        }

        struct timeval timeout;
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_RCVTIMEO)";
            close(conn_fd);
            return ERR_SOCKET;
        }

        if (connect(conn_fd, addr->ai_addr, addr->ai_addrlen)) {
            PLOG(ERROR) << "SocketHandShakePlugin: connect()"
                        << getNetworkAddress(addr->ai_addr);
            close(conn_fd);
            return ERR_SOCKET;
        }

        return 0;
    }

    int doSend(struct addrinfo *addr, const Json::Value &local,
               Json::Value &peer) {
        int conn_fd = -1;
        int ret = doConnect(addr, conn_fd);
        if (ret) {
            return ret;
        }

        ret = writeString(conn_fd, HandShakeRequestType::Connection,
                          Json::FastWriter{}.write(local));
        if (ret) {
            LOG(ERROR)
                << "SocketHandShakePlugin: failed to send handshake message: "
                   "malformed json format, check tcp connection";
            close(conn_fd);
            return ret;
        }

        Json::Reader reader;
        auto [type, json_str] = readString(conn_fd);
        if (type != HandShakeRequestType::Connection) {
            LOG(ERROR)
                << "SocketHandShakePlugin: unexpected handshake message type";
            close(conn_fd);
            return ERR_SOCKET;
        }

        if (!reader.parse(json_str, peer)) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to receive handshake "
                          "message: "
                          "malformed json format, check tcp connection";
            close(conn_fd);
            return ERR_MALFORMED_JSON;
        }

        close(conn_fd);
        return 0;
    }

    virtual int exchangeMetadata(std::string ip_or_host_name, uint16_t rpc_port,
                                 const Json::Value &local_metadata,
                                 Json::Value &peer_metadata) {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        char service[16];
        sprintf(service, "%u", rpc_port);
        if (getaddrinfo(ip_or_host_name.c_str(), service, &hints, &result)) {
            PLOG(ERROR)
                << "SocketHandShakePlugin: failed to get IP address of peer "
                   "server "
                << ip_or_host_name << ":" << rpc_port
                << ", check DNS and /etc/hosts, or use IPv4 address instead";
            return ERR_DNS;
        }

        int ret = 0;
        for (rp = result; rp; rp = rp->ai_next) {
            ret = doSendMetadata(rp, local_metadata, peer_metadata);
            if (ret == 0) {
                freeaddrinfo(result);
                return 0;
            }
            if (ret == ERR_MALFORMED_JSON) {
                return ret;
            }
        }

        freeaddrinfo(result);
        return ret;
    }

    int doSendMetadata(struct addrinfo *addr, const Json::Value &local_metadata,
                       Json::Value &peer_metadata) {
        int conn_fd = -1;
        int ret = doConnect(addr, conn_fd);
        if (ret) {
            return ret;
        }

        ret = writeString(conn_fd, HandShakeRequestType::Metadata,
                          Json::FastWriter{}.write(local_metadata));
        if (ret) {
            LOG(ERROR)
                << "SocketHandShakePlugin: failed to send metadata message: "
                   "malformed json format, check tcp connection";
            close(conn_fd);
            return ret;
        }

        Json::Reader reader;
        auto [type, json_str] = readString(conn_fd);
        if (type != HandShakeRequestType::Metadata) {
            LOG(ERROR)
                << "SocketHandShakePlugin: unexpected handshake message type";
            close(conn_fd);
            return ERR_SOCKET;
        }

        // LOG(INFO) << "SocketHandShakePlugin: received metadata message: "
        //           << json_str;

        if (!reader.parse(json_str, peer_metadata)) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to receive metadata "
                          "message, malformed json format: "
                       << reader.getFormattedErrorMessages();
            close(conn_fd);
            return ERR_MALFORMED_JSON;
        }

        close(conn_fd);
        return 0;
    }

    std::atomic<bool> listener_running_;
    std::thread listener_;
    int listen_fd_;

    OnReceiveCallBack on_connection_callback_;
    OnReceiveCallBack on_metadata_callback_;
};

std::shared_ptr<HandShakePlugin> HandShakePlugin::Create(
    const std::string &conn_string) {
    return std::make_shared<SocketHandShakePlugin>();
}

std::vector<std::string> findLocalIpAddresses() {
    std::vector<std::string> ips;
    struct ifaddrs *ifaddr, *ifa;

    if (getifaddrs(&ifaddr) == -1) {
        PLOG(ERROR) << "getifaddrs failed";
        return ips;
    }

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }

        if (ifa->ifa_addr->sa_family == AF_INET) {
            if (strcmp(ifa->ifa_name, "lo") == 0) {
                continue;
            }

            char host[NI_MAXHOST];
            if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host,
                            NI_MAXHOST, nullptr, 0, NI_NUMERICHOST) == 0) {
                ips.push_back(host);
            }
        }
    }

    freeifaddrs(ifaddr);
    return ips;
}

uint16_t findAvailableTcpPort(int &sockfd) {
    static std::random_device rand_gen;
    std::uniform_int_distribution rand_dist;
    const int min_port = 15000;
    const int max_port = 17000;
    const int max_attempts = 500;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        int port = min_port + rand_dist(rand_gen) % (max_port - min_port + 1);
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            continue;
        }

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            close(sockfd);
            sockfd = -1;
            continue;
        }

        int on = 1;
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            close(sockfd);
            sockfd = -1;
            continue;
        }

        sockaddr_in bind_address;
        memset(&bind_address, 0, sizeof(sockaddr_in));
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(port);
        bind_address.sin_addr.s_addr = INADDR_ANY;
        if (bind(sockfd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) < 0) {
            close(sockfd);
            sockfd = -1;
            continue;
        }

        return port;
    }
    return 0;
}

}  // namespace mooncake