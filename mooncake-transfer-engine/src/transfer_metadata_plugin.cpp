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

#include "transfer_metadata_plugin.h"

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <jsoncpp/json/value.h>
#include <netdb.h>
#include <sys/socket.h>

#ifdef USE_REDIS
#include <hiredis/hiredis.h>
#endif

#include <cassert>
#include <set>

#include "common.h"
#include "config.h"
#include "error.h"

namespace mooncake {
#ifdef USE_REDIS
struct RedisStoragePlugin : public MetadataStoragePlugin {
    RedisStoragePlugin(const std::string &metadata_uri)
        : client_(nullptr), metadata_uri_(metadata_uri) {
        auto hostname_port = parseHostNameWithPort(metadata_uri);
        client_ =
            redisConnect(hostname_port.first.c_str(), hostname_port.second);
        if (!client_ || client_->err) {
            LOG(ERROR) << "redis error: " << client_->errstr;
        }
    }

    virtual ~RedisStoragePlugin() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        redisReply *resp =
            (redisReply *)redisCommand(client_, "GET %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "Error from redis client uri: " << metadata_uri_;
            return false;
        }
        auto json_file = std::string(resp->str);
        freeReplyObject(resp);
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "Get segment desc, key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "Put segment desc, key=" << key
                      << ", value=" << json_file;

        redisReply *resp = (redisReply *)redisCommand(
            client_, "SET %s %s", key.c_str(), json_file.c_str());
        if (!resp) {
            LOG(ERROR) << "Error from redis client uri: " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    virtual bool remove(const std::string &key) {
        redisReply *resp =
            (redisReply *)redisCommand(client_, "DEL %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "Error from redis client uri: " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    redisContext *client_;
    const std::string metadata_uri_;
};
#endif  // USE_REDIS

struct EtcdStoragePlugin : public MetadataStoragePlugin {
    EtcdStoragePlugin(const std::string &metadata_uri)
        : client_(metadata_uri), metadata_uri_(metadata_uri) {}

    virtual ~EtcdStoragePlugin() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        auto resp = client_.get(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "Error from etcd client, uri: " << metadata_uri_
                       << " error: " << resp.error_code()
                       << " message: " << resp.error_message();
            return false;
        }
        auto json_file = resp.value().as_string();
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "Get segment desc, key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "Put segment desc, key=" << key
                      << ", value=" << json_file;
        auto resp = client_.put(key, json_file);
        if (!resp.is_ok()) {
            LOG(ERROR) << "Error from etcd client, uri: " << metadata_uri_
                       << " error: " << resp.error_code()
                       << " message: " << resp.error_message();
            return false;
        }
        return true;
    }

    virtual bool remove(const std::string &key) {
        auto resp = client_.rm(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "Error from etcd client, uri: " << metadata_uri_
                       << " error: " << resp.error_code()
                       << " message: " << resp.error_message();
            return false;
        }
        return true;
    }

    etcd::SyncClient client_;
    const std::string metadata_uri_;
};

std::pair<std::string, std::string> parseConnectionString(
    const std::string &conn_string) {
    std::pair<std::string, std::string> result;
    std::string proto = "etcd";
    std::string domain;
    std::size_t pos = conn_string.find("://");

    if (pos != std::string::npos) {
        proto = conn_string.substr(0, pos);
        domain = conn_string.substr(pos + 3);
    } else {
        domain = conn_string;
    }

    result.first = proto;
    result.second = domain;
    return result;
}

std::shared_ptr<MetadataStoragePlugin> MetadataStoragePlugin::Create(
    const std::string &conn_string) {
    auto parsed_conn_string = parseConnectionString(conn_string);
    if (parsed_conn_string.first == "etcd") {
        return std::make_shared<EtcdStoragePlugin>(parsed_conn_string.second);
#ifdef USE_REDIS
    } else if (parsed_conn_string.first == "redis") {
        return std::make_shared<RedisStoragePlugin>(parsed_conn_string.second);
        if (!impl_) {
            LOG(ERROR) << "Cannot allocate TransferMetadataImpl objects";
            exit(EXIT_FAILURE);
        }
#endif  // USE_REDIS
    } else {
        LOG(ERROR) << "Unsupported metdata protocol "
                   << parsed_conn_string.first;
        return nullptr;
    }
}

static inline const std::string toString(struct sockaddr *addr) {
    if (addr->sa_family == AF_INET) {
        struct sockaddr_in *sock_addr = (struct sockaddr_in *)addr;
        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin_addr), ip,
                      INET_ADDRSTRLEN) != NULL)
            return ip;
    } else if (addr->sa_family == AF_INET6) {
        struct sockaddr_in6 *sock_addr = (struct sockaddr_in6 *)addr;
        char ip[INET6_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin6_addr), ip,
                      INET6_ADDRSTRLEN) != NULL)
            return ip;
    }
    LOG(ERROR) << "Invalid address, cannot convert to string";
    return "<unknown>";
}

struct SocketHandShakePlugin : public MetadataHandShakePlugin {
    SocketHandShakePlugin() : listener_running_(false) {}

    virtual ~SocketHandShakePlugin() {
        if (listener_running_) {
            listener_running_ = false;
            listener_.join();
        }
    }

    virtual int startDaemon(OnReceiveCallBack on_recv_callback,
                            uint16_t listen_port) {
        sockaddr_in bind_address;
        int on = 1, listen_fd = -1;
        memset(&bind_address, 0, sizeof(sockaddr_in));
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(listen_port);
        bind_address.sin_addr.s_addr = INADDR_ANY;

        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) {
            PLOG(ERROR) << "Failed to create socket";
            return ERR_SOCKET;
        }

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        if (setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            PLOG(ERROR) << "Failed to set socket timeout";
            close(listen_fd);
            return ERR_SOCKET;
        }

        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "Failed to set address reusable";
            close(listen_fd);
            return ERR_SOCKET;
        }

        if (bind(listen_fd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) <
            0) {
            PLOG(ERROR) << "Failed to bind address, rpc port: " << listen_port;
            close(listen_fd);
            return ERR_SOCKET;
        }

        if (listen(listen_fd, 5)) {
            PLOG(ERROR) << "Failed to listen";
            close(listen_fd);
            return ERR_SOCKET;
        }

        listener_running_ = true;
        listener_ = std::thread([this, listen_fd, on_recv_callback]() {
            while (listener_running_) {
                sockaddr_in addr;
                socklen_t addr_len = sizeof(sockaddr_in);
                int conn_fd = accept(listen_fd, (sockaddr *)&addr, &addr_len);
                if (conn_fd < 0) {
                    if (errno != EWOULDBLOCK)
                        PLOG(ERROR) << "Failed to accept socket connection";
                    continue;
                }

                if (addr.sin_family != AF_INET && addr.sin_family != AF_INET6) {
                    LOG(ERROR) << "Unsupported socket type, should be AF_INET "
                                  "or AF_INET6";
                    close(conn_fd);
                    continue;
                }

                struct timeval timeout;
                timeout.tv_sec = 60;
                timeout.tv_usec = 0;
                if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                               sizeof(timeout))) {
                    PLOG(ERROR) << "Failed to set socket timeout";
                    close(conn_fd);
                    continue;
                }

                if (globalConfig().verbose)
                    LOG(INFO) << "New connection: "
                              << toString((struct sockaddr *)&addr) << ":"
                              << ntohs(addr.sin_port);

                Json::Value local, peer;
                Json::Reader reader;
                if (!reader.parse(readString(conn_fd), peer)) {
                    LOG(ERROR) << "Failed to receive handshake message: malformed json "
                                  "format, check tcp connection";
                    close(conn_fd);
                    continue;;
                }

                on_recv_callback(peer, local);
                int ret = writeString(conn_fd, Json::FastWriter{}.write(local));
                if (ret) {
                    PLOG(ERROR)
                        << "Failed to send handshake message: malformed "
                           "json format, check tcp connection";
                    close(conn_fd);
                    continue;
                }

                close(conn_fd);
            }
            return;
        });

        return 0;
    }

    virtual int send(TransferMetadata::RpcMetaDesc peer_location,
                     const Json::Value &local, Json::Value &peer) {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        char service[16];
        sprintf(service, "%u", peer_location.rpc_port);
        if (getaddrinfo(peer_location.ip_or_host_name.c_str(), service, &hints,
                        &result)) {
            PLOG(ERROR)
                << "Failed to get IP address of peer server "
                << peer_location.ip_or_host_name << ":"
                << peer_location.rpc_port
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

    int doSend(struct addrinfo *addr, const Json::Value &local, Json::Value &peer) {
        if (globalConfig().verbose)
            LOG(INFO) << "Try connecting " << toString(addr->ai_addr);

        int on = 1;
        int conn_fd =
            socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (conn_fd == -1) {
            PLOG(ERROR) << "Failed to create socket";
            return ERR_SOCKET;
        }
        if (setsockopt(conn_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "Failed to set address reusable";
            close(conn_fd);
            return ERR_SOCKET;
        }

        struct timeval timeout;
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            PLOG(ERROR) << "Failed to set socket timeout";
            close(conn_fd);
            return ERR_SOCKET;
        }

        if (connect(conn_fd, addr->ai_addr, addr->ai_addrlen)) {
            PLOG(ERROR) << "Failed to connect " << toString(addr->ai_addr)
                        << " via socket";
            close(conn_fd);
            return ERR_SOCKET;
        }

        int ret = writeString(conn_fd, Json::FastWriter{}.write(local));
        if (ret) {
            LOG(ERROR) << "Failed to send handshake message: malformed json "
                          "format, check tcp connection";
            close(conn_fd);
            return ret;
        }

        Json::Reader reader;
        if (!reader.parse(readString(conn_fd), peer)) {
            LOG(ERROR) << "Failed to receive handshake message: malformed json "
                          "format, check tcp connection";
            close(conn_fd);
            return ERR_MALFORMED_JSON;
        }

        close(conn_fd);
        return 0;
    }

    std::atomic<bool> listener_running_;
    std::thread listener_;
};

std::shared_ptr<MetadataHandShakePlugin> MetadataHandShakePlugin::Create(
    const std::string &conn_string) {
    return std::make_shared<SocketHandShakePlugin>();
}

}  // namespace mooncake