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

#ifdef USE_HTTP
#include <curl/curl.h>
#endif

#ifdef USE_ETCD
#include <etcd/SyncClient.hpp>
#endif  // USE_ETCD

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
            LOG(ERROR) << "RedisStoragePlugin: unable to connect "
                       << metadata_uri_ << ": " << client_->errstr;
            client_ = nullptr;
        }
    }

    virtual ~RedisStoragePlugin() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        redisReply *resp =
            (redisReply *)redisCommand(client_, "GET %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "RedisStoragePlugin: unable to get " << key
                       << " from " << metadata_uri_;
            return false;
        }
        auto json_file = std::string(resp->str);
        freeReplyObject(resp);
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "RedisStoragePlugin: get: key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "RedisStoragePlugin: set: key=" << key
                      << ", value=" << json_file;
        redisReply *resp = (redisReply *)redisCommand(
            client_, "SET %s %s", key.c_str(), json_file.c_str());
        if (!resp) {
            LOG(ERROR) << "RedisStoragePlugin: unable to put " << key
                       << " from " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    virtual bool remove(const std::string &key) {
        redisReply *resp =
            (redisReply *)redisCommand(client_, "DEL %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "RedisStoragePlugin: unable to remove " << key
                       << " from " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    redisContext *client_;
    const std::string metadata_uri_;
};
#endif  // USE_REDIS

#ifdef USE_HTTP
struct HTTPStoragePlugin : public MetadataStoragePlugin {
    HTTPStoragePlugin(const std::string &metadata_uri)
        : client_(nullptr), metadata_uri_(metadata_uri) {
        curl_global_init(CURL_GLOBAL_ALL);
        client_ = curl_easy_init();
        if (!client_) {
            LOG(ERROR) << "Cannot allocate CURL objects";
            exit(EXIT_FAILURE);
        }
    }

    virtual ~HTTPStoragePlugin() {
        curl_easy_cleanup(client_);
        curl_global_cleanup();
    }

    static size_t writeCallback(void *contents, size_t size, size_t nmemb,
                                std::string *userp) {
        userp->append(static_cast<char *>(contents), size * nmemb);
        return size * nmemb;
    }

    std::string encodeUrl(const std::string &key) {
        char *newkey = curl_easy_escape(client_, key.c_str(), key.size());
        std::string encodedKey(newkey);
        std::string url = metadata_uri_ + "?key=" + encodedKey;
        curl_free(newkey);
        return url;
    }

    virtual bool get(const std::string &key, Json::Value &value) {
        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);
        CURLcode res = curl_easy_perform(client_);
        if (res != CURLE_OK) {
            LOG(ERROR) << "Error from http client, GET " << url
                       << " error: " << curl_easy_strerror(res);
            return false;
        }

        // Get the HTTP response code
        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            LOG(ERROR) << "Unexpected code in http response, GET " << url
                       << " response code: " << responseCode
                       << " response body: " << readBuffer;
            return false;
        }

        if (globalConfig().verbose)
            LOG(INFO) << "Get segment desc, key=" << key
                      << ", value=" << readBuffer;

        Json::Reader reader;
        if (!reader.parse(readBuffer, value)) return false;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "Put segment desc, key=" << key
                      << ", value=" << json_file;

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(client_, CURLOPT_POSTFIELDS, json_file.c_str());
        curl_easy_setopt(client_, CURLOPT_POSTFIELDSIZE, json_file.size());
        curl_easy_setopt(client_, CURLOPT_CUSTOMREQUEST, "PUT");

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);

        // set content-type to application/json
        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(client_, CURLOPT_HTTPHEADER, headers);
        CURLcode res = curl_easy_perform(client_);
        curl_slist_free_all(headers);  // Free headers
        if (res != CURLE_OK) {
            LOG(ERROR) << "Error from http client, PUT " << url
                       << " error: " << curl_easy_strerror(res);
            return false;
        }

        // Get the HTTP response code
        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            LOG(ERROR) << "Unexpected code in http response, PUT " << url
                       << " response code: " << responseCode
                       << " response body: " << readBuffer;
            return false;
        }

        return true;
    }

    virtual bool remove(const std::string &key) {
        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        if (globalConfig().verbose)
            LOG(INFO) << "Remove segment desc, key=" << key;

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(client_, CURLOPT_CUSTOMREQUEST, "DELETE");

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);
        CURLcode res = curl_easy_perform(client_);
        if (res != CURLE_OK) {
            LOG(ERROR) << "Error from http client, DELETE " << url
                       << " error: " << curl_easy_strerror(res);
            return false;
        }

        // Get the HTTP response code
        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            LOG(ERROR) << "Unexpected code in http response, DELETE " << url
                       << " response code: " << responseCode
                       << " response body: " << readBuffer;
            return false;
        }
        return true;
    }

    CURL *client_;
    const std::string metadata_uri_;
};
#endif  // USE_HTTP

#ifdef USE_ETCD
struct EtcdStoragePlugin : public MetadataStoragePlugin {
    EtcdStoragePlugin(const std::string &metadata_uri)
        : client_(metadata_uri), metadata_uri_(metadata_uri) {}

    virtual ~EtcdStoragePlugin() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        auto resp = client_.get(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to get " << key << " from "
                       << metadata_uri_ << ": " << resp.error_message();
            return false;
        }
        auto json_file = resp.value().as_string();
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "EtcdStoragePlugin: get: key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "EtcdStoragePlugin: set: key=" << key
                      << ", value=" << json_file;
        auto resp = client_.put(key, json_file);
        if (!resp.is_ok()) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to set " << key << " from "
                       << metadata_uri_ << ": " << resp.error_message();
            return false;
        }
        return true;
    }

    virtual bool remove(const std::string &key) {
        auto resp = client_.rm(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to delete " << key
                       << " from " << metadata_uri_ << ": "
                       << resp.error_message();
            return false;
        }
        return true;
    }

    etcd::SyncClient client_;
    const std::string metadata_uri_;
};
#endif  // USE_ETCD

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
#ifdef USE_ETCD
    if (parsed_conn_string.first == "etcd") {
        return std::make_shared<EtcdStoragePlugin>(parsed_conn_string.second);
    }
#endif  // USE_ETCD

#ifdef USE_REDIS
    if (parsed_conn_string.first == "redis") {
        return std::make_shared<RedisStoragePlugin>(parsed_conn_string.second);
    }
#endif  // USE_REDIS

#ifdef USE_HTTP
    if (parsed_conn_string.first == "http" ||
        parsed_conn_string.first == "https") {
        return std::make_shared<HTTPStoragePlugin>(
            conn_string);  // including prefix
    }
#endif  // USE_HTTP

    LOG(FATAL) << "Unable to find metadata storage plugin "
               << parsed_conn_string.first;
    return nullptr;
}

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
            LOG(INFO) << "SocketHandShakePlugin: closing listen socket";
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

    virtual int startDaemon(OnReceiveCallBack on_recv_callback,
                            uint16_t listen_port) {
        sockaddr_in bind_address;
        int on = 1;
        memset(&bind_address, 0, sizeof(sockaddr_in));
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(listen_port);
        bind_address.sin_addr.s_addr = INADDR_ANY;

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

        if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_REUSEADDR)";
            closeListen();
            return ERR_SOCKET;
        }

        if (bind(listen_fd_, (sockaddr *)&bind_address, sizeof(sockaddr_in)) <
            0) {
            PLOG(ERROR) << "SocketHandShakePlugin: bind (port " << listen_port
                        << ")";
            closeListen();
            return ERR_SOCKET;
        }

        if (listen(listen_fd_, 5)) {
            PLOG(ERROR) << "SocketHandShakePlugin: listen()";
            closeListen();
            return ERR_SOCKET;
        }

        listener_running_ = true;
        listener_ = std::thread([this, on_recv_callback]() {
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
                if (globalConfig().verbose)
                    LOG(INFO) << "SocketHandShakePlugin: new connection: "
                              << peer_hostname.c_str();

                Json::Value local, peer;
                Json::Reader reader;
                if (!reader.parse(readString(conn_fd), peer)) {
                    LOG(ERROR) << "SocketHandShakePlugin: failed to receive "
                                  "handshake message: "
                                  "malformed json format, check tcp connection";
                    close(conn_fd);
                    continue;
                }

                on_recv_callback(peer, local);
                int ret = writeString(conn_fd, Json::FastWriter{}.write(local));
                if (ret) {
                    LOG(ERROR) << "SocketHandShakePlugin: failed to send "
                                  "handshake message: "
                                  "malformed json format, check tcp connection";
                    close(conn_fd);
                    continue;
                }

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

    int doSend(struct addrinfo *addr, const Json::Value &local,
               Json::Value &peer) {
        if (globalConfig().verbose)
            LOG(INFO) << "SocketHandShakePlugin: connecting "
                      << getNetworkAddress(addr->ai_addr);

        int on = 1;
        int conn_fd =
            socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
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

        int ret = writeString(conn_fd, Json::FastWriter{}.write(local));
        if (ret) {
            LOG(ERROR)
                << "SocketHandShakePlugin: failed to send handshake message: "
                   "malformed json format, check tcp connection";
            close(conn_fd);
            return ret;
        }

        Json::Reader reader;
        if (!reader.parse(readString(conn_fd), peer)) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to receive handshake "
                          "message: "
                          "malformed json format, check tcp connection";
            close(conn_fd);
            return ERR_MALFORMED_JSON;
        }

        close(conn_fd);
        return 0;
    }

    std::atomic<bool> listener_running_;
    std::thread listener_;
    int listen_fd_;
};

std::shared_ptr<HandShakePlugin> HandShakePlugin::Create(
    const std::string &conn_string) {
    return std::make_shared<SocketHandShakePlugin>();
}

}  // namespace mooncake