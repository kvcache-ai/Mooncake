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
#include <ifaddrs.h>
#include <json/value.h>
#include <net/if.h>
#include <netdb.h>
#include <sys/socket.h>

#include <random>

#ifdef USE_REDIS
#include <hiredis/hiredis.h>

#include <mutex>
#endif

#ifdef USE_HTTP
#include <curl/curl.h>
#endif

#ifdef USE_ETCD
#ifdef USE_ETCD_LEGACY
#include <etcd/SyncClient.hpp>
#else
#include <libetcd_wrapper.h>
#endif
#endif  // USE_ETCD

#include <cassert>
#include <set>

#include "common.h"
#include "config.h"
#include "error.h"

// Helper function to parse JSON string using thread-safe CharReaderBuilder
static bool parseJsonString(const std::string &json_str, Json::Value &value,
                            std::string *error_msg = nullptr) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    bool success = reader->parse(
        json_str.data(), json_str.data() + json_str.size(), &value, &errs);
    if (!success && error_msg) {
        *error_msg = errs;
    }
    return success;
}

namespace mooncake {

bool MetadataStoragePlugin::get(const std::string &key, Json::Value &value,
                                bool silent) {
    std::string error_msg;
    if (tryGet(key, value, &error_msg)) {
        return true;
    }
    if (!silent && !error_msg.empty()) {
        LOG(ERROR) << error_msg;
    }
    return false;
}
#ifdef USE_REDIS
struct RedisStoragePlugin : public MetadataStoragePlugin {
    RedisStoragePlugin(const std::string &metadata_uri)
        : client_(nullptr), metadata_uri_(metadata_uri) {
        auto hostname_port = parseHostNameWithPort(metadata_uri);
        client_ =
            redisConnect(hostname_port.first.c_str(), hostname_port.second);
        if (!client_) {
            LOG(ERROR) << "RedisStoragePlugin: unable to connect "
                       << metadata_uri_;
            return;
        }
        if (client_->err) {
            LOG(ERROR) << "RedisStoragePlugin: unable to connect "
                       << metadata_uri_ << ": " << client_->errstr;
            redisFree(client_);
            client_ = nullptr;
            return;
        }
    }

    RedisStoragePlugin(const std::string &metadata_uri,
                       const std::string &password, const uint8_t &db_index)
        : RedisStoragePlugin(metadata_uri) {
        if (!client_) {
            return;
        }

        if (!password.empty()) {
            auto *reply = static_cast<redisReply *>(
                redisCommand(client_, "AUTH %s", password.c_str()));
            if (!reply || reply->type == REDIS_REPLY_ERROR) {
                LOG(ERROR) << "RedisStoragePlugin: authentication failed for "
                           << metadata_uri_;
                freeReplyObject(reply);
                redisFree(client_);
                client_ = nullptr;
                return;
            }
            freeReplyObject(reply);
        }

        if (db_index != 0) {
            auto *reply = static_cast<redisReply *>(
                redisCommand(client_, "SELECT %d", db_index));
            if (!reply || reply->type == REDIS_REPLY_ERROR) {
                LOG(ERROR) << "RedisStoragePlugin: failed to select database "
                           << (int)db_index << " for " << metadata_uri_;
                freeReplyObject(reply);
                redisFree(client_);
                client_ = nullptr;
                return;
            }
            freeReplyObject(reply);
        }
    }

    virtual ~RedisStoragePlugin() {
        if (client_) {
            redisFree(client_);
            client_ = nullptr;
        }
    }

    virtual bool tryGet(const std::string &key, Json::Value &value,
                        std::string *error_msg = nullptr) override {
        std::lock_guard<std::mutex> lock(access_client_mutex_);
        if (!client_) return false;

        redisReply *resp =
            (redisReply *)redisCommand(client_, "GET %s", key.c_str());
        if (!resp) {
            if (error_msg) {
                *error_msg = "RedisStoragePlugin: unable to get " + key +
                             " from " + metadata_uri_;
            }
            return false;
        }
        if (!resp->str) {
            if (error_msg) {
                *error_msg = "RedisStoragePlugin: unable to get " + key +
                             " from " + metadata_uri_;
            }
            freeReplyObject(resp);
            return false;
        }

        auto json_file = std::string(resp->str);
        freeReplyObject(resp);

        std::string errs;
        if (!parseJsonString(json_file, value, &errs)) {
            if (error_msg) {
                *error_msg = "RedisStoragePlugin: JSON parse error: " + errs;
            }
            return false;
        }
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        std::lock_guard<std::mutex> lock(access_client_mutex_);
        if (!client_) return false;

        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
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
        std::lock_guard<std::mutex> lock(access_client_mutex_);
        if (!client_) return false;

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
    std::mutex access_client_mutex_;
};
#endif  // USE_REDIS

#ifdef USE_HTTP
struct HTTPStoragePlugin : public MetadataStoragePlugin {
    explicit HTTPStoragePlugin(const std::string &metadata_uri)
        : metadata_uri_(metadata_uri) {
        global_init_once();
    }

    ~HTTPStoragePlugin() override = default;

    static void global_init_once() {
        static std::once_flag once;
        std::call_once(once, [] { curl_global_init(CURL_GLOBAL_ALL); });
    }

    struct ThreadLocalCurl {
        CURL *h = nullptr;
        ThreadLocalCurl() {
            h = curl_easy_init();
            if (!h)
                throw std::runtime_error(
                    "HTTPStoragePlugin: curl_easy_init failed; cannot "
                    "initialize HTTP storage plugin functionality.");
            curl_easy_setopt(h, CURLOPT_NOSIGNAL, 1L);
            curl_easy_setopt(h, CURLOPT_TCP_KEEPALIVE, 1L);
            curl_easy_setopt(h, CURLOPT_ACCEPT_ENCODING, "");
        }
        ~ThreadLocalCurl() {
            if (h) curl_easy_cleanup(h);
        }
        ThreadLocalCurl(const ThreadLocalCurl &) = delete;
        ThreadLocalCurl &operator=(const ThreadLocalCurl &) = delete;
    };

    static CURL *tl_easy() {
        thread_local ThreadLocalCurl tls;
        return tls.h;
    }

    static size_t writeCallback(void *contents, size_t size, size_t nmemb,
                                void *userp) {
        auto *out = static_cast<std::string *>(userp);
        out->append(static_cast<const char *>(contents), size * nmemb);
        return size * nmemb;
    }

    std::string encodeUrl(const std::string &key) const {
        CURL *h = tl_easy();
        char *esc =
            curl_easy_escape(h, key.c_str(), static_cast<int>(key.size()));
        std::string url = metadata_uri_ + "?key=" + (esc ? esc : "");
        if (esc) curl_free(esc);
        return url;
    }

    static inline bool is_200(long code) { return code == 200; }

    bool tryGet(const std::string &key, Json::Value &value,
                std::string *error_msg = nullptr) override {
        CURL *h = tl_easy();
        curl_easy_reset(h);

        std::string readBody;
        char errbuf[CURL_ERROR_SIZE] = {0};

        curl_easy_setopt(h, CURLOPT_TIMEOUT_MS, 3000L);
        curl_easy_setopt(h, CURLOPT_CONNECTTIMEOUT_MS, 1500L);

        const std::string url = encodeUrl(key);
        curl_easy_setopt(h, CURLOPT_URL, url.c_str());
        curl_easy_setopt(h, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(h, CURLOPT_WRITEDATA, &readBody);
        curl_easy_setopt(h, CURLOPT_ERRORBUFFER, errbuf);

        CURLcode rc = curl_easy_perform(h);
        if (rc != CURLE_OK) {
            if (error_msg) {
                *error_msg = "GET " + url + " curl: " + curl_easy_strerror(rc) +
                             " err: " + errbuf;
            }
            return false;
        }

        long code = 0;
        curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &code);
        if (!is_200(code)) {
            if (error_msg) {
                *error_msg = "GET " + url + " http=" + std::to_string(code) +
                             " body: " + readBody;
            }
            return false;
        }

        Json::CharReaderBuilder b;
        std::string errs;
        std::unique_ptr<Json::CharReader> r(b.newCharReader());
        if (!r->parse(readBody.data(), readBody.data() + readBody.size(),
                      &value, &errs)) {
            if (error_msg) {
                *error_msg = "GET " + url + " json parse error: " + errs;
            }
            return false;
        }
        return true;
    }

    bool set(const std::string &key, const Json::Value &value) override {
        CURL *h = tl_easy();
        curl_easy_reset(h);

        Json::StreamWriterBuilder wb;
        wb["indentation"] = "";
        const std::string payload = Json::writeString(wb, value);

        std::string readBody;
        char errbuf[CURL_ERROR_SIZE] = {0};

        curl_easy_setopt(h, CURLOPT_TIMEOUT_MS, 3000L);
        curl_easy_setopt(h, CURLOPT_CONNECTTIMEOUT_MS, 1500L);

        const std::string url = encodeUrl(key);
        curl_easy_setopt(h, CURLOPT_URL, url.c_str());
        curl_easy_setopt(h, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(h, CURLOPT_POSTFIELDS, payload.c_str());
        curl_easy_setopt(h, CURLOPT_POSTFIELDSIZE, payload.size());

        curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(h, CURLOPT_WRITEDATA, &readBody);
        curl_easy_setopt(h, CURLOPT_ERRORBUFFER, errbuf);

        struct curl_slist *headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(h, CURLOPT_HTTPHEADER, headers);

        CURLcode rc = curl_easy_perform(h);
        curl_slist_free_all(headers);

        if (rc != CURLE_OK) {
            LOG(ERROR) << "PUT " << url << " curl: " << curl_easy_strerror(rc)
                       << " err: " << errbuf;
            return false;
        }

        long code = 0;
        curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &code);
        if (!is_200(code)) {
            LOG(ERROR) << "PUT " << url << " http=" << code
                       << " body: " << readBody;
            return false;
        }
        return true;
    }

    // ---- DELETE ----
    bool remove(const std::string &key) override {
        CURL *h = tl_easy();
        curl_easy_reset(h);

        std::string readBody;
        char errbuf[CURL_ERROR_SIZE] = {0};

        curl_easy_setopt(h, CURLOPT_TIMEOUT_MS, 3000L);
        curl_easy_setopt(h, CURLOPT_CONNECTTIMEOUT_MS, 1500L);

        const std::string url = encodeUrl(key);
        curl_easy_setopt(h, CURLOPT_URL, url.c_str());
        curl_easy_setopt(h, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(h, CURLOPT_WRITEDATA, &readBody);
        curl_easy_setopt(h, CURLOPT_ERRORBUFFER, errbuf);

        CURLcode rc = curl_easy_perform(h);
        if (rc != CURLE_OK) {
            LOG(ERROR) << "DELETE " << url
                       << " curl: " << curl_easy_strerror(rc)
                       << " err: " << errbuf;
            return false;
        }

        long code = 0;
        curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &code);
        if (!is_200(code)) {
            LOG(ERROR) << "DELETE " << url << " http=" << code
                       << " body: " << readBody;
            return false;
        }
        return true;
    }

   private:
    const std::string metadata_uri_;
};

#endif  // USE_HTTP

#ifdef USE_ETCD
#ifdef USE_ETCD_LEGACY
struct EtcdStoragePlugin : public MetadataStoragePlugin {
    EtcdStoragePlugin(const std::string &metadata_uri)
        : client_(metadata_uri), metadata_uri_(metadata_uri) {}

    virtual ~EtcdStoragePlugin() {}

    virtual bool tryGet(const std::string &key, Json::Value &value,
                        std::string *error_msg = nullptr) override {
        auto resp = client_.get(key);
        if (!resp.is_ok()) {
            if (error_msg) {
                *error_msg = "EtcdStoragePlugin: unable to get " + key +
                             " from " + metadata_uri_ + ": " +
                             resp.error_message();
            }
            return false;
        }
        auto json_file = resp.value().as_string();

        std::string errs;
        if (!parseJsonString(json_file, value, &errs)) {
            if (error_msg) {
                *error_msg = "EtcdStoragePlugin: JSON parse error: " + errs;
            }
            return false;
        }
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
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
#else
struct EtcdStoragePlugin : public MetadataStoragePlugin {
    EtcdStoragePlugin(const std::string &metadata_uri)
        : metadata_uri_(metadata_uri) {
        auto ret = NewEtcdClient((char *)metadata_uri_.c_str(), &err_msg_);
        if (ret) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to connect "
                       << metadata_uri_ << ": " << err_msg_;
            // free the memory for storing error message
            free(err_msg_);
            err_msg_ = nullptr;
        }
    }

    virtual ~EtcdStoragePlugin() { EtcdCloseWrapper(); }

    virtual bool tryGet(const std::string &key, Json::Value &value,
                        std::string *error_msg = nullptr) override {
        char *json_data = nullptr;
        auto ret = EtcdGetWrapper((char *)key.c_str(), &json_data, &err_msg_);
        if (ret) {
            if (error_msg) {
                *error_msg = "EtcdStoragePlugin: unable to get " + key +
                             " in " + metadata_uri_ + ": " +
                             std::string(err_msg_ ? err_msg_ : "");
            }
            // free the memory for storing error message
            free(err_msg_);
            err_msg_ = nullptr;
            return false;
        }
        if (!json_data) {
            return false;
        }
        auto json_file = std::string(json_data);
        // free the memory allocated by EtcdGetWrapper
        free(json_data);

        std::string errs;
        if (!parseJsonString(json_file, value, &errs)) {
            if (error_msg) {
                *error_msg = "EtcdStoragePlugin: JSON parse error: " + errs;
            }
            return false;
        }
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        auto ret = EtcdPutWrapper((char *)key.c_str(),
                                  (char *)json_file.c_str(), &err_msg_);
        if (ret) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to set " << key << " in "
                       << metadata_uri_ << ": " << err_msg_;
            // free the memory for storing error message
            free(err_msg_);
            err_msg_ = nullptr;
            return false;
        }
        return true;
    }

    virtual bool remove(const std::string &key) {
        auto ret = EtcdDeleteWrapper((char *)key.c_str(), &err_msg_);
        if (ret) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to remove " << key
                       << " in " << metadata_uri_ << ": " << err_msg_;
            // free the memory for storing error message
            free(err_msg_);
            err_msg_ = nullptr;
            return false;
        }
        return true;
    }

    const std::string metadata_uri_;
    char *err_msg_;
};
#endif
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
        const char *password = std::getenv("MC_REDIS_PASSWORD");
        std::string password_str = password ? password : "";

        uint8_t db_index = 0;
        const char *db_index_str = std::getenv("MC_REDIS_DB_INDEX");
        if (db_index_str) {
            try {
                int index = std::stoi(db_index_str);
                if (index >= 0 && index <= 255) {
                    db_index = static_cast<uint8_t>(index);
                } else {
                    LOG(WARNING) << "Invalid Redis DB index: " << index
                                 << ", using default 0";
                }
            } catch (const std::exception &e) {
                LOG(WARNING)
                    << "Failed to parse MC_REDIS_DB_INDEX: " << e.what()
                    << ", using default 0";
            }
        }

        return std::make_shared<RedisStoragePlugin>(parsed_conn_string.second,
                                                    password_str, db_index);
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
               << parsed_conn_string.first
               << " with conn string: " << conn_string;
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
    SocketHandShakePlugin() : listener_running_(false), listen_fd_(-1) {
        auto &config = globalConfig();
        listen_backlog_ = config.handshake_listen_backlog;
    }

    void closeListen() {
        if (listen_fd_ >= 0) {
            // LOG(INFO) << "SocketHandShakePlugin: closing listen socket";
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    virtual ~SocketHandShakePlugin() {
        if (listener_running_) {
            listener_running_ = false;
            listener_.join();
        }
        closeListen();
    }

    virtual void registerOnConnectionCallBack(OnReceiveCallBack callback) {
        on_connection_callback_ = callback;
    }

    virtual void registerOnMetadataCallBack(OnReceiveCallBack callback) {
        on_metadata_callback_ = callback;
    }

    virtual void registerOnNotifyCallBack(OnReceiveCallBack callback) {
        on_notify_callback_ = callback;
    }

    virtual void registerOnDeleteEndpointCallBack(OnReceiveCallBack callback) {
        on_delete_endpoint_callback_ = callback;
    }

    virtual int startDaemon(uint16_t listen_port, int sockfd) {
        if (listener_running_) {
            // LOG(INFO) << "SocketHandShakePlugin: listener already running";
            return 0;
        }

        int on = 1;

        if (sockfd >= 0) {
            listen_fd_ = sockfd;
        } else {
            listen_fd_ = socket(globalConfig().use_ipv6 ? AF_INET6 : AF_INET,
                                SOCK_STREAM, 0);
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

            if (globalConfig().use_ipv6) {
                sockaddr_in6 bind_address;
                memset(&bind_address, 0, sizeof(sockaddr_in6));
                bind_address.sin6_family = AF_INET6;
                bind_address.sin6_port = htons(listen_port);
                bind_address.sin6_addr = IN6ADDR_ANY_INIT;

                if (bind(listen_fd_, (sockaddr *)&bind_address,
                         sizeof(sockaddr_in6)) < 0) {
                    PLOG(ERROR) << "SocketHandShakePlugin: bind (port "
                                << listen_port << ")";
                    closeListen();
                    return ERR_SOCKET;
                }
            } else {
                sockaddr_in bind_address;
                memset(&bind_address, 0, sizeof(sockaddr_in));
                bind_address.sin_family = AF_INET;
                bind_address.sin_port = htons(listen_port);
                bind_address.sin_addr.s_addr = INADDR_ANY;

                if (bind(listen_fd_, (sockaddr *)&bind_address,
                         sizeof(sockaddr_in)) < 0) {
                    PLOG(ERROR) << "SocketHandShakePlugin: bind (port "
                                << listen_port << ")";
                    closeListen();
                    return ERR_SOCKET;
                }
            }
        }

        if (listen(listen_fd_, listen_backlog_)) {
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
                    if (errno != EWOULDBLOCK && errno != EINTR)
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

                auto [type, json_str] = readString(conn_fd);
                std::string errs;
                if (!parseJsonString(json_str, peer, &errs)) {
                    LOG(ERROR)
                        << "SocketHandShakePlugin: failed to receive "
                           "handshake message, "
                           "malformed json format: "
                        << errs << ", json string length: " << json_str.size()
                        << ", json string content: " << json_str;
                    close(conn_fd);
                    continue;
                }

                // old protocol equals Connection type
                if (type == HandShakeRequestType::Connection ||
                    type == HandShakeRequestType::OldProtocol) {
                    if (on_connection_callback_)
                        on_connection_callback_(peer, local);
                } else if (type == HandShakeRequestType::Metadata) {
                    if (on_metadata_callback_)
                        on_metadata_callback_(peer, local);
                } else if (type == HandShakeRequestType::Notify) {
                    if (on_notify_callback_) on_notify_callback_(peer, local);
                } else if (type == HandShakeRequestType::DeleteEndpoint) {
                    if (on_delete_endpoint_callback_)
                        on_delete_endpoint_callback_(peer, local);
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

    virtual int sendNotify(std::string ip_or_host_name, uint16_t rpc_port,
                           const Json::Value &local, Json::Value &peer) {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = globalConfig().use_ipv6 ? AF_INET6 : AF_INET;
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
            ret = doSendNotify(rp, local, peer);
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

    virtual int send(std::string ip_or_host_name, uint16_t rpc_port,
                     const Json::Value &local, Json::Value &peer) {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = globalConfig().use_ipv6 ? AF_INET6 : AF_INET;
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

        auto [type, json_str] = readString(conn_fd);
        if (type != HandShakeRequestType::Connection) {
            LOG(ERROR)
                << "SocketHandShakePlugin: unexpected handshake message type";
            close(conn_fd);
            return ERR_SOCKET;
        }

        std::string errs;
        if (!parseJsonString(json_str, peer, &errs)) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to receive handshake "
                          "message: malformed json format: "
                       << errs;
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
        hints.ai_family = globalConfig().use_ipv6 ? AF_INET6 : AF_INET;
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

    int doSendNotify(struct addrinfo *addr, const Json::Value &local_notify,
                     Json::Value &peer_notify) {
        int conn_fd = -1;
        int ret = doConnect(addr, conn_fd);
        if (ret) {
            return ret;
        }

        ret = writeString(conn_fd, HandShakeRequestType::Notify,
                          Json::FastWriter{}.write(local_notify));
        if (ret) {
            LOG(ERROR)
                << "SocketHandShakePlugin: failed to send metadata message: "
                   "malformed json format, check tcp connection";
            close(conn_fd);
            return ret;
        }

        auto [type, json_str] = readString(conn_fd);
        if (type != HandShakeRequestType::Notify) {
            LOG(ERROR)
                << "SocketHandShakePlugin: unexpected handshake message type";
            close(conn_fd);
            return ERR_SOCKET;
        }

        // LOG(INFO) << "SocketHandShakePlugin: received metadata message: "
        //           << json_str;

        std::string errs;
        if (!parseJsonString(json_str, peer_notify, &errs)) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to receive metadata "
                          "message, malformed json format: "
                       << errs;
            close(conn_fd);
            return ERR_MALFORMED_JSON;
        }

        close(conn_fd);
        return 0;
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

        auto [type, json_str] = readString(conn_fd);
        if (type != HandShakeRequestType::Metadata) {
            LOG(ERROR)
                << "SocketHandShakePlugin: unexpected handshake message type";
            close(conn_fd);
            return ERR_SOCKET;
        }

        // LOG(INFO) << "SocketHandShakePlugin: received metadata message: "
        //           << json_str;

        std::string errs;
        if (!parseJsonString(json_str, peer_metadata, &errs)) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to receive metadata "
                          "message, malformed json format: "
                       << errs;
            close(conn_fd);
            return ERR_MALFORMED_JSON;
        }

        close(conn_fd);
        return 0;
    }

    virtual int sendDeleteEndpoint(std::string ip_or_host_name,
                                   uint16_t rpc_port,
                                   const Json::Value &local) {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = globalConfig().use_ipv6 ? AF_INET6 : AF_INET;
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
            ret = doSendDeleteEndpoint(rp, local);
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

    int doSendDeleteEndpoint(struct addrinfo *addr,
                             const Json::Value &local_delete_endpoint) {
        int conn_fd = -1;
        int ret = doConnect(addr, conn_fd);
        if (ret) {
            return ret;
        }

        ret = writeString(conn_fd, HandShakeRequestType::DeleteEndpoint,
                          Json::FastWriter{}.write(local_delete_endpoint));
        if (ret) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to send delete "
                          "endpoint message: "
                          "malformed json format, check tcp connection";
            close(conn_fd);
            return ret;
        }

        auto [type, json_str] = readString(conn_fd);
        if (type != HandShakeRequestType::DeleteEndpoint) {
            LOG(ERROR)
                << "SocketHandShakePlugin: unexpected handshake message type";
            close(conn_fd);
            return ERR_SOCKET;
        }

        close(conn_fd);
        return 0;
    }

    std::atomic<bool> listener_running_;
    std::thread listener_;
    int listen_fd_;
    int listen_backlog_;

    OnReceiveCallBack on_connection_callback_;
    OnReceiveCallBack on_metadata_callback_;
    OnReceiveCallBack on_notify_callback_;
    OnReceiveCallBack on_delete_endpoint_callback_;
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

    auto use_ipv6 = globalConfig().use_ipv6;
    sa_family_t family = use_ipv6 ? AF_INET6 : AF_INET;

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }

        if (ifa->ifa_addr->sa_family == family) {
            if (strcmp(ifa->ifa_name, "lo") == 0) {
                continue;
            }

            // Check if interface is UP and RUNNING
            if (!(ifa->ifa_flags & IFF_UP) || !(ifa->ifa_flags & IFF_RUNNING)) {
                LOG(INFO) << "Skipping interface " << ifa->ifa_name
                          << " (not UP or not RUNNING)";
                continue;
            }

            char host[NI_MAXHOST];
            if (getnameinfo(ifa->ifa_addr,
                            use_ipv6 ? sizeof(struct sockaddr_in6)
                                     : sizeof(struct sockaddr_in),
                            host, NI_MAXHOST, nullptr, 0,
                            NI_NUMERICHOST) == 0) {
                LOG(INFO) << "Found active interface " << ifa->ifa_name
                          << " with IP " << host;
                ips.push_back(host);
            }
        }
    }

    freeifaddrs(ifaddr);
    return ips;
}

uint16_t findAvailableTcpPort(int &sockfd, bool set_range) {
    static std::random_device rand_gen;
    std::uniform_int_distribution rand_dist;
    int min_port = globalConfig().rpc_min_port;
    int max_port = globalConfig().rpc_max_port;
#ifdef USE_BAREX
    if (set_range) {
        min_port = 17000;
        max_port = 35000;
        const char *min_port_env = std::getenv("ACCL_MIN_PORT");
        const char *max_port_env = std::getenv("ACCL_MAX_PORT");
        if (min_port_env) {
            int val = atoi(min_port_env);
            if (val > 1024 && val < 65536) {
                min_port = val;
            }
        }
        if (max_port_env) {
            int val = atoi(max_port_env);
            if (val > 1024 && val < 65536 && val > min_port) {
                max_port = val;
            }
        }
    }
#endif
    const int max_attempts = 500;
    bool use_ipv6 = globalConfig().use_ipv6;

    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        int port = min_port + rand_dist(rand_gen) % (max_port - min_port + 1);
        sockfd = socket(use_ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
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

        if (use_ipv6) {
            sockaddr_in6 bind_address;
            memset(&bind_address, 0, sizeof(sockaddr_in6));
            bind_address.sin6_family = AF_INET6;
            bind_address.sin6_port = htons(port);
            bind_address.sin6_addr = IN6ADDR_ANY_INIT;
            if (bind(sockfd, (sockaddr *)&bind_address, sizeof(sockaddr_in6)) <
                0) {
                close(sockfd);
                sockfd = -1;
                continue;
            }
        } else {
            sockaddr_in bind_address;
            memset(&bind_address, 0, sizeof(sockaddr_in));
            bind_address.sin_family = AF_INET;
            bind_address.sin_port = htons(port);
            bind_address.sin_addr.s_addr = INADDR_ANY;
            if (bind(sockfd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) <
                0) {
                close(sockfd);
                sockfd = -1;
                continue;
            }
        }

        return port;
    }
    return 0;
}

}  // namespace mooncake
