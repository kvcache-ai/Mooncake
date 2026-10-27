#include "storage/distributed/valkey_adapter.h"

#include <glog/logging.h>
#include <hiredis/hiredis.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <thread>
#include <utility>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

using Reply = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

Reply MakeReply(void* reply) {
    return Reply(static_cast<redisReply*>(reply), &freeReplyObject);
}

// One command as binary-safe argv.
struct Command {
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;

    Command& Arg(const char* data, size_t len) {
        argv.push_back(data);
        argvlen.push_back(len);
        return *this;
    }
    Command& Arg(const std::string& value) {
        return Arg(value.data(), value.size());
    }
};

Reply Run(redisContext* context, const Command& command) {
    // hiredis takes `const char**` but does not modify the array.
    return MakeReply(redisCommandArgv(
        context, static_cast<int>(command.argv.size()),
        const_cast<const char**>(command.argv.data()), command.argvlen.data()));
}

// Escape glob metacharacters so a key prefix matches literally in SCAN.
std::string GlobEscape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        if (c == '*' || c == '?' || c == '[' || c == ']' || c == '\\') {
            escaped.push_back('\\');
        }
        escaped.push_back(c);
    }
    return escaped;
}

tl::expected<void, ErrorCode> PutResult(const Reply& reply) {
    if (!reply) return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    if (reply->type == REDIS_REPLY_STATUS) return {};
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "Valkey BLOB.TCP_SET failed: "
                   << std::string(reply->str, reply->len);
    }
    return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
}

tl::expected<size_t, ErrorCode> GetResult(const Reply& reply, void* buf,
                                          size_t len) {
    if (!reply) return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    if (reply->type == REDIS_REPLY_NIL) {
        return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
    }
    if (reply->type != REDIS_REPLY_STRING) {
        if (reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "Valkey BLOB.TCP_GET failed: "
                       << std::string(reply->str, reply->len);
        }
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    // Like a ranged object-store read: the caller asks for exactly `len`
    // bytes from the start of the object.
    if (reply->len < len) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    std::memcpy(buf, reply->str, len);
    return len;
}

}  // namespace

// ─── Config ──────────────────────────────────────────────────────────────────

tl::expected<ValkeyAdapterConfig, ErrorCode>
ValkeyAdapterConfig::FromEnvironment() {
    using Variables = ValkeyAdapterEnvironmentVariables;
    ValkeyAdapterConfig config;
    const auto addr = Environ::Read(Variables::MOONCAKE_VALKEY_ADDR);
    if (addr.has_value() && !addr->empty()) {
        const auto colon = addr->rfind(':');
        if (colon == std::string::npos || colon == 0) {
            LOG(ERROR) << "MOONCAKE_VALKEY_ADDR must be host:port: " << *addr;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const auto port =
            TryParseEnvironmentValue<int>(addr->substr(colon + 1));
        if (!port.has_value() || *port <= 0 || *port > 65535) {
            LOG(ERROR) << "MOONCAKE_VALKEY_ADDR has an invalid port: " << *addr;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.host = addr->substr(0, colon);
        config.port = *port;
    }
    config.username =
        Environ::Read(Variables::MOONCAKE_VALKEY_USERNAME).value_or("");
    config.password =
        Environ::Read(Variables::MOONCAKE_VALKEY_PASSWORD).value_or("");
    config.pool_size =
        Environ::ReadOr(Variables::MOONCAKE_VALKEY_POOL_SIZE, config.pool_size);
    config.timeout_ms = Environ::ReadOr(Variables::MOONCAKE_VALKEY_TIMEOUT_MS,
                                        config.timeout_ms);
    if (config.pool_size <= 0 || config.timeout_ms <= 0) {
        LOG(ERROR)
            << "MOONCAKE_VALKEY_POOL_SIZE and MOONCAKE_VALKEY_TIMEOUT_MS "
               "must be positive";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config;
}

// ─── Connection pool ─────────────────────────────────────────────────────────

class ValkeyObjectStorageAdapter::Lease {
   public:
    explicit Lease(ValkeyObjectStorageAdapter& adapter)
        : adapter_(adapter), context_(adapter.Acquire()) {}
    ~Lease() {
        if (context_) adapter_.Release(context_);
    }
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;

    redisContext* get() const { return context_; }

   private:
    ValkeyObjectStorageAdapter& adapter_;
    redisContext* context_;
};

ValkeyObjectStorageAdapter::ValkeyObjectStorageAdapter(std::string key_prefix)
    : key_prefix_(std::move(key_prefix)) {
    const size_t first = key_prefix_.find_first_not_of('/');
    if (first == std::string::npos) {
        key_prefix_.clear();
        return;
    }
    key_prefix_.erase(0, first);
    while (!key_prefix_.empty() && key_prefix_.back() == '/') {
        key_prefix_.pop_back();
    }
}

ValkeyObjectStorageAdapter::~ValkeyObjectStorageAdapter() {
    for (redisContext* context : all_) redisFree(context);
}

std::string ValkeyObjectStorageAdapter::PhysicalPrefix() const {
    return key_prefix_.empty() ? std::string() : key_prefix_ + "/";
}

std::string ValkeyObjectStorageAdapter::PhysicalKey(
    const std::string& logical_key) const {
    return PhysicalPrefix() + logical_key;
}

tl::expected<redisContext*, ErrorCode> ValkeyObjectStorageAdapter::Connect()
    const {
    const timeval timeout{config_.timeout_ms / 1000,
                          (config_.timeout_ms % 1000) * 1000};
    redisContext* context =
        redisConnectWithTimeout(config_.host.c_str(), config_.port, timeout);
    if (!context || context->err) {
        LOG(ERROR) << "Valkey connect to " << config_.host << ":"
                   << config_.port << " failed: "
                   << (context ? context->errstr : "out of memory");
        if (context) redisFree(context);
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    redisSetTimeout(context, timeout);
    if (!config_.password.empty()) {
        Command auth;
        auth.Arg("AUTH", 4);
        if (!config_.username.empty()) auth.Arg(config_.username);
        auth.Arg(config_.password);
        auto reply = Run(context, auth);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "Valkey AUTH failed";
            redisFree(context);
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
    }
    return context;
}

redisContext* ValkeyObjectStorageAdapter::Acquire() {
    std::unique_lock<std::mutex> lock(pool_mutex_);
    pool_cv_.wait(lock, [this] { return !idle_.empty(); });
    redisContext* context = idle_.back();
    idle_.pop_back();
    return context;
}

void ValkeyObjectStorageAdapter::Release(redisContext* context) {
    // A connection that hit an I/O error (e.g. a timeout) cannot be reused:
    // replace it, or keep it out of rotation if the server is unreachable.
    if (context->err) {
        auto fresh = Connect();
        if (fresh) {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            std::replace(all_.begin(), all_.end(), context, *fresh);
            redisFree(context);
            context = *fresh;
        } else {
            redisReconnect(context);
        }
    }
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        idle_.push_back(context);
    }
    pool_cv_.notify_one();
}

tl::expected<void, ErrorCode> ValkeyObjectStorageAdapter::Init() {
    auto config = ValkeyAdapterConfig::FromEnvironment();
    if (!config) return tl::make_unexpected(config.error());
    config_ = std::move(*config);
    for (int i = 0; i < config_.pool_size; ++i) {
        auto context = Connect();
        if (!context) return tl::make_unexpected(context.error());
        all_.push_back(*context);
        idle_.push_back(*context);
    }
    // Fail fast if the server lacks the large-object module.
    Lease lease(*this);
    auto reply =
        MakeReply(redisCommand(lease.get(), "COMMAND INFO BLOB.TCP_SET"));
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0 ||
        reply->element[0]->type == REDIS_REPLY_NIL) {
        LOG(ERROR) << "Valkey at " << config_.host << ":" << config_.port
                   << " has no BLOB.TCP_SET; load the valkey-large-object "
                      "module";
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    LOG(INFO) << "Valkey adapter initialized: " << config_.host << ":"
              << config_.port << ", pool=" << config_.pool_size
              << ", key_prefix=" << key_prefix_;
    return {};
}

// ─── Object operations ───────────────────────────────────────────────────────

tl::expected<void, ErrorCode> ValkeyObjectStorageAdapter::Put(
    const std::string& logical_key, std::span<const char> data) {
    if (data.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const std::string key = PhysicalKey(logical_key);
    Command command;
    command.Arg("BLOB.TCP_SET", 12).Arg(key).Arg(data.data(), data.size());
    Lease lease(*this);
    return PutResult(Run(lease.get(), command));
}

tl::expected<void, ErrorCode> ValkeyObjectStorageAdapter::PutV(
    const std::string& logical_key, const iovec* iov, int iovcnt) {
    if (iovcnt < 0 || (!iov && iovcnt > 0)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (iovcnt == 1) {
        return Put(logical_key,
                   {static_cast<const char*>(iov[0].iov_base), iov[0].iov_len});
    }
    // BLOB.TCP_SET takes the object as one argument.
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) total += iov[i].iov_len;
    std::string data;
    data.reserve(total);
    for (int i = 0; i < iovcnt; ++i) {
        data.append(static_cast<const char*>(iov[i].iov_base), iov[i].iov_len);
    }
    return Put(logical_key, {data.data(), data.size()});
}

tl::expected<size_t, ErrorCode> ValkeyObjectStorageAdapter::Get(
    const std::string& logical_key, void* buf, size_t len) {
    if (len == 0) return 0;
    if (!buf) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const std::string key = PhysicalKey(logical_key);
    Command command;
    command.Arg("BLOB.TCP_GET", 12).Arg(key);
    Lease lease(*this);
    return GetResult(Run(lease.get(), command), buf, len);
}

std::vector<tl::expected<void, ErrorCode>> ValkeyObjectStorageAdapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    const size_t lanes = std::min<size_t>(
        requests.size(), static_cast<size_t>(config_.pool_size));
    std::vector<std::thread> workers;
    for (size_t lane = 0; lane < lanes; ++lane) {
        workers.emplace_back([&, lane] {
            for (size_t i = lane; i < requests.size(); i += lanes) {
                results[i] = PutV(requests[i].logical_key, requests[i].iov,
                                  requests[i].iovcnt);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    return results;
}

std::vector<tl::expected<size_t, ErrorCode>>
ValkeyObjectStorageAdapter::GetBatch(
    const std::vector<ObjectGetRequest>& requests) {
    std::vector<tl::expected<size_t, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    const size_t lanes = std::min<size_t>(
        requests.size(), static_cast<size_t>(config_.pool_size));
    std::vector<std::thread> workers;
    for (size_t lane = 0; lane < lanes; ++lane) {
        workers.emplace_back([&, lane] {
            // Pipeline this lane's requests on one connection.
            Lease lease(*this);
            std::vector<size_t> sent;
            for (size_t i = lane; i < requests.size(); i += lanes) {
                if (requests[i].size == 0) {
                    results[i] = 0;
                    continue;
                }
                if (!requests[i].buffer) {
                    results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    continue;
                }
                const std::string key = PhysicalKey(requests[i].logical_key);
                Command command;
                command.Arg("BLOB.TCP_GET", 12).Arg(key);
                redisAppendCommandArgv(
                    lease.get(), static_cast<int>(command.argv.size()),
                    command.argv.data(), command.argvlen.data());
                sent.push_back(i);
            }
            for (size_t i : sent) {
                void* raw = nullptr;
                if (redisGetReply(lease.get(), &raw) != REDIS_OK) {
                    results[i] =
                        tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
                    continue;
                }
                results[i] = GetResult(MakeReply(raw), requests[i].buffer,
                                       requests[i].size);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    return results;
}

tl::expected<bool, ErrorCode> ValkeyObjectStorageAdapter::Exists(
    const std::string& logical_key) {
    const std::string key = PhysicalKey(logical_key);
    Command command;
    command.Arg("EXISTS", 6).Arg(key);
    Lease lease(*this);
    auto reply = Run(lease.get(), command);
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    return reply->integer > 0;
}

tl::expected<void, ErrorCode> ValkeyObjectStorageAdapter::Delete(
    const std::string& logical_key) {
    const std::string key = PhysicalKey(logical_key);
    Command command;
    command.Arg("DEL", 3).Arg(key);
    Lease lease(*this);
    auto reply = Run(lease.get(), command);
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    return {};
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
ValkeyObjectStorageAdapter::ListKeys() {
    const std::string prefix = PhysicalPrefix();
    const std::string pattern = GlobEscape(prefix) + "*";
    std::vector<std::string> keys;
    Lease lease(*this);
    std::string cursor = "0";
    do {
        Command scan;
        scan.Arg("SCAN", 4)
            .Arg(cursor)
            .Arg("MATCH", 5)
            .Arg(pattern)
            .Arg("COUNT", 5)
            .Arg("1000", 4);
        auto reply = Run(lease.get(), scan);
        if (!reply || reply->type != REDIS_REPLY_ARRAY ||
            reply->elements != 2) {
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        cursor.assign(reply->element[0]->str, reply->element[0]->len);
        const redisReply* batch = reply->element[1];
        for (size_t i = 0; i < batch->elements; ++i) {
            keys.emplace_back(batch->element[i]->str, batch->element[i]->len);
        }
    } while (cursor != "0");

    // Sizes, pipelined. A key evicted between SCAN and BLOB.INFO is skipped.
    for (const auto& key : keys) {
        Command info;
        info.Arg("BLOB.INFO", 9).Arg(key).Arg("LEN", 3);
        redisAppendCommandArgv(lease.get(), static_cast<int>(info.argv.size()),
                               info.argv.data(), info.argvlen.data());
    }
    std::vector<KeyInfo> infos;
    infos.reserve(keys.size());
    for (const auto& key : keys) {
        void* raw = nullptr;
        if (redisGetReply(lease.get(), &raw) != REDIS_OK) {
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        auto reply = MakeReply(raw);
        if (!reply || reply->type != REDIS_REPLY_INTEGER) continue;
        infos.push_back(KeyInfo{key.substr(prefix.size()),
                                static_cast<size_t>(reply->integer)});
    }
    return infos;
}

tl::expected<void, ErrorCode> ValkeyObjectStorageAdapter::CheckHealth() {
    const std::string logical_key = "__mooncake_health_check__";
    const std::string payload = "mooncake";
    if (auto put = Put(logical_key, {payload.data(), payload.size()}); !put) {
        return put;
    }
    std::string read(payload.size(), '\0');
    auto get = Get(logical_key, read.data(), read.size());
    (void)Delete(logical_key);
    if (!get) return tl::make_unexpected(get.error());
    if (read != payload) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return {};
}

}  // namespace mooncake
