#include "ha/common/redis/redis_cluster_client.h"

#include <algorithm>
#include <cctype>
#include <set>
#include <sstream>

#include "config/redis_connection_config.h"

#ifdef STORE_USE_REDIS
#include <arpa/inet.h>
#include <hiredis/hiredis.h>
#endif

namespace mooncake::ha::common::redis {
namespace {
constexpr auto kError = ErrorCode::ETCD_OPERATION_ERROR;

std::string Address(const RedisEndpoint& endpoint) {
    return (endpoint.host.find(':') == std::string::npos
                ? endpoint.host
                : "[" + endpoint.host + "]") +
           ":" + std::to_string(endpoint.port);
}

#ifdef STORE_USE_REDIS
RedisReplyPtr Command(redisContext* context,
                      const std::vector<std::string>& args) {
    if (!context) return {};
    std::vector<const char*> argv;
    std::vector<size_t> lengths;
    for (const auto& arg : args) {
        argv.push_back(arg.data());
        lengths.push_back(arg.size());
    }
    return RedisReplyPtr(static_cast<redisReply*>(redisCommandArgv(
        context, static_cast<int>(argv.size()), argv.data(), lengths.data())));
}

std::string Bytes(const redisReply* reply) {
    return reply && reply->str ? std::string(reply->str, reply->len) : "";
}

// RESP2 represents SHARDS maps as alternating field/value arrays.
bool Fields(const redisReply* reply,
            std::map<std::string, const redisReply*>& fields) {
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements % 2)
        return false;
    for (size_t i = 0; i < reply->elements; i += 2) {
        if (!IsStringReply(reply->element[i]) || !reply->element[i + 1] ||
            !fields.emplace(Bytes(reply->element[i]), reply->element[i + 1])
                 .second)
            return false;
    }
    return true;
}

bool ShardsUnsupported(const redisReply* reply) {
    if (!reply || reply->type != REDIS_REPLY_ERROR) return false;
    auto message = Bytes(reply);
    std::transform(message.begin(), message.end(), message.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return message.find("unknown subcommand") != std::string::npos &&
           message.find("shards") != std::string::npos;
}

tl::expected<RedisEndpoint, ErrorCode> ClusterEndpoint(
    std::string_view target) {
    auto parsed = ParseRedisEndpoint(target);
    if (parsed) return parsed;
    // Cluster replies can use unbracketed IPv6. Validate the entire host
    // literal before interpreting the last colon as a port separator.
    const auto colon = target.rfind(':');
    if (colon != std::string_view::npos) {
        const std::string host(target.substr(0, colon));
        in6_addr address{};
        auto port = ParsePositiveInt(target.substr(colon + 1), 1, 65535);
        if (port && inet_pton(AF_INET6, host.c_str(), &address) == 1)
            return RedisEndpoint{host, *port};
    }
    return parsed;
}
#endif
}  // namespace

uint16_t RedisClusterClient::HashSlot(std::string_view key) {
    const auto open = key.find('{');
    if (open != std::string_view::npos) {
        const auto close = key.find('}', open + 1);
        if (close != std::string_view::npos && close > open + 1)
            key = key.substr(open + 1, close - open - 1);
    }
    return Crc16Xmodem(key) % 16384;
}

const char* RedisClusterClient::RoutingErrorName(RoutingError error) {
    switch (error) {
        case RoutingError::None:
            return "none";
        case RoutingError::InvalidConfiguration:
            return "invalid-configuration";
        case RoutingError::Unsupported:
            return "unsupported";
        case RoutingError::ServerError:
            return "server-error";
        case RoutingError::Transport:
            return "transport";
        case RoutingError::InvalidTopology:
            return "invalid-topology";
        case RoutingError::ReplicaCountMismatch:
            return "replica-count-mismatch";
        case RoutingError::RedirectLoop:
            return "redirect-loop";
        case RoutingError::RedirectLimit:
            return "redirect-limit";
        case RoutingError::MalformedRedirect:
            return "malformed-redirect";
        case RoutingError::RedirectSlotMismatch:
            return "redirect-slot-mismatch";
        case RoutingError::UnknownTarget:
            return "unknown-target";
        case RoutingError::MembershipChanged:
            return "membership-changed";
    }
    return "unknown";
}

ErrorCode RedisClusterClient::Connect(std::string connstring,
                                      size_t expected_replicas,
                                      bool read_only) {
    return ConnectDetailed(std::move(connstring), expected_replicas, read_only)
        .error;
}

RedisClusterClient::ConnectResult RedisClusterClient::ConnectDetailed(
    std::string connstring, size_t expected_replicas, bool read_only) {
    std::lock_guard<std::mutex> lock(mutex_);
    refresh_error_ = RoutingError::None;
    const auto error =
        ConnectLocked(std::move(connstring), expected_replicas, read_only);
    if (error == ErrorCode::OK) return {};
    if (error == ErrorCode::INVALID_PARAMS)
        return {error, RoutingError::InvalidConfiguration};
    if (error == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE)
        return {error, RoutingError::Unsupported};
    return {error, refresh_error_ == RoutingError::None
                       ? RoutingError::Transport
                       : refresh_error_};
}

ErrorCode RedisClusterClient::ConnectLocked(std::string connstring,
                                            size_t expected_replicas,
                                            bool read_only) {
    connections_.clear();
    slots_only_endpoints_.clear();
    shards_.clear();
    shards_snapshot_ = false;
    topology_valid_ = false;
    deadline_ = std::chrono::steady_clock::time_point::max();
    seeds_.clear();
    expected_replicas_ = expected_replicas;
    read_only_ = read_only;
    constexpr std::string_view scheme = "redis+cluster://";
    cluster_ = connstring.compare(0, scheme.size(), scheme) == 0;
    if (cluster_) connstring.erase(0, scheme.size());
    const auto config = RedisConnectionConfig::FromEnvironment();
    if (!config) return config.error();
    command_timeout_ms_ = config->command_timeout_ms;
    topology_refresh_ms_ = config->topology_refresh_ms;
    max_redirects_ = config->max_redirects;
    cluster_ = cluster_ || config->cluster_enabled;
    if ((!read_only_ && expected_replicas == 0) ||
        (cluster_ && config->db_index != 0))
        return ErrorCode::INVALID_PARAMS;
    size_t begin = 0;
    do {
        const auto end = connstring.find(',', begin);
        auto endpoint = ParseRedisEndpoint(
            std::string_view(connstring)
                .substr(begin, end == std::string::npos ? end : end - begin));
        if (!endpoint) return endpoint.error();
        seeds_.push_back(Address(*endpoint));
        if (end == std::string::npos) break;
        begin = end + 1;
    } while (true);
    if (!cluster_ && seeds_.size() != 1) return ErrorCode::INVALID_PARAMS;
#ifndef STORE_USE_REDIS
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#else
    return cluster_ ? Refresh()
                    : (Connection(seeds_[0]) ? ErrorCode::OK : kError);
#endif
}

redisContext* RedisClusterClient::Connection(const std::string& endpoint) {
#ifdef STORE_USE_REDIS
    auto& context = connections_[endpoint];
    if (context && context->err) context.reset();
    if (!context) {
        auto connected = ConnectRedis(endpoint, kError, deadline_);
        if (!connected) return nullptr;
        context = std::move(*connected);
    }
    return context.get();
#else
    (void)endpoint;
    return nullptr;
#endif
}

RedisReplyPtr RedisClusterClient::Run(redisContext* context,
                                      const std::vector<std::string>& command) {
#ifdef STORE_USE_REDIS
    if (!context) return {};
    int64_t ms = command_timeout_ms_;
    if (deadline_ != std::chrono::steady_clock::time_point::max())
        ms = std::min(ms, std::chrono::duration_cast<std::chrono::milliseconds>(
                              deadline_ - std::chrono::steady_clock::now())
                              .count());
    if (ms <= 0) return {};
    timeval timeout{static_cast<long>(ms / 1000),
                    static_cast<long>((ms % 1000) * 1000)};
    if (redisSetTimeout(context, timeout) != REDIS_OK) return {};
    return Command(context, command);
#else
    (void)context;
    (void)command;
    return {};
#endif
}

bool RedisClusterClient::ParseShards(const redisReply* reply) {
#ifdef STORE_USE_REDIS
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0)
        return false;
    std::array<Shard, 16384> next;
    std::map<std::string, Shard> shards;
    std::set<std::string> ids, addresses;
    for (size_t i = 0; i < reply->elements; ++i) {
        std::map<std::string, const redisReply*> fields;
        if (!Fields(reply->element[i], fields)) return false;
        const auto* ranges = fields["slots"];
        const auto* nodes = fields["nodes"];
        if (!ranges || ranges->type != REDIS_REPLY_ARRAY ||
            ranges->elements % 2 || !nodes ||
            nodes->type != REDIS_REPLY_ARRAY || nodes->elements == 0)
            return false;
        Shard shard;
        std::string primary_identity;
        std::vector<std::string> replicas;
        for (size_t j = 0; j < nodes->elements; ++j) {
            std::map<std::string, const redisReply*> node;
            if (!Fields(nodes->element[j], node)) return false;
            if (!IsStringReply(node["id"]) || !IsStringReply(node["role"]) ||
                !IsStringReply(node["health"]))
                return false;
            const auto id = Bytes(node["id"]);
            const auto role = Bytes(node["role"]);
            const auto health = Bytes(node["health"]);
            auto host = Bytes(node["endpoint"]);
            if (host.empty() || host == "?") host = Bytes(node["ip"]);
            if (host.empty() || host == "?") host = Bytes(node["hostname"]);
            const auto* port = node["port"];
            if (id.empty() || host.empty() || host == "?" || !port ||
                port->type != REDIS_REPLY_INTEGER || port->integer < 1 ||
                port->integer > 65535 ||
                (role != "master" && role != "replica") ||
                (health != "online" && health != "loading" && health != "fail"))
                return false;
            // Keep unhealthy replicas in the configured membership. WAIT
            // must still acknowledge all N; health is not a reason to lower N.
            const auto address =
                Address({host, static_cast<int>(port->integer)});
            if (!ids.insert(id).second || !addresses.insert(address).second)
                return false;
            if (role == "master") {
                if (!shard.primary.empty() || health != "online") return false;
                shard.primary = address;
                primary_identity = id + "@" + address;
            } else {
                replicas.push_back(id + "@" + address);
            }
        }
        if (shard.primary.empty()) return false;
        shard.replicas = replicas.size();
        if (!read_only_ && shard.replicas != expected_replicas_) {
            refresh_error_ = RoutingError::ReplicaCountMismatch;
            return false;
        }
        std::sort(replicas.begin(), replicas.end());
        shard.membership = primary_identity + "\n";
        for (const auto& replica : replicas) shard.membership += replica + "\n";
        shards.emplace(shard.primary, shard);
        for (size_t j = 0; j < ranges->elements; j += 2) {
            const auto* first = ranges->element[j];
            const auto* last = ranges->element[j + 1];
            if (!first || !last || first->type != REDIS_REPLY_INTEGER ||
                last->type != REDIS_REPLY_INTEGER || first->integer < 0 ||
                last->integer >= 16384 || first->integer > last->integer)
                return false;
            for (int slot = first->integer; slot <= last->integer; ++slot) {
                if (!next[slot].primary.empty()) return false;
                next[slot] = shard;
            }
        }
    }
    for (const auto& shard : next)
        if (shard.primary.empty()) return false;
    slots_ = std::move(next);
    shards_ = std::move(shards);
    for (const auto& address : addresses) connections_.try_emplace(address);
    shards_snapshot_ = true;
    topology_valid_ = true;
    refreshed_at_ = std::chrono::steady_clock::now();
    return true;
#else
    (void)reply;
    return false;
#endif
}

ErrorCode RedisClusterClient::Refresh() {
    topology_valid_ = false;
    refresh_error_ = RoutingError::Transport;
#ifdef STORE_USE_REDIS
    // Keep semantic failures when a later discovery candidate is unreachable.
    const auto record_error = [&](RoutingError error) {
        const auto rank = [](RoutingError e) {
            if (e == RoutingError::ReplicaCountMismatch) return 3;
            if (e == RoutingError::InvalidTopology) return 2;
            if (e == RoutingError::ServerError) return 1;
            return 0;
        };
        if (rank(error) > rank(refresh_error_)) refresh_error_ = error;
    };
    // CLUSTER SLOTS includes primary and replica identities (Redis >= 4).
    // Require full coverage and complete, unique node metadata; never infer
    // the expected replica count from the response.
    std::vector<std::string> candidates = seeds_;
    for (const auto& entry : connections_) candidates.push_back(entry.first);
    for (const auto& endpoint : candidates) {
        if (!slots_only_endpoints_.count(endpoint)) {
            auto reply = Run(Connection(endpoint), {"CLUSTER", "SHARDS"});
            if (ParseShards(reply.get())) return ErrorCode::OK;
            // Malformed topology, ACL errors and transport failures must not
            // silently switch to a weaker discovery protocol.
            if (!ShardsUnsupported(reply.get())) {
                if (reply)
                    record_error(reply->type == REDIS_REPLY_ERROR
                                     ? RoutingError::ServerError
                                     : RoutingError::InvalidTopology);
                continue;
            }
            slots_only_endpoints_.insert(endpoint);
        }
        auto reply = Run(Connection(endpoint), {"CLUSTER", "SLOTS"});
        if (!reply || reply->type != REDIS_REPLY_ARRAY) {
            if (reply)
                record_error(reply->type == REDIS_REPLY_ERROR
                                 ? RoutingError::ServerError
                                 : RoutingError::InvalidTopology);
            continue;
        }
        std::array<Shard, 16384> next;
        std::set<std::string> discovered_endpoints;
        // A primary can own multiple disjoint ranges, but each occurrence
        // must describe the same shard. A node cannot change address, role,
        // or parent across ranges, nor can two IDs share an address.
        std::map<std::string, std::string> node_metadata, address_ids;
        std::map<std::string, Shard> next_shards;
        bool valid = reply->elements > 0;
        for (size_t i = 0; valid && i < reply->elements; ++i) {
            const auto* range = reply->element[i];
            if (!range || range->type != REDIS_REPLY_ARRAY ||
                range->elements < 3) {
                valid = false;
                break;
            }
            auto* first = range->element[0];
            auto* last = range->element[1];
            if (!first || !last || first->type != REDIS_REPLY_INTEGER ||
                last->type != REDIS_REPLY_INTEGER || first->integer < 0 ||
                last->integer >= 16384 || first->integer > last->integer) {
                valid = false;
                break;
            }
            Shard shard;
            std::string primary_id;
            std::set<std::string> nodes, addresses;
            std::vector<std::string> identities;
            for (size_t j = 2; j < range->elements; ++j) {
                const auto* node = range->element[j];
                if (!node || node->type != REDIS_REPLY_ARRAY ||
                    node->elements < 3 || !IsStringReply(node->element[0]) ||
                    !node->element[1] ||
                    node->element[1]->type != REDIS_REPLY_INTEGER ||
                    !IsStringReply(node->element[2])) {
                    valid = false;
                    break;
                }
                const auto host = Bytes(node->element[0]);
                const auto id = Bytes(node->element[2]);
                const auto port = node->element[1]->integer;
                if (host.empty() || id.empty() || port < 1 || port > 65535 ||
                    !nodes.insert(id).second) {
                    valid = false;
                    break;
                }
                const auto address = Address({host, static_cast<int>(port)});
                if (j == 2) primary_id = id;
                const auto metadata = address + "\n" +
                                      (j == 2 ? "master" : "replica") + "\n" +
                                      primary_id;
                const auto [identity_entry, new_identity] =
                    node_metadata.emplace(id, metadata);
                const auto [address_entry, new_address] =
                    address_ids.emplace(address, id);
                if ((!new_identity && identity_entry->second != metadata) ||
                    (!new_address && address_entry->second != id)) {
                    valid = false;
                    break;
                }
                discovered_endpoints.insert(address);
                if (!addresses.insert(address).second) {
                    valid = false;
                    break;
                }
                identities.push_back(id + "@" + address);
                if (j == 2) shard.primary = address;
            }
            if (!valid) break;
            shard.replicas = range->elements - 3;
            // Keep primary identity distinct, sort replicas for stable
            // comparison.
            std::sort(identities.begin() + 1, identities.end());
            for (const auto& identity : identities)
                shard.membership += identity + "\n";
            const auto [entry, inserted] =
                next_shards.emplace(shard.primary, shard);
            if (!inserted && entry->second.membership != shard.membership) {
                valid = false;
                break;
            }
            for (int slot = first->integer; slot <= last->integer; ++slot) {
                if (!next[slot].primary.empty()) {
                    valid = false;
                    break;
                }
                next[slot] = shard;
            }
        }
        for (const auto& shard : next) {
            if (shard.primary.empty()) valid = false;
            if (!read_only_ && !shard.primary.empty() &&
                shard.replicas != expected_replicas_) {
                record_error(RoutingError::ReplicaCountMismatch);
                valid = false;
            }
        }
        if (valid) {
            slots_ = std::move(next);
            shards_ = std::move(next_shards);
            shards_snapshot_ = false;
            topology_valid_ = true;
            refreshed_at_ = std::chrono::steady_clock::now();
            for (const auto& node : discovered_endpoints)
                connections_.try_emplace(node);
            return ErrorCode::OK;
        }
        record_error(RoutingError::InvalidTopology);
    }
    return kError;
#else
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
}

std::optional<RedisClusterClient::Shard> RedisClusterClient::SlotlessShard(
    const std::string& endpoint) {
#ifdef STORE_USE_REDIS
    auto reply = Run(Connection(endpoint), {"CLUSTER", "NODES"});
    if (!IsStringReply(reply.get())) return std::nullopt;
    struct Node {
        std::string id, address, parent;
        bool primary;
        bool healthy;
    };
    std::vector<Node> nodes;
    std::set<std::string> ids, addresses;
    std::istringstream lines(Bytes(reply.get()));
    std::string line, primary_id;
    while (std::getline(lines, line)) {
        std::istringstream fields(line);
        std::string id, address, flags, parent, ping, pong, epoch, link;
        if (!(fields >> id >> address >> flags >> parent >> ping >> pong >>
              epoch >> link))
            return std::nullopt;
        address = address.substr(0, address.find('@'));
        auto parsed = ClusterEndpoint(address);
        if (!parsed) return std::nullopt;
        address = Address(*parsed);
        if (!ids.insert(id).second || !addresses.insert(address).second)
            return std::nullopt;
        const auto has = [&](const std::string& flag) {
            return ("," + flags + ",").find("," + flag + ",") !=
                   std::string::npos;
        };
        const bool primary = has("master");
        if (primary == has("slave")) return std::nullopt;
        const bool healthy = link == "connected" && !has("fail") &&
                             !has("fail?") && !has("handshake") &&
                             !has("noaddr");
        if (address == endpoint) {
            if (!primary || !healthy || parent != "-") return std::nullopt;
            primary_id = id;
        }
        nodes.push_back({id, address, parent, primary, healthy});
    }
    if (primary_id.empty()) return std::nullopt;
    Shard shard{endpoint, primary_id + "@" + endpoint + "\n", 0};
    std::vector<std::string> replicas;
    for (const auto& node : nodes) {
        if (node.parent != primary_id) continue;
        if (node.primary || !node.healthy) return std::nullopt;
        replicas.push_back(node.id + "@" + node.address);
    }
    shard.replicas = replicas.size();
    if (!read_only_ && shard.replicas != expected_replicas_)
        return std::nullopt;
    std::sort(replicas.begin(), replicas.end());
    for (const auto& replica : replicas) shard.membership += replica + "\n";
    return shard;
#else
    (void)endpoint;
    return std::nullopt;
#endif
}

RedisClusterClient::Lease RedisClusterClient::Execute(
    const std::vector<std::string>& command, std::string_view key, bool write,
    std::chrono::steady_clock::time_point deadline) {
    Lease lease;
    lease.lock = std::unique_lock<std::mutex>(mutex_);
    deadline_ = deadline;
    if (write && read_only_) {
        lease.error = ErrorCode::INVALID_PARAMS;
        return lease;
    }
#ifdef STORE_USE_REDIS
    const auto slot = HashSlot(key);
    lease.slot = slot;
    if (command.size() >= 3 && command[0] == "EVAL") {
        auto count = ParsePositiveInt(command[2], 1, 16384);
        if (!count || command.size() < 3 + static_cast<size_t>(*count)) {
            lease.error = ErrorCode::INVALID_PARAMS;
            return lease;
        }
        for (int i = 0; i < *count; ++i) {
            if (HashSlot(command[3 + i]) != slot) {
                lease.error = ErrorCode::INVALID_PARAMS;
                return lease;
            }
        }
    }
    const bool expired =
        !topology_valid_ || std::chrono::steady_clock::now() - refreshed_at_ >=
                                std::chrono::milliseconds(topology_refresh_ms_);
    if (seeds_.empty() || (cluster_ && expired && Refresh() != ErrorCode::OK)) {
        lease.error = kError;
        lease.routing_error = refresh_error_;
        return lease;
    }
    std::string endpoint = cluster_ ? slots_[slot].primary : seeds_[0];
    bool asking = false;
    std::set<std::string> visited;
    lease.routing_error = RoutingError::Transport;
    for (int hop = 0; hop <= max_redirects_; ++hop) {
        if (!visited.insert(endpoint).second) {
            lease.routing_error = RoutingError::RedirectLoop;
            break;
        }
        lease.routing_error = RoutingError::Transport;
        lease.context = Connection(endpoint);
        if (!lease.context) break;
        lease.endpoint = endpoint;
        if (cluster_ && write) {
            lease.slot_owner = slots_[slot].primary;
            auto shard = shards_.find(endpoint);
            if (shard == shards_.end()) {
                auto slotless = asking && !shards_snapshot_
                                    ? SlotlessShard(endpoint)
                                    : std::nullopt;
                if (!slotless) {
                    lease.routing_error = RoutingError::UnknownTarget;
                    break;
                }
                lease.membership = slotless->membership;
            } else {
                lease.membership = shard->second.membership;
            }
        }
        if (asking) {
            auto reply = Run(lease.context, {"ASKING"});
            if (!reply || reply->type != REDIS_REPLY_STATUS ||
                Bytes(reply.get()) != "OK")
                break;
        }
        lease.may_have_executed = true;
        lease.reply = Run(lease.context, command);
        if (!lease.reply) {
            topology_valid_ = false;
            lease.error = kError;
            return lease;
        }
        if (!cluster_ || lease.reply->type != REDIS_REPLY_ERROR) {
            lease.routing_error = RoutingError::None;
            return lease;
        }
        const auto message = Bytes(lease.reply.get());
        if (message.compare(0, 6, "MOVED ") != 0 &&
            message.compare(0, 4, "ASK ") != 0) {
            topology_valid_ = false;
            lease.routing_error = RoutingError::None;
            return lease;
        }
        // Redis rejects top-level MOVED/ASK before executing the command.
        // Transport failures and Lua runtime errors never enter this retry
        // path.
        lease.may_have_executed = false;
        topology_valid_ = false;
        lease.routing_error = RoutingError::MalformedRedirect;
        std::istringstream redirect(message);
        std::string kind, slot_text, target, extra;
        redirect >> kind >> slot_text >> target;
        if (redirect >> extra) break;
        auto parsed_slot = ParsePositiveInt(slot_text, 0, 16383);
        auto parsed_endpoint = ClusterEndpoint(target);
        if (!parsed_slot || !parsed_endpoint) break;
        if (*parsed_slot != slot) {
            lease.routing_error = RoutingError::RedirectSlotMismatch;
            break;
        }
        if (hop == max_redirects_) {
            lease.routing_error = RoutingError::RedirectLimit;
            break;
        }
        if (Refresh() != ErrorCode::OK) {
            lease.routing_error = refresh_error_;
            break;
        }
        endpoint = Address(*parsed_endpoint);
        asking = kind == "ASK";
        lease.routing_error = RoutingError::UnknownTarget;
        if (!asking && slots_[slot].primary != endpoint) break;
    }
    lease.reply.reset();
    topology_valid_ = false;
    lease.error = kError;
#else
    (void)command;
    (void)key;
    (void)write;
    lease.error = ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
    return lease;
}

tl::expected<size_t, ErrorCode> RedisClusterClient::Wait(
    Lease& lease, size_t replicas, std::chrono::milliseconds timeout) {
#ifdef STORE_USE_REDIS
    if (!lease.context || !lease.lock.owns_lock() ||
        replicas != expected_replicas_)
        return tl::make_unexpected(kError);
    // A failed confirmation must not leave a reusable topology snapshot.
    // Successful cluster confirmation below installs a newly checked one.
    topology_valid_ = false;
    auto ms = timeout.count() + 1000;
    if (deadline_ != std::chrono::steady_clock::time_point::max())
        ms = std::min(ms, std::chrono::duration_cast<std::chrono::milliseconds>(
                              deadline_ - std::chrono::steady_clock::now())
                              .count());
    if (ms <= 0) return tl::make_unexpected(kError);
    timeval deadline{static_cast<long>(ms / 1000),
                     static_cast<long>((ms % 1000) * 1000)};
    if (redisSetTimeout(lease.context, deadline) != REDIS_OK)
        return tl::make_unexpected(kError);
    auto reply = Command(lease.context, {"WAIT", std::to_string(replicas),
                                         std::to_string(timeout.count())});
    timeval ordinary{command_timeout_ms_ / 1000,
                     (command_timeout_ms_ % 1000) * 1000};
    if (redisSetTimeout(lease.context, ordinary) != REDIS_OK || !reply ||
        reply->type != REDIS_REPLY_INTEGER || reply->integer < 0) {
        lease.routing_error = RoutingError::Transport;
        return tl::make_unexpected(kError);
    }
    if (cluster_) {
        if (Refresh() != ErrorCode::OK) {
            lease.routing_error = refresh_error_;
            return tl::make_unexpected(kError);
        }
        const auto shard = shards_.find(lease.endpoint);
        const auto slotless = shard == shards_.end() && !shards_snapshot_
                                  ? SlotlessShard(lease.endpoint)
                                  : std::nullopt;
        const auto membership = shard != shards_.end()
                                    ? shard->second.membership
                                : slotless ? slotless->membership
                                           : "";
        if (membership != lease.membership ||
            slots_[lease.slot].primary != lease.slot_owner) {
            lease.routing_error = RoutingError::MembershipChanged;
            return tl::make_unexpected(kError);
        }
    }
    return static_cast<size_t>(reply->integer);
#else
    (void)lease;
    (void)replicas;
    (void)timeout;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#endif
}
}  // namespace mooncake::ha::common::redis
