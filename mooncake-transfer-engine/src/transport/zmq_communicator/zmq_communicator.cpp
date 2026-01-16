#include "zmq_communicator.h"
#include "patterns.h"
#include <glog/logging.h>
#include <cstdlib>
#include "default_config.h"

namespace mooncake {

ZmqCommunicator::ZmqCommunicator() = default;

ZmqCommunicator::~ZmqCommunicator() { shutdown(); }

bool ZmqCommunicator::initialize(const ZmqConfig& config) {
    config_ = config;
    init_ylt_log_level();

    // Check for RDMA
    const char* protocol = std::getenv("MC_RPC_PROTOCOL");
    bool use_rdma = (protocol && std::string_view(protocol) == "rdma") ||
                    config.enable_rdma;

    // Configure client pool
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
    // TODO: RDMA support requires additional configuration
    // if (use_rdma) {
    //     pool_conf.client_config.socket_config =
    //     coro_io::ib_socket_t::config_t{}; LOG(INFO) << "ZMQ Communicator
    //     using RDMA transport";
    // } else {
    LOG(INFO)
        << "ZMQ Communicator using TCP transport (RDMA not yet configured)";
    // }

    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);

    LOG(INFO) << "ZMQ Communicator initialized with pool_size="
              << config.pool_size;
    return true;
}

void ZmqCommunicator::shutdown() {
    std::lock_guard lock(sockets_mutex_);

    // Stop all servers
    for (auto& [endpoint, server] : servers_) {
        if (server) {
            server->stop();
        }
    }
    servers_.clear();

    // Clear sockets
    sockets_.clear();

    LOG(INFO) << "ZMQ Communicator shutdown";
}

int ZmqCommunicator::createSocket(ZmqSocketType type) {
    std::lock_guard lock(sockets_mutex_);

    int socket_id = next_socket_id_.fetch_add(1);

    SocketInfo info;
    info.id = socket_id;
    info.type = type;
    // Create pattern immediately so callbacks and subscriptions work before
    // bind/connect
    info.pattern = createPattern(type, "");

    sockets_[socket_id] = std::move(info);

    LOG(INFO) << "Created socket " << socket_id << " of type "
              << static_cast<int>(type);

    return socket_id;
}

bool ZmqCommunicator::closeSocket(int socket_id) {
    std::lock_guard lock(sockets_mutex_);

    auto it = sockets_.find(socket_id);
    if (it == sockets_.end()) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return false;
    }

    sockets_.erase(it);
    LOG(INFO) << "Closed socket " << socket_id;
    return true;
}

std::shared_ptr<BasePattern> ZmqCommunicator::createPattern(
    ZmqSocketType type, const std::string& endpoint) {
    coro_rpc::coro_rpc_server* server = nullptr;

    // Create server if needed (for REP, SUB, PULL, PAIR)
    if (type == ZmqSocketType::REP || type == ZmqSocketType::SUB ||
        type == ZmqSocketType::PULL || type == ZmqSocketType::PAIR) {
        server = getOrCreateServer(endpoint);
    }

    // Create appropriate pattern
    switch (type) {
        case ZmqSocketType::REQ:
            return std::make_shared<ReqRepPattern>(client_pools_, server, true);
        case ZmqSocketType::REP:
            return std::make_shared<ReqRepPattern>(client_pools_, server,
                                                   false);
        case ZmqSocketType::PUB:
            return std::make_shared<PubSubPattern>(client_pools_, server, true);
        case ZmqSocketType::SUB:
            return std::make_shared<PubSubPattern>(client_pools_, server,
                                                   false);
        case ZmqSocketType::PUSH:
            return std::make_shared<PushPullPattern>(client_pools_, server,
                                                     true);
        case ZmqSocketType::PULL:
            return std::make_shared<PushPullPattern>(client_pools_, server,
                                                     false);
        case ZmqSocketType::PAIR:
            return std::make_shared<PairPattern>(client_pools_, server);
        default:
            LOG(ERROR) << "Unknown socket type";
            return nullptr;
    }
}

coro_rpc::coro_rpc_server* ZmqCommunicator::getOrCreateServer(
    const std::string& endpoint) {
    auto it = servers_.find(endpoint);
    if (it != servers_.end()) {
        return it->second.get();
    }

    auto server = std::make_unique<coro_rpc::coro_rpc_server>(
        config_.thread_count, endpoint,
        std::chrono::seconds(config_.timeout_seconds));

    // TODO: RDMA initialization
    // const char* protocol = std::getenv("MC_RPC_PROTOCOL");
    // if ((protocol && std::string_view(protocol) == "rdma") ||
    // config_.enable_rdma) {
    //     try {
    //         server->init_ibv();
    //         LOG(INFO) << "RDMA initialized for server on " << endpoint;
    //     } catch (const std::exception& e) {
    //         LOG(WARNING) << "RDMA init failed, using TCP: " << e.what();
    //     }
    // }

    auto* server_ptr = server.get();
    servers_[endpoint] = std::move(server);

    return server_ptr;
}

bool ZmqCommunicator::bind(int socket_id, const std::string& endpoint) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return false;
    }

    // Create pattern if not exists
    if (!info->pattern) {
        info->pattern = createPattern(info->type, endpoint);
        if (!info->pattern) {
            LOG(ERROR) << "Failed to create pattern";
            return false;
        }
    }

    // Bind pattern
    if (!info->pattern->bind(endpoint)) {
        LOG(ERROR) << "Pattern bind failed";
        return false;
    }

    info->local_endpoint = endpoint;
    info->is_bound = true;

    LOG(INFO) << "Socket " << socket_id << " bound to " << endpoint;
    return true;
}

bool ZmqCommunicator::connect(int socket_id, const std::string& endpoint) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return false;
    }

    // Create pattern if not exists
    if (!info->pattern) {
        info->pattern = createPattern(info->type, "");
        if (!info->pattern) {
            LOG(ERROR) << "Failed to create pattern";
            return false;
        }
    }

    // Connect pattern
    if (!info->pattern->connect(endpoint)) {
        LOG(ERROR) << "Pattern connect failed";
        return false;
    }

    info->remote_endpoints.push_back(endpoint);

    LOG(INFO) << "Socket " << socket_id << " connected to " << endpoint;
    return true;
}

bool ZmqCommunicator::startServer(int socket_id) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return false;
    }

    if (!info->is_bound) {
        LOG(ERROR) << "Socket " << socket_id << " not bound";
        return false;
    }

    if (info->is_server_started) {
        LOG(WARNING) << "Server already started for socket " << socket_id;
        return true;
    }

    // Get or create server for this endpoint
    auto* server = getOrCreateServer(info->local_endpoint);
    if (!server) {
        LOG(ERROR) << "Failed to get/create server for endpoint "
                   << info->local_endpoint;
        return false;
    }

    // Register handlers before starting server
    if (info->type == ZmqSocketType::REP) {
        auto* rep_pattern = dynamic_cast<ReqRepPattern*>(info->pattern.get());
        if (rep_pattern) {
            rep_pattern->registerHandlers(server);
        }
    }

    // Start server
    auto ec = server->async_start();
    if (!ec.hasResult()) {
        info->is_server_started = true;
        LOG(INFO) << "Server started for socket " << socket_id << " on "
                  << info->local_endpoint;
        return true;
    } else {
        LOG(ERROR) << "Failed to start server";
        return false;
    }
}

async_simple::coro::Lazy<RpcResult> ZmqCommunicator::sendDataAsync(
    int socket_id, const void* data, size_t data_size,
    const std::optional<std::string>& topic,
    const std::optional<std::string>& target_endpoint) {
    SocketInfo* info;
    {
        std::lock_guard lock(sockets_mutex_);
        info = getSocketInfo(socket_id);
        if (!info || !info->pattern) {
            LOG(ERROR) << "Socket " << socket_id
                       << " not found or pattern not created";
            co_return RpcResult{-1, "Invalid socket"};
        }
    }

    std::string endpoint = target_endpoint.value_or("");
    auto result =
        co_await info->pattern->sendAsync(endpoint, data, data_size, topic);
    co_return result;
}

async_simple::coro::Lazy<int> ZmqCommunicator::sendTensorAsync(
    int socket_id, const TensorInfo& tensor,
    const std::optional<std::string>& topic,
    const std::optional<std::string>& target_endpoint) {
    SocketInfo* info;
    {
        std::lock_guard lock(sockets_mutex_);
        info = getSocketInfo(socket_id);
        if (!info || !info->pattern) {
            LOG(ERROR) << "Socket " << socket_id
                       << " not found or pattern not created";
            co_return -1;
        }
    }

    std::string endpoint = target_endpoint.value_or("");
    auto result =
        co_await info->pattern->sendTensorAsync(endpoint, tensor, topic);
    co_return result;
}

void ZmqCommunicator::setReceiveCallback(
    int socket_id, std::function<void(std::string_view, std::string_view,
                                      const std::optional<std::string>&)>
                       callback) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info || !info->pattern) {
        LOG(ERROR) << "Socket " << socket_id
                   << " not found or pattern not created";
        return;
    }

    info->pattern->setReceiveCallback(callback);
}

void ZmqCommunicator::setTensorReceiveCallback(
    int socket_id, std::function<void(std::string_view, const TensorInfo&,
                                      const std::optional<std::string>&)>
                       callback) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info || !info->pattern) {
        LOG(ERROR) << "Socket " << socket_id
                   << " not found or pattern not created";
        return;
    }

    info->pattern->setTensorReceiveCallback(callback);
}

bool ZmqCommunicator::subscribe(int socket_id, const std::string& topic) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info || !info->pattern) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return false;
    }

    if (info->type != ZmqSocketType::SUB) {
        LOG(ERROR) << "Socket is not SUB type";
        return false;
    }

    auto* sub_pattern = dynamic_cast<PubSubPattern*>(info->pattern.get());
    if (sub_pattern) {
        return sub_pattern->subscribe(topic);
    }

    return false;
}

bool ZmqCommunicator::unsubscribe(int socket_id, const std::string& topic) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info || !info->pattern) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return false;
    }

    if (info->type != ZmqSocketType::SUB) {
        LOG(ERROR) << "Socket is not SUB type";
        return false;
    }

    auto* sub_pattern = dynamic_cast<PubSubPattern*>(info->pattern.get());
    if (sub_pattern) {
        return sub_pattern->unsubscribe(topic);
    }

    return false;
}

void ZmqCommunicator::sendReply(int socket_id, const void* data,
                                size_t data_size) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info || !info->pattern) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return;
    }

    if (info->type != ZmqSocketType::REP) {
        LOG(ERROR) << "Socket is not REP type";
        return;
    }

    auto* rep_pattern = dynamic_cast<ReqRepPattern*>(info->pattern.get());
    if (rep_pattern) {
        rep_pattern->sendReply(data, data_size);
    }
}

void ZmqCommunicator::sendReplyTensor(int socket_id, const TensorInfo& tensor) {
    std::lock_guard lock(sockets_mutex_);

    auto* info = getSocketInfo(socket_id);
    if (!info || !info->pattern) {
        LOG(ERROR) << "Socket " << socket_id << " not found";
        return;
    }

    if (info->type != ZmqSocketType::REP) {
        LOG(ERROR) << "Socket is not REP type";
        return;
    }

    auto* rep_pattern = dynamic_cast<ReqRepPattern*>(info->pattern.get());
    if (rep_pattern) {
        rep_pattern->sendReplyTensor(tensor);
    }
}

ZmqCommunicator::SocketInfo* ZmqCommunicator::getSocketInfo(int socket_id) {
    auto it = sockets_.find(socket_id);
    if (it != sockets_.end()) {
        return &it->second;
    }
    return nullptr;
}

}  // namespace mooncake
