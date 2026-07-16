#include "conductor/zmq/zmq_client.h"

#include <glog/logging.h>
#include <zmq_addon.hpp>

#include <iterator>
#include <utility>
#include <vector>

#include "conductor/zmq/msg_decoder.h"

namespace conductor {
namespace zmq {

namespace {

// 8-byte big-endian sequence number frames.
uint64_t BigEndianToU64(const unsigned char* b) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | b[i];
    }
    return v;
}

void U64ToBigEndian(uint64_t v, unsigned char* out) {
    for (int i = 7; i >= 0; --i) {
        out[i] = static_cast<unsigned char>(v & 0xFF);
        v >>= 8;
    }
}

}  // namespace

std::string ValidateConfig(const ZMQClientConfig& config) {
    if (config.endpoint.empty()) {
        return "endpoint is required";
    }
    return "";
}

ZMQClient::ZMQClient(ZMQClientConfig config,
                     std::shared_ptr<EventHandler> handler)
    : config_(std::move(config)),
      event_handler_(std::move(handler)),
      reconnect_delay_(config_.reconnect_delay) {}

ZMQClient::~ZMQClient() { Stop(); }

std::string ZMQClient::Start() {
    // Attempt initial connection
    if (auto err = Connect(); !err.empty()) {
        return "initial connection failed: " + err;
    }

    loop_thread_ = std::thread([this] { Loop(); });

    LOG(INFO) << "ZMQ client started service=" << config_.cache_pool_key
              << " endpoint=" << config_.endpoint << " publisher_kind="
              << common::PublisherKindName(config_.publisher_kind);
    return "";
}

void ZMQClient::Stop() {
    std::lock_guard<std::mutex> stop_lock(stop_mu_);
    stop_requested_.store(true);
    if (loop_thread_.joinable()) {
        loop_thread_.join();
    }

    {
        std::unique_lock lock(mu_);
        CleanupSocketsLocked();
    }

    LOG(INFO) << "ZMQ client stopped service=" << config_.cache_pool_key;
}

// Loop is the main background loop handling events and reconnections.
// Fixed reconnect interval, single loop structure.
void ZMQClient::Loop() {
    while (true) {
        // Check if we should stop
        if (stop_requested_.load()) {
            return;
        }

        // 1. If disconnected, wait for the delay then try to reconnect
        if (!IsConnected()) {
            HandleReconnect();
            continue;
        }

        // 2. If connected, consume events
        if (auto err = Consume(); !err.empty()) {
            LOG(ERROR) << "Consumption error service=" << config_.cache_pool_key
                       << " error=" << err;
            MarkDisconnected();
        }
    }
}

void ZMQClient::HandleReconnect() {
    LOG(INFO) << "Attempting to reconnect to the service. service="
              << config_.cache_pool_key
              << " reconnectDelay=" << reconnect_delay_.count() << "ms";

    // Poll the stop flag in slices so Stop() is honored within ~one poll
    // interval.
    const auto deadline =
        std::chrono::steady_clock::now() + config_.reconnect_delay;
    while (std::chrono::steady_clock::now() < deadline) {
        if (stop_requested_.load()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (auto err = Connect(); !err.empty()) {
        LOG(ERROR) << "Reconnect failed service=" << config_.cache_pool_key
                   << " error=" << err;
        return;
    }

    const int64_t last_seq = GetLastSequence();
    if (last_seq >= 0 && !config_.replay_endpoint.empty()) {
        LOG(INFO) << "Reconnected service=" << config_.cache_pool_key
                  << " resuming_from=" << (last_seq + 1);
        if (auto err = RequestReplay(last_seq + 1); !err.empty()) {
            LOG(WARNING) << "Failed to request replay after reconnect "
                            "service="
                         << config_.cache_pool_key << " error=" << err;
        }
    }
}

std::string ZMQClient::Connect() {
    std::unique_lock lock(mu_);

    if (connected_) {
        return "";
    }

    // Ensure clean state
    CleanupSocketsLocked();

    try {
        auto sock = std::make_unique<::zmq::socket_t>(zmq_context_,
                                                      ::zmq::socket_type::sub);
        // Enable IPv6 for dual-stack support
        sock->set(::zmq::sockopt::ipv6, 1);
        sock->connect(config_.endpoint);
        // Important: Subscribe to all topics
        sock->set(::zmq::sockopt::subscribe, "");

        sub_socket_ = std::move(sock);
        if (!config_.replay_endpoint.empty()) {
            auto replay_socket = std::make_unique<::zmq::socket_t>(
                zmq_context_, ::zmq::socket_type::dealer);
            replay_socket->set(::zmq::sockopt::ipv6, 1);
            replay_socket->connect(config_.replay_endpoint);
            replay_socket_ = std::move(replay_socket);
        }
        connected_ = true;

        reconnect_delay_ = config_.reconnect_delay;
    } catch (const ::zmq::error_t& e) {
        CleanupSocketsLocked();
        return std::string("failed to connect to ") + config_.endpoint + ": " +
               e.what();
    }

    LOG(INFO) << "Successfully connected to publisher service="
              << config_.cache_pool_key << " endpoint=" << config_.endpoint
              << " publisher_kind="
              << common::PublisherKindName(config_.publisher_kind)
              << " live_only=" << config_.replay_endpoint.empty();

    return "";
}

std::string ZMQClient::Consume() {
    // Grab the socket pointer under the read lock and poll outside the
    // lock; the socket is only destroyed by Stop() (after this thread
    // joins) or by Connect() on this same thread, so that is safe here.
    ::zmq::socket_t* socket;
    {
        std::shared_lock lock(mu_);
        socket = sub_socket_.get();
    }
    if (socket == nullptr) {
        return "socket is nil";
    }

    try {
        ::zmq::pollitem_t items[] = {{socket->handle(), 0, ZMQ_POLLIN, 0}};
        const int rc = ::zmq::poll(items, 1, config_.poll_timeout);
        if (rc == 0) {
            return "";  // No data, continue loop
        }
        if (!(items[0].revents & ZMQ_POLLIN)) {
            return "";
        }
    } catch (const ::zmq::error_t& e) {
        return std::string("poll error: ") + e.what();
    }

    if (auto err = ProcessMessage(); !err.empty()) {
        return "failed to process message: " + err;
    }

    return "";
}

std::string ZMQClient::ProcessMessage() {
    ::zmq::socket_t* socket;
    {
        std::shared_lock lock(mu_);
        socket = sub_socket_.get();
    }
    if (socket == nullptr) {
        return "socket is nil";
    }

    // Once the first frame is readable, the complete multipart message is
    // available. Consume it through the final frame so malformed frame counts
    // cannot block shutdown or leak a tail into the next message.
    std::vector<::zmq::message_t> frames;
    try {
        const auto frame_count = ::zmq::recv_multipart(
            *socket, std::back_inserter(frames), ::zmq::recv_flags::none);
        if (!frame_count) {
            return "failed to receive multipart message";
        }
    } catch (const ::zmq::error_t& e) {
        return std::string("recv error: ") + e.what();
    }

    if (frames.size() != 3) {
        return "invalid multipart frame count: expected 3, got " +
               std::to_string(frames.size());
    }

    auto& topic_msg = frames[0];
    auto& seq_msg = frames[1];
    auto& payload_msg = frames[2];

    if (seq_msg.size() != 8) {
        return "invalid sequence length";
    }
    const int64_t seq = static_cast<int64_t>(
        BigEndianToU64(static_cast<const unsigned char*>(seq_msg.data())));

    int64_t last_seq;
    {
        std::shared_lock lock(mu_);
        last_seq = last_seq_;
    }

    if (last_seq != -1 && seq > last_seq + 1) {
        LOG(WARNING) << "Event gap detected service=" << config_.cache_pool_key
                     << " missed=" << (seq - last_seq - 1)
                     << " last=" << last_seq << " current=" << seq;
        // BUG: seq gap detected but no automatic replay triggered.
    }

    // Update Sequence immediately to keep state fresh
    {
        std::unique_lock lock(mu_);
        last_seq_ = seq;
    }

    const std::string topic(static_cast<const char*>(topic_msg.data()),
                            topic_msg.size());
    const MessageMetadata metadata{
        .publisher_kind = config_.publisher_kind,
        .endpoint = config_.endpoint,
        .topic = topic,
        .sequence = seq,
    };

    DecodedBatch batch;
    std::string decode_error;
    if (config_.publisher_kind == common::PublisherKind::kMooncake) {
        auto decoded = DecodeMooncakeEventBatch(
            static_cast<const char*>(payload_msg.data()), payload_msg.size());
        if (decoded.ok) {
            batch = std::move(decoded.batch);
        } else {
            decode_error = std::move(decoded.error);
        }
    } else {
        auto decoded = DecodeVllmEventBatch(
            static_cast<const char*>(payload_msg.data()), payload_msg.size());
        if (decoded.ok) {
            batch = std::move(decoded.batch);
        } else {
            decode_error = std::move(decoded.error);
        }
    }
    if (!decode_error.empty()) {
        LOG(WARNING) << "Rejected KV event envelope endpoint="
                     << metadata.endpoint << " topic=" << metadata.topic
                     << " seq=" << metadata.sequence << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " error=" << decode_error;
        return "";
    }
    if (event_handler_ == nullptr) {
        return "event handler is nil";
    }
    if (auto err = event_handler_->HandleBatch(batch, metadata); !err.empty()) {
        LOG(ERROR) << "Handler error service=" << config_.cache_pool_key
                   << " endpoint=" << metadata.endpoint
                   << " topic=" << metadata.topic
                   << " seq=" << metadata.sequence << " error=" << err;
    }

    VLOG(1) << "Processed batch service=" << config_.cache_pool_key
            << " seq=" << seq << " topic=" << topic;
    return "";
}

std::string ZMQClient::RequestReplay(int64_t from_seq) {
    ::zmq::socket_t* socket;
    {
        std::shared_lock lock(mu_);
        socket = replay_socket_.get();
    }
    if (socket == nullptr) {
        return "replay socket is nil";
    }

    unsigned char req[8];
    U64ToBigEndian(static_cast<uint64_t>(from_seq), req);

    try {
        if (!socket->send(::zmq::buffer(req, sizeof(req)),
                          ::zmq::send_flags::none)) {
            return "failed to send replay request";
        }

        // Ideally we should wait for an ACK here if the protocol supports
        // it; read a response from the DEALER socket.
        socket->set(::zmq::sockopt::rcvtimeo,
                    static_cast<int>(config_.replay_timeout.count()));

        ::zmq::message_t resp;
        if (!socket->recv(resp, ::zmq::recv_flags::none)) {
            return "failed to receive replay response: resource temporarily "
                   "unavailable";
        }

        LOG(INFO) << "Replay requested service=" << config_.cache_pool_key
                  << " from=" << from_seq << " resp_len=" << resp.size();
    } catch (const ::zmq::error_t& e) {
        return std::string("failed to send replay request: ") + e.what();
    }
    return "";
}

void ZMQClient::CleanupSocketsLocked() {
    if (sub_socket_) {
        sub_socket_->close();
        sub_socket_.reset();
    }
    if (replay_socket_) {
        replay_socket_->close();
        replay_socket_.reset();
    }
    connected_ = false;
}

void ZMQClient::MarkDisconnected() {
    std::unique_lock lock(mu_);
    connected_ = false;
}

bool ZMQClient::IsConnected() const {
    std::shared_lock lock(mu_);
    return connected_;
}

int64_t ZMQClient::GetLastSequence() const {
    std::shared_lock lock(mu_);
    return last_seq_;
}

}  // namespace zmq
}  // namespace conductor
