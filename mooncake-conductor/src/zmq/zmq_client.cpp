#include "conductor/zmq/zmq_client.h"

#include <glog/logging.h>
#include <zmq_addon.hpp>

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
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

bool ReplayEnabled(const ZMQClientConfig& config) {
    return config.publisher_kind == common::PublisherKind::kVllm &&
           !config.replay_endpoint.empty();
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

    int64_t last_seq;
    std::deque<ReplayRange> pending_ranges;
    {
        std::shared_lock lock(mu_);
        last_seq = last_seq_;
        pending_ranges = pending_replay_ranges_;
    }
    if (ReplayEnabled(config_) && (last_seq >= 0 || !pending_ranges.empty())) {
        bool replay_succeeded = true;
        for (const auto& range : pending_ranges) {
            LOG(INFO) << "Reconnected service=" << config_.cache_pool_key
                      << " resuming_from=" << range.from
                      << " resuming_until=" << range.until;
            if (auto err = RequestReplay(range.from, range.until);
                !err.empty()) {
                LOG(WARNING) << "Failed to request replay after reconnect "
                                "service="
                             << config_.cache_pool_key << " from=" << range.from
                             << " until=" << range.until << " error=" << err;
                replay_succeeded = false;
                break;
            }
            std::unique_lock lock(mu_);
            if (!pending_replay_ranges_.empty() &&
                pending_replay_ranges_.front().from == range.from &&
                pending_replay_ranges_.front().until == range.until) {
                pending_replay_ranges_.pop_front();
            }
        }
        if (replay_succeeded && last_seq >= 0) {
            const int64_t replay_from = last_seq + 1;
            LOG(INFO) << "Reconnected service=" << config_.cache_pool_key
                      << " resuming_from=" << replay_from;
            if (auto err = RequestReplay(replay_from); !err.empty()) {
                LOG(WARNING) << "Failed to request replay after reconnect "
                                "service="
                             << config_.cache_pool_key << " error=" << err;
            }
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
        // Set the receive HWM before connect; ZeroMQ applies it at connection
        // setup.
        if (config_.rcv_hwm > 0) {
            sock->set(::zmq::sockopt::rcvhwm, config_.rcv_hwm);
        }
        sock->connect(config_.endpoint);
        // Important: Subscribe to all topics
        sock->set(::zmq::sockopt::subscribe, "");

        sub_socket_ = std::move(sock);
        if (ReplayEnabled(config_)) {
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
              << " live_only=" << !ReplayEnabled(config_);
    if (!config_.replay_endpoint.empty() && !ReplayEnabled(config_)) {
        LOG(WARNING) << "Ignoring replay_endpoint for publisher kind="
                     << common::PublisherKindName(config_.publisher_kind)
                     << "; replay is supported only for vLLM";
    }

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

    const std::string topic(static_cast<const char*>(topic_msg.data()),
                            topic_msg.size());
    int64_t last_seq;
    {
        std::shared_lock lock(mu_);
        last_seq = last_seq_;
    }

    const bool new_gap = last_seq != -1 && seq > last_seq + 1;
    if (new_gap) {
        const int64_t missed = seq - last_seq - 1;
        const int64_t total = dropped_events_.fetch_add(missed) + missed;
        const int64_t gaps = gap_count_.fetch_add(1) + 1;
        LOG(WARNING) << "Event gap detected service=" << config_.cache_pool_key
                     << " missed=" << missed << " last=" << last_seq
                     << " current=" << seq << " cumulative_dropped=" << total
                     << " gaps=" << gaps;
        if (!ReplayEnabled(config_)) {
            LOG(WARNING) << "No replay_endpoint configured; " << missed
                         << " events are permanently lost from the index "
                            "service="
                         << config_.cache_pool_key;
        } else {
            std::unique_lock lock(mu_);
            pending_replay_ranges_.push_back({last_seq + 1, seq});
        }
    }

    if (ReplayEnabled(config_)) {
        while (true) {
            ReplayRange range;
            {
                std::shared_lock lock(mu_);
                if (pending_replay_ranges_.empty()) break;
                range = pending_replay_ranges_.front();
            }
            if (auto err = RequestReplay(range.from, range.until);
                !err.empty()) {
                LOG(WARNING) << "Gap replay request failed service="
                             << config_.cache_pool_key << " from=" << range.from
                             << " until=" << range.until << " error=" << err;
                break;
            }
            std::unique_lock lock(mu_);
            if (!pending_replay_ranges_.empty() &&
                pending_replay_ranges_.front().from == range.from &&
                pending_replay_ranges_.front().until == range.until) {
                pending_replay_ranges_.pop_front();
            }
        }
    }

    UpdateLastSequence(seq);
    return DispatchMessage(topic, seq,
                           static_cast<const char*>(payload_msg.data()),
                           payload_msg.size());
}

std::string ZMQClient::DispatchMessage(const std::string& topic,
                                       int64_t sequence, const char* payload,
                                       size_t payload_size) {
    const MessageMetadata metadata{
        .publisher_kind = config_.publisher_kind,
        .endpoint = config_.endpoint,
        .topic = topic,
        .sequence = sequence,
    };

    DecodedBatch batch;
    std::string decode_error;
    if (config_.publisher_kind == common::PublisherKind::kMooncake) {
        auto decoded = DecodeMooncakeEventBatch(payload, payload_size);
        if (decoded.ok) {
            batch = std::move(decoded.batch);
        } else {
            decode_error = std::move(decoded.error);
        }
    } else {
        auto decoded = DecodeVllmEventBatch(payload, payload_size);
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
            << " seq=" << sequence << " topic=" << topic;
    return "";
}

void ZMQClient::UpdateLastSequence(int64_t sequence) {
    std::unique_lock lock(mu_);
    last_seq_ = std::max(last_seq_, sequence);
}

std::string ZMQClient::RequestReplay(int64_t from_seq,
                                     std::optional<int64_t> until_seq) {
    ::zmq::socket_t* socket;
    {
        std::shared_lock lock(mu_);
        socket = replay_socket_.get();
    }
    if (socket == nullptr) {
        return "replay socket is nil";
    }

    auto fail = [this](std::string error) {
        if (auto reset_error = ResetReplaySocket(); !reset_error.empty()) {
            error += "; failed to reset replay socket: " + reset_error;
        }
        return error;
    };

    unsigned char req[8];
    U64ToBigEndian(static_cast<uint64_t>(from_seq), req);

    try {
        socket->set(::zmq::sockopt::rcvtimeo,
                    static_cast<int>(config_.replay_timeout.count()));

        // A DEALER must add the empty delimiter that a REQ socket would add
        // automatically. vLLM's ROUTER expects [identity, empty, from_seq].
        const std::string empty;
        const std::array<::zmq::const_buffer, 2> request = {
            ::zmq::buffer(empty),
            ::zmq::buffer(req, sizeof(req)),
        };
        if (!::zmq::send_multipart(*socket, request)) {
            return fail("failed to send replay request");
        }

        struct ReplayMessage {
            std::string topic;
            int64_t sequence;
            std::string payload;
        };
        std::vector<ReplayMessage> messages;
        int64_t next_expected = from_seq;
        while (true) {
            std::vector<::zmq::message_t> frames;
            const auto frame_count = ::zmq::recv_multipart(
                *socket, std::back_inserter(frames), ::zmq::recv_flags::none);
            if (!frame_count) {
                return fail("failed to receive replay response: timed out");
            }
            if (frames.size() != 4 || !frames[0].empty()) {
                return fail("invalid replay response frame count or delimiter");
            }

            auto& topic_msg = frames[1];
            auto& seq_msg = frames[2];
            auto& payload_msg = frames[3];
            if (seq_msg.size() != 8) {
                return fail("invalid replay sequence length");
            }
            const uint64_t raw_seq = BigEndianToU64(
                static_cast<const unsigned char*>(seq_msg.data()));
            if (raw_seq == std::numeric_limits<uint64_t>::max()) {
                if (!topic_msg.empty() || !payload_msg.empty()) {
                    return fail("invalid replay end marker");
                }
                if (until_seq.has_value() && next_expected < *until_seq) {
                    return fail(
                        "replay buffer did not contain every missing sequence");
                }
                for (const auto& message : messages) {
                    if (auto err = DispatchMessage(
                            message.topic, message.sequence,
                            message.payload.data(), message.payload.size());
                        !err.empty()) {
                        return fail("failed to process replay message: " + err);
                    }
                    UpdateLastSequence(message.sequence);
                }
                LOG(INFO) << "Replay completed service="
                          << config_.cache_pool_key << " from=" << from_seq
                          << " replayed=" << messages.size();
                return "";
            }
            if (raw_seq >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                return fail("replay sequence exceeds int64 range");
            }

            const int64_t replay_seq = static_cast<int64_t>(raw_seq);
            if (replay_seq < from_seq ||
                (until_seq.has_value() && replay_seq >= *until_seq)) {
                continue;
            }
            if (replay_seq < next_expected) {
                continue;
            }
            if (replay_seq > next_expected) {
                return fail("replay response skipped a sequence");
            }
            if (replay_seq == std::numeric_limits<int64_t>::max()) {
                return fail("replay sequence cannot be incremented");
            }
            next_expected = replay_seq + 1;

            messages.push_back(
                {.topic =
                     std::string(static_cast<const char*>(topic_msg.data()),
                                 topic_msg.size()),
                 .sequence = replay_seq,
                 .payload =
                     std::string(static_cast<const char*>(payload_msg.data()),
                                 payload_msg.size())});
        }
    } catch (const ::zmq::error_t& e) {
        return fail(std::string("replay request failed: ") + e.what());
    }
}

std::string ZMQClient::ResetReplaySocket() {
    std::unique_lock lock(mu_);
    if (replay_socket_) {
        try {
            replay_socket_->close();
        } catch (const ::zmq::error_t& e) {
            replay_socket_.reset();
            return e.what();
        }
        replay_socket_.reset();
    }
    if (!connected_ || !ReplayEnabled(config_)) {
        return "";
    }

    try {
        auto socket = std::make_unique<::zmq::socket_t>(
            zmq_context_, ::zmq::socket_type::dealer);
        socket->set(::zmq::sockopt::ipv6, 1);
        socket->connect(config_.replay_endpoint);
        replay_socket_ = std::move(socket);
    } catch (const ::zmq::error_t& e) {
        return e.what();
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
