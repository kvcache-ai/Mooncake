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

namespace mooncake::conductor::zmq {

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
    return (config.publisher_kind == common::PublisherKind::kVllm ||
            config.publisher_kind == common::PublisherKind::kSglang) &&
           !config.replay_endpoint.empty();
}

}  // namespace

std::string ValidateConfig(const ZMQClientConfig& config) {
    if (config.endpoint.empty()) {
        return "endpoint is required";
    }
    if (config.replay_timeout <= std::chrono::milliseconds::zero()) {
        return "replay_timeout must be positive";
    }
    if (config.replay_recovery_timeout <= std::chrono::milliseconds::zero()) {
        return "replay_recovery_timeout must be positive";
    }
    if (config.max_recovery_buffered_messages == 0) {
        return "max_recovery_buffered_messages must be positive";
    }
    if (config.max_recovery_buffered_bytes == 0) {
        return "max_recovery_buffered_bytes must be positive";
    }
    return "";
}

ZMQClient::ZMQClient(ZMQClientConfig config,
                     std::shared_ptr<EventHandler> handler)
    : config_(std::move(config)), event_handler_(std::move(handler)) {}

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
              << " reconnectDelay=" << config_.reconnect_delay.count() << "ms";

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
    bool stale;
    {
        std::shared_lock lock(mu_);
        last_seq = last_seq_;
        stale = stale_;
    }
    if (ReplayEnabled(config_) && last_seq >= 0 &&
        last_seq != std::numeric_limits<int64_t>::max() && !stale) {
        LOG(INFO) << "Reconnected service=" << config_.cache_pool_key
                  << " resuming_from=" << last_seq + 1;
        StartRecovery("connection was interrupted");
        if (auto err = AttemptRecovery(); !err.empty()) {
            LOG(ERROR) << "Failed to process replay after reconnect service="
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
                     << "; replay is supported only for vLLM and SGLang";
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
            return AttemptRecovery();
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
    const uint64_t raw_seq =
        BigEndianToU64(static_cast<const unsigned char*>(seq_msg.data()));
    if (raw_seq > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return "sequence exceeds int64 range";
    }
    const int64_t seq = static_cast<int64_t>(raw_seq);

    const std::string topic(static_cast<const char*>(topic_msg.data()),
                            topic_msg.size());
    int64_t last_live_seq;
    bool stale;
    {
        std::shared_lock lock(mu_);
        last_live_seq = last_live_seq_;
        stale = stale_;
    }

    if (stale) {
        VLOG(1) << "Dropping event from stale source service="
                << config_.cache_pool_key << " seq=" << seq;
        return "";
    }

    const bool new_gap = last_live_seq != -1 &&
                         last_live_seq != std::numeric_limits<int64_t>::max() &&
                         seq > last_live_seq + 1;
    if (new_gap) {
        const int64_t missed = seq - last_live_seq - 1;
        const int64_t total = dropped_events_.fetch_add(missed) + missed;
        const int64_t gaps = gap_count_.fetch_add(1) + 1;
        LOG(WARNING) << "Event gap detected service=" << config_.cache_pool_key
                     << " missed=" << missed << " last=" << last_live_seq
                     << " current=" << seq << " cumulative_dropped=" << total
                     << " gaps=" << gaps;
        if (!ReplayEnabled(config_)) {
            LOG(WARNING) << "No replay_endpoint configured; " << missed
                         << " events are permanently lost from the index "
                            "service="
                         << config_.cache_pool_key;
            MarkStale(
                "sequence gap cannot be recovered without a replay "
                "endpoint");
            return "";
        }
    }
    {
        std::unique_lock lock(mu_);
        last_live_seq_ = std::max(last_live_seq_, seq);
    }

    if (!ReplayEnabled(config_)) {
        if (auto err = DispatchMessage(
                topic, seq, static_cast<const char*>(payload_msg.data()),
                payload_msg.size());
            !err.empty()) {
            // The handler may have applied only part of the batch.  The
            // source therefore cannot safely advance or retry this sequence
            // without a full resynchronization.
            MarkStale("failed to dispatch live sequence " +
                      std::to_string(seq) + ": " + err);
            return "";
        }
        UpdateLastSequence(seq);
        return "";
    }

    std::string buffer_error;
    if (!BufferMessage({.topic = topic,
                        .sequence = seq,
                        .payload = std::string(
                            static_cast<const char*>(payload_msg.data()),
                            payload_msg.size())},
                       &buffer_error)) {
        if (!buffer_error.empty()) {
            MarkStale(buffer_error);
            return "";
        }
        return AttemptRecovery();
    }

    bool allow_initial_baseline = false;
    {
        std::shared_lock lock(mu_);
        allow_initial_baseline = last_seq_ == -1 && !recovery_in_progress_;
    }
    if (auto err = DrainBufferedMessages(allow_initial_baseline);
        !err.empty()) {
        return err;
    }

    bool gap_remains = false;
    {
        std::shared_lock lock(mu_);
        gap_remains = !buffered_messages_.empty() && last_seq_ >= 0 &&
                      last_seq_ != std::numeric_limits<int64_t>::max() &&
                      buffered_messages_.begin()->first > last_seq_ + 1;
    }
    if (gap_remains) {
        StartRecovery("live sequence gap detected");
    }
    return AttemptRecovery();
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
    } else if (config_.publisher_kind == common::PublisherKind::kSglang) {
        auto decoded = DecodeSglangEventBatch(payload, payload_size);
        if (decoded.ok) {
            batch = std::move(decoded.batch);
        } else {
            // A SGLang-backed Mooncake Store still uses the Mooncake map
            // envelope.  Native SGLang is attempted first; fall back to the
            // Mooncake decoder for that deployment mode.
            auto mooncake = DecodeMooncakeEventBatch(payload, payload_size);
            if (mooncake.ok) {
                batch = std::move(mooncake.batch);
            } else {
                decode_error = "SGLang decode failed: " + decoded.error +
                               "; Mooncake fallback failed: " + mooncake.error;
            }
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
        return "event handler failed: " + err;
    }

    VLOG(1) << "Processed batch service=" << config_.cache_pool_key
            << " seq=" << sequence << " topic=" << topic;
    return "";
}

void ZMQClient::UpdateLastSequence(int64_t sequence) {
    std::unique_lock lock(mu_);
    last_seq_ = std::max(last_seq_, sequence);
}

bool ZMQClient::BufferMessage(BufferedMessage message, std::string* error) {
    const size_t message_bytes = message.topic.size() + message.payload.size();
    std::unique_lock lock(mu_);
    if (stale_ || message.sequence <= last_seq_) {
        return false;
    }
    if (const auto existing = buffered_messages_.find(message.sequence);
        existing != buffered_messages_.end()) {
        if (existing->second.payload != message.payload) {
            *error = "conflicting payloads for sequence " +
                     std::to_string(message.sequence);
        }
        return false;
    }
    if (buffered_messages_.size() + 1 >
        config_.max_recovery_buffered_messages) {
        *error = "recovery message buffer limit exceeded";
        return false;
    }
    if (message_bytes > config_.max_recovery_buffered_bytes ||
        buffered_message_bytes_ >
            config_.max_recovery_buffered_bytes - message_bytes) {
        *error = "recovery byte buffer limit exceeded";
        return false;
    }
    buffered_message_bytes_ += message_bytes;
    buffered_messages_.emplace(message.sequence, std::move(message));
    return true;
}

std::string ZMQClient::DrainBufferedMessages(bool allow_initial_baseline) {
    while (true) {
        BufferedMessage message;
        {
            std::unique_lock lock(mu_);
            if (stale_ || buffered_messages_.empty()) return "";

            auto next = buffered_messages_.end();
            if (last_seq_ == -1) {
                if (!allow_initial_baseline) return "";
                next = buffered_messages_.begin();
            } else {
                if (last_seq_ == std::numeric_limits<int64_t>::max()) {
                    return "";
                }
                next = buffered_messages_.find(last_seq_ + 1);
                if (next == buffered_messages_.end()) return "";
            }

            message = std::move(next->second);
            buffered_message_bytes_ -=
                message.topic.size() + message.payload.size();
            buffered_messages_.erase(next);
        }

        if (auto err =
                DispatchMessage(message.topic, message.sequence,
                                message.payload.data(), message.payload.size());
            !err.empty()) {
            MarkStale("failed to dispatch buffered sequence " +
                      std::to_string(message.sequence) + ": " + err);
            return "";
        }
        UpdateLastSequence(message.sequence);
        allow_initial_baseline = false;
    }
}

void ZMQClient::StartRecovery(const std::string& reason) {
    bool started = false;
    {
        std::unique_lock lock(mu_);
        if (!stale_ && !recovery_in_progress_) {
            recovery_in_progress_ = true;
            recovery_deadline_ = std::chrono::steady_clock::now() +
                                 config_.replay_recovery_timeout;
            started = true;
        }
    }
    if (started) {
        LOG(WARNING) << "Starting bounded replay recovery service="
                     << config_.cache_pool_key << " reason=" << reason
                     << " timeout_ms="
                     << config_.replay_recovery_timeout.count();
    }
}

std::string ZMQClient::AttemptRecovery() {
    if (!ReplayEnabled(config_)) return "";

    while (true) {
        int64_t from_seq;
        std::optional<int64_t> until_seq;
        std::chrono::steady_clock::time_point deadline;
        {
            std::shared_lock lock(mu_);
            if (stale_ || !recovery_in_progress_) return "";
            deadline = recovery_deadline_;
            if (last_seq_ == std::numeric_limits<int64_t>::max()) {
                lock.unlock();
                MarkStale("sequence space exhausted during replay recovery");
                return "";
            }
            from_seq = last_seq_ + 1;
            if (!buffered_messages_.empty()) {
                until_seq = buffered_messages_.begin()->first;
            }
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            MarkStale("replay recovery deadline exceeded");
            return "";
        }

        // A message may have become contiguous after replay records from the
        // previous iteration were inserted.
        if (until_seq.has_value() && *until_seq == from_seq) {
            if (auto err = DrainBufferedMessages(); !err.empty()) return err;
            continue;
        }

        auto result = RequestReplay(from_seq, until_seq, deadline);
        if (!result.ok()) {
            LOG(WARNING) << "Replay recovery request failed service="
                         << config_.cache_pool_key << " from=" << from_seq
                         << (until_seq.has_value()
                                 ? " until=" + std::to_string(*until_seq)
                                 : "")
                         << " error=" << result.error;
            if (result.failure == ReplayFailure::kUnrecoverable ||
                std::chrono::steady_clock::now() >= deadline) {
                MarkStale("unable to recover sequence range starting at " +
                          std::to_string(from_seq) + ": " + result.error);
            }
            return "";
        }

        const int64_t before = GetLastSequence();
        for (auto& message : result.messages) {
            std::string buffer_error;
            if (!BufferMessage(std::move(message), &buffer_error) &&
                !buffer_error.empty()) {
                MarkStale(buffer_error);
                return "";
            }
        }
        if (auto err = DrainBufferedMessages(); !err.empty()) return err;

        bool gap_remains;
        {
            std::unique_lock lock(mu_);
            if (stale_) return "";
            gap_remains = !buffered_messages_.empty() && last_seq_ >= 0 &&
                          last_seq_ != std::numeric_limits<int64_t>::max() &&
                          buffered_messages_.begin()->first > last_seq_ + 1;
            if (!gap_remains) {
                recovery_in_progress_ = false;
            }
        }
        if (!gap_remains) {
            LOG(INFO) << "Replay recovery completed service="
                      << config_.cache_pool_key
                      << " last_sequence=" << GetLastSequence();
            return "";
        }
        if (GetLastSequence() == before) {
            MarkStale("replay completed without closing the sequence gap");
            return "";
        }
    }
}

void ZMQClient::MarkStale(const std::string& reason) {
    MessageMetadata metadata;
    {
        std::unique_lock lock(mu_);
        if (stale_) return;
        stale_ = true;
        stale_reason_ = reason;
        recovery_in_progress_ = false;
        buffered_messages_.clear();
        buffered_message_bytes_ = 0;
        metadata = {
            .publisher_kind = config_.publisher_kind,
            .endpoint = config_.endpoint,
            .topic = "",
            .sequence = last_seq_,
        };
    }

    LOG(ERROR) << "ZMQ event source marked stale service="
               << config_.cache_pool_key << " endpoint=" << config_.endpoint
               << " last_sequence=" << metadata.sequence
               << " reason=" << reason;
    if (event_handler_ != nullptr) {
        event_handler_->OnSourceStale(config_.cache_pool_key, metadata, reason);
    }
}

ZMQClient::ReplayResult ZMQClient::RequestReplay(
    int64_t from_seq, std::optional<int64_t> until_seq,
    std::chrono::steady_clock::time_point recovery_deadline) {
    ::zmq::socket_t* socket;
    {
        std::shared_lock lock(mu_);
        socket = replay_socket_.get();
    }
    if (socket == nullptr) {
        return {.error = "replay socket is nil",
                .failure = ReplayFailure::kRetryable};
    }

    auto fail = [this](std::string error, ReplayFailure failure) {
        if (auto reset_error = ResetReplaySocket(); !reset_error.empty()) {
            error += "; failed to reset replay socket: " + reset_error;
        }
        return ReplayResult{.error = std::move(error), .failure = failure};
    };

    unsigned char req[8];
    U64ToBigEndian(static_cast<uint64_t>(from_seq), req);

    try {
        // A DEALER must add the empty delimiter that a REQ socket would add
        // automatically. vLLM's ROUTER expects [identity, empty, from_seq].
        const std::string empty;
        const std::array<::zmq::const_buffer, 2> request = {
            ::zmq::buffer(empty),
            ::zmq::buffer(req, sizeof(req)),
        };
        if (!::zmq::send_multipart(*socket, request)) {
            return fail("failed to send replay request",
                        ReplayFailure::kRetryable);
        }

        size_t existing_messages;
        size_t existing_bytes;
        {
            std::shared_lock lock(mu_);
            existing_messages = buffered_messages_.size();
            existing_bytes = buffered_message_bytes_;
        }

        std::vector<BufferedMessage> messages;
        size_t replay_bytes = 0;
        int64_t next_expected = from_seq;
        while (true) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= recovery_deadline) {
                return fail("replay recovery deadline exceeded",
                            ReplayFailure::kRetryable);
            }
            auto receive_timeout =
                std::min(config_.replay_timeout,
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                             recovery_deadline - now));
            receive_timeout =
                std::max(receive_timeout, std::chrono::milliseconds(1));
            socket->set(
                ::zmq::sockopt::rcvtimeo,
                static_cast<int>(std::min<int64_t>(
                    receive_timeout.count(), std::numeric_limits<int>::max())));

            std::vector<::zmq::message_t> frames;
            const auto frame_count = ::zmq::recv_multipart(
                *socket, std::back_inserter(frames), ::zmq::recv_flags::none);
            if (!frame_count) {
                return fail("failed to receive replay response: timed out",
                            ReplayFailure::kRetryable);
            }
            // DEALER strips only the ROUTER identity. SGLang/older vLLM reply
            // [empty, sequence, payload]; newer vLLM adds topic after empty,
            // including an empty topic in its end marker.
            if ((frames.size() != 3 && frames.size() != 4) ||
                !frames[0].empty()) {
                return fail("invalid replay response frame count or delimiter",
                            ReplayFailure::kRetryable);
            }

            auto& seq_msg = frames[frames.size() - 2];
            auto& payload_msg = frames.back();
            if (seq_msg.size() != 8) {
                return fail("invalid replay sequence length",
                            ReplayFailure::kRetryable);
            }
            const uint64_t raw_seq = BigEndianToU64(
                static_cast<const unsigned char*>(seq_msg.data()));
            if (raw_seq == std::numeric_limits<uint64_t>::max()) {
                if (!payload_msg.empty()) {
                    return fail("invalid replay end marker",
                                ReplayFailure::kRetryable);
                }
                if (until_seq.has_value() && next_expected < *until_seq) {
                    return fail(
                        "replay buffer did not contain every missing sequence",
                        ReplayFailure::kUnrecoverable);
                }
                LOG(INFO) << "Replay completed service="
                          << config_.cache_pool_key << " from=" << from_seq
                          << " replayed=" << messages.size();
                return {.messages = std::move(messages)};
            }
            if (raw_seq >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                return fail("replay sequence exceeds int64 range",
                            ReplayFailure::kRetryable);
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
                return fail("replay response skipped a sequence",
                            ReplayFailure::kUnrecoverable);
            }
            if (replay_seq == std::numeric_limits<int64_t>::max()) {
                return fail("replay sequence cannot be incremented",
                            ReplayFailure::kUnrecoverable);
            }
            next_expected = replay_seq + 1;

            const size_t payload_size = payload_msg.size();
            const size_t topic_size = frames.size() == 4 ? frames[1].size() : 0;
            const bool message_limit_exceeded =
                existing_messages >= config_.max_recovery_buffered_messages ||
                messages.size() >=
                    config_.max_recovery_buffered_messages - existing_messages;
            const bool byte_limit_exceeded =
                existing_bytes > config_.max_recovery_buffered_bytes ||
                replay_bytes >
                    config_.max_recovery_buffered_bytes - existing_bytes ||
                payload_size > config_.max_recovery_buffered_bytes -
                                   existing_bytes - replay_bytes ||
                topic_size > config_.max_recovery_buffered_bytes -
                                 existing_bytes - replay_bytes - payload_size;
            if (message_limit_exceeded || byte_limit_exceeded) {
                return fail("replay response exceeds recovery buffer limits",
                            ReplayFailure::kUnrecoverable);
            }
            replay_bytes += payload_size + topic_size;
            messages.push_back(
                {.topic =
                     frames.size() == 4 ? frames[1].to_string() : std::string{},
                 .sequence = replay_seq,
                 .payload =
                     std::string(static_cast<const char*>(payload_msg.data()),
                                 payload_size)});
        }
    } catch (const ::zmq::error_t& e) {
        return fail(std::string("replay request failed: ") + e.what(),
                    ReplayFailure::kRetryable);
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

bool ZMQClient::IsStale() const {
    std::shared_lock lock(mu_);
    return stale_;
}

std::string ZMQClient::GetStaleReason() const {
    std::shared_lock lock(mu_);
    return stale_reason_;
}

}  // namespace mooncake::conductor::zmq
