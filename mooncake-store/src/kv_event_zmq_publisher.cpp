#include "kv_event_zmq_publisher.h"

#include <glog/logging.h>
#include <zmq.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <mutex>

namespace mooncake {

namespace {

// Current wall-clock time in seconds since the epoch, matching the engine-side
// batch timestamp semantics Dynamo expects in payload frame 3.
double NowSeconds() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration<double>(now).count();
}

// Encode a 64-bit value as 8 big-endian bytes (frame 2 of the wire format).
std::array<char, 8> EncodeBigEndian64(uint64_t value) {
    std::array<char, 8> out{};
    for (int i = 7; i >= 0; --i) {
        out[i] = static_cast<char>(value & 0xFFu);
        value >>= 8;
    }
    return out;
}

}  // namespace

struct ZmqKvEventPublisher::Impl {
    explicit Impl(const ZmqKvEventPublisherConfig& config)
        : topic(config.topic),
          context(1),
          socket(context, zmq::socket_type::pub) {
        socket.set(zmq::sockopt::linger, config.linger_ms);
        if (config.send_hwm > 0) {
            socket.set(zmq::sockopt::sndhwm, config.send_hwm);
        }
        if (config.bind) {
            socket.bind(config.endpoint);
        } else {
            socket.connect(config.endpoint);
        }
        // Resolve the concrete endpoint (handles wildcard ports on bind).
        endpoint = socket.get(zmq::sockopt::last_endpoint);
        if (endpoint.empty()) {
            endpoint = config.endpoint;
        }
    }

    std::string topic;
    zmq::context_t context;
    zmq::socket_t socket;
    std::string endpoint;
    std::mutex send_mutex;
    std::atomic<uint64_t> seq{0};
    std::atomic<uint64_t> published{0};
};

ZmqKvEventPublisher::ZmqKvEventPublisher(
    const ZmqKvEventPublisherConfig& config)
    : impl_(std::make_unique<Impl>(config)) {
    LOG(INFO) << "ZmqKvEventPublisher started, endpoint=" << impl_->endpoint
              << ", topic='" << impl_->topic << "'";
}

ZmqKvEventPublisher::~ZmqKvEventPublisher() = default;

void ZmqKvEventPublisher::Publish(const KvEvent& event) {
    // Single-event batch: Mooncake centralizes KV state, so one PUB stream
    // carries every worker's events with worker_id encoded per event.
    std::optional<int32_t> dp_rank;
    if (event.dp_rank.has_value()) {
        dp_rank = static_cast<int32_t>(event.dp_rank.value());
    }
    const std::string payload =
        EncodeKvEventBatchPayload({event}, NowSeconds(), dp_rank);

    const uint64_t seq = impl_->seq.fetch_add(1);
    const std::array<char, 8> seq_frame = EncodeBigEndian64(seq);

    std::lock_guard<std::mutex> lock(impl_->send_mutex);
    try {
        impl_->socket.send(zmq::buffer(impl_->topic),
                           zmq::send_flags::sndmore);
        impl_->socket.send(zmq::buffer(seq_frame.data(), seq_frame.size()),
                           zmq::send_flags::sndmore);
        impl_->socket.send(zmq::buffer(payload), zmq::send_flags::none);
        impl_->published.fetch_add(1);
    } catch (const zmq::error_t& e) {
        LOG(ERROR) << "ZmqKvEventPublisher failed to publish event_id="
                   << event.event_id << ", error=" << e.what();
    }
}

const std::string& ZmqKvEventPublisher::endpoint() const {
    return impl_->endpoint;
}

uint64_t ZmqKvEventPublisher::published_count() const {
    return impl_->published.load();
}

}  // namespace mooncake
