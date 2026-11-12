#pragma once

#include <zmq.h>

#include <cerrno>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mooncake::conductor::zmq::detail {

class Error : public std::runtime_error {
   public:
    Error() : std::runtime_error(zmq_strerror(zmq_errno())) {}
};

inline int Check(int result) {
    if (result < 0) throw Error();
    return result;
}

// Owners are deliberately noncopyable: sockets must be closed before their
// context, and used by only one thread at a time.
class Context {
   public:
    explicit Context(int threads = 1) : context_(zmq_ctx_new()) {
        if (!context_) throw Error();
        if (zmq_ctx_set(context_, ZMQ_IO_THREADS, threads) < 0) {
            const Error error;
            zmq_ctx_term(context_);
            throw error;
        }
    }
    ~Context() {
        while (zmq_ctx_term(context_) < 0 && zmq_errno() == EINTR) {
        }
    }
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;
    void* handle() const { return context_; }

   private:
    void* context_;
};

class Message {
   public:
    Message() { Check(zmq_msg_init(&message_)); }
    ~Message() { zmq_msg_close(&message_); }
    Message(const Message&) = delete;
    Message& operator=(const Message&) = delete;
    Message(Message&& other) : Message() {
        Check(zmq_msg_move(&message_, &other.message_));
    }
    void* data() { return zmq_msg_data(&message_); }
    size_t size() const { return zmq_msg_size(&message_); }
    bool empty() const { return size() == 0; }
    std::string to_string() {
        return std::string(static_cast<const char*>(data()), size());
    }
    bool more() const { return zmq_msg_more(&message_) != 0; }
    std::optional<size_t> Receive(void* socket) {
        const int result = zmq_msg_recv(&message_, socket, 0);
        if (result < 0 && zmq_errno() == EAGAIN) return std::nullopt;
        return static_cast<size_t>(Check(result));
    }

   private:
    zmq_msg_t message_;
};

class Socket {
   public:
    Socket(Context& context, int type)
        : socket_(zmq_socket(context.handle(), type)) {
        if (!socket_) throw Error();
        // Discard queued replay requests on shutdown/reset. Otherwise an
        // unreachable peer can keep context termination blocked indefinitely.
        const int linger = 0;
        if (zmq_setsockopt(socket_, ZMQ_LINGER, &linger, sizeof(linger)) < 0) {
            const Error error;
            zmq_close(socket_);
            throw error;
        }
    }
    ~Socket() { close(); }
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    void close() noexcept {
        if (socket_) {
            zmq_close(socket_);
            socket_ = nullptr;
        }
    }
    void* handle() const { return socket_; }
    void set(int option, int value) {
        Check(zmq_setsockopt(socket_, option, &value, sizeof(value)));
    }
    void SubscribeAll() {
        Check(zmq_setsockopt(socket_, ZMQ_SUBSCRIBE, "", 0));
    }
    void connect(const std::string& endpoint) {
        Check(zmq_connect(socket_, endpoint.c_str()));
    }
    void bind(const std::string& endpoint) {
        Check(zmq_bind(socket_, endpoint.c_str()));
    }
    std::string Endpoint() {
        char endpoint[256];
        size_t size = sizeof(endpoint);
        Check(zmq_getsockopt(socket_, ZMQ_LAST_ENDPOINT, endpoint, &size));
        return std::string(endpoint, size ? size - 1 : 0);
    }
    std::optional<size_t> recv(Message& message) {
        return message.Receive(socket_);
    }

   private:
    void* socket_;
};

inline std::string_view Buffer(std::string_view value) { return value; }
inline std::string_view Buffer(const void* data, size_t size) {
    return {static_cast<const char*>(data), size};
}

template <typename Frames>
bool SendMultipart(Socket& socket, const Frames& frames) {
    size_t index = 0;
    for (const auto& frame : frames) {
        const int flags = ++index < frames.size() ? ZMQ_SNDMORE : 0;
        const int result =
            zmq_send(socket.handle(), frame.data(), frame.size(), flags);
        if (result < 0 && zmq_errno() == EAGAIN) return false;
        Check(result);
    }
    return true;
}

inline bool ReceiveMultipart(Socket& socket, std::vector<Message>& frames) {
    do {
        Message message;
        if (!socket.recv(message)) return false;
        const bool more = message.more();
        frames.push_back(std::move(message));
        if (!more) return true;
    } while (true);
}

}  // namespace mooncake::conductor::zmq::detail
