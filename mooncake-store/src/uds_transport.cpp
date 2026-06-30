#include "uds_transport.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace mooncake {
namespace {

std::string errnoMessage(const std::string &operation) {
    return operation + ": " + strerror(errno);
}

tl::expected<void, std::string> makeAbstractAddress(
    const std::string &socket_name, sockaddr_un &addr, socklen_t &addr_len) {
    if (socket_name.size() > sizeof(addr.sun_path) - 2) {
        return tl::make_unexpected("UDS socket name too long: " +
                                   socket_name);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    addr.sun_path[0] = '\0';
    strncpy(addr.sun_path + 1, socket_name.c_str(), sizeof(addr.sun_path) - 2);
    addr_len = sizeof(sa_family_t) + 1 + socket_name.length();
    return {};
}

tl::expected<int, std::string> createConnectedSocket(
    const std::string &socket_name) {
    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        return tl::make_unexpected(errnoMessage("Failed to create UDS socket"));
    }

    sockaddr_un addr;
    socklen_t addr_len = 0;
    auto addr_result = makeAbstractAddress(socket_name, addr, addr_len);
    if (!addr_result) {
        close(sock_fd);
        return tl::make_unexpected(addr_result.error());
    }

    if (::connect(sock_fd, reinterpret_cast<sockaddr *>(&addr), addr_len) <
        0) {
        auto error = errnoMessage("Failed to connect UDS socket '" +
                                  socket_name + "'");
        close(sock_fd);
        return tl::make_unexpected(error);
    }

    return sock_fd;
}

}  // namespace

UdsConnection::UdsConnection(int fd) : fd_(fd) {}

UdsConnection::~UdsConnection() { close(); }

UdsConnection::UdsConnection(UdsConnection &&other) noexcept
    : fd_(other.release()) {}

UdsConnection &UdsConnection::operator=(UdsConnection &&other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.release();
    }
    return *this;
}

bool UdsConnection::valid() const { return fd_ >= 0; }

int UdsConnection::fd() const { return fd_; }

int UdsConnection::release() {
    int fd = fd_;
    fd_ = -1;
    return fd;
}

void UdsConnection::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

int UdsConnection::sendRaw(const void *data, size_t len) {
    const char *pos = static_cast<const char *>(data);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t sent = ::send(fd_, pos, remaining, 0);
        if (sent < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (sent <= 0) return -1;
        pos += sent;
        remaining -= static_cast<size_t>(sent);
    }
    return 0;
}

int UdsConnection::recvRaw(void *data, size_t len) {
    char *pos = static_cast<char *>(data);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t received = ::recv(fd_, pos, remaining, 0);
        if (received < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (received <= 0) return -1;
        pos += received;
        remaining -= static_cast<size_t>(received);
    }
    return 0;
}

int UdsConnection::sendFd(int fd, void *data, size_t data_len) {
    msghdr msg;
    memset(&msg, 0, sizeof(msg));
    iovec iov;
    char buf[CMSG_SPACE(sizeof(int))];
    memset(buf, 0, sizeof(buf));

    iov.iov_base = data;
    iov.iov_len = data_len;

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &fd, sizeof(int));

    while (true) {
        ssize_t sent = sendmsg(fd_, &msg, 0);
        if (sent < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        return sent < 0 ? -1 : 0;
    }
}

int UdsConnection::recvFd(void *data, size_t data_len) {
    msghdr msg;
    memset(&msg, 0, sizeof(msg));
    iovec iov;
    char buf[CMSG_SPACE(sizeof(int))];
    memset(buf, 0, sizeof(buf));

    iov.iov_base = data;
    iov.iov_len = data_len;

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    while (true) {
        ssize_t received = recvmsg(fd_, &msg, 0);
        if (received < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (received < 0) return -1;
        break;
    }

    cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET &&
        cmsg->cmsg_type == SCM_RIGHTS) {
        int received_fd = -1;
        memcpy(&received_fd, CMSG_DATA(cmsg), sizeof(int));
        return received_fd;
    }
    return -1;
}

UdsConnector::UdsConnector(std::string socket_name)
    : socket_name_(std::move(socket_name)) {}

tl::expected<std::unique_ptr<UdsConnection>, std::string>
UdsConnector::connect() {
    auto fd = createConnectedSocket(socket_name_);
    if (!fd) return tl::make_unexpected(fd.error());
    return std::make_unique<UdsConnection>(*fd);
}

UdsAcceptor::UdsAcceptor(std::string socket_name)
    : socket_name_(std::move(socket_name)) {}

UdsAcceptor::~UdsAcceptor() { stop(); }

void UdsAcceptor::registerHandler(Handler handler) {
    handler_ = std::move(handler);
}

tl::expected<void, std::string> UdsAcceptor::start() {
    if (running_.load()) return {};
    if (!handler_) {
        return tl::make_unexpected("UDS acceptor handler is not registered");
    }

    listen_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return tl::make_unexpected(errnoMessage("Failed to create UDS socket"));
    }

    sockaddr_un addr;
    socklen_t addr_len = 0;
    auto addr_result = makeAbstractAddress(socket_name_, addr, addr_len);
    if (!addr_result) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return tl::make_unexpected(addr_result.error());
    }

    if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), addr_len) < 0) {
        auto error =
            errnoMessage("Failed to bind UDS socket '" + socket_name_ + "'");
        ::close(listen_fd_);
        listen_fd_ = -1;
        return tl::make_unexpected(error);
    }

    if (listen(listen_fd_, 5) < 0) {
        auto error = errnoMessage("Failed to listen on UDS socket '" +
                                  socket_name_ + "'");
        ::close(listen_fd_);
        listen_fd_ = -1;
        return tl::make_unexpected(error);
    }

    running_ = true;
    thread_ = std::jthread([this]() { acceptLoop(); });
    return {};
}

void UdsAcceptor::stop() {
    if (!running_.exchange(false)) return;
    wakeAccept();
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void UdsAcceptor::acceptLoop() {
    while (running_.load()) {
        int client_sock = accept(listen_fd_, nullptr, nullptr);
        if (client_sock < 0) {
            if (running_.load()) {
                LOG(ERROR) << "Accept failed: " << strerror(errno);
            }
            continue;
        }

        UdsConnection connection(client_sock);
        if (!running_.load()) break;

        timeval tv = {.tv_sec = 5, .tv_usec = 0};
        setsockopt(connection.fd(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (handler_) handler_(connection);
    }

    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void UdsAcceptor::wakeAccept() {
    // accept() is a blocking syscall on the listener thread. Connecting to the
    // same abstract UDS name creates one local connection, wakes accept(), and
    // lets the loop observe running_ == false without relying on signals.
    auto fd = createConnectedSocket(socket_name_);
    if (fd) ::close(*fd);
}

}  // namespace mooncake
