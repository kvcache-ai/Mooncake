#include "uds_transport.h"

#include <glog/logging.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace mooncake {
namespace {

std::string errnoMessage(const std::string &operation) {
    return operation + ": " + strerror(errno);
}

int createCloexecUdsSocket() {
#ifdef SOCK_CLOEXEC
    return socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
#else
    // SOCK_CLOEXEC is Linux-specific; set FD_CLOEXEC after creation instead.
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd >= 0 && fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }
    return fd;
#endif
}

bool isFilesystemSocketName(const std::string &socket_name) {
    return !socket_name.empty() && socket_name[0] == '/';
}

tl::expected<void, std::string> makeSocketAddress(
    const std::string &socket_name, sockaddr_un &addr, socklen_t &addr_len) {
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;

    if (isFilesystemSocketName(socket_name)) {
        // Keep room for the terminating NUL that memset already provided.
        if (socket_name.size() >= sizeof(addr.sun_path)) {
            return tl::make_unexpected("UDS socket path too long: " +
                                       socket_name);
        }
        memcpy(addr.sun_path, socket_name.c_str(), socket_name.size());
        addr_len = offsetof(sockaddr_un, sun_path) + socket_name.size() + 1;
        return {};
    }

    if (socket_name.size() > sizeof(addr.sun_path) - 2) {
        return tl::make_unexpected("UDS socket name too long: " + socket_name);
    }
    addr.sun_path[0] = '\0';
    strncpy(addr.sun_path + 1, socket_name.c_str(), sizeof(addr.sun_path) - 2);
    addr_len = sizeof(sa_family_t) + 1 + socket_name.length();
    return {};
}

tl::expected<void, std::string> setSendTimeout(
    int fd, std::chrono::milliseconds timeout) {
    timeval tv = {
        .tv_sec = static_cast<decltype(tv.tv_sec)>(
            std::chrono::duration_cast<std::chrono::seconds>(timeout).count()),
        .tv_usec = static_cast<decltype(tv.tv_usec)>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                timeout % std::chrono::seconds(1))
                .count()),
    };
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        return tl::make_unexpected(
            errnoMessage("Failed to set UDS send timeout"));
    }
    return {};
}

// A filesystem socket left behind by a crashed process makes bind() fail with
// EADDRINUSE. Remove it only when connecting is refused (nobody listening), so
// a live server, a socket we cannot probe, or an unrelated file is never
// clobbered.
tl::expected<void, std::string> removeStaleSocketFile(
    const std::string &socket_path) {
    struct stat st;
    if (lstat(socket_path.c_str(), &st) < 0) {
        if (errno == ENOENT) return {};
        return tl::make_unexpected(errnoMessage(
            "Failed to stat UDS socket path '" + socket_path + "'"));
    }
    if (!S_ISSOCK(st.st_mode)) {
        return tl::make_unexpected("UDS socket path '" + socket_path +
                                   "' exists and is not a socket");
    }

    sockaddr_un addr;
    socklen_t addr_len = 0;
    auto addr_result = makeSocketAddress(socket_path, addr, addr_len);
    if (!addr_result) return addr_result;

    int probe_fd = createCloexecUdsSocket();
    if (probe_fd < 0) {
        return tl::make_unexpected(errnoMessage("Failed to create UDS socket"));
    }
    // Bound the probe in case a live owner's backlog is full.
    auto timeout_result = setSendTimeout(probe_fd, std::chrono::seconds(1));
    if (!timeout_result) {
        ::close(probe_fd);
        return tl::make_unexpected(timeout_result.error());
    }
    int connect_result =
        ::connect(probe_fd, reinterpret_cast<sockaddr *>(&addr), addr_len);
    int connect_errno = errno;
    ::close(probe_fd);
    if (connect_result == 0) {
        return tl::make_unexpected("UDS socket path '" + socket_path +
                                   "' is already in use by another process");
    }
    // The file disappeared after lstat(); nothing left to clean up.
    if (connect_errno == ENOENT) return {};
    if (connect_errno != ECONNREFUSED) {
        errno = connect_errno;
        return tl::make_unexpected(errnoMessage(
            "Failed to probe existing UDS socket '" + socket_path + "'"));
    }

    if (unlink(socket_path.c_str()) < 0 && errno != ENOENT) {
        return tl::make_unexpected(errnoMessage(
            "Failed to remove stale UDS socket '" + socket_path + "'"));
    }
    LOG(INFO) << "Removed stale UDS socket file: " << socket_path;
    return {};
}

// Takes an exclusive flock on "<socket_path>.lock" and returns its fd. Every
// acceptor takes this lock before probing, removing, or binding the socket
// path and holds it until stop(), so a probe that gets ECONNREFUSED can only
// see a dead owner: a live owner between bind() and listen() still holds the
// lock. The kernel drops the lock when its holder exits, so a crash never
// leaves the path locked. The lock file itself is never removed, because two
// starters could otherwise lock different inodes.
tl::expected<int, std::string> acquireOwnershipLock(
    const std::string &socket_path) {
    const std::string lock_path = socket_path + ".lock";
    int fd = ::open(lock_path.c_str(),
                    O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0660);
    if (fd < 0) {
        return tl::make_unexpected(
            errnoMessage("Failed to open UDS lock file '" + lock_path + "'"));
    }
    while (flock(fd, LOCK_EX | LOCK_NB) < 0) {
        if (errno == EINTR) continue;
        int lock_errno = errno;
        ::close(fd);
        if (lock_errno == EWOULDBLOCK) {
            return tl::make_unexpected("UDS socket path '" + socket_path +
                                       "' is already in use by another "
                                       "acceptor");
        }
        errno = lock_errno;
        return tl::make_unexpected(
            errnoMessage("Failed to lock UDS lock file '" + lock_path + "'"));
    }
    return fd;
}

tl::expected<int, std::string> createConnectedSocket(
    const std::string &socket_name, std::chrono::milliseconds connect_timeout) {
    if (connect_timeout.count() <= 0) {
        return tl::make_unexpected("UDS connect timeout must be positive");
    }

    int sock_fd = createCloexecUdsSocket();
    if (sock_fd < 0) {
        return tl::make_unexpected(errnoMessage("Failed to create UDS socket"));
    }

    sockaddr_un addr;
    socklen_t addr_len = 0;
    auto addr_result = makeSocketAddress(socket_name, addr, addr_len);
    if (!addr_result) {
        close(sock_fd);
        return tl::make_unexpected(addr_result.error());
    }

    auto timeout_result = setSendTimeout(sock_fd, connect_timeout);
    if (!timeout_result) {
        close(sock_fd);
        return tl::make_unexpected(timeout_result.error());
    }

    if (::connect(sock_fd, reinterpret_cast<sockaddr *>(&addr), addr_len) < 0) {
        const int connect_errno = errno;
        auto error =
            connect_errno == EAGAIN || connect_errno == EINPROGRESS
                ? "Timed out connecting UDS socket '" + socket_name + "'"
                : errnoMessage("Failed to connect UDS socket '" + socket_name +
                               "'");
        if (connect_errno == EACCES) {
            // Filesystem sockets are mode 0660; see UdsAcceptor::start().
            error +=
                " (run this process with the same uid as the socket owner or "
                "in its group)";
        }
        close(sock_fd);
        return tl::make_unexpected(error);
    }

    auto clear_timeout_result =
        setSendTimeout(sock_fd, std::chrono::milliseconds(0));
    if (!clear_timeout_result) {
        close(sock_fd);
        return tl::make_unexpected(clear_timeout_result.error());
    }

    return sock_fd;
}

}  // namespace

std::string resolveClientIpcSocketName(const std::string &configured,
                                       int port) {
    if (!configured.empty()) return configured;
    const char *from_env = std::getenv(kIpcSocketPathEnv);
    if (from_env != nullptr && from_env[0] != '\0') return from_env;
    return "@mooncake_client_" + std::to_string(port) + ".sock";
}

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

tl::expected<void, std::string> UdsConnection::setRecvTimeout(
    std::chrono::seconds timeout) {
    timeval tv = {.tv_sec = timeout.count(), .tv_usec = 0};
    if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        return tl::make_unexpected(
            errnoMessage("Failed to set UDS recv timeout"));
    }
    return {};
}

int UdsConnection::sendRaw(const void *data, size_t len) {
    const char *pos = static_cast<const char *>(data);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t sent = ::send(fd_, pos, remaining, 0);
        if (sent < 0 && errno == EINTR) continue;
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
        if (received < 0 && errno == EINTR) continue;
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
        if (sent < 0 && errno == EINTR) continue;
        if (sent < 0) return -1;
        return sent == static_cast<ssize_t>(data_len) ? 0 : -1;
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

    ssize_t received = 0;
    while (true) {
        received = recvmsg(fd_, &msg, 0);
        if (received < 0 && errno == EINTR) continue;
        if (received <= 0) return -1;
        break;
    }

    cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET &&
        cmsg->cmsg_type == SCM_RIGHTS) {
        int received_fd = -1;
        memcpy(&received_fd, CMSG_DATA(cmsg), sizeof(int));
        // Even for malformed messages, close any delivered fd instead of
        // leaking it into this process.
        if (received != static_cast<ssize_t>(data_len) ||
            (msg.msg_flags & (MSG_TRUNC | MSG_CTRUNC))) {
            ::close(received_fd);
            return -1;
        }
        return received_fd;
    }
    return -1;
}

UdsConnector::UdsConnector(std::string socket_name,
                           std::chrono::milliseconds connect_timeout)
    : socket_name_(std::move(socket_name)), connect_timeout_(connect_timeout) {}

tl::expected<std::unique_ptr<UdsConnection>, std::string>
UdsConnector::connect() {
    auto fd = createConnectedSocket(socket_name_, connect_timeout_);
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

    listen_fd_ = createCloexecUdsSocket();
    if (listen_fd_ < 0) {
        return tl::make_unexpected(errnoMessage("Failed to create UDS socket"));
    }

    sockaddr_un addr;
    socklen_t addr_len = 0;
    auto addr_result = makeSocketAddress(socket_name_, addr, addr_len);
    if (!addr_result) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return tl::make_unexpected(addr_result.error());
    }

    const bool is_filesystem = isFilesystemSocketName(socket_name_);
    if (is_filesystem) {
        auto lock_result = acquireOwnershipLock(socket_name_);
        if (!lock_result) {
            ::close(listen_fd_);
            listen_fd_ = -1;
            return tl::make_unexpected(lock_result.error());
        }
        lock_fd_ = lock_result.value();

        auto cleanup_result = removeStaleSocketFile(socket_name_);
        if (!cleanup_result) {
            ::close(listen_fd_);
            listen_fd_ = -1;
            releaseOwnershipLock();
            return tl::make_unexpected(cleanup_result.error());
        }
    }

    if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), addr_len) < 0) {
        auto error =
            errnoMessage("Failed to bind UDS socket '" + socket_name_ + "'");
        ::close(listen_fd_);
        listen_fd_ = -1;
        releaseOwnershipLock();
        return tl::make_unexpected(error);
    }

    if (is_filesystem) {
        bound_socket_path_ = socket_name_;
        // The endpoint accepts shared-memory fds and hands out the hot-cache
        // fd without further authentication, so only the owner's uid and gid
        // may connect. Peers in other pods must share one of them. Nothing can
        // connect before listen(), so the mode is final by the time it matters.
        if (chmod(socket_name_.c_str(), 0660) < 0) {
            auto error = errnoMessage("Failed to chmod UDS socket '" +
                                      socket_name_ + "'");
            ::close(listen_fd_);
            listen_fd_ = -1;
            unlinkBoundPath();
            releaseOwnershipLock();
            return tl::make_unexpected(error);
        }
    }

    if (listen(listen_fd_, 5) < 0) {
        auto error = errnoMessage("Failed to listen on UDS socket '" +
                                  socket_name_ + "'");
        ::close(listen_fd_);
        listen_fd_ = -1;
        unlinkBoundPath();
        releaseOwnershipLock();
        return tl::make_unexpected(error);
    }

    running_ = true;
    thread_ = std::thread([this]() { acceptLoop(); });
    return {};
}

void UdsAcceptor::stop() {
    if (!running_.exchange(false)) return;
    // Remove the path while still listening: once the listener closes, a
    // successor could treat the file as stale and rebind it, and unlinking
    // afterwards would delete the successor's socket.
    unlinkBoundPath();
    wakeAccept();
    int active_client_fd = active_client_fd_.load();
    if (active_client_fd >= 0) {
        // Stop should unblock handlers waiting on either read or write.
        ::shutdown(active_client_fd, SHUT_RDWR);
    }
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    // Release last, after the path is gone, so a successor that takes the lock
    // never sees this acceptor's socket.
    releaseOwnershipLock();
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

        active_client_fd_ = connection.fd();
        if (handler_) handler_(connection);
        active_client_fd_ = -1;
    }
}

void UdsAcceptor::wakeAccept() {
    // accept() is a blocking syscall on the listener thread. Shutting down the
    // listener wakes accept() so the loop can observe running_ == false.
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
    }
}

void UdsAcceptor::unlinkBoundPath() {
    if (bound_socket_path_.empty()) return;
    if (unlink(bound_socket_path_.c_str()) < 0 && errno != ENOENT) {
        LOG(WARNING) << "Failed to remove UDS socket file "
                     << bound_socket_path_ << ": " << strerror(errno);
    }
    bound_socket_path_.clear();
}

void UdsAcceptor::releaseOwnershipLock() {
    if (lock_fd_ < 0) return;
    // Closing the fd drops the flock.
    ::close(lock_fd_);
    lock_fd_ = -1;
}

}  // namespace mooncake
