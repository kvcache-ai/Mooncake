// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/intranode_nvlink_transport/vmm_fd_channel.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iomanip>
#include <random>
#include <sstream>

namespace mooncake {
namespace intranode_vmm {

namespace {

constexpr char kInfoMagic[8] = {'M', 'C', 'V', 'M', 'M', 'F', 'D', '1'};
constexpr size_t kInfoHeaderSize = sizeof(kInfoMagic) + 8 + 4 + 4;
constexpr size_t kLegacyIpcHandleSize = 64;  // sizeof(cudaIpcMemHandle_t)
constexpr size_t kMaxEndpointLength = 1024;

constexpr uint32_t kRequestMagic = 0x4d435646;  // "MCVF"
constexpr uint32_t kProtocolVersion = 1;
// SCM_RIGHTS accepts up to 253 fds per message on Linux.
constexpr size_t kFdsPerMessage = 64;
constexpr int kServerIoTimeoutMs = 5000;

struct FdRequest {
    uint32_t magic;
    uint32_t version;
    uint64_t buffer_id;
};

struct FdReply {
    uint32_t magic;
    int32_t status;
    uint32_t count;
    uint32_t reserved;
};

template <typename T>
void appendRaw(std::vector<unsigned char> &out, const T &value) {
    const auto *p = reinterpret_cast<const unsigned char *>(&value);
    out.insert(out.end(), p, p + sizeof(T));
}

template <typename T>
T readRaw(const unsigned char *p) {
    T value;
    memcpy(&value, p, sizeof(T));
    return value;
}

// Endpoints starting with '@' name an abstract socket.
bool makeAddress(const std::string &endpoint, sockaddr_un &addr,
                 socklen_t &len) {
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    if (endpoint.empty() || endpoint.size() >= sizeof(addr.sun_path)) {
        return false;
    }
    if (endpoint[0] == '@') {
        addr.sun_path[0] = '\0';
        memcpy(addr.sun_path + 1, endpoint.data() + 1, endpoint.size() - 1);
    } else {
        memcpy(addr.sun_path, endpoint.data(), endpoint.size());
    }
    len = offsetof(sockaddr_un, sun_path) + endpoint.size();
    return true;
}

void setIoTimeout(int fd, int timeout_ms) {
    timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

void closeAll(std::vector<int> &fds) {
    for (int fd : fds) {
        if (fd >= 0) close(fd);
    }
    fds.clear();
}

int sendReply(int fd, int32_t status, uint32_t count) {
    FdReply reply{kRequestMagic, status, count, 0};
    if (send(fd, &reply, sizeof(reply), MSG_NOSIGNAL) !=
        (ssize_t)sizeof(reply)) {
        return -1;
    }
    return 0;
}

int sendFds(int fd, const std::vector<int> &fds) {
    for (size_t sent = 0; sent < fds.size(); sent += kFdsPerMessage) {
        size_t n = std::min(kFdsPerMessage, fds.size() - sent);
        char byte = 0;
        iovec iov{&byte, 1};
        alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int) * kFdsPerMessage)];
        msghdr msg{};
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control;
        msg.msg_controllen = CMSG_SPACE(sizeof(int) * n);
        cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(sizeof(int) * n);
        memcpy(CMSG_DATA(cmsg), fds.data() + sent, sizeof(int) * n);
        if (sendmsg(fd, &msg, MSG_NOSIGNAL) != 1) return -1;
    }
    return 0;
}

}  // namespace

std::vector<unsigned char> encodeVmmBufferInfo(const VmmBufferInfo &info) {
    std::vector<unsigned char> out;
    if (info.chunks.empty() || info.endpoint.empty() ||
        info.endpoint.size() > kMaxEndpointLength) {
        return out;
    }
    out.insert(out.end(), kInfoMagic, kInfoMagic + sizeof(kInfoMagic));
    appendRaw(out, info.buffer_id);
    appendRaw(out, (uint32_t)info.chunks.size());
    appendRaw(out, (uint32_t)info.endpoint.size());
    out.insert(out.end(), info.endpoint.begin(), info.endpoint.end());
    for (const auto &chunk : info.chunks) {
        appendRaw(out, chunk.offset);
        appendRaw(out, chunk.size);
    }
    if (out.size() == kLegacyIpcHandleSize) out.clear();
    return out;
}

bool decodeVmmBufferInfo(const std::vector<unsigned char> &data,
                         VmmBufferInfo &info) {
    if (data.size() < kInfoHeaderSize ||
        memcmp(data.data(), kInfoMagic, sizeof(kInfoMagic)) != 0) {
        return false;
    }
    const unsigned char *p = data.data() + sizeof(kInfoMagic);
    uint64_t buffer_id = readRaw<uint64_t>(p);
    uint32_t num_chunks = readRaw<uint32_t>(p + 8);
    uint32_t endpoint_len = readRaw<uint32_t>(p + 12);
    if (num_chunks == 0 || endpoint_len == 0 ||
        endpoint_len > kMaxEndpointLength) {
        return false;
    }
    size_t expected =
        kInfoHeaderSize + endpoint_len + (size_t)num_chunks * 2 * 8;
    if (data.size() != expected) return false;
    p = data.data() + kInfoHeaderSize;
    info.buffer_id = buffer_id;
    info.endpoint.assign(reinterpret_cast<const char *>(p), endpoint_len);
    p += endpoint_len;
    info.chunks.resize(num_chunks);
    uint64_t next_offset = 0;
    for (uint32_t i = 0; i < num_chunks; ++i) {
        info.chunks[i].offset = readRaw<uint64_t>(p);
        info.chunks[i].size = readRaw<uint64_t>(p + 8);
        p += 16;
        // Chunks are contiguous and cover the buffer from offset 0.
        if (info.chunks[i].offset != next_offset || info.chunks[i].size == 0) {
            return false;
        }
        next_offset += info.chunks[i].size;
    }
    return true;
}

VmmFdServer::~VmmFdServer() { stop(); }

int VmmFdServer::start(const std::string &dir, ExportFn export_fn) {
    if (running()) return 0;
    std::random_device rd;
    uint64_t nonce = ((uint64_t)rd() << 32) | rd();
    std::ostringstream name;
    name << "mooncake-nvlink-" << getpid() << "-" << std::hex << std::setw(16)
         << std::setfill('0') << nonce;
    std::string endpoint =
        dir.empty() ? "@" + name.str() : dir + "/" + name.str() + ".sock";

    sockaddr_un addr;
    socklen_t addr_len = 0;
    if (!makeAddress(endpoint, addr, addr_len)) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: VMM fd server address "
                   << endpoint << " is too long";
        return -1;
    }
    int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        PLOG(ERROR) << "IntraNodeNvlinkTransport: VMM fd server socket failed";
        return -1;
    }
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), addr_len) != 0 ||
        listen(fd, 64) != 0) {
        PLOG(ERROR) << "IntraNodeNvlinkTransport: VMM fd server cannot listen "
                       "on "
                    << endpoint;
        close(fd);
        return -1;
    }
    if (pipe2(wake_fds_, O_CLOEXEC) != 0) {
        PLOG(ERROR) << "IntraNodeNvlinkTransport: VMM fd server pipe failed";
        close(fd);
        if (!dir.empty()) unlink(endpoint.c_str());
        return -1;
    }
    listen_fd_ = fd;
    endpoint_ = endpoint;
    socket_path_ = dir.empty() ? "" : endpoint;
    export_fn_ = std::move(export_fn);
    thread_ = std::thread(&VmmFdServer::serve, this);
    LOG(INFO) << "IntraNodeNvlinkTransport: VMM fd server listening on "
              << endpoint_;
    return 0;
}

void VmmFdServer::stop() {
    if (!running()) return;
    char byte = 0;
    if (write(wake_fds_[1], &byte, 1) < 0) {
        PLOG(WARNING)
            << "IntraNodeNvlinkTransport: VMM fd server wakeup failed";
    }
    if (thread_.joinable()) thread_.join();
    close(listen_fd_);
    close(wake_fds_[0]);
    close(wake_fds_[1]);
    listen_fd_ = -1;
    wake_fds_[0] = wake_fds_[1] = -1;
    if (!socket_path_.empty()) unlink(socket_path_.c_str());
    socket_path_.clear();
    endpoint_.clear();
}

void VmmFdServer::serve() {
    while (true) {
        pollfd pfds[2] = {{listen_fd_, POLLIN, 0}, {wake_fds_[0], POLLIN, 0}};
        int rc = poll(pfds, 2, -1);
        if (rc < 0) {
            if (errno == EINTR) continue;
            PLOG(ERROR)
                << "IntraNodeNvlinkTransport: VMM fd server poll failed";
            return;
        }
        if (pfds[1].revents) return;
        if (!(pfds[0].revents & POLLIN)) continue;
        int client = accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
        if (client < 0) continue;
        handleClient(client);
        close(client);
    }
}

void VmmFdServer::handleClient(int fd) {
    setIoTimeout(fd, kServerIoTimeoutMs);
    // Anyone who can reach the socket could map the memory, like a cudaIpc
    // handle. Limit that to the same user, or root.
    ucred cred{};
    socklen_t cred_len = sizeof(cred);
    if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &cred_len) != 0 ||
        (cred.uid != geteuid() && cred.uid != 0)) {
        LOG(WARNING) << "IntraNodeNvlinkTransport: VMM fd server rejected a "
                        "peer with uid "
                     << cred.uid;
        sendReply(fd, -EACCES, 0);
        return;
    }
    FdRequest request{};
    if (recv(fd, &request, sizeof(request), 0) != (ssize_t)sizeof(request) ||
        request.magic != kRequestMagic || request.version != kProtocolVersion) {
        sendReply(fd, -EPROTO, 0);
        return;
    }
    std::vector<int> fds;
    int status = export_fn_(request.buffer_id, fds);
    if (status != 0) {
        closeAll(fds);
        sendReply(fd, status < 0 ? status : -status, 0);
        return;
    }
    if (sendReply(fd, 0, (uint32_t)fds.size()) != 0 || sendFds(fd, fds) != 0) {
        PLOG(WARNING) << "IntraNodeNvlinkTransport: failed to send fds of VMM "
                         "buffer "
                      << request.buffer_id << " to pid " << cred.pid;
    }
    closeAll(fds);
}

int fetchVmmFds(const std::string &endpoint, uint64_t buffer_id,
                size_t expected, std::vector<int> &fds, int timeout_ms) {
    fds.clear();
    sockaddr_un addr;
    socklen_t addr_len = 0;
    if (!makeAddress(endpoint, addr, addr_len)) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: invalid VMM fd server address "
                   << endpoint;
        return -1;
    }
    int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        PLOG(ERROR) << "IntraNodeNvlinkTransport: socket failed";
        return -1;
    }
    setIoTimeout(fd, timeout_ms);
    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), addr_len) != 0) {
        PLOG(ERROR) << "IntraNodeNvlinkTransport: cannot connect to the VMM fd "
                       "server "
                    << endpoint
                    << ". An abstract socket ('@...') is only reachable from "
                       "the same network namespace (e.g. docker run "
                       "--network host); otherwise set MC_NVLINK_FD_DIR to a "
                       "directory both peers share";
        close(fd);
        return -1;
    }
    int rc = -1;
    FdRequest request{kRequestMagic, kProtocolVersion, buffer_id};
    FdReply reply{};
    if (send(fd, &request, sizeof(request), MSG_NOSIGNAL) !=
        (ssize_t)sizeof(request)) {
        PLOG(ERROR) << "IntraNodeNvlinkTransport: VMM fd request failed";
    } else if (recv(fd, &reply, sizeof(reply), 0) != (ssize_t)sizeof(reply) ||
               reply.magic != kRequestMagic) {
        PLOG(ERROR) << "IntraNodeNvlinkTransport: no valid reply from the VMM "
                       "fd server "
                    << endpoint;
    } else if (reply.status != 0) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: VMM fd server " << endpoint
                   << " refused buffer " << buffer_id << ": "
                   << strerror(-reply.status);
    } else if (reply.count != expected) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: VMM fd server sent "
                   << reply.count << " fds for buffer " << buffer_id
                   << ", expected " << expected;
    } else {
        rc = 0;
        while (fds.size() < expected) {
            char byte;
            iovec iov{&byte, 1};
            alignas(
                cmsghdr) char control[CMSG_SPACE(sizeof(int) * kFdsPerMessage)];
            msghdr msg{};
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            msg.msg_control = control;
            msg.msg_controllen = sizeof(control);
            ssize_t n = recvmsg(fd, &msg, MSG_CMSG_CLOEXEC);
            if (n != 1) {
                PLOG(ERROR) << "IntraNodeNvlinkTransport: receiving VMM fds "
                               "failed";
                rc = -1;
                break;
            }
            for (cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg;
                 cmsg = CMSG_NXTHDR(&msg, cmsg)) {
                if (cmsg->cmsg_level != SOL_SOCKET ||
                    cmsg->cmsg_type != SCM_RIGHTS) {
                    continue;
                }
                size_t count = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
                const int *received =
                    reinterpret_cast<const int *>(CMSG_DATA(cmsg));
                fds.insert(fds.end(), received, received + count);
            }
            if (msg.msg_flags & MSG_CTRUNC) {
                LOG(ERROR) << "IntraNodeNvlinkTransport: VMM fds were "
                              "truncated; check RLIMIT_NOFILE";
                rc = -1;
                break;
            }
        }
        if (rc == 0 && fds.size() != expected) rc = -1;
    }
    close(fd);
    if (rc != 0) closeAll(fds);
    return rc;
}

}  // namespace intranode_vmm
}  // namespace mooncake
