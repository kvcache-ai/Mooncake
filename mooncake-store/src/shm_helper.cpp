#include "shm_helper.h"

#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <glog/logging.h>

#include "utils.h"

#ifndef MOONCAKE_SHM_NAME
#define MOONCAKE_SHM_NAME "mooncake_shm"
#endif

namespace mooncake {

std::mutex ShmHelper::shm_mutex_;

static int memfd_create_wrapper(const char* name, unsigned int flags) {
#ifdef __NR_memfd_create
    return syscall(__NR_memfd_create, name, flags);
#else
    return -1;
#endif
}

ShmHelper* ShmHelper::getInstance() {
    static ShmHelper instance;
    return &instance;
}

ShmHelper::ShmHelper() {
    const char* hp = std::getenv("MC_STORE_USE_HUGEPAGE");
    use_hugepage_ = (hp != nullptr);
}

ShmHelper::~ShmHelper() { cleanup(); }

bool ShmHelper::cleanup() {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    bool ret = true;
    for (auto& shm : shms_) {
        if (shm->fd != -1) {
            close(shm->fd);
            shm->fd = -1;
        }
        if (shm->base_addr) {
            if (munmap(shm->base_addr, shm->size) == -1) {
                LOG(ERROR) << "Failed to unmap shared memory: "
                           << strerror(errno);
                ret = false;
            }
            shm->base_addr = nullptr;
        }
    }
    shms_.clear();
    return ret;
}

void* ShmHelper::allocate(size_t size) {
    std::lock_guard<std::mutex> lock(shm_mutex_);

    unsigned int flags = MFD_CLOEXEC;
    if (use_hugepage_) {
        bool use_memfd = true;
        size = align_up(size, get_hugepage_size_from_env(&flags, use_memfd));
        LOG(INFO) << "Using huge pages for shared memory, size: " << size;
    }

    int fd = memfd_create_wrapper(MOONCAKE_SHM_NAME, flags);
    if (fd == -1) {
        std::string extra_msg =
            use_hugepage_ ? " (Check /proc/sys/vm/nr_hugepages?)" : "";
        throw std::runtime_error("Failed to create anonymous shared memory" +
                                 extra_msg + ": " +
                                 std::string(strerror(errno)));
    }

    if (ftruncate(fd, size) == -1) {
        close(fd);
        throw std::runtime_error("Failed to set shared memory size: " +
                                 std::string(strerror(errno)));
    }

    void* base_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_POPULATE, fd, 0);
    if (base_addr == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Failed to map shared memory: " +
                                 std::string(strerror(errno)));
    }

    auto shm = std::make_shared<ShmSegment>();
    shm->fd = fd;
    shm->base_addr = base_addr;
    shm->size = size;
    shm->name = MOONCAKE_SHM_NAME;
    shm->registered = false;
    shms_.push_back(shm);

    return base_addr;
}

int ShmHelper::free(void* addr) {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    for (auto it = shms_.begin(); it != shms_.end(); ++it) {
        if ((*it)->base_addr == addr) {
            if ((*it)->fd != -1) {
                close((*it)->fd);
            }
            if ((*it)->base_addr &&
                munmap((*it)->base_addr, (*it)->size) == -1) {
                LOG(ERROR) << "Failed to unmap shared memory during free: "
                           << strerror(errno);
                return -1;
            }
            LOG(INFO) << "Freed shared memory at " << addr
                      << ", size: " << (*it)->size;
            shms_.erase(it);
            return 0;
        }
    }
    LOG(ERROR) << "Attempted to free unknown shared memory address: " << addr;
    return -1;
}

std::shared_ptr<ShmHelper::ShmSegment> ShmHelper::get_shm(void* addr) {
    std::lock_guard<std::mutex> lock(shm_mutex_);
    for (auto& shm : shms_) {
        if (addr >= shm->base_addr &&
            reinterpret_cast<uint8_t*>(addr) <
                reinterpret_cast<uint8_t*>(shm->base_addr) + shm->size) {
            return shm;
        }
    }
    return nullptr;
}

/* ================================================================== */
/*  ipc_send_fd / ipc_recv_fd: SCM_RIGHTS fd passing over Unix socket */
/* ================================================================== */

int ipc_send_fd(int socket, int fd, void* data, size_t data_len) {
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    struct iovec iov;
    char buf[CMSG_SPACE(sizeof(int))];
    memset(buf, 0, sizeof(buf));

    iov.iov_base = data;
    iov.iov_len = data_len;

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &fd, sizeof(int));

    return sendmsg(socket, &msg, 0);
}

int ipc_recv_fd(int socket, void* data, size_t data_len) {
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    struct iovec iov;
    char buf[CMSG_SPACE(sizeof(int))];
    memset(buf, 0, sizeof(buf));

    iov.iov_base = data;
    iov.iov_len = data_len;

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    if (recvmsg(socket, &msg, 0) < 0) return -1;

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET &&
        cmsg->cmsg_type == SCM_RIGHTS) {
        int fd;
        memcpy(&fd, CMSG_DATA(cmsg), sizeof(int));
        return fd;
    }
    return -1;
}

}  // namespace mooncake
