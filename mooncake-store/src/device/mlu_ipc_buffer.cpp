#include "device/mlu_ipc_buffer.h"

#include <glog/logging.h>

#include <limits>
#include <utility>

#ifdef USE_MLU
#include <cn_api.h>
#include <cnrt.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#endif

namespace mooncake::device {
namespace {

#ifdef USE_MLU
// OFD locks also serialize separate opens in the same process. Byte 0 proves
// the exporter is alive; byte 1 protects active imports against retirement.
bool Lock(int fd, short type, off_t start, bool wait = false) {
    struct flock lock{};
    lock.l_type = type;
    lock.l_whence = SEEK_SET;
    lock.l_start = start;
    lock.l_len = 1;
    int rc;
    do {
        rc = fcntl(fd, wait ? F_OFD_SETLKW : F_OFD_SETLK, &lock);
    } while (rc < 0 && errno == EINTR);
    return rc == 0;
}

bool ValidLeaseName(const std::string &name) {
    constexpr auto prefix = "mooncake-store-mlu-";
    return name.size() == std::strlen(prefix) + 6 &&
           name.compare(0, std::strlen(prefix), prefix) == 0 &&
           name.find_first_not_of(
               "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
               "-") == std::string::npos;
}

std::string LeasePath(const std::string &name) { return "/dev/shm/" + name; }

struct ContextGuard {
    CNcontext previous = nullptr;
    CNresult status = cnCtxGetCurrent(&previous);
    ~ContextGuard() {
        if (status == CN_SUCCESS) cnCtxSetCurrent(previous);
    }
};

int AcquireLease(const MluIpcBufferHandle &handle) {
    if (!ValidLeaseName(handle.lease)) return -1;
    int fd = open(LeasePath(handle.lease).c_str(),
                  O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return -1;
    struct stat file{};
    char live = 0;
    struct flock owner{};
    owner.l_type = F_RDLCK;
    owner.l_whence = SEEK_SET;
    owner.l_len = 1;
    if (fstat(fd, &file) || !S_ISREG(file.st_mode) ||
        file.st_uid != geteuid() || file.st_nlink != 1 || file.st_size != 1 ||
        uint64_t(file.st_dev) != handle.lease_device ||
        uint64_t(file.st_ino) != handle.lease_inode || !Lock(fd, F_RDLCK, 1) ||
        pread(fd, &live, 1, 0) != 1 || live != 1 ||
        fcntl(fd, F_OFD_GETLK, &owner) || owner.l_type != F_WRLCK) {
        close(fd);
        return -1;
    }
    return fd;
}
#endif

}  // namespace

struct MluIpcBufferExport::Impl {
    MluIpcBufferHandle handle;
#ifdef USE_MLU
    int lease_fd = -1;
    ~Impl() {
        if (lease_fd < 0) return;
        // Stop late RPC handlers before waiting for any current importer.
        const char retired = 0;
        CHECK_EQ(pwrite(lease_fd, &retired, 1, 0), 1)
            << "Cannot revoke MLU IPC export safely";
        CHECK(Lock(lease_fd, F_WRLCK, 1, true))
            << "Cannot drain MLU IPC export safely";
        unlink(LeasePath(handle.lease).c_str());
        close(lease_fd);
    }
#endif
};

struct MluIpcBufferMapping::Impl {
    void *ptr = nullptr;
#ifdef USE_MLU
    CNaddr base = 0;
    CNcontext context = nullptr;
    CNdev device = 0;
    bool retained = false;
    int lease_fd = -1;
    ~Impl() {
        ContextGuard guard;
        if (base && (guard.status != CN_SUCCESS ||
                     cnCtxSetCurrent(context) != CN_SUCCESS ||
                     cnIpcCloseMemHandle(base) != CN_SUCCESS)) {
            // Retain the context and borrow until process exit if unmapping
            // failed. Releasing the lease would let the exporter free live
            // VRAM.
            LOG(ERROR) << "Retaining MLU IPC lease after mapping close failure";
            return;
        }
        if (retained) cnSharedContextRelease(device);
        if (lease_fd >= 0) close(lease_fd);
    }
#endif
};

MluIpcBufferExport::MluIpcBufferExport() = default;
MluIpcBufferExport::~MluIpcBufferExport() = default;
MluIpcBufferExport::MluIpcBufferExport(MluIpcBufferExport &&) noexcept =
    default;
MluIpcBufferExport &MluIpcBufferExport::operator=(
    MluIpcBufferExport &&) noexcept = default;
const MluIpcBufferHandle &MluIpcBufferExport::handle() const {
    return impl_->handle;
}

tl::expected<MluIpcBufferExport, ErrorCode> MluIpcBufferExport::Create(
    const void *ptr, size_t size) {
#ifdef USE_MLU
    if (!ptr || !size || cnInit(0) != CN_SUCCESS)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    cnrtPointerAttributes_t attr{};
    if (cnrtPointerGetAttributes(&attr, const_cast<void *>(ptr)) !=
            cnrtSuccess ||
        attr.type != cnrtMemTypeDevice)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    ContextGuard guard;
    CNcontext context = nullptr;
    const auto address = reinterpret_cast<CNaddr>(ptr);
    if (guard.status != CN_SUCCESS ||
        cnGetMemAttribute(&context, CN_MEM_ATTRIBUTE_CONTEXT, address) !=
            CN_SUCCESS ||
        cnCtxSetCurrent(context) != CN_SUCCESS)
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    CNaddr base = 0;
    cn_uint64_t allocation_size = 0;
    if (cnMemGetAddressRange(&base, &allocation_size, address) != CN_SUCCESS ||
        address < base || address - base > allocation_size ||
        size > allocation_size - (address - base))
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    CNdev device;
    char uuid[64]{};
    CNIpcMemHandle ipc = nullptr;
    if (cnCtxGetDevice(&device) != CN_SUCCESS ||
        cnDeviceGetUuidStr(uuid, sizeof(uuid), device) != CN_SUCCESS ||
        cnIpcGetMemHandle(&ipc, base) != CN_SUCCESS || !ipc)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);

    MluIpcBufferExport result;
    result.impl_ = std::make_unique<Impl>();
    auto &out = *result.impl_;
    out.handle.handle = reinterpret_cast<uint64_t>(ipc);
    out.handle.offset = address - base;
    out.handle.size = size;
    out.handle.device_uuid = uuid;
    char path[] = "/dev/shm/mooncake-store-mlu-XXXXXX";
    const int fd = mkostemp(path, O_CLOEXEC);
    if (fd < 0) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    struct stat file{};
    const char live = 1;
    if (fstat(fd, &file) || !Lock(fd, F_WRLCK, 0) ||
        pwrite(fd, &live, 1, 0) != 1) {
        unlink(path);
        close(fd);
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    out.handle.lease = std::string(path).substr(9);
    out.handle.lease_device = file.st_dev;
    out.handle.lease_inode = file.st_ino;
    out.lease_fd = fd;
    return result;
#else
    (void)ptr;
    (void)size;
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
#endif
}

MluIpcBufferMapping::MluIpcBufferMapping() = default;
MluIpcBufferMapping::~MluIpcBufferMapping() = default;
MluIpcBufferMapping::MluIpcBufferMapping(MluIpcBufferMapping &&) noexcept =
    default;
MluIpcBufferMapping &MluIpcBufferMapping::operator=(
    MluIpcBufferMapping &&) noexcept = default;
void *MluIpcBufferMapping::ptr() const { return impl_ ? impl_->ptr : nullptr; }

tl::expected<MluIpcBufferMapping, ErrorCode> MluIpcBufferMapping::Open(
    const MluIpcBufferHandle &handle) {
#ifdef USE_MLU
    if (!handle.handle || !handle.size || handle.device_uuid.empty() ||
        handle.offset > std::numeric_limits<uint64_t>::max() - handle.size ||
        cnInit(0) != CN_SUCCESS)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    MluIpcBufferMapping result;
    result.impl_ = std::make_unique<Impl>();
    auto &out = *result.impl_;
    out.lease_fd = AcquireLease(handle);
    if (out.lease_fd < 0) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    ContextGuard guard;
    if (guard.status != CN_SUCCESS ||
        cnDeviceGetByUuidStr(&out.device, handle.device_uuid.c_str()) !=
            CN_SUCCESS ||
        cnSharedContextAcquire(&out.context, out.device) != CN_SUCCESS)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    out.retained = true;
    if (cnCtxSetCurrent(out.context) != CN_SUCCESS ||
        cnIpcOpenMemHandle(&out.base,
                           reinterpret_cast<CNIpcMemHandle>(handle.handle),
                           CN_IPC_MEM_NEED_PEER_ACCESS) != CN_SUCCESS)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    CNaddr base = 0;
    cn_uint64_t size = 0;
    if (cnMemGetAddressRange(&base, &size, out.base) != CN_SUCCESS ||
        base != out.base || handle.offset > size ||
        handle.size > size - handle.offset)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    out.ptr = reinterpret_cast<void *>(out.base + handle.offset);
    return result;
#else
    (void)handle;
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
#endif
}

}  // namespace mooncake::device
