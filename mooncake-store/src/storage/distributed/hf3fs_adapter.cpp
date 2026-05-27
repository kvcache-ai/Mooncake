#include "storage/distributed/hf3fs_adapter.h"

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>

#include "hf3fs/hf3fs.h"

namespace mooncake {

// Destructor must be defined in .cpp where USRBIOResourceManager is complete.
Hf3fsAdapter::~Hf3fsAdapter() = default;

Hf3fsAdapter::Hf3fsAdapter() = default;

tl::expected<void, ErrorCode> Hf3fsAdapter::Init(
    const std::string& mount_path) {
    mount_path_ = mount_path;
    resource_manager_ = std::make_unique<USRBIOResourceManager>();
    Hf3fsConfig config{};
    config.mount_root = mount_path;
    resource_manager_->setDefaultParams(config);
    initialized_ = true;
    return {};
}

tl::expected<void, ErrorCode> Hf3fsAdapter::Shutdown() {
    resource_manager_.reset();
    initialized_ = false;
    return {};
}

tl::expected<size_t, ErrorCode> Hf3fsAdapter::WriteFile(
    const std::string& path, std::span<const char> data) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    if (hf3fs_reg_fd(fd, 0) > 0) {
        close(fd);
        ::unlink(path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        hf3fs_dereg_fd(fd);
        close(fd);
        ::unlink(path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto& threefs_iov = resource->iov_;
    auto& ior_write = resource->ior_write_;
    const char* data_ptr = data.data();
    size_t length = data.size();
    size_t total_written = 0;
    off_t offset = 0;

    while (total_written < length) {
        size_t chunk =
            std::min(length - total_written, resource->config_.iov_size);
        memcpy(threefs_iov.base, data_ptr + total_written, chunk);

        int ret = hf3fs_prep_io(&ior_write, &threefs_iov, false,
                                threefs_iov.base, fd, offset, chunk, nullptr);
        if (ret < 0) break;

        ret = hf3fs_submit_ios(&ior_write);
        if (ret < 0) break;

        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_write, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) break;

        size_t bytes_written = cqe.result;
        total_written += bytes_written;
        offset += bytes_written;
        if (bytes_written < chunk) break;
    }

    hf3fs_dereg_fd(fd);
    close(fd);

    if (total_written != length) {
        auto unlink_ret = ::unlink(path.c_str());
        if (unlink_ret != 0) {
            LOG(WARNING) << "Failed to clean up partial write: " << path;
        }
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return total_written;
}

tl::expected<size_t, ErrorCode> Hf3fsAdapter::ReadFile(const std::string& path,
                                                       void* buf, size_t len) {
    if (!buf && len > 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    if (hf3fs_reg_fd(fd, 0) > 0) {
        close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        hf3fs_dereg_fd(fd);
        close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto& threefs_iov = resource->iov_;
    auto& ior_read = resource->ior_read_;
    char* dest = static_cast<char*>(buf);
    size_t total_read = 0;
    off_t offset = 0;

    while (total_read < len) {
        size_t chunk = std::min(len - total_read, resource->config_.iov_size);

        int ret = hf3fs_prep_io(&ior_read, &threefs_iov, true, threefs_iov.base,
                                fd, offset, chunk, nullptr);
        if (ret < 0) break;

        ret = hf3fs_submit_ios(&ior_read);
        if (ret < 0) break;

        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_read, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) break;

        size_t bytes_read = cqe.result;
        if (bytes_read == 0) break;

        memcpy(dest + total_read, threefs_iov.base, bytes_read);
        total_read += bytes_read;
        offset += bytes_read;
        if (bytes_read < chunk) break;
    }

    hf3fs_dereg_fd(fd);
    close(fd);

    if (total_read != len) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return total_read;
}

tl::expected<size_t, ErrorCode> Hf3fsAdapter::VectorWriteFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    if (hf3fs_reg_fd(fd, 0) > 0) {
        close(fd);
        ::unlink(path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        hf3fs_dereg_fd(fd);
        close(fd);
        ::unlink(path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto& threefs_iov = resource->iov_;
    auto& ior_write = resource->ior_write_;

    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) total_length += iov[i].iov_len;

    size_t total_written = 0;
    off_t current_offset = offset;
    size_t remaining = total_length;
    int iov_idx = 0;
    size_t iov_off = 0;

    while (remaining > 0) {
        size_t chunk = std::min(remaining, resource->config_.iov_size);

        // Copy from iovec to shared buffer
        size_t copied = 0;
        char* dest = reinterpret_cast<char*>(threefs_iov.base);
        while (copied < chunk && iov_idx < iovcnt) {
            size_t n = std::min(chunk - copied, iov[iov_idx].iov_len - iov_off);
            memcpy(dest + copied,
                   static_cast<char*>(iov[iov_idx].iov_base) + iov_off, n);
            copied += n;
            iov_off += n;
            if (iov_off >= iov[iov_idx].iov_len) {
                iov_idx++;
                iov_off = 0;
            }
        }

        int ret =
            hf3fs_prep_io(&ior_write, &threefs_iov, false, threefs_iov.base, fd,
                          current_offset, chunk, nullptr);
        if (ret < 0) break;
        ret = hf3fs_submit_ios(&ior_write);
        if (ret < 0) break;
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_write, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) break;

        size_t bytes_written = cqe.result;
        total_written += bytes_written;
        current_offset += bytes_written;
        remaining -= bytes_written;
        if (bytes_written < chunk) break;
    }

    hf3fs_dereg_fd(fd);
    close(fd);

    if (total_written != total_length) {
        auto unlink_ret = ::unlink(path.c_str());
        if (unlink_ret != 0) {
            LOG(WARNING) << "Failed to clean up partial write: " << path;
        }
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return total_written;
}

tl::expected<size_t, ErrorCode> Hf3fsAdapter::VectorReadFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    for (int i = 0; i < iovcnt; ++i) {
        if (!iov[i].iov_base && iov[i].iov_len > 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    if (hf3fs_reg_fd(fd, 0) > 0) {
        close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        hf3fs_dereg_fd(fd);
        close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto& threefs_iov = resource->iov_;
    auto& ior_read = resource->ior_read_;

    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) total_length += iov[i].iov_len;

    size_t total_read = 0;
    off_t current_offset = offset;
    size_t remaining = total_length;
    int iov_idx = 0;
    size_t iov_off = 0;

    while (remaining > 0) {
        size_t chunk = std::min(remaining, resource->config_.iov_size);

        int ret = hf3fs_prep_io(&ior_read, &threefs_iov, true, threefs_iov.base,
                                fd, current_offset, chunk, nullptr);
        if (ret < 0) break;
        ret = hf3fs_submit_ios(&ior_read);
        if (ret < 0) break;
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_read, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) break;

        size_t bytes_read = cqe.result;
        if (bytes_read == 0) break;

        // Copy from shared buffer to iovec
        size_t to_copy = bytes_read;
        char* src = reinterpret_cast<char*>(threefs_iov.base);
        while (to_copy > 0 && iov_idx < iovcnt) {
            size_t n = std::min(to_copy, iov[iov_idx].iov_len - iov_off);
            memcpy(static_cast<char*>(iov[iov_idx].iov_base) + iov_off, src, n);
            src += n;
            to_copy -= n;
            total_read += n;
            remaining -= n;
            current_offset += n;
            iov_off += n;
            if (iov_off >= iov[iov_idx].iov_len) {
                iov_idx++;
                iov_off = 0;
            }
        }
        if (bytes_read < chunk) break;
    }

    hf3fs_dereg_fd(fd);
    close(fd);

    if (total_read != total_length) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return total_read;
}

tl::expected<void, ErrorCode> Hf3fsAdapter::DeleteFile(
    const std::string& path) {
    if (::unlink(path.c_str()) != 0) {
        if (errno == ENOENT) {
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        }
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

// Note: FileExists and DeleteFile use POSIX access()/unlink() via the
// kernel VFS mount. This assumes 3FS's FUSE mount namespace is consistent
// with the USRBIO namespace used for read/write I/O.

tl::expected<bool, ErrorCode> Hf3fsAdapter::FileExists(
    const std::string& path) {
    if (::access(path.c_str(), F_OK) == 0) {
        return true;
    }
    if (errno == ENOENT) {
        return false;
    }
    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
}

tl::expected<std::vector<std::string>, ErrorCode> Hf3fsAdapter::ListFiles(
    const std::string& dir) {
    DIR* d = opendir(dir.c_str());
    if (!d) {
        if (errno == ENOENT) {
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        }
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    std::vector<std::string> result;

    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
        std::string name = entry->d_name;
        if (name == "." || name == "..") continue;
        result.push_back(name);
    }
    closedir(d);
    return result;
}

}  // namespace mooncake
