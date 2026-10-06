#include "storage/distributed/hf3fs_adapter.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <deque>
#include <filesystem>
#include <limits>

#include <glog/logging.h>

#include "hf3fs/hf3fs.h"

namespace mooncake {

// Destructor must be defined in .cpp where USRBIOResourceManager is complete.
Hf3fsAdapter::~Hf3fsAdapter() = default;

Hf3fsAdapter::Hf3fsAdapter() = default;

tl::expected<void, ErrorCode> Hf3fsAdapter::Init(
    const std::string& mount_path) {
    resource_manager_ = std::make_unique<USRBIOResourceManager>();
    Hf3fsConfig config{};
    char hf3fs_mount_point[PATH_MAX] = {};
    int ret = hf3fs_extract_mount_point(
        hf3fs_mount_point, sizeof(hf3fs_mount_point), mount_path.c_str());
    if (ret > 0 && ret <= static_cast<int>(sizeof(hf3fs_mount_point))) {
        config.mount_root = hf3fs_mount_point;
    } else {
        config.mount_root = mount_path;
    }
    resource_manager_->setDefaultParams(config);
    return {};
}

tl::expected<void, ErrorCode> Hf3fsAdapter::Shutdown() {
    resource_manager_.reset();
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
    if (fd < 0) {
        if (errno == ENOENT) {
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        }
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

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
    for (int i = 0; i < iovcnt; ++i) {
        if (!iov[i].iov_base && iov[i].iov_len > 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

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
    if (fd < 0) {
        if (errno == ENOENT) {
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        }
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

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
        if (entry->d_type == DT_DIR) continue;
        if (entry->d_type == DT_UNKNOWN) {
            struct stat st;
            if (::stat((dir + "/" + name).c_str(), &st) == 0 &&
                S_ISDIR(st.st_mode))
                continue;
        }
        result.push_back(name);
    }
    closedir(d);
    return result;
}

tl::expected<int, ErrorCode> Hf3fsAdapter::OpenFile(const std::string& path) {
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    if (hf3fs_reg_fd(fd, 0) > 0) {
        close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    return fd;
}

tl::expected<int, ErrorCode> Hf3fsAdapter::OpenExistingFile(
    const std::string& path) {
    int fd;
    do {
        fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return tl::make_unexpected(errno == ENOENT ? ErrorCode::FILE_NOT_FOUND
                                                   : ErrorCode::FILE_OPEN_FAIL);
    }
    if (hf3fs_reg_fd(fd, 0) > 0) {
        close(fd);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    return fd;
}

tl::expected<void, ErrorCode> Hf3fsAdapter::CloseFile(int fd) {
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    hf3fs_dereg_fd(fd);
    if (close(fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    }
    return {};
}

tl::expected<void, ErrorCode> Hf3fsAdapter::PreallocateFile(
    const std::string& path, uint64_t size) {
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);

    int rc = fallocate(fd, 0, 0, static_cast<off_t>(size));
    if (rc != 0) {
        rc = ftruncate(fd, static_cast<off_t>(size));
    }
    int saved_errno = errno;
    close(fd);

    if (rc != 0) {
        errno = saved_errno;
        LOG(ERROR) << "Failed to preallocate DFS file " << path
                   << ", size=" << size << ", error=" << strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

namespace {

struct IovCursor {
    int index = 0;
    size_t offset = 0;

    void Copy(const iovec* iov, uint8_t* buffer, size_t length, bool read) {
        while (length > 0) {
            if (offset == iov[index].iov_len) {
                ++index;
                offset = 0;
                continue;
            }
            const size_t count = std::min(length, iov[index].iov_len - offset);
            auto* data = static_cast<char*>(iov[index].iov_base) + offset;
            if (read) {
                memcpy(data, buffer, count);
            } else {
                memcpy(buffer, data, count);
            }
            buffer += count;
            offset += count;
            length -= count;
        }
    }
};

}  // namespace

tl::expected<size_t, ErrorCode> Hf3fsAdapter::WriteAt(int fd, const iovec* iov,
                                                      int iovcnt,
                                                      int64_t offset) {
    const FdIoRequest request{fd, const_cast<iovec*>(iov), iovcnt, offset};
    return BatchWriteAt({&request, 1}).front();
}

tl::expected<size_t, ErrorCode> Hf3fsAdapter::ReadAt(int fd, iovec* iov,
                                                     int iovcnt,
                                                     int64_t offset) {
    const FdIoRequest request{fd, iov, iovcnt, offset};
    return BatchReadAt({&request, 1}).front();
}

std::vector<tl::expected<size_t, ErrorCode>> Hf3fsAdapter::BatchWriteAt(
    std::span<const FdIoRequest> requests) {
    return BatchIo(requests, false);
}

std::vector<tl::expected<size_t, ErrorCode>> Hf3fsAdapter::BatchReadAt(
    std::span<const FdIoRequest> requests) {
    return BatchIo(requests, true);
}

std::vector<tl::expected<size_t, ErrorCode>> Hf3fsAdapter::BatchIo(
    std::span<const FdIoRequest> requests, bool read) {
    const auto io_error =
        read ? ErrorCode::FILE_READ_FAIL : ErrorCode::FILE_WRITE_FAIL;
    std::vector<tl::expected<size_t, ErrorCode>> results(
        requests.size(), tl::make_unexpected(io_error));
    struct RequestState {
        size_t length = 0;
        size_t completed = 0;
        IovCursor cursor;
    };
    std::vector<RequestState> states(requests.size());
    std::deque<size_t> pending;
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.fd < 0 || request.offset < 0 || request.iovcnt < 0 ||
            (request.iovcnt > 0 && !request.iov)) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        // Bound both the aggregate length and every later positional offset.
        const auto max_length = static_cast<size_t>(
            std::numeric_limits<int64_t>::max() - request.offset);
        auto& state = states[i];
        bool valid = true;
        for (int j = 0; j < request.iovcnt; ++j) {
            const auto& iov = request.iov[j];
            if ((!iov.iov_base && iov.iov_len > 0) ||
                iov.iov_len > max_length - state.length) {
                valid = false;
                break;
            }
            state.length += iov.iov_len;
        }
        if (!valid) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        } else if (state.length == 0) {
            results[i] = size_t{0};
        } else {
            pending.push_back(i);
        }
    }
    if (pending.empty()) return results;

    auto* resource =
        resource_manager_ ? resource_manager_->getThreadResource() : nullptr;
    if (!resource) {
        for (const auto i : pending) {
            results[i] = tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        return results;
    }
    auto& iov = resource->iov_;
    auto& ior = read ? resource->ior_read_ : resource->ior_write_;
    const int entries = hf3fs_io_entries(&ior);
    if (entries <= 0 || !iov.base || iov.size == 0) return results;

    // A slot owns its staging slice until its CQE is reaped. Limit each logical
    // request to one chunk per wave so a short transfer cannot submit its tail.
    const size_t slot_count =
        std::min({pending.size(), static_cast<size_t>(entries), iov.size});
    struct Chunk {
        size_t request = 0;
        uint8_t* buffer = nullptr;
        size_t length = 0;
        int index = -1;
        bool active = false;
    };
    std::vector<Chunk> chunks(slot_count);
    std::vector<hf3fs_cqe> cqes(slot_count);
    while (!pending.empty()) {
        // Give the remaining requests larger slices as their peers finish.
        const size_t wave_slots = std::min(slot_count, pending.size());
        const size_t slot_size = iov.size / wave_slots;
        size_t prepared = 0;
        while (!pending.empty() && prepared < wave_slots) {
            const size_t i = pending.front();
            pending.pop_front();
            auto& state = states[i];
            const auto& request = requests[i];
            auto& chunk = chunks[prepared];
            chunk = {i, iov.base + prepared * slot_size,
                     std::min(state.length - state.completed, slot_size)};
            auto cursor = state.cursor;
            if (!read)
                cursor.Copy(request.iov, chunk.buffer, chunk.length, false);
            const int ret = hf3fs_prep_io(
                &ior, &iov, read, chunk.buffer, request.fd,
                request.offset + state.completed, chunk.length, &chunk);
            if (ret == -EAGAIN && prepared > 0) {
                pending.push_front(i);
                break;
            }
            if (ret < 0) continue;
            chunk.index = ret;
            chunk.active = true;
            if (!read) state.cursor = cursor;
            ++prepared;
        }
        if (prepared == 0) continue;

        // prep_io publishes immediately; even a failed submit notification
        // must not abandon prepared requests or recycle their staging slices.
        const bool submit_failed = hf3fs_submit_ios(&ior) < 0;
        size_t outstanding = prepared;
        while (outstanding > 0) {
            const int count = hf3fs_wait_for_ios(
                &ior, cqes.data(), static_cast<int>(outstanding), 1, nullptr);
            if (count == -EINTR || count == -EAGAIN || count == 0) continue;
            if (count < 0 || static_cast<size_t>(count) > outstanding) {
                LOG(ERROR) << "3FS batch completion wait failed: " << count;
                // No reliable way to reap this ring remains. Retire both rings
                // and the shared buffer so later calls cannot reuse live slices
                // or consume stale CQEs. The resource manager recreates them.
                resource->Cleanup();
                return results;
            }
            for (int c = 0; c < count; ++c) {
                const auto& cqe = cqes[c];
                auto it = std::find_if(
                    chunks.begin(), chunks.begin() + prepared,
                    [&](const Chunk& chunk) {
                        return &chunk == cqe.userdata && chunk.active &&
                               chunk.index == cqe.index;
                    });
                if (it == chunks.begin() + prepared) {
                    LOG(ERROR) << "Unexpected 3FS batch completion";
                    resource->Cleanup();
                    return results;
                }
                auto& chunk = *it;
                chunk.active = false;
                --outstanding;
                if (submit_failed || cqe.result < 0 ||
                    static_cast<uint64_t>(cqe.result) > chunk.length) {
                    continue;
                }
                auto& state = states[chunk.request];
                const auto bytes = static_cast<size_t>(cqe.result);
                if (read) {
                    state.cursor.Copy(requests[chunk.request].iov, chunk.buffer,
                                      bytes, true);
                }
                state.completed += bytes;
                // Keep the existing full-transfer-or-error adapter contract.
                if (bytes != chunk.length) continue;
                if (state.completed == state.length) {
                    results[chunk.request] = state.completed;
                } else {
                    pending.push_back(chunk.request);
                }
            }
        }
        if (submit_failed) return results;
    }
    return results;
}

}  // namespace mooncake
