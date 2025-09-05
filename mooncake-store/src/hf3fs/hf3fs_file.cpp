#include <glog/logging.h>

#include "file_interface.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>

namespace mooncake {

ThreeFSFile::ThreeFSFile(const std::string& filename, int fd,
                         USRBIOResourceManager* resource_manager)
    : StorageFile(filename, fd), resource_manager_(resource_manager) {}

ThreeFSFile::~ThreeFSFile() {
    // Deregister and close file descriptor
    if (fd_ >= 0) {
        hf3fs_dereg_fd(fd_);
        if (close(fd_) == -1) {
            LOG(WARNING) << "Failed to close file: " << filename_;
        }
        fd_ = -1;
    }

    // Delete potentially corrupted file if write failed
    if (error_code_ == ErrorCode::FILE_WRITE_FAIL) {
        if (::unlink(filename_.c_str()) == -1) {
            LOG(ERROR) << "Failed to delete corrupted file: " << filename_;
        } else {
            LOG(INFO) << "Deleted corrupted file: " << filename_;
        }
    }
}

tl::expected<size_t, ErrorCode> ThreeFSFile::write(const std::string& buffer,
                                                   size_t length) {
    return write(std::span<const char>(buffer.data(), length), length);
}

tl::expected<size_t, ErrorCode> ThreeFSFile::write(std::span<const char> data,
                                                   size_t length) {
    // 1. Parameter validation
    if (length == 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    // 2. Get thread resources
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        return make_error<size_t>(ErrorCode::FILE_OPEN_FAIL);
    }

    // 3. Write in chunks
    auto& threefs_iov = resource->iov_;
    auto& ior_write = resource->ior_write_;
    const char* data_ptr = data.data();
    size_t total_bytes_written = 0;
    off_t current_offset = 0;
    const size_t max_chunk_size = resource->config_.iov_size;

    while (total_bytes_written < length) {
        // Calculate current chunk size
        size_t chunk_size =
            std::min(length - total_bytes_written, max_chunk_size);

        // Copy data to shared buffer
        memcpy(threefs_iov.base, data_ptr + total_bytes_written, chunk_size);

        // Prepare IO request
        int ret =
            hf3fs_prep_io(&ior_write, &threefs_iov, false, threefs_iov.base,
                          fd_, current_offset, chunk_size, nullptr);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        // Submit IO request
        ret = hf3fs_submit_ios(&ior_write);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        // Wait for IO completion
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_write, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        size_t bytes_written = cqe.result;
        total_bytes_written += bytes_written;
        current_offset += bytes_written;

        if (bytes_written < chunk_size) {
            break;  // Short write, possibly disk full
        }
    }

    if (total_bytes_written != length) {
        return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
    }

    return total_bytes_written;
}

tl::expected<size_t, ErrorCode> ThreeFSFile::read(std::string& buffer,
                                                  size_t length) {
    // 1. Parameter validation
    if (length == 0) {
        return make_error<size_t>(ErrorCode::FILE_INVALID_BUFFER);
    }

    // 2. Get thread resources
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        return make_error<size_t>(ErrorCode::FILE_OPEN_FAIL);
    }

    // 3. Prepare buffer
    buffer.clear();
    buffer.reserve(length);
    size_t total_bytes_read = 0;
    off_t current_offset = 0;
    auto& threefs_iov = resource->iov_;
    auto& ior_read = resource->ior_read_;

    // 5. Read in chunks
    while (total_bytes_read < length) {
        // Calculate current chunk size
        size_t chunk_size = std::min<size_t>(length - total_bytes_read,
                                             resource->config_.iov_size);

        // Prepare IO request
        int ret = hf3fs_prep_io(&ior_read, &threefs_iov, true, threefs_iov.base,
                                fd_, current_offset, chunk_size, nullptr);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        // Submit IO request
        ret = hf3fs_submit_ios(&ior_read);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        // Wait for IO completion
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_read, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        size_t bytes_read = cqe.result;
        if (bytes_read == 0) {  // EOF
            break;
        }

        // Append data to buffer
        buffer.append(reinterpret_cast<char*>(threefs_iov.base), bytes_read);
        total_bytes_read += bytes_read;
        current_offset += bytes_read;

        if (bytes_read < chunk_size) {  // Short read
            break;
        }
    }

    if (total_bytes_read != length) {
        return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
    }

    return total_bytes_read;
}

tl::expected<size_t, ErrorCode> ThreeFSFile::vector_write(const iovec* iov,
                                                          int iovcnt,
                                                          off_t offset) {
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        return make_error<size_t>(ErrorCode::FILE_OPEN_FAIL);
    }

    auto& threefs_iov = resource->iov_;
    auto& ior_write = resource->ior_write_;

    // 1. Calculate total length
    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) {
        total_length += iov[i].iov_len;
    }

    size_t total_bytes_written = 0;
    off_t current_offset = offset;
    size_t bytes_remaining = total_length;
    int current_iov_index = 0;
    size_t current_iov_offset = 0;

    while (bytes_remaining > 0) {
        // 2. Determine current write chunk size (not exceeding shared buffer
        // size)
        size_t current_chunk_size =
            std::min<size_t>(bytes_remaining, resource->config_.iov_size);

        // 3. Copy data from user IOV to shared buffer
        size_t bytes_copied = 0;
        char* dest_ptr = reinterpret_cast<char*>(threefs_iov.base);

        while (bytes_copied < current_chunk_size &&
               current_iov_index < iovcnt) {
            const iovec* current_iov = &iov[current_iov_index];
            size_t copy_size =
                std::min(current_chunk_size - bytes_copied,
                         current_iov->iov_len - current_iov_offset);

            memcpy(dest_ptr + bytes_copied,
                   reinterpret_cast<char*>(current_iov->iov_base) +
                       current_iov_offset,
                   copy_size);

            bytes_copied += copy_size;
            current_iov_offset += copy_size;

            if (current_iov_offset >= current_iov->iov_len) {
                current_iov_index++;
                current_iov_offset = 0;
            }
        }

        // 4. Prepare and submit IO request
        int ret =
            hf3fs_prep_io(&ior_write, &threefs_iov, false, threefs_iov.base,
                          fd_, current_offset, current_chunk_size, nullptr);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        ret = hf3fs_submit_ios(&ior_write);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        // 5. Wait for IO completion
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_write, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) {
            return make_error<size_t>(ErrorCode::FILE_WRITE_FAIL);
        }

        size_t bytes_written = cqe.result;
        total_bytes_written += bytes_written;
        bytes_remaining -= bytes_written;
        current_offset += bytes_written;

        if (bytes_written < current_chunk_size) {
            break;  // Short write, possibly disk full
        }
    }

    return total_bytes_written;
}

tl::expected<size_t, ErrorCode> ThreeFSFile::vector_read(const iovec* iov,
                                                         int iovcnt,
                                                         off_t offset) {
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        return make_error<size_t>(ErrorCode::FILE_OPEN_FAIL);
    }

    auto& threefs_iov = resource->iov_;
    auto& ior_read = resource->ior_read_;

    // Calculate total length
    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) {
        total_length += iov[i].iov_len;
    }

    size_t total_bytes_read = 0;
    off_t current_offset = offset;
    size_t bytes_remaining = total_length;
    int current_iov_index = 0;
    size_t current_iov_offset = 0;

    while (bytes_remaining > 0) {
        // Determine current block size
        size_t current_chunk_size =
            std::min<size_t>(bytes_remaining, resource->config_.iov_size);

        // Prepare IO request
        int ret =
            hf3fs_prep_io(&ior_read, &threefs_iov, true, threefs_iov.base, fd_,
                          current_offset, current_chunk_size, nullptr);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        // Submit IO request
        ret = hf3fs_submit_ios(&ior_read);
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        // Wait for IO completion
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_read, &cqe, 1, 1, nullptr);
        size_t bytes_read = cqe.result;
        if (ret < 0) {
            return make_error<size_t>(ErrorCode::FILE_READ_FAIL);
        }

        // Copy data from shared buffer to user IOV
        size_t bytes_to_copy = bytes_read;
        char* src_ptr = reinterpret_cast<char*>(threefs_iov.base);

        while (bytes_to_copy > 0 && current_iov_index < iovcnt) {
            const iovec* current_iov = &iov[current_iov_index];
            size_t copy_size = std::min(
                bytes_to_copy, current_iov->iov_len - current_iov_offset);

            memcpy(
                static_cast<char*>(current_iov->iov_base) + current_iov_offset,
                src_ptr, copy_size);

            src_ptr += copy_size;
            bytes_to_copy -= copy_size;
            total_bytes_read += copy_size;
            bytes_remaining -= copy_size;
            current_offset += copy_size;

            current_iov_offset += copy_size;
            if (current_iov_offset >= current_iov->iov_len) {
                current_iov_index++;
                current_iov_offset = 0;
            }
        }
        if (bytes_read < current_chunk_size) {
            // If bytes read is less than requested chunk size, we've reached
            // EOF
            break;
        }
    }

    return total_bytes_read;
}

}  // namespace mooncake