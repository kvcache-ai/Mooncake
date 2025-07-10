#include "file_interface.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <cstring>
#include <stdexcept>

namespace mooncake {

ThreeFSFile::ThreeFSFile(const std::string& filename, int fd, USRBIOResourceManager* resource_manager) 
    : StorageFile(filename), fd_(fd), resource_manager_(resource_manager) {}

ThreeFSFile::~ThreeFSFile() {
    // 释放锁资源
    if (is_locked_) {
        release_lock();
    }

    // 注销并关闭文件描述符
    if (fd_ >= 0) {
        hf3fs_dereg_fd(fd_);
        if (close(fd_) == -1) {
            LOG(WARNING) << "Failed to close file: " << filename_;
        }
        fd_ = -1;
    }

    // 如果写入失败，删除可能损坏的文件
    if (error_code_ == ErrorCode::FILE_WRITE_FAIL) {
        if (::unlink(filename_.c_str()) == -1) {
            LOG(ERROR) << "Failed to delete corrupted file: " << filename_;
        } else {
            LOG(INFO) << "Deleted corrupted file: " << filename_;
        }
    }
}

std::unique_ptr<ThreeFSFile> ThreeFSFile::create(
    const std::string& filename, int fd, USRBIOResourceManager* resource_manager) {
    if (fd < 0 || !resource_manager) {
        return nullptr;
    }
    
    return std::unique_ptr<ThreeFSFile>(new ThreeFSFile(filename, fd, resource_manager));
}


ssize_t ThreeFSFile::write(const std::string& buffer, size_t length) {
    // 1. 参数校验
    if (buffer.empty() || length == 0) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }
    if (length > buffer.size()) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }
    if (length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    // 2. 获取线程资源
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        error_code_ = ErrorCode::FILE_OPEN_FAIL;
        return -1;
    }

    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    // 3. 获取写锁
    if (acquire_write_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    // 4. 分块写入
    auto& threefs_iov = resource->iov_;
    auto& ior_write = resource->ior_write_;
    const char* data_ptr = buffer.data();
    size_t total_bytes_written = 0;
    off_t current_offset = 0;
    const size_t max_chunk_size = resource->params.iov_size;

    while (total_bytes_written < length) {
        // 计算当前块大小
        size_t chunk_size = std::min(length - total_bytes_written, max_chunk_size);

        // 拷贝数据到共享缓冲区
        memcpy(threefs_iov.base, data_ptr + total_bytes_written, chunk_size);

        // 准备IO请求
        int ret = hf3fs_prep_io(&ior_write, &threefs_iov, false,
                               threefs_iov.base, fd_, current_offset, chunk_size, nullptr);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_WRITE_FAIL;
            release_lock();
            return -1;
        }

        // 提交IO请求
        ret = hf3fs_submit_ios(&ior_write);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_WRITE_FAIL;
            release_lock();
            return -1;
        }

        // 等待IO完成
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_write, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) {
            error_code_ = ErrorCode::FILE_WRITE_FAIL;
            release_lock();
            return -1;
        }

        size_t bytes_written = cqe.result;
        total_bytes_written += bytes_written;
        current_offset += bytes_written;

        if (bytes_written < chunk_size) {
            break; // 短写，可能磁盘已满
        }
    }

    // 5. 释放锁
    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    // 6. 返回结果
    return total_bytes_written > 0 ? static_cast<ssize_t>(total_bytes_written) : -1;
}

ssize_t ThreeFSFile::read(std::string& buffer, size_t length) {
    // 1. 获取线程资源
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        error_code_ = ErrorCode::FILE_OPEN_FAIL;
        return -1;
    }

    // 2. 参数校验
    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }
    if (length == 0 || length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::FILE_INVALID_BUFFER;
        return -1;
    }

    // 3. 获取读锁
    if (acquire_read_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    // 4. 准备缓冲区
    buffer.clear();
    buffer.reserve(length);
    size_t total_bytes_read = 0;
    off_t current_offset = 0;
    auto& threefs_iov = resource->iov_;
    auto& ior_read = resource->ior_read_;

    // 5. 分块读取循环
    while (total_bytes_read < length) {
        // 计算当前块大小
        size_t chunk_size = std::min<size_t>(
            length - total_bytes_read,
            resource->params.iov_size
        );

        // 准备IO请求
        int ret = hf3fs_prep_io(&ior_read, &threefs_iov, true,
                               threefs_iov.base, fd_, current_offset, chunk_size, nullptr);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_READ_FAIL;
            release_lock();
            return -1;
        }

        // 提交IO请求
        ret = hf3fs_submit_ios(&ior_read);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_READ_FAIL;
            release_lock();
            return -1;
        }

        // 等待IO完成
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_read, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) {
            error_code_ = ErrorCode::FILE_READ_FAIL;
            release_lock();
            return -1;
        }

        size_t bytes_read = cqe.result;
        if (bytes_read == 0) { // EOF
            break;
        }

        // 追加数据到缓冲区
        buffer.append(reinterpret_cast<char*>(threefs_iov.base), bytes_read);
        total_bytes_read += bytes_read;
        current_offset += bytes_read;

        if (bytes_read < chunk_size) { // 短读
            break;
        }
    }

    // 6. 释放锁
    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    // 7. 返回结果
    return total_bytes_read > 0 ? static_cast<ssize_t>(total_bytes_read) : -1;
}

ssize_t ThreeFSFile::pwritev(const iovec* iov, int iovcnt, off_t offset) {
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        error_code_ = ErrorCode::FILE_OPEN_FAIL;
        return -1;
    }

    auto& iov_ = resource->iov_;
    auto& ior_write_ = resource->ior_write_;

    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    if (acquire_write_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    // 1. 计算总长度并验证缓冲区
    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_base == nullptr || iov[i].iov_len == 0) {
            error_code_ = ErrorCode::FILE_INVALID_BUFFER;
            release_lock();
            return -1;
        }
        total_length += iov[i].iov_len;
    }

    size_t total_bytes_written = 0;
    off_t current_offset = offset;
    size_t bytes_remaining = total_length;
    int  current_iov_index = 0;
    size_t current_iov_offset = 0;

    while (bytes_remaining > 0) {
        // 2. 确定当前写入块大小（不超过共享缓冲区大小）
        size_t current_chunk_size = std::min<size_t>(
            bytes_remaining,
            resource->params.iov_size
        );

        // 3. 将数据从用户IOV复制到共享缓冲区
        size_t bytes_copied = 0;
        char* dest_ptr = reinterpret_cast<char*>(iov_.base);
        
        while (bytes_copied < current_chunk_size && current_iov_index < iovcnt) {
            const iovec* current_iov = &iov[current_iov_index];
            size_t copy_size = std::min(
                current_chunk_size - bytes_copied,
                current_iov->iov_len - current_iov_offset
            );

            memcpy(
                dest_ptr + bytes_copied,
                reinterpret_cast<char*>(current_iov->iov_base) + current_iov_offset,
                copy_size
            );

            bytes_copied += copy_size;
            current_iov_offset += copy_size;
            
            if (current_iov_offset >= current_iov->iov_len) {
                current_iov_index++;
                current_iov_offset = 0;
            }
        }

        // 4. 准备并提交IO请求
        int ret = hf3fs_prep_io(&ior_write_, &iov_, false,
                               iov_.base, fd_, current_offset, current_chunk_size, nullptr);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_WRITE_FAIL;
            release_lock();
            return -1;
        }

        ret = hf3fs_submit_ios(&ior_write_);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_WRITE_FAIL;
            release_lock();
            return -1;
        }

        // 5. 等待IO完成
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_write_, &cqe, 1, 1, nullptr);
        if (ret < 0 || cqe.result < 0) {
            error_code_ = ErrorCode::FILE_WRITE_FAIL;
            release_lock();
            return -1;
        }

        size_t bytes_written = cqe.result;
        total_bytes_written += bytes_written;
        bytes_remaining -= bytes_written;
        current_offset += bytes_written;

        if (bytes_written < current_chunk_size) {
            break; // 短写，可能磁盘已满
        }
    }

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    return total_bytes_written > 0 ? total_bytes_written : -1;
}

// ssize_t ThreeFSFile::preadv(const iovec* iov, int iovcnt, off_t offset) {

//     auto* resource = resource_manager_->getThreadResource();
//     if (!resource || !resource->initialized) {
//         error_code_ = ErrorCode::FILE_OPEN_FAIL;
//         return -1;
//     }

//     auto& iov_ = resource->iov_;
//     auto& ior_read_ = resource->ior_read_;

//     if (fd_ < 0) {
//         error_code_ = ErrorCode::FILE_NOT_FOUND;
//         return -1;
//     }

//     if (acquire_read_lock() == -1) {
//         error_code_ = ErrorCode::FILE_LOCK_FAIL;
//         return -1;
//     }

//     // 计算总长度
//     size_t total_length = 0;
//     for (int i = 0; i < iovcnt; ++i) {
//         total_length += iov[i].iov_len;
//     }

//     size_t total_bytes_read = 0;
//     off_t current_offset = offset;
//     size_t bytes_remaining = total_length;
//     int current_iov_index = 0;
//     size_t current_iov_offset = 0;

//     while(bytes_remaining > 0) {
//         // Determine current block size
//         size_t current_chunk_size =
//             std::min<size_t>(bytes_remaining, resource->params.iov_size);

//                     // 准备IO请求
//         int ret = hf3fs_prep_io(&ior_read_, &iov_, true, 
//                             iov_.base, fd_, current_offset, current_chunk_size, nullptr);
//         if (ret < 0) {
//             error_code_ = ErrorCode::FILE_READ_FAIL;
//             release_lock();
//             return -1;
//         }

//         // 提交IO请求
//         ret = hf3fs_submit_ios(&ior_read_);
//         if (ret < 0) {
//             error_code_ = ErrorCode::FILE_READ_FAIL;
//             release_lock();
//             return -1;
//         }

//         // 等待IO完成
//         struct hf3fs_cqe cqe;
//         ret = hf3fs_wait_for_ios(&ior_read_, &cqe, 1, 1, nullptr);
//         size_t bytes_read = cqe.result;
//         if (ret < 0) {
//             error_code_ = ErrorCode::FILE_READ_FAIL;
//             release_lock();
//             return -1;
//         }

//         // 将数据从共享缓冲区复制到用户IOV
//         size_t bytes_to_copy = bytes_read;
//         char* src_ptr = reinterpret_cast<char*>(iov_.base);
        
//         while (bytes_to_copy > 0 && current_iov_index < iovcnt) {
//             const iovec* current_iov = &iov[current_iov_index];
//             size_t copy_size = std::min(
//                 bytes_to_copy,
//                 current_iov->iov_len - current_iov_offset
//             );

//             memcpy(
//                 static_cast<char*>(current_iov->iov_base) + current_iov_offset,
//                 src_ptr,
//                 copy_size
//             );

//             src_ptr += copy_size;
//             bytes_to_copy -= copy_size;
//             total_bytes_read += copy_size;
//             bytes_remaining -= copy_size;
//             current_offset += copy_size;

//             current_iov_offset += copy_size;
//             if (current_iov_offset >= current_iov->iov_len) {
//                 current_iov_index++;
//                 current_iov_offset = 0;
//             }
//         }
//         if(bytes_read < current_chunk_size) {
//             // 如果读取的字节数小于请求的块大小，说明已经到达文件末尾
//             break;
//         }
//     }


//     if (release_lock() == -1) {
//         error_code_ = ErrorCode::FILE_LOCK_FAIL;
//         LOG(INFO) << "Failed to release lock on file: " << filename_;
//     }

//     return total_bytes_read > 0 ? total_bytes_read : -1;
// }

ssize_t ThreeFSFile::preadv(const iovec* iov, int iovcnt, off_t offset) {
    auto* resource = resource_manager_->getThreadResource();
    if (!resource || !resource->initialized) {
        error_code_ = ErrorCode::FILE_OPEN_FAIL;
        return -1;
    }

    auto& iov_ = resource->iov_;
    auto& ior_read_ = resource->ior_read_;

    if (fd_ < 0) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    if (acquire_read_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    // 时间记录变量
    std::chrono::microseconds total_wait_time{0};
    std::chrono::microseconds total_copy_time{0};
    auto total_start = std::chrono::high_resolution_clock::now();

    // 计算总长度
    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) {
        total_length += iov[i].iov_len;
    }

    size_t total_bytes_read = 0;
    off_t current_offset = offset;
    size_t bytes_remaining = total_length;
    int current_iov_index = 0;
    size_t current_iov_offset = 0;

    while(bytes_remaining > 0) {
        // Determine current block size
        size_t current_chunk_size =
            std::min<size_t>(bytes_remaining, resource->params.iov_size);

        // 准备IO请求
        int ret = hf3fs_prep_io(&ior_read_, &iov_, true, 
                            iov_.base, fd_, current_offset, current_chunk_size, nullptr);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_READ_FAIL;
            release_lock();
            return -1;
        }

        // 提交IO请求
        ret = hf3fs_submit_ios(&ior_read_);
        if (ret < 0) {
            error_code_ = ErrorCode::FILE_READ_FAIL;
            release_lock();
            return -1;
        }

        // 记录等待IO开始时间
        auto wait_start = std::chrono::high_resolution_clock::now();
        
        // 等待IO完成
        struct hf3fs_cqe cqe;
        ret = hf3fs_wait_for_ios(&ior_read_, &cqe, 1, 1, nullptr);
        size_t bytes_read = cqe.result;
        
        // 记录等待IO结束时间
        auto wait_end = std::chrono::high_resolution_clock::now();
        total_wait_time += std::chrono::duration_cast<std::chrono::microseconds>(wait_end - wait_start);

        if (ret < 0) {
            error_code_ = ErrorCode::FILE_READ_FAIL;
            release_lock();
            return -1;
        }

        // 记录拷贝开始时间
        auto copy_start = std::chrono::high_resolution_clock::now();
        
        // 将数据从共享缓冲区复制到用户IOV
        size_t bytes_to_copy = bytes_read;
        char* src_ptr = reinterpret_cast<char*>(iov_.base);
        
        while (bytes_to_copy > 0 && current_iov_index < iovcnt) {
            const iovec* current_iov = &iov[current_iov_index];
            size_t copy_size = std::min(
                bytes_to_copy,
                current_iov->iov_len - current_iov_offset
            );

            memcpy(
                static_cast<char*>(current_iov->iov_base) + current_iov_offset,
                src_ptr,
                copy_size
            );

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
        
        // 记录拷贝结束时间
        auto copy_end = std::chrono::high_resolution_clock::now();
        total_copy_time += std::chrono::duration_cast<std::chrono::microseconds>(copy_end - copy_start);

        if(bytes_read < current_chunk_size) {
            // 如果读取的字节数小于请求的块大小，说明已经到达文件末尾
            break;
        }
    }

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    // 计算总时间
    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(total_end - total_start);

    // 输出时间占比
    LOG(INFO) << "preadv time analysis for " << filename_ << ":"
              << "\n  Total time: " << total_time.count() << "μs"
              << "\n  IO wait time: " << total_wait_time.count() << "μs (" 
              << (total_wait_time.count() * 100 / total_time.count()) << "%)"
              << "\n  Copy time: " << total_copy_time.count() << "μs (" 
              << (total_copy_time.count() * 100 / total_time.count()) << "%)"
              << "\n  Other overhead: " << (total_time - total_wait_time - total_copy_time).count() 
              << "μs (" << ((total_time - total_wait_time - total_copy_time).count() * 100 / total_time.count()) << "%)";

    return total_bytes_read > 0 ? total_bytes_read : -1;
}


int ThreeFSFile::acquire_write_lock() {
    if (flock(fd_, LOCK_EX) == -1) {
        return -1;
    }
    is_locked_ = true;
    return 0;
}

int ThreeFSFile::acquire_read_lock() {
    if (flock(fd_, LOCK_SH) == -1) {
        return -1;
    }
    is_locked_ = true;
    return 0;
}

int ThreeFSFile::release_lock() {
    if (!is_locked_) return 0;
    if (flock(fd_, LOCK_UN) == -1) {
        return -1;
    }
    is_locked_ = false;
    return 0;
}

// USRBIO Resource manager implementation

// threefs.cpp 中添加
bool ThreadUSRBIOResource::Initialize(const ThreeFSParams &params) {
  if (initialized) {
    return true;
  }

  this->params = params;

  // Create shared memory
  int ret =
      hf3fs_iovcreate(&iov_, params.mount_root.c_str(), params.iov_size, 0, -1);
  if (ret < 0) {
    return false;
  }

  // Create read I/O ring
  ret =
      hf3fs_iorcreate4(&ior_read_, params.mount_root.c_str(), params.ior_entries,
                       true, params.io_depth, params.ior_timeout, -1, 0);
  if (ret < 0) {
    hf3fs_iovdestroy(&iov_);
    return false;
  }

  // Create write I/O ring
  ret = hf3fs_iorcreate4(&ior_write_, params.mount_root.c_str(),
                         params.ior_entries, false, params.io_depth,
                         params.ior_timeout, -1, 0);
  if (ret < 0) {
    hf3fs_iordestroy(&ior_read_);
    hf3fs_iovdestroy(&iov_);
    return false;
  }

  initialized = true;
  return true;
}

void ThreadUSRBIOResource::Cleanup() {
  if (!initialized) {
    return;
  }

  // Destroy USRBIO resources
  hf3fs_iordestroy(&ior_write_);
  hf3fs_iordestroy(&ior_read_);
  hf3fs_iovdestroy(&iov_);

  initialized = false;
}

// Resource manager implementation
struct ThreadUSRBIOResource *USRBIOResourceManager::getThreadResource(
    const ThreeFSParams &params) {
  std::thread::id thread_id = std::this_thread::get_id();

  {
    std::lock_guard<std::mutex> lock(resource_map_mutex);

    // Find if current thread already has resources
    auto it = thread_resources.find(thread_id);
    if (it != thread_resources.end()) {
      return it->second;
    }

    // Create new thread resources
    ThreadUSRBIOResource *resource = new ThreadUSRBIOResource();
    if (!resource->Initialize(params)) {
      delete resource;
      return nullptr;
    }

    // Store resource mapping
    thread_resources[thread_id] = resource;
    return resource;
  }
}

USRBIOResourceManager::~USRBIOResourceManager() {
  // Clean up all thread resources
  for (auto &pair : thread_resources) {
    delete pair.second;
  }
  thread_resources.clear();
}

} // namespace mooncake