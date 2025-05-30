#include <fstream>
#include <string>
#include <vector>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>

#include "local_file.h"

namespace mooncake {
LocalFile::LocalFile(const std::string& filename,FILE *file,ErrorCode ec) : filename_(filename),file_(file),error_code_(ec) {}

LocalFile::~LocalFile() {
    if (file_) {
        fclose(file_);
    }
    file_ = nullptr;
}

int LocalFile::acquire_write_lock(){
    if (flock(fileno(file_), LOCK_EX) == -1) {
        return -1;
    }
    return 0;
}

int LocalFile::acquire_read_lock(){
    if (flock(fileno(file_), LOCK_SH) == -1) {
        return -1;
    }
    return 0;
}

int LocalFile::release_lock(){
    if (flock(fileno(file_), LOCK_UN) == -1) {
        return -1;
    }
    return 0;
}

ssize_t LocalFile::write(std::string &buffer, size_t length){
    if (file_ == nullptr) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }
    if (length == 0 || buffer.empty()) {
        error_code_ = ErrorCode::FILE_BUFFER_INVALID;
        return -1;
    }

    if(length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::FILE_BUFFER_INVALID;
        return -1;
    }

    if (acquire_write_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    size_t written_bytes = fwrite(buffer.data(), 1, length, file_);

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    if (ferror(file_)) {
        error_code_ = ErrorCode::FILE_WRITE_FAIL;
        return -1;
    }

    return written_bytes;
}

ssize_t LocalFile::read(std::string &buffer, size_t length){
    if (file_ == nullptr) {
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }
    if (length == 0) {
        error_code_ = ErrorCode::FILE_BUFFER_INVALID;
        return -1;
    }

    if(length > static_cast<size_t>(std::numeric_limits<ssize_t>::max())) {
        error_code_ = ErrorCode::FILE_BUFFER_INVALID;
        return -1;
    }

    if (acquire_read_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    buffer.resize(length);
    size_t read_bytes = fread(&buffer[0], 1, length, file_);

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    if (ferror(file_)) {
        error_code_ = ErrorCode::FILE_READ_FAIL;
        buffer.clear();
        return -1;
    }

    buffer.resize(read_bytes); // shrink to actual read size
    return read_bytes;
}

// ssize_t LocalFile::read(void *buffer, size_t length){
//     if (file_ == NULL) 
//     {
//         error_code_ = ErrorCode::FILE_NOT_FOUND;
//         return -1;  
//     }

//     if(buffer == NULL || length == 0)
//     {
//         error_code_ = ErrorCode::FILE_BUFFER_INVALID;
//         return -1;  
//     }

//     // 尝试从文件中读取指定长度的数据
//     size_t read_bytes = fread(buffer, 1, length, file_);

//     // 检查是否发生读取错误
//     if (ferror(file_))
//     {
//         error_code_ = ErrorCode::FILE_READ_FAIL;
//         return -1;  // 出错，返回读取的字节数为 0
//     }

//     // 返回实际读取的字节数
//     return read_bytes;
// }


// ssize_t LocalFile::write(const void *buffer, size_t length){
//     if (file_ == NULL) 
//     {
//         error_code_ = ErrorCode::FILE_NOT_FOUND;
//         return -1;  
//     }

//     if(buffer == NULL || length == 0)
//     {
//         error_code_ = ErrorCode::FILE_BUFFER_INVALID;
//         return -1;  
//     }

//     // 尝试向文件中写入指定长度的数据
//     size_t written_bytes = fwrite(buffer, 1, length, file_);

//     // 检查是否发生写入错误
//     if (ferror(file_))
//     {
//         error_code_ = ErrorCode::FILE_WRITE_FAIL;
//         return -1;  // 出错，返回写入的字节数为 0
//     }

//     // 返回实际写入的字节数
//     return written_bytes;
// }

ssize_t LocalFile::pwritev(const iovec *iov, int iovcnt, off_t offset){
    if(!file_){
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    int fd=fileno(file_);
    if (fd == -1) {
        error_code_ = ErrorCode::FILE_INVALID_HANDLE;
        LOG(ERROR) << "Invalid file handle for: " << filename_;
        return -1;
    }

    if (acquire_write_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    size_t total_length = 0;
    for (int i = 0; i < iovcnt; ++i) {
        total_length += iov[i].iov_len;
    }

    ssize_t ret = ::pwritev(fd, iov, iovcnt, offset);

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }
    if (ret < 0) {
        error_code_ = ErrorCode::FILE_WRITE_FAIL;
        return -1;
    }

    // // 检查写入的数据是否正确
    // std::vector<char> buffer(total_length);
    // ssize_t read_ret = pread(fd, buffer.data(), total_length, offset);

    // if (read_ret != ret) {
    //     error_code_ = ErrorCode::FILE_READ_FAIL;
    //     LOG(ERROR) << "Read verification failed: expected " << ret << " bytes, got " << read_ret << " bytes.";
    //     return -1;
    // }

    // // 比较写入的数据和读取的数据
    // size_t pos = 0;
    // for (int i = 0; i < iovcnt; ++i) {
    //     if (std::memcmp(buffer.data() + pos, iov[i].iov_base, iov[i].iov_len) != 0) {
    //         LOG(ERROR) << "Data mismatch after write operation.";
    //         return -1;
    //     }
    //     pos += iov[i].iov_len;
    // }
    return ret;
}


ssize_t LocalFile::preadv(const iovec *iov, int iovcnt, off_t offset){
    if(!file_){
        error_code_ = ErrorCode::FILE_NOT_FOUND;
        return -1;
    }

    int fd=fileno(file_);

    if (acquire_read_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        return -1;
    }

    ssize_t ret = ::preadv(fd, iov, iovcnt, offset);

    if (release_lock() == -1) {
        error_code_ = ErrorCode::FILE_LOCK_FAIL;
        LOG(INFO) << "Failed to release lock on file: " << filename_;
    }

    if (ret < 0) {
        error_code_ = ErrorCode::FILE_READ_FAIL;
        return -1;
    }
    return ret;
}





}

