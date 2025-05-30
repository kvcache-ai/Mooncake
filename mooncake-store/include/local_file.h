#pragma once

#include <string>
#include <sys/uio.h>
#include <cstdio>
#include "types.h"

namespace mooncake {

// LocalFile is a simple file storage class that provides methods to write
// and read data to and from a local file. It is designed to be used for
// storing and retrieving data in a straightforward manner, without any
// complex serialization or deserialization logic.
class LocalFile {
public:
    explicit LocalFile(const std::string& filename,FILE *file,ErrorCode ec=ErrorCode::OK);
    ~LocalFile();

    ssize_t read(std::string &buffer, size_t length);

    ssize_t write(std::string &buffer, size_t length);

    ssize_t preadv(const iovec *iov, int iovcnt, off_t offset);

    ssize_t pwritev(const iovec *iov, int iovcnt, off_t offset);

private:

    int acquire_write_lock();

    int acquire_read_lock();

    int release_lock();

    std::string filename_;
    FILE *file_;
    ErrorCode error_code_;

};

} // namespace mooncake


