#pragma once

#include <cstdlib>
#include <functional>
#include <memory>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "types.h"

namespace mooncake {

/**
 * @brief LocalFile class to handle kv cache I/O on file
 */
class LocalFile {
   public:
    LocalFile(const std::string &file_name, FILE *file,
        ErrorCode ec = ErrorCode::OK);
    ~LocalFile();

    /**
     * @brief Read content from current file to buffer
     * @param buffer [in] buffer to store the content
     * @param length [in] length in bytes of content to read
     * @return ssize_t, the length of content actually read, -1 indicates an error
     */
    ssize_t read(void *buffer, size_t length);

    /**
     * @brief Write content from buffer to current file
     * @param buffer [in] buffer to write from
     * @param length [in] length in bytes of content to write
     * @return ssize_t, the length of content actually write, -1 indicates an error
     */
    ssize_t write(const void *buffer, size_t length);

    /**
     * @brief Read iovcnt buffers from current file into the buffers described by iov
     * @param iov [in] points to an array of iovec structures, refer to preadv(2)
     * @param iovcnt [in] count of iovec
     * @param offset [in] file offset from which to read in bytes
     * @return ssize_t, the length of content actually read, -1 indicates an error
     */
    ssize_t preadv(const iovec *iov, int iovcnt, off_t offset);

    /**
     * @brief Write content from buffer to current file, file offset is not changed
     * @param iov [in] buffer to write from
     * @param iovcnt [in] count of iovec
     * @param offset [in] file offset from which to write in bytes
     * @return ssize_t, the length of content actually write, -1 indicates an error
     */
    ssize_t pwritev(const iovec *iov, int iovcnt, off_t offset);

    const char *getFileName() const { return file_name_.c_str(); }

    ErrorCode error_code() const { return error_code_; }

   private:
    std::string file_name_;
    FILE *file_;
    ErrorCode error_code_;
};

}  // namespace mooncake
