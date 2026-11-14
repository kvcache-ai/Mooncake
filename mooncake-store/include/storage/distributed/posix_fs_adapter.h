#pragma once

#include <string>

#include "storage/distributed/fs_adapter.h"

namespace mooncake {

class PosixFsAdapter : public FileSystemAdapter {
   public:
    PosixFsAdapter() = default;

    // With direct_io, every fd returned by OpenFile/OpenExistingFile has
    // O_DIRECT set, so DFS reads and writes bypass the page cache:
    //  - WriteAt needs an `alignment`-aligned offset (INVALID_PARAMS
    //    otherwise), writes the whole request or fails, and zero-fills the
    //    file from the end of the data to the next `alignment` boundary. The
    //    caller must own that padding, as DFS descriptors do through
    //    aligned_size. It returns the unpadded length.
    //  - ReadAt reads straight into the caller's iovecs when the offset and
    //    every buffer and length are aligned, otherwise through a bounce
    //    buffer.
    // Writes copy only the unaligned tail into a padded block when every
    // buffer and every length but the last are aligned, otherwise they are
    // gathered through a bounce buffer.
    // There is no page-cache fallback. Init fails if `alignment` is not a
    // power of two >= 512, or if a probe file under the mount cannot be
    // written, read back and, with direct_fsync, fsynced through O_DIRECT.
    // An open that cannot set O_DIRECT fails with FILE_OPEN_FAIL.
    // With direct_fsync, RequiresWriteSync() is true and SyncFile() fsyncs
    // the fd, so the backend fsyncs each written file once per batch.
    // direct_fsync without direct_io is ignored with a warning.
    PosixFsAdapter(bool direct_io, uint64_t alignment,
                   bool direct_fsync = false);

    tl::expected<void, ErrorCode> DeleteFile(const std::string& path) override;

    tl::expected<bool, ErrorCode> FileExists(const std::string& path) override;

    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string& dir) override;

    tl::expected<int, ErrorCode> OpenFile(const std::string& path) override;

    tl::expected<int, ErrorCode> OpenExistingFile(
        const std::string& path) override;

    tl::expected<void, ErrorCode> CloseFile(int fd) override;

    tl::expected<void, ErrorCode> PreallocateFile(const std::string& path,
                                                  uint64_t size) override;

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override;

    tl::expected<size_t, ErrorCode> ReadAt(int fd, iovec* iov, int iovcnt,
                                           int64_t offset) override;

    tl::expected<void, ErrorCode> Init(const std::string& mount_path) override;

    tl::expected<void, ErrorCode> Shutdown() override;

    bool RequiresWriteSync() const override { return direct_fsync_; }

    tl::expected<void, ErrorCode> SyncFile(int fd) override;

    bool SupportsFdReopen() const override { return true; }

    // Buffered preadv() has no alignment or registration requirements, and
    // O_DIRECT I/O bounces unaligned requests internally.
    bool SupportsArbitraryHostReadBuffers() const override { return true; }

    const char* GetName() const override { return "posix"; }

   private:
    tl::expected<void, ErrorCode> ProbeDirectIo();
    tl::expected<void, ErrorCode> EnableDirectIo(int fd,
                                                 const std::string& path);
    tl::expected<size_t, ErrorCode> DirectWrite(int fd, const iovec* iov,
                                                int iovcnt, int64_t offset);
    tl::expected<size_t, ErrorCode> DirectRead(int fd, iovec* iov, int iovcnt,
                                               int64_t offset);

    std::string mount_path_;
    bool direct_io_ = false;
    bool direct_fsync_ = false;
    uint64_t alignment_ = 4096;
};

}  // namespace mooncake
