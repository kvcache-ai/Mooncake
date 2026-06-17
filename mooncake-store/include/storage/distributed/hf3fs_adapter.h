#pragma once

#include <memory>

#include "storage/distributed/fs_adapter.h"

namespace mooncake {

// Forward declaration: avoid including hf3fs/hf3fs.h in the header.
// Full type is only used in hf3fs_adapter.cpp.
class USRBIOResourceManager;

class Hf3fsAdapter : public FileSystemAdapter {
   public:
    Hf3fsAdapter();
    ~Hf3fsAdapter() override;  // defined in .cpp (needs complete type for
                               // unique_ptr deleter)

    tl::expected<size_t, ErrorCode> WriteFile(
        const std::string& path, std::span<const char> data) override;

    tl::expected<size_t, ErrorCode> ReadFile(const std::string& path, void* buf,
                                             size_t len) override;

    tl::expected<size_t, ErrorCode> VectorWriteFile(const std::string& path,
                                                    const iovec* iov,
                                                    int iovcnt,
                                                    off_t offset) override;

    tl::expected<size_t, ErrorCode> VectorReadFile(const std::string& path,
                                                   const iovec* iov, int iovcnt,
                                                   off_t offset) override;

    tl::expected<void, ErrorCode> DeleteFile(const std::string& path) override;

    tl::expected<bool, ErrorCode> FileExists(const std::string& path) override;

    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string& dir) override;

    tl::expected<void, ErrorCode> Init(const std::string& mount_path) override;

    tl::expected<void, ErrorCode> Shutdown() override;

    const char* GetName() const override { return "hf3fs"; }

   private:
    std::unique_ptr<USRBIOResourceManager> resource_manager_;
};

}  // namespace mooncake
