#include "nvme_kv/executor.h"
#include "nvme_kv/executor_util.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace mooncake {
namespace {

class NvmeKvStubExecutor : public NvmeKvCommandExecutor {
   public:
    explicit NvmeKvStubExecutor(std::filesystem::path storage_path)
        : storage_path_(std::move(storage_path)),
          capabilities_(BuildNvmeKvCapabilities(
              1, ParseNvmeKvU32EnvOr("MOONCAKE_NVME_KV_QUEUE_DEPTH", 1),
              ParseNvmeKvU32EnvOr("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT",
                                  128 * 1024))) {}

    tl::expected<void, ErrorCode> Store(const PhysicalKey &key,
                                        std::string value,
                                        StoreMode mode) override {
        const auto blob_path = BlobPath(key);
        if (mode == StoreMode::kUpdate && !std::filesystem::exists(blob_path)) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        std::string pending = blob_path.string() + ".XXXXXX";
        const int fd = ::mkstemp(pending.data());
        if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        auto *file = ::fdopen(fd, "wb");
        if (!file) {
            ::close(fd);
            ::unlink(pending.c_str());
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        const bool written =
            std::fwrite(value.data(), 1, value.size(), file) == value.size();
        const bool closed = std::fclose(file) == 0;
        if (!written || !closed) {
            ::unlink(pending.c_str());
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        // Publish complete files: link is insert-only, rename replaces a root.
        const int status = mode == StoreMode::kInsert
                               ? ::link(pending.c_str(), blob_path.c_str())
                               : ::rename(pending.c_str(), blob_path.c_str());
        const int error = errno;
        if (mode == StoreMode::kInsert || status != 0)
            ::unlink(pending.c_str());
        if (status != 0) {
            return tl::make_unexpected(error == EEXIST
                                           ? ErrorCode::OBJECT_ALREADY_EXISTS
                                           : ErrorCode::FILE_WRITE_FAIL);
        }
        return {};
    }

    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey &key, uint32_t /*size_hint*/ = 0) const override {
        const auto blob_path = BlobPath(key);
        std::ifstream in(blob_path, std::ios::binary);
        if (!in) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        std::string value((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
        if (!in.good() && !in.eof()) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        return value;
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey &key) override {
        const auto blob_path = BlobPath(key);
        std::error_code ec;
        const bool removed = std::filesystem::remove(blob_path, ec);
        if (ec) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (!removed) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        return {};
    }

    const Capabilities &GetCapabilities() const override {
        return capabilities_;
    }

   private:
    std::filesystem::path BlobPath(const PhysicalKey &key) const {
        return storage_path_ / (NvmeKvPhysicalKeyToHex(key) + ".blob");
    }

    std::filesystem::path storage_path_;
    Capabilities capabilities_;
};

}  // namespace

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvStubExecutor(
    std::filesystem::path storage_path) {
    return std::make_unique<NvmeKvStubExecutor>(std::move(storage_path));
}

}  // namespace mooncake
