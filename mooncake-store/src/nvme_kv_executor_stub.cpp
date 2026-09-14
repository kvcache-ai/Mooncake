#include "nvme_kv_executor.h"
#include "nvme_kv_executor_util.h"

#include <filesystem>
#include <fstream>
#include <system_error>
#include <utility>

#include "mutex.h"

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
                                        std::string value) override {
        const auto blob_path = BlobPath(key);
        {
            SharedMutexLocker lock(&mutex_, shared_lock);
            if (objects_.find(key) != objects_.end()) {
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
        }
        if (std::filesystem::exists(blob_path)) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        std::ofstream out(blob_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        out.write(value.data(), static_cast<std::streamsize>(value.size()));
        if (!out.good()) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        SharedMutexLocker lock(&mutex_);
        objects_[key] = std::move(value);
        return {};
    }

    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey &key, uint32_t /*size_hint*/ = 0) const override {
        {
            SharedMutexLocker lock(&mutex_, shared_lock);
            auto it = objects_.find(key);
            if (it != objects_.end()) {
                return it->second;
            }
        }
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
        {
            SharedMutexLocker lock(&mutex_);
            objects_[key] = value;
        }
        return value;
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey &key) override {
        const auto blob_path = BlobPath(key);
        {
            SharedMutexLocker lock(&mutex_);
            objects_.erase(key);
        }
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
    struct PhysicalKeyHash {
        size_t operator()(const PhysicalKey &key) const {
            size_t seed = 0;
            for (uint8_t b : key) {
                seed = (seed * 131) ^ b;
            }
            return seed;
        }
    };

    std::filesystem::path BlobPath(const PhysicalKey &key) const {
        return storage_path_ / (NvmeKvPhysicalKeyToHex(key) + ".blob");
    }

    std::filesystem::path storage_path_;
    mutable SharedMutex mutex_;
    mutable std::unordered_map<PhysicalKey, std::string, PhysicalKeyHash>
        objects_ GUARDED_BY(mutex_);
    Capabilities capabilities_;
};

}  // namespace

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvStubExecutor(
    std::filesystem::path storage_path) {
    return std::make_unique<NvmeKvStubExecutor>(std::move(storage_path));
}

}  // namespace mooncake
