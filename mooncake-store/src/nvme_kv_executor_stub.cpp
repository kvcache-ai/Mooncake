#include "nvme_kv_executor.h"

#include <fstream>
#include <sstream>
#include <system_error>
#include <utility>

#include "mutex.h"

namespace mooncake {
namespace {

class NvmeKvStubExecutor : public NvmeKvCommandExecutor {
   public:
    explicit NvmeKvStubExecutor(std::filesystem::path storage_path)
        : storage_path_(std::move(storage_path)) {}

    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value,
                                        StoreOptions options = {}) override {
        const auto blob_path = BlobPath(key);
        {
            SharedMutexLocker lock(&mutex_, shared_lock);
            if (options.if_not_exists && objects_.find(key) != objects_.end()) {
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
        }
        if (options.if_not_exists && std::filesystem::exists(blob_path)) {
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
        const PhysicalKey& key, uint32_t /*size_hint*/ = 0) const override {
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

    tl::expected<void, ErrorCode> Delete(const PhysicalKey& key) override {
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

    tl::expected<void, ErrorCode> Iterate(
        const std::function<tl::expected<void, ErrorCode>(
            const PhysicalKey& key)>& visitor) const override {
        std::error_code ec;
        if (!std::filesystem::exists(storage_path_, ec)) {
            return {};
        }
        for (const auto& entry :
             std::filesystem::directory_iterator(storage_path_, ec)) {
            if (ec) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto path = entry.path();
            if (path.extension() != ".blob") {
                continue;
            }
            const auto stem = path.stem().string();
            if (stem.size() != 32) {
                continue;
            }
            PhysicalKey key{};
            try {
                for (size_t i = 0; i < key.size(); ++i) {
                    key[i] = static_cast<uint8_t>(
                        std::stoul(stem.substr(i * 2, 2), nullptr, 16));
                }
            } catch (const std::exception&) {
                continue;
            }
            auto visit_result = visitor(key);
            if (!visit_result) {
                return visit_result;
            }
        }
        if (ec) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        return {};
    }

    const Capabilities& GetCapabilities() const override {
        return capabilities_;
    }

    std::string GetBackendType() const override { return "stub"; }

    std::optional<CommandAuditInfo> GetLastCommandAuditInfo() const override {
        return std::nullopt;
    }

   private:
    struct PhysicalKeyHash {
        size_t operator()(const PhysicalKey& key) const {
            size_t seed = 0;
            for (uint8_t b : key) {
                seed = (seed * 131) ^ b;
            }
            return seed;
        }
    };

    std::filesystem::path BlobPath(const PhysicalKey& key) const {
        std::ostringstream oss;
        oss << std::hex;
        for (uint8_t b : key) {
            oss.width(2);
            oss.fill('0');
            oss << static_cast<int>(b);
        }
        return storage_path_ / (oss.str() + ".blob");
    }

    std::filesystem::path storage_path_;
    mutable SharedMutex mutex_;
    mutable std::unordered_map<PhysicalKey, std::string, PhysicalKeyHash>
        objects_ GUARDED_BY(mutex_);
    Capabilities capabilities_{
        .max_key_size = 16,
        .max_value_size = 128 * 1024,
        .runtime_transfer_limit = 128 * 1024,
        .effective_max_value_size = 128 * 1024,
        .queue_depth = 1,
        .supports_iterate = true,
        .supports_batch_submit = false,
        .supports_conditional_store = true,
    };
};

}  // namespace

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvStubExecutor(
    std::filesystem::path storage_path) {
    return std::make_unique<NvmeKvStubExecutor>(std::move(storage_path));
}

}  // namespace mooncake
