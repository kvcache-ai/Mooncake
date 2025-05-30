#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <types.h>
#include <storage_backend.h>
#include <local_file.h>
#include <filesystem>

namespace mooncake {

class FileStorageBackend : public StorageBackend {
   public:
    explicit FileStorageBackend(const std::string& root_dir): root_dir_(root_dir) {}

    static std::shared_ptr<FileStorageBackend> Create(const std::string& root_dir) {
        namespace fs = std::filesystem;
        if (!fs::exists(root_dir)) {
            LOG(INFO) << "Root directory does not exist: " << root_dir;
            return nullptr;
        } else if (!fs::is_directory(root_dir)) {
            LOG(INFO) << "Root path is not a directory: " << root_dir;
            return nullptr;
        }
        return std::make_shared<FileStorageBackend>(root_dir);
    }  
    
    ErrorCode StoreObject(const ObjectKey& key, const std::vector<Slice>& slices) override;
    ErrorCode StoreObject(const ObjectKey& key, std::string& str) override;
    ErrorCode LoadObject(const ObjectKey& key, std::vector<Slice>& slices) override;
    ErrorCode LoadObject(const ObjectKey& key, std::string& str) override;

    std::string root_dir_;
    std::mutex mutex_;
    
   private:
    std::string SanitizeKey(const ObjectKey& key) const;
    std::string ResolvePath(const ObjectKey& key) const;

};

}  // namespace mooncake