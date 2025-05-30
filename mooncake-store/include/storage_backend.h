#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <types.h>

namespace mooncake {

class StorageBackend {
   public:
    virtual ~StorageBackend() = default; 

    virtual ErrorCode StoreObject(const ObjectKey& key, const std::vector<Slice>& slices) =0;
    virtual ErrorCode StoreObject(const ObjectKey& key, std::string& str) =0;
    virtual ErrorCode LoadObject(const ObjectKey& key, std::vector<Slice>& slices) =0;
    virtual ErrorCode LoadObject(const ObjectKey& key, std::string& str) =0;

};

}  // namespace mooncake