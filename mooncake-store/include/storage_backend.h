#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <types.h>

namespace mooncake {

/**
 * @class StorageBackend
 * @brief Abstract base class defining the interface for object storage operations.
 * 
 * Provides pure virtual methods for storing/loading objects in either:
 * - Slice-based format (for scattered data)
 * - Contiguous string format
 */
class StorageBackend {
public:
    virtual ~StorageBackend() = default; 

    /// Stores object composed of multiple data slices
    virtual ErrorCode StoreObject(const ObjectKey& key, const std::vector<Slice>& slices) = 0;
    
    /// Stores object from contiguous string data
    virtual ErrorCode StoreObject(const ObjectKey& key, std::string& str) = 0;
    
    /// Loads object into multiple slices
    virtual ErrorCode LoadObject(const ObjectKey& key, std::vector<Slice>& slices) = 0;
    
    /// Loads object as contiguous string
    virtual ErrorCode LoadObject(const ObjectKey& key, std::string& str) = 0;
};

}  // namespace mooncake