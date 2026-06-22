// mooncake-store/include/oplog_store_factory.h
#pragma once

#include <algorithm>
#include <memory>
#include <string>

#include "ha/oplog/oplog_store.h"

namespace mooncake {

enum class OpLogStoreRole {
    WRITER,  // Primary: enable batch_write + batch_update
    READER,  // Standby: read-only
};

enum class OpLogStoreType {
    ETCD,
    LOCAL_FS,
};

#ifdef STORE_USE_ETCD
static constexpr OpLogStoreType kDefaultOpLogStoreType = OpLogStoreType::ETCD;
#else
static constexpr OpLogStoreType kDefaultOpLogStoreType =
    OpLogStoreType::LOCAL_FS;
#endif

// Parse string to OpLogStoreType (case-insensitive).
// Parse string to OpLogStoreType (case-insensitive).
// Returns kDefaultOpLogStoreType for unrecognized strings.
inline OpLogStoreType ParseOpLogStoreType(const std::string& type_str) {
    std::string lower = type_str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    if (lower == "localfs" || lower == "local_fs") {
        return OpLogStoreType::LOCAL_FS;
    }
    if (lower == "etcd") {
        return OpLogStoreType::ETCD;
    }
    return kDefaultOpLogStoreType;
}

inline std::string OpLogStoreTypeToString(OpLogStoreType type) {
    switch (type) {
        case OpLogStoreType::LOCAL_FS:
            return "localfs";
        case OpLogStoreType::ETCD:
        default:
            return "etcd";
    }
}

// Default configuration values for LocalFS OpLog store
static constexpr const char* kDefaultOpLogRootDir = "/tmp/mooncake_oplog";
static constexpr int kDefaultOpLogPollIntervalMs = 1000;

class OpLogStoreFactory {
   public:
    /**
     * @brief Create and initialize an OpLogStore instance.
     *
     * The returned instance is fully initialized (Init() has already been
     * called internally). Callers must NOT call Init() again.
     * Returns nullptr if the requested backend is unavailable or
     * initialization fails.
     */
    static std::unique_ptr<OpLogStore> Create(
        OpLogStoreType type, const std::string& cluster_id, OpLogStoreRole role,
        const std::string& oplog_root_dir = kDefaultOpLogRootDir,
        int poll_interval_ms = kDefaultOpLogPollIntervalMs);
};

}  // namespace mooncake
