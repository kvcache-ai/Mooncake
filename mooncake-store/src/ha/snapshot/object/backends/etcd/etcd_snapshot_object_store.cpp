#include "ha/snapshot/object/backends/etcd/etcd_snapshot_object_store.h"

#ifdef STORE_USE_ETCD

#include <cstdlib>
#include <string>

#include <fmt/format.h>
#include <glog/logging.h>

#include "libetcd_wrapper.h"

namespace mooncake {

EtcdSnapshotObjectStore::EtcdSnapshotObjectStore(
    const std::string& endpoints)
    : endpoints_(endpoints) {
    LOG(INFO) << "EtcdSnapshotObjectStore: initializing with endpoints="
              << endpoints_;

    char* err_msg = nullptr;
    auto ret = NewSnapshotEtcdClient(
        const_cast<char*>(endpoints_.c_str()), &err_msg);
    if (ret != 0 && ret != -2) {  // -2 means already initialized
        std::string error = err_msg ? err_msg : "unknown error";
        if (err_msg) free(err_msg);
        LOG(ERROR) << "Failed to initialize snapshot etcd client: " << error;
        throw std::runtime_error(
            "Failed to initialize snapshot etcd client: " + error);
    }
    if (err_msg) free(err_msg);
}

tl::expected<void, std::string> EtcdSnapshotObjectStore::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    char* err_msg = nullptr;
    auto ret = SnapshotStorePutWrapper(
        const_cast<char*>(key.c_str()), static_cast<int>(key.size()),
        const_cast<char*>(reinterpret_cast<const char*>(buffer.data())),
        static_cast<int>(buffer.size()), &err_msg);

    if (ret != 0) {
        std::string error = err_msg ? err_msg : "unknown error";
        if (err_msg) free(err_msg);
        return tl::make_unexpected(
            fmt::format("etcd put failed: key={}, error={}", key, error));
    }

    VLOG(1) << "EtcdSnapshotObjectStore: uploaded key=" << key
            << ", size=" << buffer.size();
    return {};
}

tl::expected<void, std::string> EtcdSnapshotObjectStore::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    char* value = nullptr;
    int value_size = 0;
    GoInt64 revision_id = 0;
    char* err_msg = nullptr;

    auto ret = SnapshotStoreGetWrapper(
        const_cast<char*>(key.c_str()), static_cast<int>(key.size()),
        &value, &value_size, &revision_id, &err_msg);

    if (ret != 0) {
        std::string error = err_msg ? err_msg : "unknown error";
        if (err_msg) free(err_msg);
        return tl::make_unexpected(
            fmt::format("etcd get failed: key={}, error={}", key, error));
    }

    buffer.assign(reinterpret_cast<uint8_t*>(value),
                  reinterpret_cast<uint8_t*>(value) + value_size);
    free(value);

    VLOG(1) << "EtcdSnapshotObjectStore: downloaded key=" << key
            << ", size=" << buffer.size();
    return {};
}

tl::expected<void, std::string> EtcdSnapshotObjectStore::UploadString(
    const std::string& key, const std::string& data) {
    std::vector<uint8_t> buffer(data.begin(), data.end());
    return UploadBuffer(key, buffer);
}

tl::expected<void, std::string> EtcdSnapshotObjectStore::DownloadString(
    const std::string& key, std::string& data) {
    std::vector<uint8_t> buffer;
    auto result = DownloadBuffer(key, buffer);
    if (!result) {
        return result;
    }
    data.assign(reinterpret_cast<char*>(buffer.data()), buffer.size());
    return {};
}

tl::expected<void, std::string> EtcdSnapshotObjectStore::DeleteObjectsWithPrefix(
    const std::string& prefix) {
    char* err_msg = nullptr;
    auto ret = SnapshotStoreDeleteWrapper(
        const_cast<char*>(prefix.c_str()), static_cast<int>(prefix.size()),
        1,  // usePrefix = true
        &err_msg);

    if (ret != 0) {
        std::string error = err_msg ? err_msg : "unknown error";
        if (err_msg) free(err_msg);
        return tl::make_unexpected(fmt::format(
            "etcd delete prefix failed: prefix={}, error={}", prefix, error));
    }

    VLOG(1) << "EtcdSnapshotObjectStore: deleted prefix=" << prefix;
    return {};
}

tl::expected<void, std::string> EtcdSnapshotObjectStore::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    object_keys.clear();

    char** keys = nullptr;
    int* key_sizes = nullptr;
    int count = 0;
    char* err_msg = nullptr;

    auto ret = SnapshotStoreListKeysWrapper(
        const_cast<char*>(prefix.c_str()), static_cast<int>(prefix.size()),
        &keys, &key_sizes, &count, &err_msg);

    if (ret != 0) {
        std::string error = err_msg ? err_msg : "unknown error";
        if (err_msg) free(err_msg);
        return tl::make_unexpected(fmt::format(
            "etcd list keys failed: prefix={}, error={}", prefix, error));
    }

    if (count > 0 && keys != nullptr && key_sizes != nullptr) {
        object_keys.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; i++) {
            if (keys[i] != nullptr) {
                object_keys.emplace_back(keys[i],
                                         static_cast<size_t>(key_sizes[i]));
                free(keys[i]);
            }
        }
        free(keys);
        free(key_sizes);
    }

    VLOG(1) << "EtcdSnapshotObjectStore: listed " << object_keys.size()
            << " keys with prefix=" << prefix;
    return {};
}

bool EtcdSnapshotObjectStore::IsNotFoundError(const std::string& error) const {
    return error.find("key not found") != std::string::npos;
}

std::string EtcdSnapshotObjectStore::GetConnectionInfo() const {
    return fmt::format("EtcdSnapshotObjectStore: endpoints={}", endpoints_);
}

}  // namespace mooncake

#endif  // STORE_USE_ETCD
