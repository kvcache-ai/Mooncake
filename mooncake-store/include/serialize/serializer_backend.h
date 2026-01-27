#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

// Snapshot storage backend type enumeration
enum class SnapshotBackendType {
    LOCAL_FILE = 0,  // Local file system (default)
    S3 = 1,          // S3 storage
    ETCD = 2         // Etcd storage
};

// Convert string to SnapshotBackendType
inline SnapshotBackendType ParseSnapshotBackendType(
    const std::string& type_str) {
    // Trim leading and trailing whitespace
    auto trim = [](const std::string& s) {
        auto start = s.find_first_not_of(" \t\n\r");
        if (start == std::string::npos) return std::string();
        auto end = s.find_last_not_of(" \t\n\r");
        return s.substr(start, end - start + 1);
    };
    std::string trimmed = trim(type_str);

#ifdef STORE_USE_ETCD
    if (trimmed == "etcd" || trimmed == "ETCD") {
        return SnapshotBackendType::ETCD;
    }
#else
    if (trimmed == "etcd" || trimmed == "ETCD") {
        throw std::invalid_argument(
            "ETCD backend requested but STORE_USE_ETCD is not enabled. "
            "Please rebuild with STORE_USE_ETCD or use 'local' backend.");
    }
#endif
#ifdef HAVE_AWS_SDK
    if (trimmed == "s3" || trimmed == "S3") {
        return SnapshotBackendType::S3;
    }
#else
    if (trimmed == "s3" || trimmed == "S3") {
        throw std::invalid_argument(
            "S3 backend requested but AWS SDK is not available. "
            "Please rebuild with HAVE_AWS_SDK or use 'local' backend.");
    }
#endif
    return SnapshotBackendType::LOCAL_FILE;  // Default to local file
}

// Convert SnapshotBackendType to string
inline std::string SnapshotBackendTypeToString(SnapshotBackendType type) {
    switch (type) {
        case SnapshotBackendType::ETCD:
            return "etcd";
        case SnapshotBackendType::S3:
            return "s3";
        case SnapshotBackendType::LOCAL_FILE:
            return "local";
        default:
            throw std::invalid_argument("Unknown SnapshotBackendType: " +
                                        std::to_string(static_cast<int>(type)));
    }
}

/**
 * @brief Abstract interface for serialization storage backend
 *
 * Defines a unified storage interface supporting different implementations (S3,
 * local file system, etc.)
 */
class SerializerBackend {
   public:
    virtual ~SerializerBackend() = default;

    /**
     * @brief Upload binary data
     * @param key Storage key (path)
     * @param buffer Binary data
     * @return Empty on success, error message on failure
     */
    virtual tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) = 0;

    /**
     * @brief Download binary data (supports chunked download for large files)
     * @param key Storage key (path)
     * @param buffer Output buffer
     * @return Empty on success, error message on failure
     */
    virtual tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) = 0;

    /**
     * @brief Upload string data
     * @param key Storage key (path)
     * @param data String data
     * @return Empty on success, error message on failure
     */
    virtual tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) = 0;

    /**
     * @brief Download string data
     * @param key Storage key (path)
     * @param data Output string
     * @return Empty on success, error message on failure
     */
    virtual tl::expected<void, std::string> DownloadString(
        const std::string& key, std::string& data) = 0;

    /**
     * @brief Delete all objects with specified prefix
     * @param prefix Prefix
     * @return Empty on success, error message on failure
     */
    virtual tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) = 0;

    /**
     * @brief List all objects with specified prefix
     * @param prefix Prefix
     * @param object_keys Output list of object keys
     * @return Empty on success, error message on failure
     */
    virtual tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix, std::vector<std::string>& object_keys) = 0;

    /**
     * @brief Get connection/configuration info (for logging)
     * @return Connection info string
     */
    virtual std::string GetConnectionInfo() const = 0;

    /**
     * @brief Factory method: create backend instance by type
     * @param type Backend type
     * @param etcd_endpoints Etcd endpoints (for ETCD backend)
     * @return Smart pointer to backend instance
     */
    static std::unique_ptr<SerializerBackend> Create(
        SnapshotBackendType type, const std::string& etcd_endpoints = "");
};

#ifdef HAVE_AWS_SDK
/**
 * @brief S3 storage backend implementation
 *
 * Wraps S3Helper to provide S3 storage functionality
 * Note: Only available when HAVE_AWS_SDK macro is defined at compile time
 */
class S3Backend : public SerializerBackend {
   public:
    S3Backend();
    ~S3Backend() override = default;

    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override;

    tl::expected<void, std::string> DownloadString(const std::string& key,
                                                   std::string& data) override;

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override;

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override;

    std::string GetConnectionInfo() const override;

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
#endif  // HAVE_AWS_SDK

#ifdef STORE_USE_ETCD
/**
 * @brief Etcd storage backend implementation
 *
 * Stores snapshot data to etcd cluster
 */
class EtcdBackend : public SerializerBackend {
   public:
    explicit EtcdBackend(const std::string& endpoints, bool force_reconnect = false);
    ~EtcdBackend() override = default;

    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override;

    tl::expected<void, std::string> DownloadString(
        const std::string& key, std::string& data) override;

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override;

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix, std::vector<std::string>& object_keys) override;

    std::string GetConnectionInfo() const override;

   private:
    std::string endpoints_;
};
#endif  // STORE_USE_ETCD

/**
 * @brief Local file storage backend implementation
 *
 * Stores snapshot data to local file system
 * Storage path is configured via SNAPSHOT_LOCAL_PATH environment variable
 */
class LocalFileBackend : public SerializerBackend {
   public:
    /**
     * @brief Default constructor
     * Reads storage path from SNAPSHOT_LOCAL_PATH environment variable
     * If not set, uses default path /tmp/mooncake_snapshots
     */
    LocalFileBackend();

    /**
     * @brief Constructor with specified path
     * @param base_path Base storage path
     */
    explicit LocalFileBackend(const std::string& base_path);

    ~LocalFileBackend() override = default;

    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override;

    tl::expected<void, std::string> DownloadString(const std::string& key,
                                                   std::string& data) override;

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override;

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override;

    std::string GetConnectionInfo() const override;

   private:
    std::string base_path_;  // Base path for local file storage

    // Convert key to full file path
    std::string KeyToPath(const std::string& key) const;

    // Ensure directory exists
    tl::expected<void, std::string> EnsureDirectoryExists(
        const std::string& dir_path) const;
};

}  // namespace mooncake
