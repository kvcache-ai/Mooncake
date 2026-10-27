#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "storage/distributed/object_storage_adapter.h"

struct redisContext;

namespace mooncake {

/**
 * @brief Connection settings for the Valkey object storage adapter.
 *
 * Read from MOONCAKE_VALKEY_ADDR (host:port), MOONCAKE_VALKEY_USERNAME,
 * MOONCAKE_VALKEY_PASSWORD, MOONCAKE_VALKEY_POOL_SIZE and
 * MOONCAKE_VALKEY_TIMEOUT_MS.
 */
struct ValkeyAdapterConfig {
    std::string host = "127.0.0.1";
    int port = 6379;
    std::string username;
    std::string password;
    int pool_size = 8;
    int timeout_ms = 30000;

    static tl::expected<ValkeyAdapterConfig, ErrorCode> FromEnvironment();
};

/**
 * @brief Stores Mooncake objects in Valkey through the valkey-large-object
 * module (https://github.com/valkey-io/valkey-large-object).
 *
 * Each logical key maps to the Valkey key `key_prefix/logical_key` (the prefix
 * with leading and trailing slashes stripped, as the OSS adapter does), holding
 * one large object written with BLOB.TCP_SET and read with BLOB.TCP_GET.
 * Logical keys are binary-safe. Valkey may evict objects under its own
 * maxmemory policy; a missing object reads as FILE_NOT_FOUND.
 *
 * Thread-safe: requests draw connections from a fixed pool, and batch
 * requests are spread across the pool and pipelined per connection.
 */
class ValkeyObjectStorageAdapter : public ObjectStorageAdapter {
   public:
    explicit ValkeyObjectStorageAdapter(std::string key_prefix);
    ~ValkeyObjectStorageAdapter() override;

    tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                      std::span<const char> data) override;
    tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                       const iovec* iov, int iovcnt) override;
    tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                        void* buf, size_t len) override;
    std::vector<tl::expected<void, ErrorCode>> PutBatch(
        const std::vector<ObjectPutRequest>& requests) override;
    std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) override;
    tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) override;
    tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) override;
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override;
    tl::expected<void, ErrorCode> Init() override;
    tl::expected<void, ErrorCode> CheckHealth() override;
    const char* GetName() const override { return "valkey"; }

   private:
    class Lease;

    // `key_prefix_/`, or empty for an empty prefix.
    std::string PhysicalPrefix() const;
    std::string PhysicalKey(const std::string& logical_key) const;
    redisContext* Acquire();
    void Release(redisContext* context);
    tl::expected<redisContext*, ErrorCode> Connect() const;

    std::string key_prefix_;
    ValkeyAdapterConfig config_;
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
    std::vector<redisContext*> idle_;
    std::vector<redisContext*> all_;
};

}  // namespace mooncake
