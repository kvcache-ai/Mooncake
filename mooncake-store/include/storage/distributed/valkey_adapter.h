#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
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
 * MOONCAKE_VALKEY_PASSWORD, MOONCAKE_VALKEY_POOL_SIZE,
 * MOONCAKE_VALKEY_TIMEOUT_MS and MOONCAKE_VALKEY_TRANSPORT (`tcp` or `rdma`).
 */
struct ValkeyAdapterConfig {
    std::string host = "127.0.0.1";
    int port = 6379;
    std::string username;
    std::string password;
    int pool_size = 8;
    int timeout_ms = 30000;
    // Move object bytes with server-initiated RDMA (BLOB.RDMA_*) between the
    // Valkey server and this process's Transfer Engine buffers.
    bool rdma = false;

    static tl::expected<ValkeyAdapterConfig, ErrorCode> FromEnvironment();
};

/**
 * @brief Stores Mooncake objects in Valkey through the valkey-large-object
 * module (https://github.com/valkey-io/valkey-large-object).
 *
 * Each logical key maps to the Valkey key `key_prefix/logical_key` (the prefix
 * with leading and trailing slashes stripped, as the OSS adapter does), holding
 * one large object. Logical keys are binary-safe. Valkey may evict objects
 * under its own maxmemory policy; a missing object reads as FILE_NOT_FOUND.
 *
 * Transport: object bytes travel over the RESP connection by default
 * (BLOB.TCP_SET / BLOB.TCP_GET). With MOONCAKE_VALKEY_TRANSPORT=rdma and a
 * Transfer Engine segment bound through BindTransferEndpoint(), the server
 * reads and writes the caller's buffers directly with one-sided RDMA
 * (BLOB.RDMA_SET / BLOB.RDMA_GET). That needs the module's Transfer Engine
 * backend on the server, and the buffers registered with this process's
 * Transfer Engine, which offload sources (mounted segments) and FileStorage's
 * load buffers are. A request the server cannot carry over RDMA, e.g. one whose
 * buffer is not registered, is redone over TCP.
 *
 * Thread-safe: requests draw connections from a fixed pool, and batch
 * requests are spread across the pool and pipelined per connection.
 */
class ValkeyObjectStorageAdapter : public ObjectStorageAdapter {
   public:
    struct TransportStats {
        uint64_t rdma_requests = 0;
        uint64_t tcp_requests = 0;
        // RDMA attempts that were redone over TCP.
        uint64_t rdma_fallbacks = 0;
    };

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
    void BindTransferEndpoint(const std::string& segment_name) override;
    const char* GetName() const override { return "valkey"; }

    TransportStats GetTransportStats() const;

   private:
    struct Connection {
        redisContext* context = nullptr;
        // BLOB.RDMA_HELLO was tried on this connection, and whether it worked.
        bool rdma_tried = false;
        bool rdma_ready = false;
    };
    class Lease;

    // `key_prefix_/`, or empty for an empty prefix.
    std::string PhysicalPrefix() const;
    std::string PhysicalKey(const std::string& logical_key) const;
    Connection* Acquire();
    void Release(Connection* connection);
    tl::expected<redisContext*, ErrorCode> Connect() const;
    // Whether this connection carries RDMA, opening its session on first use.
    bool EnsureRdma(Connection& connection);

    tl::expected<void, ErrorCode> PutOn(Connection& connection,
                                        const std::string& key,
                                        const iovec* iov, int iovcnt);
    tl::expected<void, ErrorCode> TcpPutOn(Connection& connection,
                                           const std::string& key,
                                           const iovec* iov, int iovcnt);
    tl::expected<size_t, ErrorCode> TcpGetOn(Connection& connection,
                                             const std::string& key, void* buf,
                                             size_t len);

    std::string key_prefix_;
    ValkeyAdapterConfig config_;
    std::mutex endpoint_mutex_;
    std::string transfer_endpoint_;
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
    std::vector<Connection*> idle_;
    std::vector<std::unique_ptr<Connection>> all_;
    std::atomic<uint64_t> rdma_requests_{0};
    std::atomic<uint64_t> tcp_requests_{0};
    std::atomic<uint64_t> rdma_fallbacks_{0};
};

}  // namespace mooncake
