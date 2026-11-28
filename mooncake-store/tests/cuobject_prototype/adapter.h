#pragma once

#include <memory>
#include <string>

#include "storage/distributed/s3_adapter.h"

namespace mooncake::cuobject_prototype {

// Experimental adapter: buffers must be explicitly registered before GET/PUT.
// The caller keeps the allocation and its registration alive through
// completion.
class RdmaClient {
   public:
    virtual ~RdmaClient() = default;
    virtual bool Register(void* ptr, size_t size) = 0;
    virtual void Unregister(void* ptr) = 0;
    virtual std::string Token(void* ptr, size_t size, bool upload) = 0;
    virtual void Release(const std::string& token) = 0;
    // Must revoke remote access before returning. The SDK 1.2 prototype exits
    // instead: that SDK has no token-reset API for ambiguous HTTP failures.
    virtual void Abort(const std::string& token) = 0;
};

std::shared_ptr<RdmaClient> CreateSdkClient();

class Adapter : public S3ObjectStorageAdapter {
   public:
    Adapter(std::string prefix, std::shared_ptr<RdmaClient> rdma)
        : S3ObjectStorageAdapter(std::move(prefix)), rdma_(std::move(rdma)) {}
    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      std::span<const char> data) override;
    tl::expected<void, ErrorCode> PutV(const std::string& key, const iovec* iov,
                                       int count) override;
    tl::expected<size_t, ErrorCode> Get(const std::string& key, void* ptr,
                                        size_t size) override;
    std::vector<tl::expected<void, ErrorCode>> PutBatch(
        const std::vector<ObjectPutRequest>& requests) override;
    std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) override;

   private:
    tl::expected<size_t, ErrorCode> Transfer(const std::string& key, void* ptr,
                                             size_t size, bool upload);
    std::shared_ptr<RdmaClient> rdma_;
};

}  // namespace mooncake::cuobject_prototype
