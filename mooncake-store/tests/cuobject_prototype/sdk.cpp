#include "adapter.h"

#include <cuobjclient.h>

#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <stdexcept>

namespace mooncake::cuobject_prototype {
namespace {
class SdkClient final : public RdmaClient {
   public:
    SdkClient() {
        if (!client_.isConnected())
            throw std::runtime_error("cuObject SDK initialization failed");
    }
    ~SdkClient() override {
        if (!tokens_.empty()) Abort("");
        for (const auto& [ptr, size] : buffers_) {
            if (client_.cuMemObjPutDescriptor(ptr) != CU_OBJ_SUCCESS) Abort("");
        }
    }
    bool Register(void* ptr, size_t size) override {
        std::lock_guard lock(mutex_);
        if (!ptr || !size || size > CUOBJ_MAX_MEMORY_REG_SIZE) return false;
        if (auto it = buffers_.find(ptr); it != buffers_.end())
            return it->second == size;
        if (client_.cuMemObjGetDescriptor(ptr, size) != CU_OBJ_SUCCESS)
            return false;
        buffers_.emplace(ptr, size);
        return true;
    }
    void Unregister(void* ptr) override {
        std::lock_guard lock(mutex_);
        if (!tokens_.empty()) Abort("");
        if (buffers_.contains(ptr)) {
            if (client_.cuMemObjPutDescriptor(ptr) != CU_OBJ_SUCCESS) Abort("");
            buffers_.erase(ptr);
        }
    }
    std::string Token(void* ptr, size_t size, bool upload) override {
        std::lock_guard lock(mutex_);
        auto it = buffers_.find(ptr);
        if (it == buffers_.end() || size > it->second || !size) return {};
        char* raw = nullptr;
        if (client_.cuMemObjGetRDMAToken(ptr, size, 0,
                                         upload ? CUOBJ_PUT : CUOBJ_GET,
                                         &raw) != CU_OBJ_SUCCESS ||
            !raw)
            return {};
        std::string token(raw);
        if (!tokens_.emplace(token, raw).second) Abort("");
        return token;
    }
    void Release(const std::string& token) override {
        std::lock_guard lock(mutex_);
        auto it = tokens_.find(token);
        if (it == tokens_.end() ||
            client_.cuMemObjPutRDMAToken(it->second) != CU_OBJ_SUCCESS)
            Abort("");
        tokens_.erase(it);
    }
    [[noreturn]] void Abort(const std::string&) override {
        // Do not return borrowed memory to the caller while a server could
        // still DMA to it. SDK 1.2 cannot revoke an unresolved token. Exit
        // without unwinding/freeing buffers; process teardown releases MRs.
        std::fputs(
            "cuObject prototype: unresolved transfer or cleanup failure; "
            "terminating without reusing registered buffers\n",
            stderr);
        std::_Exit(2);
    }

   private:
    CUObjOps_t ops_{};
    cuObjClient client_{ops_};
    std::mutex mutex_;
    std::map<void*, size_t> buffers_;
    std::map<std::string, char*> tokens_;
};
}  // namespace

std::shared_ptr<RdmaClient> CreateSdkClient() {
    return std::make_shared<SdkClient>();
}
}  // namespace mooncake::cuobject_prototype
