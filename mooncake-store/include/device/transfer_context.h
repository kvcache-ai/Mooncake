#pragma once

#include <string_view>

#include <glog/logging.h>

#ifdef USE_ASCEND_DIRECT
#include <acl/acl.h>
#endif

namespace mooncake::device {

// Borrow the context used to register an SSD staging arena. It must outlive
// background transfers; drain the prefetch pool before transport teardown.
// Other protocols submit host staging without an Ascend context binding.
class TransferContext {
   public:
    bool Capture(std::string_view protocol) {
#ifdef USE_ASCEND_DIRECT
        required_ = protocol == "ascend";
        context_ = nullptr;
        if (required_) {
            const auto ret = aclrtGetCurrentContext(&context_);
            if (ret != ACL_ERROR_NONE || context_ == nullptr) {
                context_ = nullptr;
                LOG(WARNING)
                    << "Cannot capture SSD staging ACL context, ret=" << ret;
                return false;
            }
        }
#else
        (void)protocol;
#endif
        return true;
    }

    class Guard {
       public:
        explicit Guard(const TransferContext& captured) {
#ifdef USE_ASCEND_DIRECT
            if (!captured.required_) return;
            if (captured.context_ == nullptr) {
                ok_ = false;
                return;
            }
            if (aclrtGetCurrentContext(&previous_) != ACL_ERROR_NONE) {
                previous_ = nullptr;
            }
            if (previous_ == captured.context_) return;
            const auto ret = aclrtSetCurrentContext(captured.context_);
            if (ret != ACL_ERROR_NONE) {
                LOG(WARNING)
                    << "Cannot bind SSD staging ACL context, ret=" << ret;
                ok_ = false;
                return;
            }
            changed_ = true;
#else
            (void)captured;
#endif
        }

        ~Guard() {
#ifdef USE_ASCEND_DIRECT
            // A fresh worker has no previous context. Keep the borrowed
            // binding there; the next job still checks its own context.
            if (changed_ && previous_ != nullptr) {
                const auto ret = aclrtSetCurrentContext(previous_);
                if (ret != ACL_ERROR_NONE) {
                    LOG(WARNING)
                        << "Cannot restore worker ACL context, ret=" << ret;
                }
            }
#endif
        }

        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;
        bool ok() const { return ok_; }

       private:
        bool ok_ = true;
#ifdef USE_ASCEND_DIRECT
        aclrtContext previous_ = nullptr;
        bool changed_ = false;
#endif
    };

   private:
#ifdef USE_ASCEND_DIRECT
    bool required_ = false;
    aclrtContext context_ = nullptr;
#endif
};

}  // namespace mooncake::device
