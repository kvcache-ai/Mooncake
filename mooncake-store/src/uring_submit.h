#pragma once

#include <cerrno>
#include <cstdint>

namespace mooncake::detail {

struct UringSubmitResult {
    unsigned submitted = 0;
    unsigned pending = 0;
    int error = 0;
};

template <typename PendingFn, typename SubmitFn, typename YieldFn>
UringSubmitResult submit_all_pending(PendingFn&& pending_fn,
                                     SubmitFn&& submit_fn, YieldFn&& yield_fn,
                                     unsigned max_transient_retries = 64) {
    UringSubmitResult result;
    unsigned transient_retries = 0;

    while ((result.pending = pending_fn()) != 0) {
        int ret = submit_fn(result.pending);
        if (ret > 0) {
            if (static_cast<unsigned>(ret) > result.pending) {
                result.error = -EIO;
                return result;
            }
            result.submitted += static_cast<unsigned>(ret);
            transient_retries = 0;
            continue;
        }

        if (ret == -EINTR || ret == -EAGAIN || ret == -ENOMEM) {
            if (transient_retries++ < max_transient_retries) {
                yield_fn();
                continue;
            }
        }

        result.error = ret == 0 ? -EIO : ret;
        result.pending = pending_fn();
        return result;
    }

    return result;
}

}  // namespace mooncake::detail
