#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "types.h"

namespace mooncake {

class K8sLeaseHelper {
   public:
    static ErrorCode Init();

    static ErrorCode RunElection(const std::string& ns,
                                 const std::string& lease,
                                 const std::string& identity, int lease_dur,
                                 int renew_deadline, int retry_period);

    static ErrorCode WaitElected(const std::string& ns,
                                 const std::string& lease, int timeout_sec,
                                 int64_t& lease_transitions);

    static ErrorCode WaitLost(const std::string& ns, const std::string& lease);

    static ErrorCode CancelElection(const std::string& ns,
                                    const std::string& lease);

    static ErrorCode GetHolder(const std::string& ns, const std::string& lease,
                               std::string& holder, int64_t& lease_transitions);

    static ErrorCode WatchHolder(
        const std::string& ns, const std::string& lease, void* callback_context,
        void (*callback_func)(void*, const char*, size_t, int64_t));

    static ErrorCode CancelWatch(const std::string& ns,
                                 const std::string& lease);

   private:
    static std::mutex init_mutex_;
    static bool initialized_;
};

}  // namespace mooncake
