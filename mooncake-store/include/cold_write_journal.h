#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "cold_write.h"

namespace mooncake {

// Durable reservation tokens fence managed data from ordinary metadata scans.
// Tokens remain after publication. Removing one requires confirmed rejection.
class ColdWriteJournal {
   public:
    explicit ColdWriteJournal(std::string storage_root);
    ~ColdWriteJournal();
    ColdWriteJournal(const ColdWriteJournal&) = delete;
    ColdWriteJournal& operator=(const ColdWriteJournal&) = delete;

    tl::expected<void, ErrorCode> Init();
    tl::expected<std::unordered_map<std::string, ColdWriteRequest>, ErrorCode>
    List();
    tl::expected<void, ErrorCode> Put(const std::string& storage_key,
                                      const ColdWriteRequest& request);
    tl::expected<void, ErrorCode> Remove(const std::string& storage_key);

   private:
    tl::expected<std::unordered_map<std::string, ColdWriteRequest>, ErrorCode>
    ListLocked();

    std::string storage_root_;
    std::string directory_;
    std::mutex mutex_;
    int lock_fd_{-1};
    bool initialized_{false};
};

}  // namespace mooncake
