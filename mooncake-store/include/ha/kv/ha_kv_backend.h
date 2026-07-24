#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake {

struct KvPair {
    std::string key;
    std::string value;
};

enum class KvCompareKind {
    kValueEquals,
    kKeyNotExists,
};

struct KvCompare {
    std::string key;
    KvCompareKind kind{KvCompareKind::kValueEquals};
    std::string expected_value;
};

struct KvTxn {
    std::vector<KvCompare> compares;
    std::vector<KvPair> puts;
};

class HaKvBackend {
   public:
    virtual ~HaKvBackend() = default;

    virtual ErrorCode Get(std::string_view key, std::string& value) = 0;
    virtual ErrorCode Put(std::string_view key, std::string_view value) = 0;
    virtual ErrorCode Range(std::string_view begin_key,
                            std::string_view end_key, size_t limit,
                            std::vector<KvPair>& kvs) = 0;
    virtual bool SupportsTxn() const = 0;
    virtual ErrorCode Txn(const KvTxn& txn) = 0;
};

}  // namespace mooncake
