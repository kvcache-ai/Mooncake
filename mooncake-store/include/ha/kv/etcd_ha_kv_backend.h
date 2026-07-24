#pragma once

#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

class EtcdHaKvBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override;
    ErrorCode Put(std::string_view key, std::string_view value) override;
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override;
    bool SupportsTxn() const override;
    ErrorCode Txn(const KvTxn& txn) override;
};

}  // namespace mooncake
