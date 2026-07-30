#include "ha/kv/etcd_ha_kv_backend.h"

#include <memory>
#include <sstream>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "etcd_helper.h"

namespace mooncake {
namespace {

EtcdHelper::TxnCompareKind ToEtcdCompareKind(KvCompareKind kind) {
    switch (kind) {
        case KvCompareKind::kValueEquals:
            return EtcdHelper::TxnCompareKind::kValueEquals;
        case KvCompareKind::kKeyNotExists:
            return EtcdHelper::TxnCompareKind::kKeyNotExists;
    }
    return EtcdHelper::TxnCompareKind::kValueEquals;
}

}  // namespace

ErrorCode EtcdHaKvBackend::Get(std::string_view key, std::string& value) {
    EtcdRevisionId revision_id = 0;
    return EtcdHelper::Get(key.data(), key.size(), value, revision_id);
}

ErrorCode EtcdHaKvBackend::Put(std::string_view key, std::string_view value) {
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdHaKvBackend::Range(std::string_view begin_key,
                                 std::string_view end_key, size_t limit,
                                 std::vector<KvPair>& kvs) {
    kvs.clear();
    std::string json;
    EtcdRevisionId revision_id = 0;
    ErrorCode err = EtcdHelper::GetRangeAsJson(
        begin_key.data(), begin_key.size(), end_key.data(), end_key.size(),
        limit, json, revision_id);
    if (err != ErrorCode::OK) {
        return err;
    }

    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        LOG(ERROR) << "Failed to parse etcd range JSON: " << errors;
        return ErrorCode::INTERNAL_ERROR;
    }

    kvs.reserve(root.size());
    for (const auto& item : root) {
        if (!item.isObject() || !item["key"].isString() ||
            !item["value"].isString()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        kvs.push_back(
            {.key = item["key"].asString(), .value = item["value"].asString()});
    }
    return ErrorCode::OK;
}

bool EtcdHaKvBackend::SupportsTxn() const { return true; }

ErrorCode EtcdHaKvBackend::Txn(const KvTxn& txn) {
    std::vector<EtcdHelper::TxnCompare> compares;
    compares.reserve(txn.compares.size());
    for (const auto& compare : txn.compares) {
        compares.push_back({.key = compare.key,
                            .kind = ToEtcdCompareKind(compare.kind),
                            .expected_value = compare.expected_value});
    }

    std::vector<EtcdHelper::TxnPut> puts;
    puts.reserve(txn.puts.size());
    for (const auto& put : txn.puts) {
        puts.push_back({.key = put.key, .value = put.value});
    }
    return EtcdHelper::TxnCompareAndPut(compares, puts);
}

}  // namespace mooncake
