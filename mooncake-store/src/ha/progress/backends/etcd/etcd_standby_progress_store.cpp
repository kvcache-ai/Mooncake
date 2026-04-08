#include "ha/progress/backends/etcd/etcd_standby_progress_store.h"

#include <sstream>
#include <utility>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "ha/common/etcd/etcd_helper.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace etcd {

namespace {

using mooncake::EtcdHelper;

std::string ExtractStandbyId(std::string_view key, std::string_view prefix) {
    if (key.substr(0, prefix.size()) != prefix) {
        return "";
    }
    return std::string(key.substr(prefix.size()));
}

}  // namespace

EtcdStandbyProgressStore::EtcdStandbyProgressStore(
    ClusterNamespace cluster_namespace)
    : cluster_namespace_(ResolveClusterNamespace(cluster_namespace)),
      prefix_(BuildProgressPrefix(cluster_namespace_)) {}

#ifndef STORE_USE_ETCD

ErrorCode EtcdStandbyProgressStore::Publish(const StandbyProgressRecord&) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

tl::expected<std::vector<StandbyProgressRecord>, ErrorCode>
EtcdStandbyProgressStore::List() {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

ErrorCode EtcdStandbyProgressStore::Delete(std::string_view) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

ClusterNamespace EtcdStandbyProgressStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string EtcdStandbyProgressStore::BuildProgressPrefix(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string EtcdStandbyProgressStore::BuildRecordKey(
    const ClusterNamespace& cluster_namespace, std::string_view standby_id) {
    (void)standby_id;
    return cluster_namespace;
}

std::string EtcdStandbyProgressStore::BuildRangeEnd(std::string_view prefix) {
    return std::string(prefix);
}

#else

ErrorCode EtcdStandbyProgressStore::Publish(
    const StandbyProgressRecord& record) {
    if (record.standby_id.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    const auto key = BuildRecordKey(cluster_namespace_, record.standby_id);
    const auto value =
        standby_progress_store_detail::SerializeStandbyProgressValue(record);
    return EtcdHelper::Put(key.c_str(), key.size(), value.c_str(),
                           value.size());
}

tl::expected<std::vector<StandbyProgressRecord>, ErrorCode>
EtcdStandbyProgressStore::List() {
    const auto range_end = BuildRangeEnd(prefix_);
    std::string json;
    EtcdRevisionId revision_id = 0;
    auto err = EtcdHelper::GetRangeAsJson(prefix_.c_str(), prefix_.size(),
                                          range_end.c_str(), range_end.size(),
                                          0, json, revision_id);
    (void)revision_id;
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string parse_errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &parse_errors)) {
        LOG(ERROR) << "Failed to parse standby progress range JSON: "
                   << parse_errors;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (!root.isArray()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<StandbyProgressRecord> records;
    for (const auto& kv : root) {
        const auto key = kv.get("key", "").asString();
        const auto value = kv.get("value", "").asString();
        const auto standby_id = ExtractStandbyId(key, prefix_);
        if (standby_id.empty()) {
            continue;
        }

        auto record =
            standby_progress_store_detail::DeserializeStandbyProgressValue(
                standby_id, value);
        if (!record) {
            return tl::make_unexpected(record.error());
        }
        records.push_back(std::move(record.value()));
    }

    return records;
}

ErrorCode EtcdStandbyProgressStore::Delete(std::string_view standby_id) {
    if (standby_id.empty()) {
        return ErrorCode::OK;
    }

    const auto key = BuildRecordKey(cluster_namespace_, standby_id);
    const auto range_end = key + '\0';
    return EtcdHelper::DeleteRange(key.c_str(), key.size(), range_end.c_str(),
                                   range_end.size());
}

ClusterNamespace EtcdStandbyProgressStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    if (cluster_namespace.empty()) {
        return "mooncake";
    }
    return cluster_namespace;
}

std::string EtcdStandbyProgressStore::BuildProgressPrefix(
    const ClusterNamespace& cluster_namespace) {
    return "/ha/standby_progress/" + cluster_namespace + "/";
}

std::string EtcdStandbyProgressStore::BuildRecordKey(
    const ClusterNamespace& cluster_namespace, std::string_view standby_id) {
    return BuildProgressPrefix(cluster_namespace) + std::string(standby_id);
}

std::string EtcdStandbyProgressStore::BuildRangeEnd(std::string_view prefix) {
    return std::string(prefix) + '\xFF';
}

#endif

}  // namespace etcd
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
