#include "rpc_service.h"

#include <cstddef>
#include <functional>
#include <optional>
#include <string_view>
#include <type_traits>

#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "master_service.h"
#include "rpc_helper.h"
#include "storage_device_maintenance.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {
namespace {

tl::expected<TenantId, ErrorCode> ResolveRequestTenantId(std::string_view raw) {
    TenantId tenant_id{std::string(raw)};
    if (!tenant_id.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return tenant_id;
}

tl::expected<TenantId, ErrorCode> ResolveTenantIdForWrite(
    std::string_view raw, bool enable_multi_tenants) {
    if (!enable_multi_tenants) {
        return TenantId::Default();
    }
    if (raw.empty()) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    TenantId tenant_id{std::string(raw)};
    if (!tenant_id.IsValid()) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    return tenant_id;
}

template <typename Fn>
auto WithRequestTenant(std::string_view raw, Fn&& fn) {
    using Result = std::invoke_result_t<Fn, const TenantId&>;
    auto tenant_id = ResolveRequestTenantId(raw);
    if (!tenant_id) {
        return Result(tl::make_unexpected(tenant_id.error()));
    }
    return std::invoke(std::forward<Fn>(fn), tenant_id.value());
}

template <typename Fn>
auto WithRequestTenantBatch(std::string_view raw, size_t result_count,
                            Fn&& fn) {
    using Results = std::invoke_result_t<Fn, const TenantId&>;
    auto tenant_id = ResolveRequestTenantId(raw);
    if (!tenant_id) {
        using Result = typename Results::value_type;
        return Results(result_count,
                       Result(tl::make_unexpected(tenant_id.error())));
    }
    return std::invoke(std::forward<Fn>(fn), tenant_id.value());
}

template <typename Fn>
auto WithWriteTenant(std::string_view raw, bool enable_multi_tenants, Fn&& fn) {
    using Result = std::invoke_result_t<Fn, const TenantId&>;
    auto tenant_id = ResolveTenantIdForWrite(raw, enable_multi_tenants);
    if (!tenant_id) {
        return Result(tl::make_unexpected(tenant_id.error()));
    }
    return std::invoke(std::forward<Fn>(fn), tenant_id.value());
}

}  // namespace

WrappedMasterService::WrappedMasterService(
    const WrappedMasterServiceConfig& config,
    HttpMetadataServer* http_metadata_server,
    const std::string& http_metadata_remote_url)
    : master_service_(MasterServiceConfig(config)) {
    // Configure metadata cleanup on client timeout. Prefer the co-located
    // in-process server; otherwise fall back to a separately-deployed HTTP
    // metadata server derived from the cluster configuration.
    if (http_metadata_server) {
        master_service_.setHttpMetadataServer(http_metadata_server);
    } else if (!http_metadata_remote_url.empty()) {
        master_service_.setHttpMetadataRemoteUrl(http_metadata_remote_url);
    }
}

WrappedMasterService::~WrappedMasterService() = default;

void WrappedMasterService::SetStorageBackendForAdmin(
    std::shared_ptr<StorageBackendInterface> storage_backend) {
    storage_backend_ = std::move(storage_backend);
}

tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
WrappedMasterService::CalcCacheStats() {
    return MasterMetricManager::instance().calculate_cache_stats();
}

tl::expected<bool, ErrorCode> WrappedMasterService::ExistKey(
    const std::string& key, const std::string& tenant_id) {
    return execute_rpc(
        "ExistKey",
        [&]() -> tl::expected<bool, ErrorCode> {
            return WithRequestTenant(
                master_service_.IsTenantQuotaEnabled()
                    ? std::string_view(tenant_id)
                    : TenantId::kDefaultValue,
                [&](const TenantId& resolved_tenant_id)
                    -> tl::expected<bool, ErrorCode> {
                    auto result =
                        master_service_.ExistKey(key, resolved_tenant_id);
                    if (result.has_value() && *result) {
                        return true;
                    }
                    if (storage_backend_) {
                        auto backend_result = storage_backend_->IsExist(key);
                        if (backend_result.has_value() && *backend_result) {
                            return true;
                        }
                    }
                    return result;
                });
        },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { MasterMetricManager::instance().inc_exist_key_requests(); },
        [] { MasterMetricManager::instance().inc_exist_key_failures(); });
}

std::vector<tl::expected<bool, ErrorCode>> WrappedMasterService::BatchExistKey(
    const std::vector<std::string>& keys, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchExistKey");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_exist_key_requests(total_keys);

    auto result = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            return master_service_.BatchExistKey(keys, resolved_tenant_id);
        });

    if (storage_backend_) {
        std::vector<std::string> miss_keys;
        std::vector<size_t> miss_indices;
        for (size_t i = 0; i < result.size(); ++i) {
            if (result[i].has_value() && !*result[i]) {
                miss_keys.push_back(keys[i]);
                miss_indices.push_back(i);
            }
        }
        if (!miss_keys.empty()) {
            auto backend_results = storage_backend_->BatchIsExist(miss_keys);
            for (size_t j = 0; j < backend_results.size(); ++j) {
                if (backend_results[j].has_value() && *backend_results[j]) {
                    result[miss_indices[j]] = true;
                }
            }
        }
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < result.size(); ++i) {
        if (!result[i].has_value()) {
            failure_count++;
            auto error = result[i].error();
            LOG(ERROR) << "BatchExistKey failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_exist_key_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_exist_key_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", result.size(),
                      ", success=", result.size() - failure_count,
                      ", failures=", failure_count);
    return result;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
WrappedMasterService::BatchQueryIp(const std::vector<UUID>& client_ids) {
    ScopedVLogTimer timer(1, "BatchQueryIp");
    const size_t total_client_ids = client_ids.size();
    timer.LogRequest("client_ids_count=", total_client_ids);
    MasterMetricManager::instance().inc_batch_query_ip_requests(
        total_client_ids);

    auto result = master_service_.BatchQueryIp(client_ids);

    size_t failure_count = 0;
    if (!result.has_value()) {
        failure_count = total_client_ids;
    } else {
        for (size_t i = 0; i < client_ids.size(); ++i) {
            const auto& client_id = client_ids[i];
            if (result.value().find(client_id) == result.value().end()) {
                failure_count++;
                VLOG(1) << "BatchQueryIp failed for client_id[" << i << "] '"
                        << client_id << "': not found in results";
            }
        }
    }

    if (failure_count == total_client_ids) {
        MasterMetricManager::instance().inc_batch_query_ip_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_query_ip_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", total_client_ids,
                      ", success=", total_client_ids - failure_count,
                      ", failures=", failure_count);
    return result;
}

tl::expected<std::vector<std::string>, ErrorCode>
WrappedMasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    ScopedVLogTimer timer(1, "BatchReplicaClear");
    const size_t total_keys = object_keys.size();
    timer.LogRequest("object_keys_count=", total_keys,
                     ", client_id=", client_id,
                     ", segment_name=", segment_name);
    MasterMetricManager::instance().inc_batch_replica_clear_requests(
        total_keys);

    auto result =
        master_service_.BatchReplicaClear(object_keys, client_id, segment_name);

    size_t failure_count = 0;
    if (!result.has_value()) {
        failure_count = total_keys;
        LOG(WARNING) << "BatchReplicaClear failed: "
                     << toString(result.error());
    } else {
        const size_t cleared_count = result.value().size();
        failure_count = total_keys - cleared_count;
        timer.LogResponse("total=", total_keys, ", cleared=", cleared_count,
                          ", failed=", failure_count);
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_replica_clear_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_replica_clear_partial_success(
            failure_count);
    }

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
WrappedMasterService::GetReplicaListByRegex(const std::string& str,
                                            const std::string& tenant_id) {
    return execute_rpc(
        "GetReplicaListByRegex",
        [&] {
            return WithRequestTenant(
                master_service_.IsTenantQuotaEnabled()
                    ? std::string_view(tenant_id)
                    : TenantId::kDefaultValue,
                [&](const TenantId& resolved_tenant_id) {
                    return master_service_.GetReplicaListByRegex(
                        str, resolved_tenant_id);
                });
        },
        [&](auto& timer) { timer.LogRequest("Regex=", str); },
        [] {
            MasterMetricManager::instance()
                .inc_get_replica_list_by_regex_requests();
        },
        [] {
            MasterMetricManager::instance()
                .inc_get_replica_list_by_regex_failures();
        });
}

tl::expected<GetReplicaListResponse, ErrorCode>
WrappedMasterService::GetReplicaList(const std::string& key,
                                     const std::string& tenant_id) {
    return execute_rpc(
        "GetReplicaList",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.GetReplicaList(
                                             key, resolved_tenant_id);
                                     });
        },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { MasterMetricManager::instance().inc_get_replica_list_requests(); },
        [] {
            MasterMetricManager::instance().inc_get_replica_list_failures();
        });
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
WrappedMasterService::BatchGetReplicaList(const std::vector<std::string>& keys,
                                          const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchGetReplicaList");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_get_replica_list_requests(
        total_keys);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> results;
    results.reserve(keys.size());

    results = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            return master_service_.BatchGetReplicaList(keys,
                                                       resolved_tenant_id);
        });

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            if (error == ErrorCode::OBJECT_NOT_FOUND ||
                error == ErrorCode::REPLICA_IS_NOT_READY) {
                VLOG(1) << "BatchGetReplicaList failed for key[" << i << "] '"
                        << keys[i] << "': " << toString(error);
            } else {
                LOG(ERROR) << "BatchGetReplicaList failed for key[" << i
                           << "] '" << keys[i] << "': " << toString(error);
            }
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_get_replica_list_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance()
            .inc_batch_get_replica_list_partial_success(failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
WrappedMasterService::BatchGetReplicaListForAdmin(
    const std::vector<std::string>& keys, const std::string& tenant_id) {
    return WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            return master_service_.BatchGetReplicaListForAdmin(
                keys, resolved_tenant_id);
        });
}

tl::expected<GetReplicaListResponse, ErrorCode>
WrappedMasterService::GetReplicaListForAdmin(const std::string& key,
                                             const std::string& tenant_id) {
    return execute_rpc(
        "GetReplicaListForAdmin",
        [&] {
            return WithRequestTenant(
                master_service_.IsTenantQuotaEnabled()
                    ? std::string_view(tenant_id)
                    : TenantId::kDefaultValue,
                [&](const TenantId& resolved_tenant_id) {
                    return master_service_.GetReplicaListForAdmin(
                        key, resolved_tenant_id);
                });
        },
        [&](auto& timer) { timer.LogRequest("key=", key); }, [] {}, [] {});
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
WrappedMasterService::PutStart(const UUID& client_id, const std::string& key,
                               const uint64_t slice_length,
                               const ReplicateConfig& config,
                               const std::string& tenant_id) {
    return execute_rpc(
        "PutStart",
        [&] {
            return WithWriteTenant(tenant_id,
                                   master_service_.IsTenantQuotaEnabled(),
                                   [&](const TenantId& resolved_tenant_id) {
                                       return master_service_.PutStart(
                                           client_id, key, resolved_tenant_id,
                                           slice_length, config);
                                   });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", slice_length=", slice_length);
        },
        [&] { MasterMetricManager::instance().inc_put_start_requests(); },
        [] { MasterMetricManager::instance().inc_put_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::PutEnd(
    const UUID& client_id, const std::string& key, ReplicaType replica_type,
    const std::string& tenant_id) {
    return execute_rpc(
        "PutEnd",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.PutEnd(
                                             client_id, key, resolved_tenant_id,
                                             replica_type);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_end_requests(); },
        [] { MasterMetricManager::instance().inc_put_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::PutRevoke(
    const UUID& client_id, const std::string& key, ReplicaType replica_type,
    const std::string& tenant_id) {
    return execute_rpc(
        "PutRevoke",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.PutRevoke(
                                             client_id, key, resolved_tenant_id,
                                             replica_type);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_put_revoke_failures(); });
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
WrappedMasterService::BatchPutStart(const UUID& client_id,
                                    const std::vector<std::string>& keys,
                                    const std::vector<uint64_t>& slice_lengths,
                                    const ReplicateConfig& config,
                                    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchPutStart");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_start_requests(total_keys);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());
    auto resolved_tenant_id = ResolveTenantIdForWrite(
        tenant_id, master_service_.IsTenantQuotaEnabled());

    if (keys.size() != slice_lengths.size()) {
        LOG(ERROR) << "BatchPutStart: keys.size()=" << keys.size()
                   << " != slice_lengths.size()=" << slice_lengths.size();
        results.assign(keys.size(),
                       tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    } else if (config.group_ids.has_value() &&
               config.group_ids->size() != keys.size()) {
        LOG(ERROR) << "BatchPutStart: group_ids.size()="
                   << config.group_ids->size()
                   << " != keys.size()=" << keys.size();
        results.assign(keys.size(),
                       tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    } else if (!resolved_tenant_id) {
        results.assign(keys.size(),
                       tl::make_unexpected(resolved_tenant_id.error()));
    } else if (config.prefer_alloc_in_same_node) {
        ReplicateConfig new_config = config;
        for (size_t i = 0; i < keys.size(); ++i) {
            auto key_config = new_config.ForSingleKey(i);
            auto result = master_service_.PutStart(
                client_id, keys[i], resolved_tenant_id.value(),
                slice_lengths[i], key_config);
            results.emplace_back(result);
            if ((i == 0) && result.has_value()) {
                std::string preferred_segment;
                for (const auto& replica : result.value()) {
                    if (replica.is_memory_replica()) {
                        auto handles =
                            replica.get_memory_descriptor().buffer_descriptor;
                        if (!handles.transport_endpoint_.empty()) {
                            preferred_segment = handles.transport_endpoint_;
                        }
                    }
                }
                if (!preferred_segment.empty()) {
                    new_config.preferred_segment = preferred_segment;
                }
            }
        }
    } else {
        for (size_t i = 0; i < keys.size(); ++i) {
            auto key_config = config.ForSingleKey(i);
            results.emplace_back(master_service_.PutStart(
                client_id, keys[i], resolved_tenant_id.value(),
                slice_lengths[i], key_config));
        }
    }

    size_t failure_count = 0;
    int no_available_handle_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            if (error == ErrorCode::OBJECT_ALREADY_EXISTS) {
                VLOG(1) << "BatchPutStart failed for key[" << i << "] '"
                        << keys[i] << "': " << toString(error);
            } else if (error == ErrorCode::NO_AVAILABLE_HANDLE) {
                no_available_handle_count++;
            } else {
                LOG(ERROR) << "BatchPutStart failed for key[" << i << "] '"
                           << keys[i] << "': " << toString(error);
            }
        }
    }

    if (no_available_handle_count > 0) {
        LOG(WARNING) << "BatchPutStart failed for " << no_available_handle_count
                     << " keys" << PUT_NO_SPACE_HELPER_STR;
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_start_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_start_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchPutEnd(
    const UUID& client_id, const std::vector<std::string>& keys,
    ReplicaType replica_type, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchPutEnd");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_end_requests(total_keys);

    auto results = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            std::vector<tl::expected<void, ErrorCode>> batch_results;
            batch_results.reserve(keys.size());
            for (const auto& key : keys) {
                batch_results.emplace_back(master_service_.PutEnd(
                    client_id, key, resolved_tenant_id, replica_type));
            }
            return batch_results;
        });

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchPutEnd failed for key[" << i << "] '" << keys[i]
                       << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_end_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_end_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    ReplicaType replica_type, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchPutRevoke");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_revoke_requests(total_keys);

    auto results = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            std::vector<tl::expected<void, ErrorCode>> batch_results;
            batch_results.reserve(keys.size());
            for (const auto& key : keys) {
                batch_results.emplace_back(master_service_.PutRevoke(
                    client_id, key, resolved_tenant_id, replica_type));
            }
            return batch_results;
        });

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchPutRevoke failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_revoke_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_revoke_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
WrappedMasterService::UpsertStart(const UUID& client_id, const std::string& key,
                                  const uint64_t slice_length,
                                  const ReplicateConfig& config,
                                  const std::string& tenant_id) {
    return execute_rpc(
        "UpsertStart",
        [&] {
            return WithWriteTenant(tenant_id,
                                   master_service_.IsTenantQuotaEnabled(),
                                   [&](const TenantId& resolved_tenant_id) {
                                       return master_service_.UpsertStart(
                                           client_id, key, resolved_tenant_id,
                                           slice_length, config);
                                   });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", slice_length=", slice_length);
        },
        [&] { MasterMetricManager::instance().inc_put_start_requests(); },
        [] { MasterMetricManager::instance().inc_put_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::UpsertEnd(
    const UUID& client_id, const std::string& key, ReplicaType replica_type,
    const std::string& tenant_id) {
    return execute_rpc(
        "UpsertEnd",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.UpsertEnd(
                                             client_id, key, resolved_tenant_id,
                                             replica_type);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_end_requests(); },
        [] { MasterMetricManager::instance().inc_put_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::UpsertRevoke(
    const UUID& client_id, const std::string& key, ReplicaType replica_type,
    const std::string& tenant_id) {
    return execute_rpc(
        "UpsertRevoke",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.UpsertRevoke(
                                             client_id, key, resolved_tenant_id,
                                             replica_type);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_put_revoke_failures(); });
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
WrappedMasterService::BatchUpsertStart(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<uint64_t>& slice_lengths, const ReplicateConfig& config,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchUpsertStart");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_start_requests(total_keys);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    if (keys.size() != slice_lengths.size()) {
        LOG(ERROR) << "BatchUpsertStart: keys.size()=" << keys.size()
                   << " != slice_lengths.size()=" << slice_lengths.size();
        results.assign(keys.size(),
                       tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    } else if (config.group_ids.has_value() &&
               config.group_ids->size() != keys.size()) {
        LOG(ERROR) << "BatchUpsertStart: group_ids.size()="
                   << config.group_ids->size()
                   << " != keys.size()=" << keys.size();
        results.assign(keys.size(),
                       tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    } else if (auto resolved_tenant_id = ResolveTenantIdForWrite(
                   tenant_id, master_service_.IsTenantQuotaEnabled());
               !resolved_tenant_id) {
        results.assign(keys.size(),
                       tl::make_unexpected(resolved_tenant_id.error()));
    } else {
        results = master_service_.BatchUpsertStart(
            client_id, keys, resolved_tenant_id.value(), slice_lengths, config);
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchUpsertStart failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_start_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_start_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchUpsertEnd(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchUpsertEnd");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_end_requests(total_keys);

    auto results = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            return master_service_.BatchUpsertEnd(client_id, keys,
                                                  resolved_tenant_id);
        });

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchUpsertEnd failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_end_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_end_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
WrappedMasterService::BatchUpsertRevoke(const UUID& client_id,
                                        const std::vector<std::string>& keys,
                                        const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchUpsertRevoke");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_revoke_requests(total_keys);

    auto results = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            return master_service_.BatchUpsertRevoke(client_id, keys,
                                                     resolved_tenant_id);
        });

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchUpsertRevoke failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_revoke_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_revoke_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<void, ErrorCode> WrappedMasterService::Remove(
    const std::string& key, bool force, const std::string& tenant_id) {
    return execute_rpc(
        "Remove",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.Remove(
                                             key, resolved_tenant_id, force);
                                     });
        },
        [&](auto& timer) { timer.LogRequest("key=", key, ", force=", force); },
        [] { MasterMetricManager::instance().inc_remove_requests(); },
        [] { MasterMetricManager::instance().inc_remove_failures(); });
}

tl::expected<long, ErrorCode> WrappedMasterService::RemoveByRegex(
    const std::string& str, bool force, const std::string& tenant_id) {
    return execute_rpc(
        "RemoveByRegex",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.RemoveByRegex(
                                             str, resolved_tenant_id, force);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("regex=", str, ", force=", force);
        },
        [] { MasterMetricManager::instance().inc_remove_by_regex_requests(); },
        [] { MasterMetricManager::instance().inc_remove_by_regex_failures(); });
}

long WrappedMasterService::RemoveAll(bool force, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);
    MasterMetricManager::instance().inc_remove_all_requests();
    auto resolved_tenant_id = ResolveRequestTenantId(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue);
    if (!resolved_tenant_id) {
        // Keep the legacy scalar wire result; unlike the other tenant-bearing
        // RPCs, RemoveAll cannot propagate an ErrorCode without a wire change.
        LOG(WARNING) << "RemoveAll rejected an invalid tenant id";
        timer.LogResponse("error=", toString(resolved_tenant_id.error()));
        return 0;
    }
    long result = master_service_.RemoveAll(resolved_tenant_id.value(), force);
    timer.LogResponse("items_removed=", result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchRemove(
    const std::vector<std::string>& keys, bool force,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "BatchRemove");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys, ", force=", force);
    MasterMetricManager::instance().inc_remove_requests(total_keys);

    auto results = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            return master_service_.BatchRemove(keys, resolved_tenant_id, force);
        });

    size_t failure_count = 0;
    for (const auto& result : results) {
        if (!result.has_value()) {
            failure_count++;
        }
    }
    if (failure_count > 0) {
        MasterMetricManager::instance().inc_remove_failures(failure_count);
    }

    timer.LogResponse("total=", total_keys, ", failures=", failure_count);
    return results;
}

tl::expected<void, ErrorCode> WrappedMasterService::MountSegment(
    const Segment& segment, const UUID& client_id) {
    return execute_rpc(
        "MountSegment",
        [&] { return master_service_.MountSegment(segment, client_id); },
        [&](auto& timer) {
            timer.LogRequest("base=", segment.base, ", size=", segment.size,
                             ", segment_name=", segment.name,
                             ", id=", segment.id);
        },
        [] { MasterMetricManager::instance().inc_mount_segment_requests(); },
        [] { MasterMetricManager::instance().inc_mount_segment_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::MountNoFSegment(
    const NoFSegment& segment, const UUID& client_id) {
    return execute_rpc(
        "MountNoFSegment",
        [&] { return master_service_.MountNoFSegment(segment, client_id); },
        [&](auto& timer) {
            timer.LogRequest("NoF segment mount: ", "base=", segment.base,
                             ", size=", segment.size,
                             ", segment_name=", segment.name,
                             ", id=", segment.id);
        },
        [] {
            MasterMetricManager::instance().inc_mount_nof_segment_requests();
        },
        [] {
            MasterMetricManager::instance().inc_mount_nof_segment_failures();
        });
}

tl::expected<void, ErrorCode> WrappedMasterService::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    return execute_rpc(
        "ReMountSegment",
        [&] { return master_service_.ReMountSegment(segments, client_id); },
        [&](auto& timer) {
            timer.LogRequest("segments_count=", segments.size(),
                             ", client_id=", client_id);
        },
        [] { MasterMetricManager::instance().inc_remount_segment_requests(); },
        [] { MasterMetricManager::instance().inc_remount_segment_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::ReMountNoFSegment(
    const std::vector<NoFSegment>& segments, const UUID& client_id) {
    return execute_rpc(
        "ReMountNoFSegment",
        [&] { return master_service_.ReMountNoFSegment(segments, client_id); },
        [&](auto& timer) {
            timer.LogRequest("NoF segment remount: ", "segments_count=",
                             segments.size(), ", client_id=", client_id);
        },
        [] {
            MasterMetricManager::instance().inc_remount_nof_segment_requests();
        },
        [] {
            MasterMetricManager::instance().inc_remount_nof_segment_failures();
        });
}

tl::expected<void, ErrorCode> WrappedMasterService::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    return execute_rpc(
        "UnmountSegment",
        [&] { return master_service_.UnmountSegment(segment_id, client_id); },
        [&](auto& timer) {
            timer.LogRequest("segment_id=", segment_id,
                             ", client_id=", client_id);
        },
        [] { MasterMetricManager::instance().inc_unmount_segment_requests(); },
        [] { MasterMetricManager::instance().inc_unmount_segment_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::GracefulUnmountSegment(
    const UUID& segment_id, const UUID& client_id, uint64_t grace_period_ms) {
    return execute_rpc(
        "GracefulUnmountSegment",
        [&] {
            return master_service_.GracefulUnmountSegment(segment_id, client_id,
                                                          grace_period_ms);
        },
        [&](auto& timer) {
            timer.LogRequest("segment_id=", segment_id,
                             ", client_id=", client_id,
                             ", grace_period_ms=", grace_period_ms);
        },
        [] {
            MasterMetricManager::instance()
                .inc_unmount_segment_requests();  // reuse metric or add new
        },
        [] {
            MasterMetricManager::instance()
                .inc_unmount_segment_failures();  // reuse metric or add new
        });
}

tl::expected<void, ErrorCode> WrappedMasterService::UnmountNoFSegment(
    const UUID& segment_id, const UUID& client_id) {
    return execute_rpc(
        "UnmountNoFSegment",
        [&] {
            return master_service_.UnmountNoFSegment(segment_id, client_id);
        },
        [&](auto& timer) {
            timer.LogRequest("NoF segment unmount: ", "segment_id=", segment_id,
                             ", client_id=", client_id);
        },
        [] {
            MasterMetricManager::instance().inc_unmount_nof_segment_requests();
        },
        [] {
            MasterMetricManager::instance().inc_unmount_nof_segment_failures();
        });
}

tl::expected<std::vector<NoFSegment>, ErrorCode>
WrappedMasterService::GetAllNoFSegments() {
    return execute_rpc(
        "GetAllNoFSegments",
        [&] { return master_service_.GetAllNoFSegments(); },
        [&](auto& timer) { timer.LogRequest("Get all NoF segments"); }, [] {},
        [] {});
}

tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>
WrappedMasterService::GetNoFSegmentsByName(const std::string& segment_name) {
    return execute_rpc(
        "GetNoFSegmentsByName",
        [&] { return master_service_.GetNoFSegmentsByName(segment_name); },
        [&](auto& timer) { timer.LogRequest("segment_name=", segment_name); },
        [] {}, [] {});
}

tl::expected<CopyStartResponse, ErrorCode> WrappedMasterService::CopyStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    return execute_rpc(
        "CopyStart",
        [&] {
            return WithWriteTenant(tenant_id,
                                   master_service_.IsTenantQuotaEnabled(),
                                   [&](const TenantId& resolved_tenant_id) {
                                       return master_service_.CopyStart(
                                           client_id, key, resolved_tenant_id,
                                           src_segment, tgt_segments);
                                   });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", tenant_id=", tenant_id,
                             ", src_segment=", src_segment,
                             ", tgt_segments_count=", tgt_segments.size());
        },
        [] { MasterMetricManager::instance().inc_copy_start_requests(); },
        [] { MasterMetricManager::instance().inc_copy_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::CopyEnd(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    return execute_rpc(
        "CopyEnd",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.CopyEnd(
                                             client_id, key,
                                             resolved_tenant_id);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", tenant_id=", tenant_id);
        },
        [] { MasterMetricManager::instance().inc_copy_end_requests(); },
        [] { MasterMetricManager::instance().inc_copy_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::CopyRevoke(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    return execute_rpc(
        "CopyRevoke",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.CopyRevoke(
                                             client_id, key,
                                             resolved_tenant_id);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", tenant_id=", tenant_id);
        },
        [] { MasterMetricManager::instance().inc_copy_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_copy_revoke_failures(); });
}

tl::expected<MoveStartResponse, ErrorCode> WrappedMasterService::MoveStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    const std::string& src_segment, const std::string& tgt_segment) {
    return execute_rpc(
        "MoveStart",
        [&] {
            return WithWriteTenant(tenant_id,
                                   master_service_.IsTenantQuotaEnabled(),
                                   [&](const TenantId& resolved_tenant_id) {
                                       return master_service_.MoveStart(
                                           client_id, key, resolved_tenant_id,
                                           src_segment, tgt_segment);
                                   });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", tenant_id=", tenant_id,
                             ", src_segment=", src_segment,
                             ", tgt_segment=", tgt_segment);
        },
        [] { MasterMetricManager::instance().inc_move_start_requests(); },
        [] { MasterMetricManager::instance().inc_move_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::MoveEnd(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    return execute_rpc(
        "MoveEnd",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.MoveEnd(
                                             client_id, key,
                                             resolved_tenant_id);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", tenant_id=", tenant_id);
        },
        [] { MasterMetricManager::instance().inc_move_end_requests(); },
        [] { MasterMetricManager::instance().inc_move_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::MoveRevoke(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    return execute_rpc(
        "MoveRevoke",
        [&] {
            return WithRequestTenant(master_service_.IsTenantQuotaEnabled()
                                         ? std::string_view(tenant_id)
                                         : TenantId::kDefaultValue,
                                     [&](const TenantId& resolved_tenant_id) {
                                         return master_service_.MoveRevoke(
                                             client_id, key,
                                             resolved_tenant_id);
                                     });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", tenant_id=", tenant_id);
        },
        [] { MasterMetricManager::instance().inc_move_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_move_revoke_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::EvictDiskReplica(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    ReplicaType replica_type) {
    return execute_rpc(
        "EvictDiskReplica",
        [&] {
            return WithRequestTenant(
                master_service_.IsTenantQuotaEnabled()
                    ? std::string_view(tenant_id)
                    : TenantId::kDefaultValue,
                [&](const TenantId& resolved_tenant_id) {
                    return master_service_.EvictDiskReplica(
                        client_id, key, resolved_tenant_id, replica_type);
                });
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", tenant_id=", tenant_id,
                             ", replica_type=", replica_type);
        },
        [] {
            MasterMetricManager::instance().inc_evict_disk_replica_requests();
        },
        [] {
            MasterMetricManager::instance().inc_evict_disk_replica_failures();
        });
}

std::vector<tl::expected<void, ErrorCode>>
WrappedMasterService::BatchEvictDiskReplica(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id, ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "BatchEvictDiskReplica");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys,
                     ", tenant_id=", tenant_id,
                     ", replica_type=", replica_type);
    MasterMetricManager::instance().inc_evict_disk_replica_requests();

    auto results = WithRequestTenantBatch(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        keys.size(), [&](const TenantId& resolved_tenant_id) {
            return master_service_.BatchEvictDiskReplica(
                client_id, keys, resolved_tenant_id, replica_type);
        });

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            LOG(WARNING) << "BatchEvictDiskReplica failed for key[" << i
                         << "] '" << keys[i]
                         << "': " << toString(results[i].error());
        }
    }
    if (failure_count > 0) {
        MasterMetricManager::instance().inc_evict_disk_replica_failures();
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<UUID, ErrorCode> WrappedMasterService::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    return execute_rpc(
        "CreateCopyTask",
        [&] {
            return WithWriteTenant(tenant_id,
                                   master_service_.IsTenantQuotaEnabled(),
                                   [&](const TenantId& resolved_tenant_id) {
                                       return master_service_.CreateCopyTask(
                                           key, resolved_tenant_id, targets);
                                   });
        },
        [&](auto& timer) {
            timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                             ", targets_size=", targets.size());
        },
        [] { MasterMetricManager::instance().inc_create_copy_task_requests(); },
        [] {
            MasterMetricManager::instance().inc_create_copy_task_failures();
        });
}

tl::expected<UUID, ErrorCode> WrappedMasterService::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    return execute_rpc(
        "CreateMoveTask",
        [&] {
            return WithWriteTenant(
                tenant_id, master_service_.IsTenantQuotaEnabled(),
                [&](const TenantId& resolved_tenant_id) {
                    return master_service_.CreateMoveTask(
                        key, resolved_tenant_id, source, target);
                });
        },
        [&](auto& timer) {
            timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                             ", source=", source, ", target=", target);
        },
        [] { MasterMetricManager::instance().inc_create_move_task_requests(); },
        [] {
            MasterMetricManager::instance().inc_create_move_task_failures();
        });
}

tl::expected<QueryTaskResponse, ErrorCode> WrappedMasterService::QueryTask(
    const UUID& task_id) {
    return execute_rpc(
        "QueryTask", [&] { return master_service_.QueryTask(task_id); },
        [&](auto& timer) { timer.LogRequest("task_id=", task_id); },
        [] { MasterMetricManager::instance().inc_query_task_requests(); },
        [] { MasterMetricManager::instance().inc_query_task_failures(); });
}

tl::expected<std::vector<TaskAssignment>, ErrorCode>
WrappedMasterService::FetchTasks(const UUID& client_id, size_t batch_size) {
    return execute_rpc(
        "FetchTasks",
        [&] { return master_service_.FetchTasks(client_id, batch_size); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id,
                             ", batch_size=", batch_size);
        },
        [] { MasterMetricManager::instance().inc_fetch_tasks_requests(); },
        [] { MasterMetricManager::instance().inc_fetch_tasks_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::MarkTaskToComplete(
    const UUID& client_id, const TaskCompleteRequest& request) {
    return execute_rpc(
        "MarkTaskToComplete",
        [&] { return master_service_.MarkTaskToComplete(client_id, request); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", task_id=", request.id);
        },
        [] { MasterMetricManager::instance().inc_update_task_requests(); },
        [] { MasterMetricManager::instance().inc_update_task_failures(); });
}

tl::expected<std::string, ErrorCode> WrappedMasterService::GetFsdir() {
    ScopedVLogTimer timer(1, "GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto result = master_service_.GetFsdir();

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
WrappedMasterService::GetStorageConfig() {
    ScopedVLogTimer timer(1, "GetStorageConfig");
    timer.LogRequest("action=get_storage_config");

    auto result = master_service_.GetStorageConfig();

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<PingResponse, ErrorCode> WrappedMasterService::Ping(
    const UUID& client_id) {
    ScopedVLogTimer timer(1, "Ping");
    timer.LogRequest("client_id=", client_id);

    MasterMetricManager::instance().inc_ping_requests();

    auto result = master_service_.Ping(client_id);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> WrappedMasterService::ServiceReady() {
    return GetMooncakeStoreVersion();
}

tl::expected<std::vector<TenantQuotaSnapshot>, ErrorCode>
WrappedMasterService::ListTenantQuotaSnapshots() {
    if (!master_service_.IsTenantQuotaEnabled()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return master_service_.ListTenantQuotaSnapshots();
}

tl::expected<TenantQuotaSnapshot, ErrorCode>
WrappedMasterService::GetTenantQuotaSnapshot(const std::string& tenant_id) {
    if (!master_service_.IsTenantQuotaEnabled()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return WithRequestTenant(
        tenant_id,
        [&](const TenantId& resolved_tenant_id)
            -> tl::expected<TenantQuotaSnapshot, ErrorCode> {
            auto snapshot =
                master_service_.GetTenantQuotaSnapshot(resolved_tenant_id);
            if (!snapshot.has_value()) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            return snapshot.value();
        });
}

tl::expected<TenantQuotaSnapshot, ErrorCode>
WrappedMasterService::UpsertTenantQuotaPolicy(const std::string& tenant_id,
                                              uint64_t requested_quota_bytes) {
    if (!master_service_.IsTenantQuotaEnabled()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (tenant_id.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    TenantId resolved_tenant_id(tenant_id);
    if (!resolved_tenant_id.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return master_service_.UpsertTenantQuotaPolicy(resolved_tenant_id,
                                                   requested_quota_bytes);
}

tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
WrappedMasterService::DeleteTenantQuotaPolicy(const std::string& tenant_id) {
    if (!master_service_.IsTenantQuotaEnabled()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (tenant_id.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    TenantId resolved_tenant_id(tenant_id);
    if (!resolved_tenant_id.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return master_service_.DeleteTenantQuotaPolicy(resolved_tenant_id);
}

tl::expected<uint64_t, ErrorCode>
WrappedMasterService::GetTenantQuotaAllocatableCapacityBytes() {
    if (!master_service_.IsTenantQuotaEnabled()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return master_service_.GetTenantQuotaAllocatableCapacityBytes();
}

tl::expected<std::vector<std::string>, ErrorCode>
WrappedMasterService::GetAllKeysForAdmin() {
    // Compatibility endpoint: /get_all_keys historically listed only the
    // default tenant's keys.
    return master_service_.GetAllKeys(TenantId::Default());
}

tl::expected<std::vector<std::string>, ErrorCode>
WrappedMasterService::GetAllSegmentsForAdmin() {
    return master_service_.GetAllSegments();
}

tl::expected<std::vector<MasterService::SegmentDetailInfo>, ErrorCode>
WrappedMasterService::GetSegmentsDetailForAdmin() {
    return master_service_.GetSegmentsDetail();
}

StorageDeviceView WrappedMasterService::StorageDeviceViewForAdmin() const {
    return StorageDeviceView(storage_backend_);
}

tl::expected<std::vector<StorageDeviceMetadata>, ErrorCode>
WrappedMasterService::GetDeviceMetadataForAdmin() {
    auto metadata = master_service_.ListNoFDeviceMetadata();
    if (!metadata.has_value()) {
        return metadata;
    }
    auto storage_metadata = StorageDeviceViewForAdmin().ListDeviceMetadata();
    metadata->insert(metadata->end(), storage_metadata.begin(),
                     storage_metadata.end());
    return metadata;
}

tl::expected<StorageDeviceMetadata, ErrorCode>
WrappedMasterService::UpdateDeviceMetadataForAdmin(
    const StorageDeviceMetadataUpdate& update) {
    if (StorageDeviceViewForAdmin().FindDeviceMetadata(
            update.identity.device_id)) {
        return storage_backend_->ApplyStorageDeviceMetadataUpdate(update);
    }
    return master_service_.UpdateNoFDeviceMetadata(update);
}

tl::expected<StorageDeviceMetadata, ErrorCode>
WrappedMasterService::RecoverDeviceForAdmin(
    const UUID& device_id, const std::string& action,
    const StorageDeviceMetadataUpdate& nof_update,
    std::string* out_rebuild_job_id) {
    if (StorageDeviceViewForAdmin().FindDeviceMetadata(device_id)) {
        if (action == "start") {
            return storage_backend_->StartStorageDeviceRecovery(device_id);
        }
        if (action == "complete") {
            return storage_backend_->CompleteStorageDeviceRecovery(device_id);
        }
        if (action == "fail") {
            return storage_backend_->FailStorageDeviceRecovery(device_id);
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (action == "start") {
        auto update_result =
            master_service_.UpdateNoFDeviceMetadata(nof_update);
        if (!update_result.has_value()) {
            return update_result;
        }
        CreateRebuildJobRequest rebuild_request;
        rebuild_request.device_ids.push_back(UuidToString(device_id));
        auto job_id = master_service_.CreateRebuildJob(rebuild_request);
        if (!job_id.has_value()) {
            StorageDeviceMetadataUpdate rollback;
            rollback.identity.device_id = device_id;
            rollback.health = StorageDeviceHealth::FAILED;
            rollback.writable = false;
            rollback.schedulable = false;
            rollback.last_error = "rebuild_job_creation_failed";
            (void)master_service_.UpdateNoFDeviceMetadata(rollback);
            return tl::make_unexpected(job_id.error());
        }
        if (out_rebuild_job_id) {
            *out_rebuild_job_id = UuidToString(*job_id);
        }
        return update_result;
    }

    if (action == "complete") {
        auto rebuild_job = FindActiveRebuildJobForDevice(device_id);
        if (rebuild_job.has_value()) {
            auto query = master_service_.QueryMigrationJob(*rebuild_job);
            if (query.has_value() && query->status != JobStatus::SUCCEEDED) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
        } else {
            auto current = master_service_.ListNoFDeviceMetadata();
            if (current.has_value()) {
                for (const auto& dev : *current) {
                    if (dev.identity.device_id == device_id &&
                        dev.health == StorageDeviceHealth::REBUILDING) {
                        return tl::make_unexpected(
                            ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                    }
                }
            }
        }
        return master_service_.UpdateNoFDeviceMetadata(nof_update);
    }

    if (action == "fail") {
        auto rebuild_job = FindActiveRebuildJobForDevice(device_id);
        if (rebuild_job.has_value()) {
            (void)master_service_.CancelMigrationJob(*rebuild_job);
        }
        return master_service_.UpdateNoFDeviceMetadata(nof_update);
    }

    return master_service_.UpdateNoFDeviceMetadata(nof_update);
}

tl::expected<StorageDeviceMetadata, ErrorCode>
WrappedMasterService::ProbeDeviceForAdmin(
    const UUID& device_id, const std::string& action, const std::string& reason,
    const StorageDeviceMetadataUpdate& nof_update) {
    if (StorageDeviceViewForAdmin().FindDeviceMetadata(device_id)) {
        if (action == "run") {
            return storage_backend_->ProbeStorageDevice(device_id);
        }
        if (action == "success") {
            return storage_backend_->RecordStorageDeviceProbeSuccess(device_id);
        }
        if (action == "fail") {
            return storage_backend_->RecordStorageDeviceProbeFailure(device_id,
                                                                     reason);
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (action == "run") {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return master_service_.UpdateNoFDeviceMetadata(nof_update);
}

tl::expected<StorageDeviceMaintenancePlan, ErrorCode>
WrappedMasterService::GetDeviceMaintenancePlanForAdmin() {
    auto plan = master_service_.BuildNoFDeviceMaintenancePlan();
    if (!plan.has_value()) {
        return plan;
    }
    StorageDeviceViewForAdmin().AppendMaintenanceCandidates(*plan);
    return plan;
}

size_t WrappedMasterService::ProbeStorageBackendDevicesForAdmin() {
    if (!storage_backend_) {
        return 0;
    }
    size_t probed = 0;
    for (const auto& metadata :
         StorageDeviceViewForAdmin().ListDeviceMetadata()) {
        storage_backend_->ProbeStorageDevice(metadata.identity.device_id);
        ++probed;
    }
    return probed;
}

tl::expected<DeviceCleanupResponse, ErrorCode>
WrappedMasterService::CleanupDeviceForAdmin(
    const DeviceCleanupRequest& request) {
    return master_service_.CleanupDevice(request, storage_backend_);
}

tl::expected<std::pair<uint64_t, uint64_t>, ErrorCode>
WrappedMasterService::QuerySegmentForAdmin(const std::string& segment) {
    return master_service_.QuerySegments(segment);
}

void WrappedMasterService::TryAutoCleanupDevices() {
    if (!storage_backend_) return;
    for (const auto& dev : StorageDeviceViewForAdmin().ListDeviceMetadata()) {
        if (dev.capacity_total <= 0) continue;
        const double usage_ratio = static_cast<double>(dev.capacity_used) /
                                   static_cast<double>(dev.capacity_total);
        if (usage_ratio >= cleanup_high_watermark_) {
            DeviceCleanupRequest req;
            req.device_id = UuidToString(dev.identity.device_id);
            req.strategy = "lru";
            req.cleanup_low_watermark = cleanup_low_watermark_;
            (void)master_service_.CleanupDevice(req, storage_backend_);
        }
    }
}

tl::expected<void, ErrorCode> WrappedMasterService::MountLocalDiskSegment(
    const UUID& client_id, bool enable_offloading) {
    ScopedVLogTimer timer(1, "MountLocalDiskSegment");
    timer.LogRequest("action=mount_local_disk_segment");
    LOG(INFO) << "Mount local disk segment with client id is : " << client_id
              << ", enable offloading is: " << enable_offloading;
    auto result =
        master_service_.MountLocalDiskSegment(client_id, enable_offloading);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
WrappedMasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                             bool enable_offloading) {
    ScopedVLogTimer timer(1, "OffloadObjectHeartbeat");
    timer.LogRequest("action=offload_object_heartbeat");
    auto result =
        master_service_.OffloadObjectHeartbeat(client_id, enable_offloading);
    return result;
}

tl::expected<void, ErrorCode> WrappedMasterService::ReportSsdCapacity(
    const UUID& client_id, int64_t ssd_total_capacity_bytes) {
    ScopedVLogTimer timer(1, "ReportSsdCapacity");
    timer.LogRequest("client_id=", client_id,
                     ", ssd_total_capacity_bytes=", ssd_total_capacity_bytes);
    auto result =
        master_service_.ReportSsdCapacity(client_id, ssd_total_capacity_bytes);
    TryAutoCleanupDevices();
    return result;
}

tl::expected<void, ErrorCode> WrappedMasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas) {
    ScopedVLogTimer timer(1, "NotifyOffloadSuccess");
    timer.LogRequest("action=notify_offload_success");

    for (const auto& task : tasks) {
        auto tenant_id =
            ResolveRequestTenantId(master_service_.IsTenantQuotaEnabled()
                                       ? std::string_view(task.tenant_id)
                                       : TenantId::kDefaultValue);
        if (!tenant_id) {
            auto result = tl::expected<void, ErrorCode>(
                tl::make_unexpected(tenant_id.error()));
            timer.LogResponseExpected(result);
            return result;
        }
    }

    auto result =
        master_service_.NotifyOffloadSuccess(client_id, tasks, metadatas);
    timer.LogResponseExpected(result);

    TryAutoCleanupDevices();
    return result;
}

tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
WrappedMasterService::PromotionObjectHeartbeat(const UUID& client_id) {
    ScopedVLogTimer timer(1, "PromotionObjectHeartbeat");
    timer.LogRequest("action=promotion_object_heartbeat");
    return master_service_.PromotionObjectHeartbeat(client_id);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
WrappedMasterService::PromotionAllocStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    uint64_t size, const std::vector<std::string>& preferred_segments) {
    ScopedVLogTimer timer(1, "PromotionAllocStart");
    timer.LogRequest("action=promotion_alloc_start");
    auto result = WithWriteTenant(
        tenant_id, master_service_.IsTenantQuotaEnabled(),
        [&](const TenantId& resolved_tenant_id) {
            return master_service_.PromotionAllocStart(
                client_id, key, resolved_tenant_id, size, preferred_segments);
        });
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> WrappedMasterService::NotifyPromotionSuccess(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "NotifyPromotionSuccess");
    timer.LogRequest("action=notify_promotion_success");
    auto result = WithRequestTenant(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        [&](const TenantId& resolved_tenant_id) {
            return master_service_.NotifyPromotionSuccess(client_id, key,
                                                          resolved_tenant_id);
        });
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> WrappedMasterService::NotifyPromotionFailure(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "NotifyPromotionFailure");
    timer.LogRequest("action=notify_promotion_failure");
    auto result = WithRequestTenant(
        master_service_.IsTenantQuotaEnabled() ? std::string_view(tenant_id)
                                               : TenantId::kDefaultValue,
        [&](const TenantId& resolved_tenant_id) {
            return master_service_.NotifyPromotionFailure(client_id, key,
                                                          resolved_tenant_id);
        });
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UUID, ErrorCode> WrappedMasterService::CreateDrainJob(
    const CreateDrainJobRequest& request) {
    return master_service_.CreateDrainJob(request);
}

tl::expected<UUID, ErrorCode> WrappedMasterService::CreateRebuildJob(
    const CreateRebuildJobRequest& request) {
    return master_service_.CreateRebuildJob(request);
}

tl::expected<QueryJobResponse, ErrorCode>
WrappedMasterService::QueryMigrationJob(const UUID& job_id) {
    return master_service_.QueryMigrationJob(job_id);
}

tl::expected<void, ErrorCode> WrappedMasterService::CancelMigrationJob(
    const UUID& job_id) {
    return master_service_.CancelMigrationJob(job_id);
}

tl::expected<SegmentStatus, ErrorCode> WrappedMasterService::QuerySegmentStatus(
    const std::string& segment_name) {
    return master_service_.QuerySegmentStatus(segment_name);
}

tl::expected<SegmentStatus, ErrorCode>
WrappedMasterService::QuerySegmentStatusById(const UUID& segment_id) {
    return master_service_.QuerySegmentStatusById(segment_id);
}

bool WrappedMasterService::KvEventsEnabled() const {
    return master_service_.KvEventsEnabled();
}

KvEventPublisher::Stats WrappedMasterService::GetKvEventStats() const {
    return master_service_.GetKvEventStats();
}

void RegisterRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedMasterService& wrapped_master_service) {
    server.register_handler<&mooncake::WrappedMasterService::ExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchQueryIp>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchReplicaClear>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::GetReplicaListByRegex>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetReplicaList>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedMasterService::BatchGetReplicaList>(
            &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UpsertStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UpsertEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UpsertRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchUpsertStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchUpsertEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchUpsertRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Remove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveByRegex>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveAll>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchRemove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MountNoFSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ReMountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ReMountNoFSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UnmountSegment>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::GracefulUnmountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UnmountNoFSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetAllNoFSegments>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::GetNoFSegmentsByName>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Ping>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetFsdir>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedMasterService::QuerySegmentStatus>(
            &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::QuerySegmentStatusById>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetStorageConfig>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ServiceReady>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::MountLocalDiskSegment>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::OffloadObjectHeartbeat>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ReportSsdCapacity>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::NotifyOffloadSuccess>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::PromotionObjectHeartbeat>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedMasterService::PromotionAllocStart>(
            &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::NotifyPromotionSuccess>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::NotifyPromotionFailure>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CopyStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CopyEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CopyRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MoveStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MoveEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MoveRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::EvictDiskReplica>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::BatchEvictDiskReplica>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CreateCopyTask>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CreateMoveTask>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::QueryTask>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::FetchTasks>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedMasterService::MarkTaskToComplete>(
            &wrapped_master_service);
}

}  // namespace mooncake
