#pragma once

#include "master_service.h"
#include "rpc_service.h"
#include <gtest/gtest.h>

#include <map>
#include <mutex>
#include <tuple>

namespace mooncake::test {

// Sequential behavior fixtures retain the grant returned to their writer, just
// as Client does. Negative stale-write tests supply explicit saved ObjectMeta
// tokens. Never obtain write authority from a query of current metadata.
class CapturedPrimaryGrantsForTesting {
   public:
    static void Remember(const void* service, UUID writer,
                         const std::string& key, const TenantId& tenant,
                         const tl::expected<std::vector<Replica::Descriptor>,
                                            ErrorCode>& result) {
        if (!result) return;
        auto generation = GetPrimaryWriteGeneration(*result);
        EXPECT_TRUE(generation) << key;
        if (!generation) return;
        std::lock_guard lock(mutex_);
        grants_[{service, writer, tenant.MakeScopedKey(key)}] = *generation;
    }

    static ObjectMeta Meta(const void* service, UUID writer, ObjectMeta meta,
                           const TenantId& tenant) {
        if (meta.write_generation && !meta.write_generation->empty())
            return meta;
        std::lock_guard lock(mutex_);
        const auto entry =
            grants_.find({service, writer, tenant.MakeScopedKey(meta.key)});
        if (entry != grants_.end()) meta.write_generation = entry->second;
        return meta;
    }

    static ObjectMeta Meta(const void* service, UUID writer,
                           const std::string& key, const TenantId& tenant) {
        return Meta(service, writer, ObjectMeta{key, std::nullopt, {}}, tenant);
    }

    static void ClearForService(const void* service) {
        std::lock_guard lock(mutex_);
        for (auto it = grants_.begin(); it != grants_.end();) {
            if (std::get<0>(it->first) == service) {
                it = grants_.erase(it);
            } else {
                ++it;
            }
        }
    }

    static void ClearAll() {
        std::lock_guard lock(mutex_);
        grants_.clear();
    }

    class ScopedClear {
       public:
        explicit ScopedClear(const void* service) : service_(service) {}
        ~ScopedClear() { ClearForService(service_); }
        ScopedClear(const ScopedClear&) = delete;
        ScopedClear& operator=(const ScopedClear&) = delete;

       private:
        const void* service_;
    };

   private:
    inline static std::mutex mutex_;
    inline static std::map<std::tuple<const void*, UUID, std::string>,
                           std::string>
        grants_;
};

inline auto PutStartWithGrantForTesting(MasterService& service,
                                        const UUID& writer,
                                        const std::string& key,
                                        const TenantId& tenant, uint64_t bytes,
                                        const ReplicateConfig& config) {
    auto result = service.PutStart(writer, key, tenant, bytes, config);
    CapturedPrimaryGrantsForTesting::Remember(&service, writer, key, tenant,
                                              result);
    return result;
}

inline auto UpsertStartWithGrantForTesting(
    MasterService& service, const UUID& writer, const std::string& key,
    const TenantId& tenant, uint64_t bytes, const ReplicateConfig& config) {
    auto result = service.UpsertStart(writer, key, tenant, bytes, config);
    CapturedPrimaryGrantsForTesting::Remember(&service, writer, key, tenant,
                                              result);
    return result;
}

inline auto PutStartWithGrantForTesting(WrappedMasterService& service,
                                        const UUID& writer,
                                        const std::string& key, uint64_t bytes,
                                        const ReplicateConfig& config,
                                        const std::string& tenant = "default") {
    auto result = service.PutStart(writer, key, bytes, config, tenant);
    CapturedPrimaryGrantsForTesting::Remember(&service, writer, key,
                                              TenantId(tenant), result);
    return result;
}

inline auto BatchPutStartWithGrantForTesting(
    WrappedMasterService& service, const UUID& writer,
    const std::vector<std::string>& keys, const std::vector<uint64_t>& sizes,
    const ReplicateConfig& config, const std::string& tenant = "default") {
    auto result = service.BatchPutStart(writer, keys, sizes, config, tenant);
    for (size_t i = 0; i < result.size() && i < keys.size(); ++i)
        CapturedPrimaryGrantsForTesting::Remember(&service, writer, keys[i],
                                                  TenantId(tenant), result[i]);
    return result;
}

inline auto BatchUpsertStartWithGrantForTesting(
    MasterService& service, const UUID& writer,
    const std::vector<std::string>& keys, const TenantId& tenant,
    const std::vector<uint64_t>& sizes, const ReplicateConfig& config) {
    auto result = service.BatchUpsertStart(writer, keys, tenant, sizes, config);
    for (size_t i = 0; i < keys.size() && i < result.size(); ++i)
        CapturedPrimaryGrantsForTesting::Remember(&service, writer, keys[i],
                                                  tenant, result[i]);
    return result;
}
inline auto BatchPutEndWithGrantForTesting(
    MasterService& service, const UUID& writer, std::vector<ObjectMeta> objects,
    const TenantId& tenant, ReplicaType type = ReplicaType::ALL) {
    for (auto& object : objects)
        object = CapturedPrimaryGrantsForTesting::Meta(
            &service, writer, std::move(object), tenant);
    return service.BatchPutEnd(writer, objects, tenant, type);
}

template <class Meta>
inline auto PutEndWithGrantForTesting(MasterService& service,
                                      const UUID& writer, const Meta& object,
                                      const TenantId& tenant,
                                      ReplicaType type) {
    return service.PutEnd(
        writer,
        CapturedPrimaryGrantsForTesting::Meta(&service, writer, object, tenant),
        tenant, type);
}

template <class Meta>
inline auto UpsertEndWithGrantForTesting(MasterService& service,
                                         const UUID& writer, const Meta& object,
                                         const TenantId& tenant,
                                         ReplicaType type) {
    return service.UpsertEnd(
        writer,
        CapturedPrimaryGrantsForTesting::Meta(&service, writer, object, tenant),
        tenant, type);
}

template <class Meta>
inline auto PutRevokeWithGrantForTesting(MasterService& service,
                                         const UUID& writer, const Meta& object,
                                         const TenantId& tenant,
                                         ReplicaType type) {
    return service.PutRevoke(
        writer,
        CapturedPrimaryGrantsForTesting::Meta(&service, writer, object, tenant),
        tenant, type);
}

inline auto PutEndWithGrantForTesting(MasterService& service,
                                      const UUID& writer, ObjectMeta object,
                                      const TenantId& tenant,
                                      ReplicaType type) {
    return service.PutEnd(writer,
                          CapturedPrimaryGrantsForTesting::Meta(
                              &service, writer, std::move(object), tenant),
                          tenant, type);
}
inline auto UpsertEndWithGrantForTesting(MasterService& service,
                                         const UUID& writer, ObjectMeta object,
                                         const TenantId& tenant,
                                         ReplicaType type) {
    return service.UpsertEnd(writer,
                             CapturedPrimaryGrantsForTesting::Meta(
                                 &service, writer, std::move(object), tenant),
                             tenant, type);
}

template <class Meta>
inline auto UpsertRevokeWithGrantForTesting(MasterService& service,
                                            const UUID& writer,
                                            const Meta& object,
                                            const TenantId& tenant,
                                            ReplicaType type) {
    return service.UpsertRevoke(
        writer,
        CapturedPrimaryGrantsForTesting::Meta(&service, writer, object, tenant),
        tenant, type);
}

inline auto PutEndWithGrantForTesting(WrappedMasterService& service,
                                      const UUID& writer, ObjectMeta object,
                                      ReplicaType type = ReplicaType::ALL,
                                      const std::string& tenant = "default") {
    return service.PutEnd(
        writer,
        CapturedPrimaryGrantsForTesting::Meta(
            &service, writer, std::move(object), TenantId(tenant)),
        type, tenant);
}

inline auto BatchPutEndWithGrantForTesting(
    WrappedMasterService& service, const UUID& writer,
    std::vector<ObjectMeta> objects, ReplicaType type = ReplicaType::ALL,
    const std::string& tenant = "default") {
    for (auto& object : objects)
        object = CapturedPrimaryGrantsForTesting::Meta(
            &service, writer, std::move(object), TenantId(tenant));
    return service.BatchPutEnd(writer, objects, type, tenant);
}

}  // namespace mooncake::test
