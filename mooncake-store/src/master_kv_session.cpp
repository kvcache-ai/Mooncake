#include "master_service.h"

#include <algorithm>
#include <unordered_set>

namespace mooncake {

tl::expected<void, ErrorCode> MasterService::SetKvSessionPin(
    const std::string& session_id, bool pinned, const TenantId& tenant) {
    return kv_session_registry_->SetPin(ResolveRequestTenantId(tenant),
                                        session_id, pinned);
}

tl::expected<void, ErrorCode> MasterService::CloseKvSession(
    const std::string& session_id, const TenantId& tenant) {
    return UpdateKvSession(session_id, {}, tenant);
}

tl::expected<void, ErrorCode> MasterService::UpdateKvSession(
    const std::string& session_id, const std::vector<std::string>& keep_keys,
    const TenantId& tenant) {
    auto valid = kv_session_registry_->ValidateUpdate(session_id, keep_keys);
    if (!valid) return valid;
    const auto& resolved = ResolveRequestTenantId(tenant);
    const std::unordered_set<std::string> keep(keep_keys.begin(),
                                               keep_keys.end());
    // Bound index-lock hold time; do not snapshot or mutate an entire session
    // under that lock. Exact pruning requires paused writes/controls; otherwise
    // concurrent associations may be visited or missed. Eviction can proceed.
    const size_t page_size =
        std::min<size_t>(256, kv_session_registry_->batch_limit());
    std::string cursor;
    do {
        auto page =
            kv_session_registry_->List(resolved, session_id, cursor, page_size);
        if (!page) {
            if (page.error() == ErrorCode::SESSION_NOT_FOUND) return {};
            return tl::make_unexpected(page.error());
        }
        for (const auto& key : page->keys) {
            if (keep.contains(key)) continue;
            std::shared_lock lock(snapshot_mutex_);
            MetadataAccessorRW accessor(this,
                                        MakeObjectIdentity(key, resolved));
            if (!accessor.Exists()) continue;
            auto& membership = accessor.Get().kv_sessions;
            if (membership) membership->RemoveSession(session_id);
        }
        cursor = std::move(page->next_cursor);
    } while (!cursor.empty());
    return {};
}

tl::expected<KvSessionInfo, ErrorCode> MasterService::GetKvSession(
    const std::string& session_id, const TenantId& tenant) {
    return kv_session_registry_->Get(ResolveRequestTenantId(tenant),
                                     session_id);
}

tl::expected<KvSessionPage, ErrorCode> MasterService::ListKvSessionKeys(
    const std::string& session_id, const std::string& cursor, uint64_t limit,
    const TenantId& tenant) {
    return kv_session_registry_->List(ResolveRequestTenantId(tenant),
                                      session_id, cursor, limit);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::AttachKvSession(
    const std::string& session_id, const std::vector<std::string>& keys,
    const TenantId& tenant) {
    using Result = tl::expected<void, ErrorCode>;
    if (keys.size() > kv_session_registry_->batch_limit()) {
        return std::vector<Result>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    const auto& resolved = ResolveRequestTenantId(tenant);
    auto valid = kv_session_registry_->Validate({session_id});
    if (!valid)
        return std::vector<Result>(keys.size(),
                                   tl::make_unexpected(valid.error()));
    std::vector<Result> results;
    results.reserve(keys.size());
    std::shared_lock lock(snapshot_mutex_);
    for (const auto& key : keys) {
        MetadataAccessorRW accessor(this, MakeObjectIdentity(key, resolved));
        if (!accessor.Exists()) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
            continue;
        }
        auto& metadata = accessor.Get();
        results.push_back(kv_session_registry_->Attach(
            metadata.kv_sessions, resolved, key, {session_id}));
    }
    return results;
}

}  // namespace mooncake
