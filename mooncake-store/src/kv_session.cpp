#include "kv_session.h"

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <stdexcept>
#include <string_view>

namespace mooncake {

KvSessionLimits KvSessionLimits::FromEnvironment() {
    KvSessionLimits limits;
    const auto read = [](const char* name, size_t& value) {
        if (const char* raw = std::getenv(name)) {
            std::string_view text(raw);
            size_t parsed = 0;
            auto [end, error] =
                std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (error != std::errc{} || end != text.data() + text.size() ||
                parsed == 0) {
                throw std::invalid_argument(std::string(name) +
                                            " must be a positive integer");
            }
            value = parsed;
        }
    };
    read("MC_KV_SESSION_MAX_SESSIONS", limits.sessions);
    read("MC_KV_SESSION_MAX_MEMBERS", limits.members_per_session);
    read("MC_KV_SESSION_MAX_PER_OBJECT", limits.sessions_per_object);
    read("MC_KV_SESSION_MAX_BATCH", limits.batch_size);
    return limits;
}

void KvSessionRegistry::EraseMemberLocked(
    const TenantId& tenant, const std::string& key,
    const std::shared_ptr<KvSessionRecord>& record) {
    record->members.erase(key);
    if (!record->members.empty()) return;
    auto it = sessions_.find(tenant.MakeScopedKey(record->session_id));
    if (it != sessions_.end() && it->second == record) sessions_.erase(it);
}

KvSessionMembership::~KvSessionMembership() {
    std::lock_guard guard(registry->mutex_);
    for (const auto& session : sessions) {
        registry->EraseMemberLocked(tenant, key, session);
    }
}

void KvSessionMembership::RemoveSession(const std::string& session_id) {
    std::lock_guard guard(registry->mutex_);
    auto record = registry->FindLocked(tenant, session_id);
    if (record && sessions.erase(*record)) {
        registry->EraseMemberLocked(tenant, key, *record);
    }
}

bool KvSessionMembership::IsPinned() const {
    // Membership is protected by the caller's object lock. Pin is a retention
    // hint, so relaxed atomic loads suffice; no index lock on eviction reads.
    return std::any_of(
        sessions.begin(), sessions.end(), [](const auto& session) {
            return session->pinned.load(std::memory_order_relaxed);
        });
}

tl::expected<std::shared_ptr<KvSessionRecord>, ErrorCode>
KvSessionRegistry::FindLocked(const TenantId& tenant,
                              const std::string& session_id) {
    auto it = sessions_.find(tenant.MakeScopedKey(session_id));
    if (it == sessions_.end()) {
        return tl::make_unexpected(ErrorCode::SESSION_NOT_FOUND);
    }
    return it->second;
}

tl::expected<void, ErrorCode> KvSessionRegistry::SetPin(
    const TenantId& tenant, const std::string& session_id, bool pinned) {
    std::lock_guard guard(mutex_);
    auto record = FindLocked(tenant, session_id);
    if (!record) return tl::make_unexpected(record.error());
    (*record)->pinned.store(pinned, std::memory_order_relaxed);
    return {};
}

tl::expected<void, ErrorCode> KvSessionRegistry::ValidateUpdate(
    const std::string& session_id,
    const std::vector<std::string>& keep_keys) const {
    auto valid = Validate({session_id});
    if (!valid) return valid;
    if (keep_keys.size() > limits_.members_per_session) {
        return tl::make_unexpected(ErrorCode::SESSION_LIMIT_EXCEEDED);
    }
    return {};
}

tl::expected<KvSessionInfo, ErrorCode> KvSessionRegistry::Get(
    const TenantId& tenant, const std::string& session_id) {
    std::lock_guard guard(mutex_);
    auto record = FindLocked(tenant, session_id);
    if (!record) return tl::make_unexpected(record.error());
    return KvSessionInfo{(*record)->session_id,
                         (*record)->pinned.load(std::memory_order_relaxed),
                         (*record)->members.size()};
}

tl::expected<KvSessionPage, ErrorCode> KvSessionRegistry::List(
    const TenantId& tenant, const std::string& session_id,
    const std::string& cursor, size_t limit) {
    if (limit == 0 || limit > limits_.batch_size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard guard(mutex_);
    auto record = FindLocked(tenant, session_id);
    if (!record) return tl::make_unexpected(record.error());
    KvSessionPage page;
    auto it = (*record)->members.upper_bound(cursor);
    while (it != (*record)->members.end() && page.keys.size() < limit) {
        page.keys.push_back(*it++);
    }
    if (it != (*record)->members.end()) page.next_cursor = page.keys.back();
    return page;
}

tl::expected<void, ErrorCode> KvSessionRegistry::Validate(
    const std::vector<std::string>& session_ids) const {
    if (session_ids.size() > limits_.sessions_per_object) {
        return tl::make_unexpected(ErrorCode::SESSION_LIMIT_EXCEEDED);
    }
    for (const auto& id : session_ids) {
        if (id.empty() || id.size() > 4096 ||
            id.find('\0') != std::string::npos) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    return {};
}

tl::expected<void, ErrorCode> KvSessionRegistry::Attach(
    std::unique_ptr<KvSessionMembership>& membership, const TenantId& tenant,
    const std::string& key, const std::vector<std::string>& session_ids) {
    if (session_ids.empty()) return {};
    auto valid = Validate(session_ids);
    if (!valid) return valid;
    if (!membership) {
        membership = std::make_unique<KvSessionMembership>(shared_from_this(),
                                                           key, tenant);
    }
    std::lock_guard guard(mutex_);
    std::vector<std::shared_ptr<KvSessionRecord>> additions;
    size_t new_sessions = 0;
    std::unordered_set<std::string_view> seen;
    for (const auto& id : session_ids) {
        if (!seen.insert(id).second) continue;
        auto it = sessions_.find(tenant.MakeScopedKey(id));
        if (it != sessions_.end()) {
            if (membership->sessions.contains(it->second)) continue;
            if (it->second->members.size() >= limits_.members_per_session &&
                !it->second->members.contains(key)) {
                return tl::make_unexpected(ErrorCode::SESSION_LIMIT_EXCEEDED);
            }
            additions.push_back(it->second);
        } else {
            auto record = std::make_shared<KvSessionRecord>();
            record->session_id = id;
            additions.push_back(std::move(record));
            ++new_sessions;
        }
    }
    if (membership->sessions.size() + additions.size() >
            limits_.sessions_per_object ||
        sessions_.size() + new_sessions > limits_.sessions) {
        return tl::make_unexpected(ErrorCode::SESSION_LIMIT_EXCEEDED);
    }
    // Publish only after validating the entire object update. Failed admission
    // must not leave empty sessions or partially accepted memberships.
    for (const auto& record : additions) {
        sessions_.try_emplace(tenant.MakeScopedKey(record->session_id), record);
        record->members.insert(key);
        membership->sessions.insert(record);
    }
    return {};
}

}  // namespace mooncake
