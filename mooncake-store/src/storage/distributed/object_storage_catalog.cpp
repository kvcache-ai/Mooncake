#include "storage/distributed/object_storage_catalog.h"

#include <algorithm>
#include <glog/logging.h>
#include <limits>
#include <utility>
#include <ylt/struct_pack.hpp>
#include "common/base64.h"

namespace mooncake {
namespace {

template <class T>
std::string Encode(const T& value) {
    const auto bytes = struct_pack::serialize(value);
    return base64::Encode(std::string(bytes.begin(), bytes.end()));
}

template <class T>
bool Decode(T& value, const std::string& encoded) {
    const auto bytes = base64::Decode(encoded);
    return base64::Encode(bytes) == encoded &&
           struct_pack::deserialize_to(value, bytes) == struct_pack::errc::ok;
}

bool IsPending(const ObjectStorageRecord& record) {
    return record.phase != ObjectStoragePhase::COMPLETE &&
           record.phase != ObjectStoragePhase::DELETED &&
           record.phase != ObjectStoragePhase::DEFERRED &&
           !(record.phase == ObjectStoragePhase::RETIRED &&
             record.upload_finished && !record.upload_started) &&
           !(record.phase == ObjectStoragePhase::PREPARED &&
             record.descriptor().write_mode ==
                 ObjectStorageWriteMode::OFFLOAD_ON_EVICT);
}

bool IsValid(const ObjectStorageRecord& record, const std::string& ns) {
    return record.target.is_object_storage_replica() && record.target.id != 0 &&
           !record.descriptor().object_key.empty() &&
           record.descriptor().namespace_id == ns &&
           record.descriptor().object_size > 0 &&
           record.phase <= ObjectStoragePhase::DELETED;
}

}  // namespace

ObjectStorageCatalog::ObjectStorageCatalog(ObjectStorageConfig config,
                                           std::shared_ptr<HaKvBackend> backend,
                                           std::string cluster_id)
    : config_(std::move(config)),
      backend_(std::move(backend)),
      prefix_("/mooncake/object-storage/" + cluster_id + "/") {}

ErrorCode ObjectStorageCatalog::Open(uint64_t leader_epoch) {
    if (!config_.Validate() || !config_.enabled || !backend_ ||
        !backend_->SupportsTxn())
        return ErrorCode::INVALID_PARAMS;
    std::lock_guard lock(mutex_);
    std::string old_fence;
    auto error = backend_->Get(prefix_ + "fence", old_fence);
    if (error != ErrorCode::OK && error != ErrorCode::ETCD_KEY_NOT_EXIST)
        return error;
    std::pair<uint64_t, std::string> previous;
    if (error == ErrorCode::OK) {
        if (!Decode(previous, old_fence)) return ErrorCode::DESERIALIZE_FAIL;
        if (previous.second != config_.NamespaceId())
            return ErrorCode::INVALID_PARAMS;
        if (leader_epoch == 0) {
            if (previous.first == std::numeric_limits<uint64_t>::max())
                return ErrorCode::INVALID_PARAMS;
            leader_epoch = previous.first + 1;
        } else if (leader_epoch <= previous.first) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    } else if (leader_epoch == 0) {
        leader_epoch = 1;
    }
    fence_ = Encode(std::make_pair(leader_epoch, config_.NamespaceId()));
    KvTxn acquire;
    acquire.compares.push_back({prefix_ + "fence",
                                error == ErrorCode::OK
                                    ? KvCompareKind::kValueEquals
                                    : KvCompareKind::kKeyNotExists,
                                old_fence});
    acquire.puts.push_back({prefix_ + "fence", fence_});
    error = backend_->Txn(acquire);
    if (error != ErrorCode::OK) return error;

    records_.clear();
    heads_.clear();
    maintenance_keys_.clear();
    retired_keys_.clear();
    ready_keys_.clear();
    object_keys_.clear();
    pending_bytes_ = pending_count_ = 0;
    std::string begin = prefix_ + "records/";
    const std::string end = prefix_ + "records0";
    while (true) {
        std::vector<KvPair> page;
        error = backend_->Range(begin, end, 512, page);
        if (error != ErrorCode::OK) return error;
        if (page.empty()) break;
        for (const auto& kv : page) {
            ObjectStorageRecord record;
            if (!Decode(record, kv.value) ||
                !IsValid(record, config_.NamespaceId()) ||
                kv.key != RecordKey(record.descriptor().object_key))
                return ErrorCode::DESERIALIZE_FAIL;
            if (record.phase == ObjectStoragePhase::DELETED) {
                error = backend_->DeleteRange(kv.key, kv.key + '\0');
                if (error != ErrorCode::OK) return error;
                continue;
            }
            if (IsPending(record)) {
                if (record.descriptor().object_size >
                    std::numeric_limits<uint64_t>::max() - pending_bytes_)
                    return ErrorCode::DESERIALIZE_FAIL;
                pending_bytes_ += record.descriptor().object_size;
                ++pending_count_;
            }
            Index(record, true);
            records_.emplace(record.descriptor().object_key, std::move(record));
        }
        begin = page.back().key + '\0';
    }
    begin = prefix_ + "heads/";
    while (true) {
        std::vector<KvPair> page;
        error = backend_->Range(begin, prefix_ + "heads0", 512, page);
        if (error != ErrorCode::OK) return error;
        if (page.empty()) break;
        for (const auto& kv : page) {
            ObjectStorageRecord record;
            if (!Decode(record, kv.value) ||
                !IsValid(record, config_.NamespaceId()) ||
                !record.foreground_complete ||
                kv.key !=
                    prefix_ + "heads/" +
                        Encode(std::make_pair(record.tenant_id, record.key)))
                return ErrorCode::DESERIALIZE_FAIL;
            heads_.insert_or_assign(
                std::make_pair(record.tenant_id, record.key),
                std::move(record));
        }
        begin = page.back().key + '\0';
    }
    return ErrorCode::OK;
}

std::string ObjectStorageCatalog::RecordKey(const std::string& key) const {
    return prefix_ + "records/" + key;
}

bool ObjectStorageCatalog::CanReserveLocked(uint64_t bytes) const {
    return bytes <= config_.max_object_bytes &&
           pending_count_ < config_.max_pending_objects &&
           pending_bytes_ <= config_.max_pending_bytes &&
           bytes <= config_.max_pending_bytes - pending_bytes_;
}

bool ObjectStorageCatalog::CanReserve(uint64_t bytes) const {
    std::lock_guard lock(mutex_);
    return CanReserveLocked(bytes);
}

void ObjectStorageCatalog::Index(const ObjectStorageRecord& record, bool add) {
    const auto& key = record.descriptor().object_key;
    const auto update = [&](auto& keys, bool selected) {
        if (add && selected)
            keys.insert(key);
        else
            keys.erase(key);
    };
    update(maintenance_keys_, record.phase != ObjectStoragePhase::COMPLETE);
    update(retired_keys_, record.phase == ObjectStoragePhase::RETIRED);
    auto& ready = ready_keys_[record.uploader];
    update(ready, record.phase == ObjectStoragePhase::READY);
    if (ready.empty()) ready_keys_.erase(record.uploader);
    const auto identity = std::make_pair(record.tenant_id, record.key);
    auto& names = object_keys_[identity];
    update(names, true);
    if (names.empty()) object_keys_.erase(identity);
}

std::vector<ObjectStorageRecord> ObjectStorageCatalog::TasksFor(
    const UUID& client) const {
    std::lock_guard lock(mutex_);
    const auto it = ready_keys_.find(client);
    if (it == ready_keys_.end() || it->second.empty()) return {};
    return {records_.at(*it->second.begin())};
}

std::vector<ObjectStorageRecord> ObjectStorageCatalog::ForObject(
    const std::string& tenant, const std::string& key) const {
    std::lock_guard lock(mutex_);
    std::vector<ObjectStorageRecord> result;
    const auto it = object_keys_.find({tenant, key});
    if (it != object_keys_.end())
        for (const auto& name : it->second) result.push_back(records_.at(name));
    return result;
}

ErrorCode ObjectStorageCatalog::Save(const ObjectStorageRecord& record) {
    std::lock_guard lock(mutex_);
    return SaveLocked(record);
}

ErrorCode ObjectStorageCatalog::SaveLocked(const ObjectStorageRecord& record) {
    if (!IsValid(record, config_.NamespaceId()))
        return ErrorCode::INVALID_PARAMS;
    const auto& key = record.descriptor().object_key;
    const auto it = records_.find(key);
    const auto identity = std::make_pair(record.tenant_id, record.key);
    const auto head = heads_.find(identity);
    std::optional<ObjectStorageRecord> replaced;
    if (it == records_.end() && record.foreground_complete &&
        record.phase == ObjectStoragePhase::READY && head != heads_.end()) {
        const auto& old = head->second;
        // NOT_STARTED proves there is no physical object or late PUT to GC.
        // Replace its retry intent atomically. It no longer consumes pending
        // capacity; the new attempt must pass normal admission below.
        if (old.phase == ObjectStoragePhase::RETIRED &&
            old.foreground_complete && old.source_released &&
            old.upload_finished && !old.upload_started && !old.object_deleted &&
            old.target.id < record.target.id &&
            old.descriptor().object_size == record.descriptor().object_size &&
            records_.contains(old.descriptor().object_key))
            replaced = old;
    }
    if (IsPending(record) && record.phase != ObjectStoragePhase::RETIRED &&
        (it == records_.end() || !IsPending(it->second)) &&
        !CanReserveLocked(record.descriptor().object_size))
        return ErrorCode::NO_AVAILABLE_HANDLE;
    if (it != records_.end()) {
        const auto& previous = it->second;
        if (previous.revision != record.revision ||
            previous.target.id != record.target.id ||
            previous.tenant_id != record.tenant_id ||
            previous.key != record.key ||
            previous.descriptor().object_size !=
                record.descriptor().object_size ||
            (previous.phase == ObjectStoragePhase::RETIRED &&
             record.phase != ObjectStoragePhase::RETIRED &&
             record.phase != ObjectStoragePhase::DELETED) ||
            (previous.upload_finished && !record.upload_finished) ||
            (previous.object_deleted && !record.object_deleted))
            return ErrorCode::INVALID_WRITE;
    }
    KvTxn txn;
    txn.compares.push_back(
        {prefix_ + "fence", KvCompareKind::kValueEquals, fence_});
    const auto prior =
        it == records_.end() ? std::string{} : Encode(it->second);
    txn.compares.push_back({RecordKey(key),
                            it == records_.end() ? KvCompareKind::kKeyNotExists
                                                 : KvCompareKind::kValueEquals,
                            prior});
    auto saved = record;
    ++saved.revision;
    const auto encoded = Encode(saved);
    txn.puts.push_back({RecordKey(key), encoded});
    if (replaced) {
        txn.compares.push_back({RecordKey(replaced->descriptor().object_key),
                                KvCompareKind::kValueEquals,
                                Encode(*replaced)});
        replaced->phase = ObjectStoragePhase::DELETED;
        ++replaced->revision;
        txn.puts.push_back(
            {RecordKey(replaced->descriptor().object_key), Encode(*replaced)});
    }
    const bool update_head =
        record.foreground_complete &&
        (head == heads_.end() || head->second.target.id <= record.target.id);
    const auto head_key = prefix_ + "heads/" + Encode(identity);
    if (update_head) {
        txn.compares.push_back(
            {head_key,
             head == heads_.end() ? KvCompareKind::kKeyNotExists
                                  : KvCompareKind::kValueEquals,
             head == heads_.end() ? std::string{} : Encode(head->second)});
        txn.puts.push_back({head_key, encoded});
    }
    const auto error = backend_->Txn(txn);
    if (error != ErrorCode::OK) {
        // Both the transaction reply and its first reconciliation can fail.
        // Refresh committed state even when it differs from this new request;
        // callers retry from that revision and replay local effects
        // idempotently.
        std::string actual_head;
        const auto head_error = backend_->Get(head_key, actual_head);
        if (head_error == ErrorCode::OK) {
            ObjectStorageRecord durable_head;
            if (!Decode(durable_head, actual_head) ||
                !IsValid(durable_head, config_.NamespaceId()) ||
                durable_head.tenant_id != record.tenant_id ||
                durable_head.key != record.key)
                return ErrorCode::DESERIALIZE_FAIL;
            ApplyLocked(durable_head);
            heads_.insert_or_assign(identity, std::move(durable_head));
        } else if (head_error != ErrorCode::ETCD_KEY_NOT_EXIST)
            return error;
        if (replaced) {
            std::string actual_old;
            if (backend_->Get(RecordKey(replaced->descriptor().object_key),
                              actual_old) != ErrorCode::OK)
                return error;
            ObjectStorageRecord durable_old;
            if (!Decode(durable_old, actual_old) ||
                !IsValid(durable_old, config_.NamespaceId()) ||
                durable_old.target.id != replaced->target.id)
                return ErrorCode::DESERIALIZE_FAIL;
            ApplyLocked(durable_old);
        }
        std::string actual;
        if (backend_->Get(RecordKey(key), actual) != ErrorCode::OK)
            return error;
        ObjectStorageRecord durable;
        if (!Decode(durable, actual) ||
            !IsValid(durable, config_.NamespaceId()) ||
            durable.descriptor().object_key != key ||
            durable.target.id != record.target.id ||
            durable.tenant_id != record.tenant_id || durable.key != record.key)
            return ErrorCode::DESERIALIZE_FAIL;
        ApplyLocked(durable);
        return actual == encoded ? ErrorCode::OK : error;
    }
    if (update_head) heads_.insert_or_assign(identity, saved);
    if (replaced) ApplyLocked(*replaced);
    ApplyLocked(saved);
    if (replaced) {
        // A failed cleanup leaves a terminal record which Open can remove.
        const auto old_key = RecordKey(replaced->descriptor().object_key);
        const auto cleanup = backend_->DeleteRange(old_key, old_key + '\0');
        if (cleanup != ErrorCode::OK)
            LOG(WARNING) << "Cannot clean up replaced object intent: "
                         << cleanup;
    }
    return ErrorCode::OK;
}

void ObjectStorageCatalog::ApplyLocked(const ObjectStorageRecord& record) {
    const auto& key = record.descriptor().object_key;
    const auto it = records_.find(key);
    if (it != records_.end()) {
        Index(it->second, false);
        if (IsPending(it->second)) {
            pending_bytes_ -= it->second.descriptor().object_size;
            --pending_count_;
        }
    }
    if (record.phase != ObjectStoragePhase::DELETED) Index(record, true);
    if (IsPending(record)) {
        pending_bytes_ += record.descriptor().object_size;
        ++pending_count_;
    }
    if (record.phase == ObjectStoragePhase::DELETED)
        records_.erase(key);
    else
        records_.insert_or_assign(key, record);
}

ErrorCode ObjectStorageCatalog::Retire(const std::string& key,
                                       uint64_t deadline) {
    std::lock_guard lock(mutex_);
    auto it = records_.find(key);
    if (it == records_.end()) return ErrorCode::OBJECT_NOT_FOUND;
    auto record = it->second;
    if (record.phase == ObjectStoragePhase::RETIRED && record.object_deleted)
        return ErrorCode::OK;
    record.phase = ObjectStoragePhase::RETIRED;
    record.object_deleted = true;
    record.retire_after_ms = std::max(record.retire_after_ms, deadline);
    return SaveLocked(record);
}

std::optional<ObjectStorageRecord> ObjectStorageCatalog::Find(
    const std::string& key) const {
    std::lock_guard lock(mutex_);
    auto it = records_.find(key);
    return it == records_.end() ? std::nullopt : std::optional(it->second);
}

std::vector<ObjectStorageRecord> ObjectStorageCatalog::Records() const {
    std::lock_guard lock(mutex_);
    std::vector<ObjectStorageRecord> records;
    records.reserve(records_.size());
    for (const auto& [key, record] : records_) records.push_back(record);
    return records;
}

std::optional<ObjectStorageRecord> ObjectStorageCatalog::Head(
    const std::string& tenant, const std::string& key) const {
    std::lock_guard lock(mutex_);
    const auto it = heads_.find({tenant, key});
    return it == heads_.end() ? std::nullopt : std::optional(it->second);
}

std::vector<ObjectStorageRecord> ObjectStorageCatalog::Heads() const {
    std::lock_guard lock(mutex_);
    std::vector<ObjectStorageRecord> result;
    result.reserve(heads_.size());
    for (const auto& [identity, record] : heads_) result.push_back(record);
    return result;
}

std::vector<ObjectStorageRecord> ObjectStorageCatalog::NextRecords(
    std::string& cursor) const {
    std::lock_guard lock(mutex_);
    std::vector<ObjectStorageRecord> result;
    auto it = maintenance_keys_.upper_bound(cursor);
    const auto count = std::min<uint64_t>(maintenance_keys_.size(),
                                          config_.maintenance_batch_size);
    for (uint64_t i = 0; i < count; ++i) {
        if (it == maintenance_keys_.end()) it = maintenance_keys_.begin();
        result.push_back(records_.at(*it));
        cursor = *it++;
    }
    return result;
}

ErrorCode ObjectStorageCatalog::Reclaim(ObjectStorageAdapter& adapter,
                                        uint64_t now_ms) {
    std::vector<ObjectStorageRecord> candidates;
    {
        std::lock_guard lock(mutex_);
        // Bound inspected records as well as DELETEs; rotate through the
        // ordered catalogue so a permanently unknown upload cannot starve later
        // work.
        auto it = retired_keys_.upper_bound(reclaim_cursor_);
        const auto count = std::min<uint64_t>(retired_keys_.size(),
                                              config_.maintenance_batch_size);
        for (uint64_t inspected = 0; inspected < count; ++inspected) {
            if (it == retired_keys_.end()) it = retired_keys_.begin();
            const auto key = *it++;
            const auto& record = records_.at(key);
            reclaim_cursor_ = key;
            if (record.phase == ObjectStoragePhase::RETIRED &&
                (!record.foreground_complete || record.object_deleted) &&
                record.retire_after_ms <= now_ms)
                candidates.push_back(record);
        }
    }
    ErrorCode first_error = ErrorCode::OK;
    for (const auto& record : candidates) {
        auto deleted = adapter.Delete(record.descriptor().object_key);
        if (!deleted) {
            if (first_error == ErrorCode::OK) first_error = deleted.error();
            continue;
        }
        // A finished upload cannot be retried: each Claim permits exactly one
        // PUT. Unknown outcomes keep the durable tombstone and repeat DELETE.
        if (record.upload_started && !record.upload_finished) continue;
        std::lock_guard lock(mutex_);
        const auto it = records_.find(record.descriptor().object_key);
        if (it == records_.end() ||
            it->second.phase != ObjectStoragePhase::RETIRED ||
            it->second.retire_after_ms > now_ms)
            continue;
        auto finished = it->second;
        finished.phase = ObjectStoragePhase::DELETED;
        const auto error = SaveLocked(finished);
        if (first_error == ErrorCode::OK) first_error = error;
        if (error == ErrorCode::OK) {
            const auto key = RecordKey(record.descriptor().object_key);
            const auto cleanup = backend_->DeleteRange(key, key + '\0');
            if (first_error == ErrorCode::OK) first_error = cleanup;
        }
    }
    return first_error;
}

}  // namespace mooncake
