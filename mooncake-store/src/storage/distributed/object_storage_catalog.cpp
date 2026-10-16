#include "storage/distributed/object_storage_catalog.h"

#include <algorithm>
#include <openssl/sha.h>
#include <limits>
#include <tuple>
#include <utility>
#include <ylt/struct_pack.hpp>

#include "common/base64.h"
#include "random.h"

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

bool Pending(const ObjectStorageRecord& record) {
    return record.phase != ObjectStoragePhase::DELETED &&
           !(record.phase == ObjectStoragePhase::COMPLETE &&
             record.foreground_complete);
}

bool Terminal(const ObjectStorageRecord& record) {
    return record.phase == ObjectStoragePhase::RETIRED ||
           record.phase == ObjectStoragePhase::DELETED;
}

}  // namespace

ObjectStorageCatalog::ObjectStorageCatalog(ObjectStorageCatalogConfig config,
                                           std::shared_ptr<HaKvBackend> backend,
                                           std::string cluster_id)
    : config_(std::move(config)),
      backend_(std::move(backend)),
      cluster_id_(std::move(cluster_id)),
      prefix_("/mooncake/object-storage/" + cluster_id_ + "/"),
      namespace_id_(Encode(config_.storage_namespace)) {}

std::string ObjectStorageCatalog::RecordKey(const std::string& key) const {
    return prefix_ + "records/" + key;
}

std::string ObjectStorageCatalog::HeadKey(const Identity& identity) const {
    return prefix_ + "heads/" + Encode(identity);
}

std::string ObjectStorageCatalog::ObjectKey(const Identity& identity,
                                            uint64_t generation) const {
    // Provider names stay bounded even for long or binary application keys.
    const auto bytes = struct_pack::serialize(cluster_id_, identity);
    unsigned char digest[SHA256_DIGEST_LENGTH];
    if (!SHA256(reinterpret_cast<const unsigned char*>(bytes.data()),
                bytes.size(), digest))
        return {};
    constexpr char hex[] = "0123456789abcdef";
    std::string name;
    for (const auto byte : digest) {
        name.push_back(hex[byte >> 4]);
        name.push_back(hex[byte & 15]);
    }
    return name + "/" + std::to_string(generation);
}

bool ObjectStorageCatalog::Valid(const ObjectStorageRecord& r) const {
    return r.generation && r.revision && r.namespace_id == namespace_id_ &&
           !r.object_key.empty() &&
           r.object_key == ObjectKey({r.tenant, r.key}, r.generation) &&
           r.phase <= ObjectStoragePhase::DELETED &&
           (!r.checksum || r.foreground_complete) &&
           (Terminal(r) || !r.retire_after_ms) &&
           (!r.upload_finished || r.upload_started == r.upload_succeeded) &&
           (r.upload_started || r.upload_finished ||
            r.phase == ObjectStoragePhase::PREPARED) &&
           (!r.upload_succeeded || (r.upload_started && r.upload_finished)) &&
           (!r.reclaim_started || Terminal(r)) &&
           (r.phase != ObjectStoragePhase::PREPARED ||
            (!r.upload_started && !r.upload_finished)) &&
           (r.phase != ObjectStoragePhase::UPLOADING ||
            (r.upload_started && !r.upload_finished)) &&
           (r.phase != ObjectStoragePhase::COMPLETE || r.upload_succeeded) &&
           (r.phase != ObjectStoragePhase::DELETED || r.upload_finished);
}

ErrorCode ObjectStorageCatalog::Open(uint64_t epoch) {
    std::lock_guard lock(mutex_);
    ready_ = false;
    const auto& ns = config_.storage_namespace;
    if (!backend_ || !backend_->SupportsTxn() || cluster_id_.empty() ||
        cluster_id_.find_first_of(std::string("/\0", 2)) != std::string::npos ||
        ns.provider.empty() || ns.endpoint.empty() || ns.bucket.empty() ||
        ns.prefix.empty() || !config_.max_object_bytes ||
        !config_.max_pending_bytes || !config_.max_pending_objects ||
        !config_.maintenance_batch_size)
        return ErrorCode::INVALID_PARAMS;
    std::string old;
    auto error = backend_->Get(prefix_ + "fence", old);
    if (error != ErrorCode::OK && error != ErrorCode::ETCD_KEY_NOT_EXIST)
        return error;
    std::tuple<uint64_t, std::string, UUID> previous{};
    if (error == ErrorCode::OK) {
        if (!Decode(previous, old)) return ErrorCode::DESERIALIZE_FAIL;
        if (!std::get<0>(previous)) return ErrorCode::DESERIALIZE_FAIL;
        if (std::get<1>(previous) != namespace_id_)
            return ErrorCode::INVALID_PARAMS;
        if (!epoch) {
            if (std::get<0>(previous) == std::numeric_limits<uint64_t>::max())
                return ErrorCode::INVALID_PARAMS;
            epoch = std::get<0>(previous) + 1;
        } else if (epoch <= std::get<0>(previous)) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    } else if (!epoch)
        epoch = 1;
    const UUID owner{
        randomUniform<uint64_t>(0, std::numeric_limits<uint64_t>::max()),
        randomUniform<uint64_t>(0, std::numeric_limits<uint64_t>::max())};
    fence_ = Encode(std::make_tuple(epoch, namespace_id_, owner));
    KvTxn txn;
    txn.compares.push_back({prefix_ + "fence",
                            error == ErrorCode::OK
                                ? KvCompareKind::kValueEquals
                                : KvCompareKind::kKeyNotExists,
                            old});
    txn.puts.push_back({prefix_ + "fence", fence_});
    error = backend_->Txn(txn);
    if (error != ErrorCode::OK) {
        if (error == ErrorCode::ETCD_TRANSACTION_FAIL) return error;
        std::string actual;
        if (backend_->Get(prefix_ + "fence", actual) != ErrorCode::OK ||
            actual != fence_)
            return error;
    }
    error = LoadLocked();
    if (error != ErrorCode::OK) return error;
    // The paginated reads must belong to this leader, including on promotion
    // during recovery. Compare-only transactions do not mutate the catalog.
    error = backend_->Txn(
        {{{prefix_ + "fence", KvCompareKind::kValueEquals, fence_}}, {}});
    ready_ = error == ErrorCode::OK;
    return error;
}

ErrorCode ObjectStorageCatalog::LoadLocked() {
    records_.clear();
    heads_.clear();
    reclaim_keys_.clear();
    reclaim_cursor_.clear();
    pending_bytes_ = pending_count_ = 0;
    for (const bool heads : {false, true}) {
        std::string begin = prefix_ + (heads ? "heads/" : "records/");
        const std::string end = prefix_ + (heads ? "heads0" : "records0");
        while (true) {
            std::vector<KvPair> page;
            auto error = backend_->Range(begin, end, 512, page);
            if (error != ErrorCode::OK) return error;
            if (page.empty()) break;
            std::string last;
            for (const auto& kv : page) {
                ObjectStorageRecord record;
                if (kv.key < begin || kv.key >= end ||
                    (!last.empty() && kv.key <= last) ||
                    !Decode(record, kv.value) || !Valid(record) ||
                    kv.key != (heads ? HeadKey({record.tenant, record.key})
                                     : RecordKey(record.object_key)))
                    return ErrorCode::DESERIALIZE_FAIL;
                last = kv.key;
                if (heads)
                    heads_.emplace(Identity{record.tenant, record.key}, record);
                else if (!ApplyLocked(record))
                    return ErrorCode::DESERIALIZE_FAIL;
            }
            begin = page.back().key + '\0';
        }
    }
    for (const auto& [identity, head] : heads_) {
        const auto record = records_.find(head.object_key);
        if ((record == records_.end() &&
             head.phase != ObjectStoragePhase::DELETED) ||
            (record != records_.end() &&
             Encode(record->second) != Encode(head)))
            return ErrorCode::DESERIALIZE_FAIL;
    }
    for (const auto& [key, record] : records_) {
        const auto head = heads_.find({record.tenant, record.key});
        if (head == heads_.end() ||
            record.generation > head->second.generation ||
            (!Terminal(record) && head->second.object_key != key))
            return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

void ObjectStorageCatalog::EraseLocked(const std::string& key) {
    const auto it = records_.find(key);
    if (it == records_.end()) return;
    if (Pending(it->second)) {
        pending_bytes_ -= it->second.object_size;
        --pending_count_;
    }
    reclaim_keys_.erase(key);
    records_.erase(it);
}

bool ObjectStorageCatalog::ApplyLocked(const ObjectStorageRecord& record) {
    EraseLocked(record.object_key);
    if (Pending(record)) {
        if (record.object_size >
                std::numeric_limits<uint64_t>::max() - pending_bytes_ ||
            pending_count_ == std::numeric_limits<uint64_t>::max())
            return false;
        pending_bytes_ += record.object_size;
        ++pending_count_;
    }
    records_.emplace(record.object_key, record);
    if (Terminal(record)) reclaim_keys_.insert(record.object_key);
    return true;
}

bool ObjectStorageCatalog::CanReserveLocked(
    uint64_t bytes, const ObjectStorageRecord* credit) const {
    const auto used =
        pending_bytes_ - (credit && Pending(*credit) ? credit->object_size : 0);
    const auto count = pending_count_ - (credit && Pending(*credit) ? 1 : 0);
    return bytes <= config_.max_object_bytes &&
           count < config_.max_pending_objects &&
           used <= config_.max_pending_bytes &&
           bytes <= config_.max_pending_bytes - used;
}

bool ObjectStorageCatalog::CanReserve(uint64_t bytes) const {
    std::lock_guard lock(mutex_);
    return ready_ && CanReserveLocked(bytes, nullptr);
}

ErrorCode ObjectStorageCatalog::PersistLocked(
    ObjectStorageRecord record, std::optional<ObjectStorageRecord> replaced) {
    if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    if (record.revision == std::numeric_limits<uint64_t>::max() ||
        (replaced &&
         replaced->revision == std::numeric_limits<uint64_t>::max()))
        return ErrorCode::INVALID_WRITE;
    ++record.revision;
    if (!Valid(record)) return ErrorCode::INVALID_WRITE;
    const auto prior = records_.find(record.object_key);
    auto used_bytes = pending_bytes_;
    auto used_count = pending_count_;
    const auto discount = [&](const ObjectStorageRecord& old) {
        if (Pending(old)) {
            used_bytes -= old.object_size;
            --used_count;
        }
    };
    if (prior != records_.end()) discount(prior->second);
    if (replaced) discount(records_.at(replaced->object_key));
    if (Pending(record) &&
        (record.object_size >
             std::numeric_limits<uint64_t>::max() - used_bytes ||
         used_count == std::numeric_limits<uint64_t>::max()))
        return ErrorCode::NO_AVAILABLE_HANDLE;
    const Identity identity{record.tenant, record.key};
    const auto head = heads_.find(identity);
    const bool update_head = head == heads_.end() ||
                             head->second.object_key == record.object_key ||
                             head->second.generation < record.generation;
    KvTxn txn;
    txn.compares.push_back(
        {prefix_ + "fence", KvCompareKind::kValueEquals, fence_});
    auto append = [&](const ObjectStorageRecord& r) {
        const auto it = records_.find(r.object_key);
        txn.compares.push_back(
            {RecordKey(r.object_key),
             it == records_.end() ? KvCompareKind::kKeyNotExists
                                  : KvCompareKind::kValueEquals,
             it == records_.end() ? std::string{} : Encode(it->second)});
        txn.puts.push_back({RecordKey(r.object_key), Encode(r)});
    };
    append(record);
    if (replaced) {
        ++replaced->revision;
        append(*replaced);
    }
    if (update_head) {
        txn.compares.push_back(
            {HeadKey(identity),
             head == heads_.end() ? KvCompareKind::kKeyNotExists
                                  : KvCompareKind::kValueEquals,
             head == heads_.end() ? std::string{} : Encode(head->second)});
        txn.puts.push_back({HeadKey(identity), Encode(record)});
    }
    auto error = backend_->Txn(txn);
    if (error != ErrorCode::OK) {
        if (error == ErrorCode::ETCD_TRANSACTION_FAIL) {
            ready_ = false;
            return error;
        }
        // A lost reply is success only if every intended write is confirmed
        // under our fence. Otherwise stop dispatch/GC until a fenced reload.
        bool confirmed = true;
        auto expected = txn.puts;
        expected.push_back({prefix_ + "fence", fence_});
        for (const auto& kv : expected) {
            std::string actual;
            if (backend_->Get(kv.key, actual) != ErrorCode::OK ||
                actual != kv.value) {
                confirmed = false;
                break;
            }
        }
        if (!confirmed) {
            ready_ = false;
            return error;
        }
    }
    if (replaced && !ApplyLocked(*replaced)) {
        ready_ = false;
        return ErrorCode::DESERIALIZE_FAIL;
    }
    if (!ApplyLocked(record)) {
        ready_ = false;
        return ErrorCode::DESERIALIZE_FAIL;
    }
    if (update_head) heads_.insert_or_assign(identity, record);
    return ErrorCode::OK;
}

tl::expected<ObjectStorageRecord, ErrorCode> ObjectStorageCatalog::Prepare(
    const std::string& tenant, const std::string& key, uint64_t bytes) {
    std::lock_guard lock(mutex_);
    if (!ready_)
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    const auto head = heads_.find({tenant, key});
    std::optional<ObjectStorageRecord> replaced;
    uint64_t generation = 1;
    if (head != heads_.end()) {
        if (!Terminal(head->second))
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        if (head->second.generation == std::numeric_limits<uint64_t>::max())
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        generation = head->second.generation + 1;
        const auto old = records_.find(head->second.object_key);
        if (old != records_.end() &&
            old->second.phase == ObjectStoragePhase::RETIRED &&
            old->second.upload_finished && !old->second.upload_started) {
            replaced = old->second;
            replaced->phase = ObjectStoragePhase::DELETED;
        }
    }
    if (!CanReserveLocked(
            bytes, replaced ? &records_.at(replaced->object_key) : nullptr))
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    ObjectStorageRecord record;
    record.tenant = tenant;
    record.key = key;
    record.object_size = bytes;
    record.generation = generation;
    record.namespace_id = namespace_id_;
    record.object_key = ObjectKey({tenant, key}, generation);
    const auto error = PersistLocked(record, replaced);
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return records_.at(record.object_key);
}

ErrorCode ObjectStorageCatalog::Claim(const std::string& key) {
    std::lock_guard lock(mutex_);
    if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    const auto it = records_.find(key);
    if (it == records_.end()) return ErrorCode::OBJECT_NOT_FOUND;
    auto record = it->second;
    if (record.phase != ObjectStoragePhase::PREPARED)
        return ErrorCode::INVALID_WRITE;
    record.phase = ObjectStoragePhase::UPLOADING;
    record.upload_started = true;
    return PersistLocked(record);
}

ErrorCode ObjectStorageCatalog::Finish(const std::string& key,
                                       ObjectUploadOutcome outcome) {
    std::lock_guard lock(mutex_);
    if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    const auto it = records_.find(key);
    if (it == records_.end()) return ErrorCode::OBJECT_NOT_FOUND;
    auto record = it->second;
    if (outcome > ObjectUploadOutcome::UNKNOWN ||
        record.phase == ObjectStoragePhase::DELETED)
        return ErrorCode::INVALID_WRITE;
    if (record.upload_finished) {
        return ((outcome == ObjectUploadOutcome::COMPLETE &&
                 record.upload_succeeded) ||
                (outcome == ObjectUploadOutcome::NOT_STARTED &&
                 !record.upload_started))
                   ? ErrorCode::OK
                   : ErrorCode::INVALID_WRITE;
    }
    if (outcome != ObjectUploadOutcome::NOT_STARTED && !record.upload_started)
        return ErrorCode::INVALID_WRITE;
    if (outcome == ObjectUploadOutcome::UNKNOWN)
        record.phase = ObjectStoragePhase::RETIRED;
    else {
        record.upload_finished = true;
        record.upload_started = outcome == ObjectUploadOutcome::COMPLETE;
        record.upload_succeeded = outcome == ObjectUploadOutcome::COMPLETE;
        if (!Terminal(record))
            record.phase = record.upload_succeeded
                               ? ObjectStoragePhase::COMPLETE
                               : ObjectStoragePhase::RETIRED;
    }
    return PersistLocked(record);
}

ErrorCode ObjectStorageCatalog::Commit(const std::string& key,
                                       std::optional<uint64_t> checksum) {
    std::lock_guard lock(mutex_);
    if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    const auto it = records_.find(key);
    if (it == records_.end()) return ErrorCode::OBJECT_NOT_FOUND;
    auto record = it->second;
    if (Terminal(record)) return ErrorCode::INVALID_WRITE;
    if (record.foreground_complete)
        return record.checksum == checksum ? ErrorCode::OK
                                           : ErrorCode::INVALID_WRITE;
    record.foreground_complete = true;
    record.checksum = checksum;
    return PersistLocked(record);
}

ErrorCode ObjectStorageCatalog::Retire(const std::string& key,
                                       uint64_t deadline) {
    std::lock_guard lock(mutex_);
    if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    const auto it = records_.find(key);
    if (it == records_.end()) return ErrorCode::OBJECT_NOT_FOUND;
    auto record = it->second;
    if (record.reclaim_started && deadline > record.retire_after_ms)
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    if (record.phase == ObjectStoragePhase::DELETED) return ErrorCode::OK;
    if (record.phase == ObjectStoragePhase::PREPARED)
        record.upload_finished = true;
    record.phase = ObjectStoragePhase::RETIRED;
    record.retire_after_ms = std::max(record.retire_after_ms, deadline);
    return PersistLocked(record);
}

tl::expected<ObjectStorageRecord, ErrorCode> ObjectStorageCatalog::Find(
    const std::string& key) const {
    std::lock_guard lock(mutex_);
    if (!ready_)
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    const auto it = records_.find(key);
    if (it == records_.end() || it->second.phase == ObjectStoragePhase::DELETED)
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    return it->second;
}

tl::expected<ObjectStorageRecord, ErrorCode> ObjectStorageCatalog::Head(
    const std::string& tenant, const std::string& key) const {
    std::lock_guard lock(mutex_);
    if (!ready_)
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    const auto it = heads_.find({tenant, key});
    if (it == heads_.end())
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    return it->second;
}

tl::expected<std::vector<ObjectStorageRecord>, ErrorCode>
ObjectStorageCatalog::Records() const {
    std::lock_guard lock(mutex_);
    if (!ready_)
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    std::vector<ObjectStorageRecord> result;
    for (const auto& [key, record] : records_)
        if (record.phase != ObjectStoragePhase::DELETED)
            result.push_back(record);
    return result;
}

ErrorCode ObjectStorageCatalog::Reclaim(ObjectStorageAdapter& adapter,
                                        uint64_t now) {
    std::vector<std::string> candidates;
    {
        std::lock_guard lock(mutex_);
        if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        auto it = reclaim_keys_.upper_bound(reclaim_cursor_);
        const auto count = std::min<uint64_t>(reclaim_keys_.size(),
                                              config_.maintenance_batch_size);
        for (uint64_t i = 0; i < count; ++i) {
            if (it == reclaim_keys_.end()) it = reclaim_keys_.begin();
            reclaim_cursor_ = *it++;
            candidates.push_back(reclaim_cursor_);
        }
    }
    ErrorCode first_error = ErrorCode::OK;
    for (const auto& key : candidates) {
        ObjectStorageRecord record;
        {
            std::lock_guard lock(mutex_);
            if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            const auto it = records_.find(key);
            if (it == records_.end() || it->second.retire_after_ms > now)
                continue;
            record = it->second;
            if (record.phase == ObjectStoragePhase::RETIRED &&
                !record.reclaim_started) {
                record.reclaim_started = true;
                const auto error = PersistLocked(record);
                if (error != ErrorCode::OK) return error;
                record = records_.at(key);
            } else if (record.phase == ObjectStoragePhase::RETIRED) {
                const auto error = backend_->Txn(
                    {{{prefix_ + "fence", KvCompareKind::kValueEquals, fence_},
                      {RecordKey(key), KvCompareKind::kValueEquals,
                       Encode(record)}},
                     {}});
                if (error != ErrorCode::OK) return error;
            }
        }
        auto error = ErrorCode::OK;
        if (record.phase != ObjectStoragePhase::DELETED &&
            record.upload_started) {
            const auto deleted =
                adapter.Delete(key);  // No metadata/catalog lock held.
            if (!deleted) error = deleted.error();
        }
        if (error == ErrorCode::OK && record.upload_started &&
            !record.upload_finished)
            continue;  // It may still materialize after this DELETE.
        if (error == ErrorCode::OK) {
            std::lock_guard lock(mutex_);
            if (!ready_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            auto it = records_.find(key);
            if (it == records_.end()) continue;
            if (it->second.phase != ObjectStoragePhase::DELETED) {
                auto finished = it->second;
                finished.phase = ObjectStoragePhase::DELETED;
                error = PersistLocked(finished);
            }
            if (error == ErrorCode::OK) {
                // The terminal name is never reused. Removing its physical KV
                // record cannot erase a later generation, even after fencing.
                error = backend_->DeleteRange(RecordKey(key),
                                              RecordKey(key) + '\0');
                if (error == ErrorCode::OK) EraseLocked(key);
            }
        }
        if (first_error == ErrorCode::OK) first_error = error;
    }
    return first_error;
}

}  // namespace mooncake
