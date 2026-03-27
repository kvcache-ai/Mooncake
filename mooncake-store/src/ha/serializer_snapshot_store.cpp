#include "ha/serializer_snapshot_store.h"

#include <algorithm>
#include <set>
#include <string_view>

namespace mooncake {
namespace ha {

namespace {
constexpr size_t kUnlimitedSnapshotList = 0;

ErrorCode ValidateBackend(SerializerBackend* backend) {
    return backend == nullptr ? ErrorCode::INVALID_PARAMS : ErrorCode::OK;
}

}  // namespace

SerializerSnapshotStore::SerializerSnapshotStore(SerializerBackend* backend)
    : backend_(backend) {}

ErrorCode SerializerSnapshotStore::Publish(const SnapshotDescriptor& snapshot) {
    auto err = ValidateBackend(backend_);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!snapshot_store_detail::IsValidSnapshotId(snapshot.snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto publish_result = backend_->UploadString(
        snapshot_store_detail::BuildLatestKey(), snapshot.snapshot_id);
    if (!publish_result) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    return ErrorCode::OK;
}

tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
SerializerSnapshotStore::GetLatest() {
    auto err = ValidateBackend(backend_);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::string latest_snapshot_id;
    auto get_result = backend_->DownloadString(
        snapshot_store_detail::BuildLatestKey(), latest_snapshot_id);
    if (!get_result) {
        if (backend_->IsNotFoundError(get_result.error())) {
            return std::optional<SnapshotDescriptor>();
        }
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    latest_snapshot_id = snapshot_store_detail::TrimAsciiWhitespace(
        std::move(latest_snapshot_id));
    if (latest_snapshot_id.empty()) {
        return std::optional<SnapshotDescriptor>();
    }
    if (!snapshot_store_detail::IsValidSnapshotId(latest_snapshot_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return std::optional<SnapshotDescriptor>(
        snapshot_store_detail::MakeSnapshotDescriptor(latest_snapshot_id));
}

tl::expected<std::vector<SnapshotDescriptor>, ErrorCode>
SerializerSnapshotStore::List(size_t limit) {
    auto err = ValidateBackend(backend_);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::vector<std::string> object_keys;
    auto list_result = backend_->ListObjectsWithPrefix(
        std::string(snapshot_store_detail::kSnapshotRoot), object_keys);
    if (!list_result) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    std::set<SnapshotId, std::greater<>> snapshot_ids;
    for (const auto& object_key : object_keys) {
        if (object_key.size() <= snapshot_store_detail::kSnapshotRoot.size()) {
            continue;
        }

        std::string_view suffix(object_key);
        suffix.remove_prefix(snapshot_store_detail::kSnapshotRoot.size());
        const size_t slash_pos = suffix.find('/');
        if (slash_pos == std::string_view::npos) {
            continue;
        }

        const auto snapshot_id = suffix.substr(0, slash_pos);
        if (!snapshot_store_detail::IsValidSnapshotId(snapshot_id)) {
            continue;
        }

        snapshot_ids.emplace(snapshot_id);
    }

    std::vector<SnapshotDescriptor> snapshots;
    const size_t target_size =
        limit == 0 ? snapshot_ids.size() : std::min(limit, snapshot_ids.size());
    snapshots.reserve(target_size);

    for (const auto& snapshot_id : snapshot_ids) {
        if (limit != 0 && snapshots.size() >= limit) {
            break;
        }
        snapshots.emplace_back(
            snapshot_store_detail::MakeSnapshotDescriptor(snapshot_id));
    }

    return snapshots;
}

ErrorCode SerializerSnapshotStore::Delete(const SnapshotId& snapshot_id) {
    auto err = ValidateBackend(backend_);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!snapshot_store_detail::IsValidSnapshotId(snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto latest_result = GetLatest();
    if (!latest_result) {
        return latest_result.error();
    }

    std::optional<SnapshotDescriptor> next_latest;
    const bool deleting_latest =
        latest_result->has_value() &&
        latest_result->value().snapshot_id == snapshot_id;
    if (deleting_latest) {
        auto list_result = List(kUnlimitedSnapshotList);
        if (!list_result) {
            return list_result.error();
        }

        for (const auto& candidate : list_result.value()) {
            if (candidate.snapshot_id != snapshot_id) {
                next_latest = candidate;
                break;
            }
        }
    }

    auto delete_result = backend_->DeleteObjectsWithPrefix(
        snapshot_store_detail::BuildSnapshotPrefix(snapshot_id));
    if (!delete_result) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    if (!deleting_latest) {
        return ErrorCode::OK;
    }

    if (next_latest.has_value()) {
        auto publish_result = backend_->UploadString(
            snapshot_store_detail::BuildLatestKey(), next_latest->snapshot_id);
        if (!publish_result) {
            return ErrorCode::PERSISTENT_FAIL;
        }
        return ErrorCode::OK;
    }

    auto clear_result = backend_->DeleteObjectsWithPrefix(
        snapshot_store_detail::BuildLatestKey());
    if (!clear_result) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    return ErrorCode::OK;
}

}  // namespace ha
}  // namespace mooncake
