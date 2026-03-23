#include "ha/serializer_snapshot_store.h"

#include <algorithm>
#include <cctype>
#include <set>
#include <string_view>

namespace mooncake {
namespace ha {

namespace {

constexpr std::string_view kSnapshotRoot = "mooncake_master_snapshot/";
constexpr std::string_view kSnapshotLatest = "latest.txt";
constexpr std::string_view kSnapshotManifest = "manifest.txt";

bool IsDigit(char ch) { return std::isdigit(static_cast<unsigned char>(ch)); }

bool IsValidSnapshotId(std::string_view snapshot_id) {
    if (snapshot_id.size() != 19) {
        return false;
    }

    for (size_t i = 0; i < snapshot_id.size(); ++i) {
        if (i == 8 || i == 15) {
            if (snapshot_id[i] != '_') {
                return false;
            }
            continue;
        }

        if (!IsDigit(snapshot_id[i])) {
            return false;
        }
    }

    return true;
}

std::string TrimAsciiWhitespace(std::string value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
        value.erase(value.begin());
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
        value.pop_back();
    }
    return value;
}

std::string BuildSnapshotPrefix(const SnapshotId& snapshot_id) {
    return std::string(kSnapshotRoot) + snapshot_id + "/";
}

std::string BuildManifestKey(const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_id) + std::string(kSnapshotManifest);
}

std::string BuildLatestKey() {
    return std::string(kSnapshotRoot) + std::string(kSnapshotLatest);
}

SnapshotDescriptor MakeSnapshotDescriptor(const SnapshotId& snapshot_id) {
    SnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.manifest_key = BuildManifestKey(snapshot_id);
    descriptor.object_prefix = BuildSnapshotPrefix(snapshot_id);
    return descriptor;
}

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
    if (!IsValidSnapshotId(snapshot.snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto publish_result =
        backend_->UploadString(BuildLatestKey(), snapshot.snapshot_id);
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
    auto get_result =
        backend_->DownloadString(BuildLatestKey(), latest_snapshot_id);
    if (!get_result) {
        if (backend_->IsNotFoundError(get_result.error())) {
            return std::optional<SnapshotDescriptor>();
        }
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    latest_snapshot_id = TrimAsciiWhitespace(std::move(latest_snapshot_id));
    if (latest_snapshot_id.empty()) {
        return std::optional<SnapshotDescriptor>();
    }
    if (!IsValidSnapshotId(latest_snapshot_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return std::optional<SnapshotDescriptor>(
        MakeSnapshotDescriptor(latest_snapshot_id));
}

tl::expected<std::vector<SnapshotDescriptor>, ErrorCode>
SerializerSnapshotStore::List(size_t limit) {
    auto err = ValidateBackend(backend_);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::vector<std::string> object_keys;
    auto list_result = backend_->ListObjectsWithPrefix(
        std::string(kSnapshotRoot), object_keys);
    if (!list_result) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    std::set<SnapshotId, std::greater<>> snapshot_ids;
    for (const auto& object_key : object_keys) {
        if (object_key.size() <= kSnapshotRoot.size()) {
            continue;
        }

        std::string_view suffix(object_key);
        suffix.remove_prefix(kSnapshotRoot.size());
        const size_t slash_pos = suffix.find('/');
        if (slash_pos == std::string_view::npos) {
            continue;
        }

        const auto snapshot_id = suffix.substr(0, slash_pos);
        if (!IsValidSnapshotId(snapshot_id)) {
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
        snapshots.emplace_back(MakeSnapshotDescriptor(snapshot_id));
    }

    return snapshots;
}

ErrorCode SerializerSnapshotStore::Delete(const SnapshotId& snapshot_id) {
    auto err = ValidateBackend(backend_);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!IsValidSnapshotId(snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto delete_result =
        backend_->DeleteObjectsWithPrefix(BuildSnapshotPrefix(snapshot_id));
    if (!delete_result) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    return ErrorCode::OK;
}

}  // namespace ha
}  // namespace mooncake
