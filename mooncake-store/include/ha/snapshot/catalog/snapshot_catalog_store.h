#pragma once

#include <cctype>
#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <msgpack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

namespace snapshot_catalog_store_detail {

constexpr std::string_view kSnapshotRoot = "mooncake_master_snapshot/";
constexpr std::string_view kSnapshotLatest = "latest.txt";
constexpr std::string_view kSnapshotManifest = "manifest.txt";
constexpr std::string_view kSnapshotDescriptor = "descriptor.msgpack";

inline bool IsAsciiDigit(char ch) {
    return std::isdigit(static_cast<unsigned char>(ch)) != 0;
}

inline bool IsValidSnapshotId(std::string_view snapshot_id) {
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

        if (!IsAsciiDigit(snapshot_id[i])) {
            return false;
        }
    }

    return true;
}

inline std::string TrimAsciiWhitespace(std::string value) {
    constexpr std::string_view kAsciiWhitespace = " \t\n\r\f\v";
    const auto first = value.find_first_not_of(kAsciiWhitespace);
    if (first == std::string::npos) {
        return "";
    }

    const auto last = value.find_last_not_of(kAsciiWhitespace);
    return value.substr(first, last - first + 1);
}

inline std::string BuildSnapshotPrefix(const SnapshotId& snapshot_id) {
    return std::string(kSnapshotRoot) + snapshot_id + "/";
}

inline std::string BuildManifestKey(const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_id) + std::string(kSnapshotManifest);
}

inline std::string BuildDescriptorKey(const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_id) + std::string(kSnapshotDescriptor);
}

inline std::string BuildLatestKey() {
    return std::string(kSnapshotRoot) + std::string(kSnapshotLatest);
}

inline SnapshotDescriptor MakeSnapshotDescriptor(
    const SnapshotId& snapshot_id) {
    SnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.manifest_key = BuildManifestKey(snapshot_id);
    descriptor.object_prefix = BuildSnapshotPrefix(snapshot_id);
    return descriptor;
}

inline tl::expected<std::string, ErrorCode> SerializeSnapshotDescriptor(
    const SnapshotDescriptor& descriptor) {
    if (!IsValidSnapshotId(descriptor.snapshot_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    try {
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> packer(buffer);
        packer.pack_map(4);
        packer.pack(std::string("snapshot_id"));
        packer.pack(descriptor.snapshot_id);
        packer.pack(std::string("last_included_seq"));
        packer.pack(descriptor.last_included_seq);
        packer.pack(std::string("producer_view_version"));
        packer.pack(descriptor.producer_view_version);
        packer.pack(std::string("created_at_ms"));
        packer.pack(descriptor.created_at_ms);
        return std::string(buffer.data(), buffer.size());
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

inline tl::expected<SnapshotDescriptor, ErrorCode>
DeserializeSnapshotDescriptor(std::string_view payload) {
    const auto trimmed = TrimAsciiWhitespace(std::string(payload));
    if (IsValidSnapshotId(trimmed)) {
        return MakeSnapshotDescriptor(trimmed);
    }

    try {
        auto handle = msgpack::unpack(payload.data(), payload.size());
        const auto& root = handle.get();
        if (root.type != msgpack::type::MAP) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto find_field =
            [&](std::string_view field_name) -> const msgpack::object* {
            for (uint32_t i = 0; i < root.via.map.size; ++i) {
                const auto& key = root.via.map.ptr[i].key;
                if (key.type != msgpack::type::STR) {
                    continue;
                }
                std::string_view candidate(key.via.str.ptr, key.via.str.size);
                if (candidate == field_name) {
                    return &root.via.map.ptr[i].val;
                }
            }
            return nullptr;
        };

        const auto* snapshot_id_field = find_field("snapshot_id");
        const auto* last_included_seq_field = find_field("last_included_seq");
        const auto* producer_view_version_field =
            find_field("producer_view_version");
        const auto* created_at_ms_field = find_field("created_at_ms");
        if (snapshot_id_field == nullptr ||
            last_included_seq_field == nullptr ||
            producer_view_version_field == nullptr ||
            created_at_ms_field == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const auto snapshot_id = snapshot_id_field->as<std::string>();
        if (!IsValidSnapshotId(snapshot_id)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto descriptor = MakeSnapshotDescriptor(snapshot_id);
        descriptor.last_included_seq =
            last_included_seq_field->as<OpLogSequenceId>();
        descriptor.producer_view_version =
            producer_view_version_field->as<ViewVersionId>();
        descriptor.created_at_ms = created_at_ms_field->as<int64_t>();
        return descriptor;
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

}  // namespace snapshot_catalog_store_detail

class SnapshotCatalogStore {
   public:
    virtual ~SnapshotCatalogStore() = default;

    virtual ErrorCode Publish(const SnapshotDescriptor& snapshot) = 0;

    virtual tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
    GetLatest() = 0;

    virtual tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) = 0;

    virtual ErrorCode Delete(const SnapshotId& snapshot_id) = 0;
};

}  // namespace ha
}  // namespace mooncake
