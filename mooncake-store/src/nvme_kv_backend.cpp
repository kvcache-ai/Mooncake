#include "nvme_kv_backend.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>

#include "nvme_kv_key_codec.h"
#include "nvme_kv_key_conflict_policy.h"
#include "nvme_kv_object_layout.h"

namespace mooncake {

namespace {

constexpr uint32_t kMinMaxValueSize = sizeof(NvmeKvObjectHeader) + 1;
constexpr char kDefaultDeviceId[] = "default";
constexpr uint32_t kDeviceFailureThreshold = 3;

std::string PhysicalKeyToHex(const NvmeKvPhysicalKey& physical_key) {
    std::ostringstream oss;
    oss << std::hex;
    for (uint8_t b : physical_key) {
        oss.width(2);
        oss.fill('0');
        oss << static_cast<int>(b);
    }
    return oss.str();
}

bool ParsePhysicalKeyHex(std::string_view physical_key_hex,
                         NvmeKvPhysicalKey& physical_key) {
    if (physical_key_hex.size() != physical_key.size() * 2) {
        return false;
    }
    try {
        for (size_t i = 0; i < physical_key.size(); ++i) {
            const auto hex = std::string(physical_key_hex.substr(i * 2, 2));
            physical_key[i] =
                static_cast<uint8_t>(std::stoul(hex, nullptr, 16));
        }
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

std::string TrimConfiguredValue(std::string value) {
    auto not_space = [](unsigned char c) { return !std::isspace(c); };
    value.erase(value.begin(),
                std::find_if(value.begin(), value.end(), not_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(),
                value.end());
    return value;
}

std::vector<std::string> ParseConfiguredDeviceIds() {
    std::vector<std::string> device_ids;
    const char* configured = std::getenv("MOONCAKE_NVME_KV_DEVICE_IDS");
    if (configured == nullptr || configured[0] == '\0') {
        return device_ids;
    }
    std::stringstream ss(configured);
    std::string item;
    while (std::getline(ss, item, ',')) {
        item = TrimConfiguredValue(std::move(item));
        if (!item.empty()) {
            device_ids.push_back(item);
        }
    }
    return device_ids;
}

bool SkipCatalogRebuildFromDevices() {
    const char* configured =
        std::getenv("MOONCAKE_NVME_KV_SKIP_STARTUP_LIST_REBUILD");
    if (configured == nullptr || configured[0] == '\0') {
        return true;
    }
    return std::strcmp(configured, "0") != 0 &&
           std::strcmp(configured, "false") != 0 &&
           std::strcmp(configured, "FALSE") != 0;
}

StorageDeviceHealth ToStorageDeviceHealth(
    const NvmeKvStorageBackend::NvmeKvDeviceSnapshot& snapshot) {
    if (snapshot.state == "disabled_by_config") {
        return StorageDeviceHealth::DISABLED;
    }
    if (snapshot.state == "disabled_by_failure") {
        return StorageDeviceHealth::FAILED;
    }
    if (snapshot.state == "disabled_by_capacity") {
        return StorageDeviceHealth::DEGRADED;
    }
    if (snapshot.consecutive_failures > 0) {
        return StorageDeviceHealth::DEGRADED;
    }
    if (snapshot.health_state == "healthy" || snapshot.health_state == "stub") {
        return StorageDeviceHealth::HEALTHY;
    }
    if (snapshot.health_state == "error") {
        return StorageDeviceHealth::FAILED;
    }
    return StorageDeviceHealth::UNKNOWN;
}

std::string NvmeKvTransport(
    const NvmeKvStorageBackend::NvmeKvDeviceSnapshot& snapshot) {
    if (snapshot.device_path.rfind("/dev/", 0) == 0) {
        return "nvme-of";
    }
    return snapshot.backend_type == "stub" ? "stub" : "nvme";
}

UUID StableNvmeKvDeviceUuid(const std::string& device_id) {
    constexpr uint64_t kOffset = 14695981039346656037ULL;
    constexpr uint64_t kPrime = 1099511628211ULL;
    uint64_t first = kOffset;
    for (unsigned char c : device_id) {
        first ^= c;
        first *= kPrime;
    }
    uint64_t second = kOffset;
    for (unsigned char c : std::string("nvme_kv:") + device_id) {
        second ^= c;
        second *= kPrime;
    }
    return {first, second};
}

}  // namespace

NvmeKvStorageBackend::NvmeKvStorageBackend(
    const FileStorageConfig& file_storage_config_)
    : StorageBackendInterface(file_storage_config_) {}

std::filesystem::path NvmeKvStorageBackend::CatalogPath() const {
    return std::filesystem::path(file_storage_config_.storage_filepath) /
           "nvme_kv_catalog.txt";
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::InitDevices() {
    SharedMutexLocker lock(&mutex_);
    devices_.clear();
    lock.unlock();
    return ReconcileDevices();
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::ReconcileDevices() {
    auto configured_device_ids = ParseConfiguredDeviceIds();
    if (configured_device_ids.empty()) {
        configured_device_ids.push_back(kDefaultDeviceId);
    }
    std::sort(configured_device_ids.begin(), configured_device_ids.end());
    configured_device_ids.erase(
        std::unique(configured_device_ids.begin(), configured_device_ids.end()),
        configured_device_ids.end());

    for (const auto& device_id : configured_device_ids) {
        std::shared_ptr<NvmeKvConnector> connector;
        DeviceState desired_state = DeviceState::ENABLED;
        uint32_t consecutive_failures = 0;
        int64_t total_size = 0;
        int64_t total_keys = 0;

        {
            SharedMutexLocker lock(&mutex_, shared_lock);
            auto it = devices_.find(device_id);
            if (it != devices_.end()) {
                desired_state = it->second.state;
                consecutive_failures = it->second.consecutive_failures;
                total_size = it->second.total_size;
                total_keys = it->second.total_keys;
                if (it->second.connector != nullptr &&
                    desired_state != DeviceState::DISABLED_BY_FAILURE) {
                    continue;
                }
            }
        }

        auto next_connector =
            std::make_shared<NvmeKvConnector>(file_storage_config_, device_id);
        auto init_res = next_connector->Init();
        if (!init_res) {
            SharedMutexLocker lock(&mutex_);
            auto& runtime = devices_[device_id];
            runtime.device_id = device_id;
            runtime.connector.reset();
            runtime.total_size = total_size;
            runtime.total_keys = total_keys;
            runtime.consecutive_failures =
                std::max(consecutive_failures, kDeviceFailureThreshold);
            if (desired_state == DeviceState::DISABLED_BY_CONFIG) {
                runtime.state = DeviceState::DISABLED_BY_CONFIG;
            } else {
                runtime.state = DeviceState::DISABLED_BY_FAILURE;
            }
            continue;
        }

        SharedMutexLocker lock(&mutex_);
        auto& runtime = devices_[device_id];
        runtime.device_id = device_id;
        runtime.connector = std::move(next_connector);
        runtime.total_size = total_size;
        runtime.total_keys = total_keys;
        if (desired_state == DeviceState::DISABLED_BY_CONFIG) {
            runtime.state = DeviceState::DISABLED_BY_CONFIG;
            runtime.consecutive_failures = 0;
        } else {
            runtime.state = DeviceState::ENABLED;
            runtime.consecutive_failures = 0;
            runtime.provider_last_error.clear();
        }
    }

    return {};
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::LoadCatalog() {
    return RefreshCatalogFromDisk();
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::RefreshCatalogFromDisk() {
    std::ifstream in(CatalogPath());
    if (!in) {
        if (SkipCatalogRebuildFromDevices()) {
            LOG(WARNING)
                << "Skipping NVMe KV startup list/catalog rebuild "
                << "because MOONCAKE_NVME_KV_SKIP_STARTUP_LIST_REBUILD "
                << "is enabled";
            {
                SharedMutexLocker lock(&mutex_);
                catalog_.clear();
                for (auto& [_, runtime] : devices_) {
                    runtime.total_size = 0;
                    runtime.total_keys = 0;
                }
            }
            total_size_.store(0, std::memory_order_relaxed);
            total_keys_.store(0, std::memory_order_relaxed);
            return PersistCatalog();
        }
        return RebuildCatalogFromDevices();
    }

    std::unordered_map<std::string, CatalogEntry> loaded_catalog;
    std::unordered_map<std::string, int64_t> per_device_sizes;
    std::unordered_map<std::string, int64_t> per_device_keys;
    int64_t total_size = 0;
    int64_t total_keys = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::istringstream iss(line);
        std::string key;
        std::string device_id;
        std::string physical_key_hex;
        uint32_t physical_key_slot = 0;
        uint32_t payload_size = 0;
        int state_int = 0;
        if (!std::getline(iss, key, '\t')) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (!std::getline(iss, device_id, '\t')) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        if (device_id.size() == 32) {
            physical_key_hex = device_id;
            device_id = kDefaultDeviceId;
            if (!(iss >> payload_size >> state_int)) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
        } else {
            if (!std::getline(iss, physical_key_hex, '\t')) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            if (!(iss >> physical_key_slot >> payload_size >> state_int)) {
                iss.clear();
                if (!(iss >> payload_size >> state_int)) {
                    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
                physical_key_slot = 0;
            }
        }

        PhysicalKey physical_key{};
        if (!ParsePhysicalKeyHex(physical_key_hex, physical_key)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        auto state = static_cast<ObjectState>(state_int);
        loaded_catalog.emplace(
            key, CatalogEntry{device_id, physical_key, physical_key_slot,
                              payload_size, state});
        if (state == ObjectState::COMMITTED) {
            total_size += payload_size;
            total_keys += 1;
            per_device_sizes[device_id] += payload_size;
            per_device_keys[device_id] += 1;
        }
    }

    SharedMutexLocker lock(&mutex_);
    catalog_ = std::move(loaded_catalog);
    for (auto& [device_id, runtime] : devices_) {
        runtime.total_size = per_device_sizes[device_id];
        runtime.total_keys = per_device_keys[device_id];
    }
    total_size_.store(total_size, std::memory_order_relaxed);
    total_keys_.store(total_keys, std::memory_order_relaxed);
    return {};
}

tl::expected<void, ErrorCode>
NvmeKvStorageBackend::RebuildCatalogFromDevices() {
    std::vector<std::pair<std::string, std::shared_ptr<NvmeKvConnector>>>
        rebuild_targets;
    bool has_non_iterable_device = false;
    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        for (const auto& [device_id, runtime] : devices_) {
            if (runtime.connector == nullptr) {
                continue;
            }
            if (!runtime.connector->GetCapabilities().supports_iterate) {
                has_non_iterable_device = true;
                continue;
            }
            rebuild_targets.emplace_back(device_id, runtime.connector);
        }
    }
    if (rebuild_targets.empty() && has_non_iterable_device) {
        LOG(WARNING) << "NVMe KV catalog rebuild requires iterate support, "
                     << "but no configured device supports iterate";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    std::unordered_map<std::string, CatalogEntry> rebuilt_catalog;
    std::unordered_map<std::string, int64_t> per_device_sizes;
    std::unordered_map<std::string, int64_t> per_device_keys;
    int64_t total_size = 0;
    int64_t total_keys = 0;
    for (const auto& [device_id, connector] : rebuild_targets) {
        auto iterate_res =
            connector->Iterate([&](const PhysicalKey& physical_key)
                                   -> tl::expected<void, ErrorCode> {
                auto object_res = connector->Retrieve(physical_key);
                if (!object_res) {
                    if (object_res.error() == ErrorCode::OBJECT_NOT_FOUND) {
                        return {};
                    }
                    return tl::make_unexpected(object_res.error());
                }

                ObjectHeader header{};
                std::string_view identity_metadata_view;
                std::string_view payload_view;
                if (!ParseNvmeKvObjectBlob(object_res.value(), header,
                                           identity_metadata_view,
                                           payload_view)) {
                    return {};
                }

                const auto object_type =
                    static_cast<NvmeKvObjectType>(header.object_type);
                if (object_type == NvmeKvObjectType::kChunk) {
                    return {};
                }
                if (object_type != NvmeKvObjectType::kInline &&
                    object_type != NvmeKvObjectType::kManifest) {
                    return {};
                }
                NvmeKvStoredIdentityView stored_identity_view{};
                if (!ParseNvmeKvStoredIdentity(identity_metadata_view,
                                               stored_identity_view) ||
                    stored_identity_view.logical_key.empty()) {
                    return {};
                }

                NvmeKvObjectIdentity identity =
                    NvmeKvKeyConflictPolicy::BuildIdentityFromStoredView(
                        stored_identity_view);
                if (!NvmeKvKeyConflictPolicy::ValidateResolvedRootPlacement(
                        identity, stored_identity_view, physical_key)) {
                    return {};
                }

                uint32_t logical_payload_size = 0;
                const uint32_t expected_identity_size =
                    ComputeNvmeKvStoredIdentityMetadataSize(identity);
                if (object_type == NvmeKvObjectType::kInline) {
                    if (!ValidateNvmeKvHeader(
                            header, identity, expected_identity_size,
                            static_cast<uint32_t>(payload_view.size()),
                            NvmeKvObjectType::kInline) ||
                        header.payload_checksum !=
                            ComputeNvmeKvPayloadChecksum(payload_view)) {
                        return {};
                    }
                    logical_payload_size =
                        static_cast<uint32_t>(payload_view.size());
                } else {
                    if (!ValidateNvmeKvHeader(
                            header, identity, expected_identity_size,
                            static_cast<uint32_t>(payload_view.size()),
                            NvmeKvObjectType::kManifest) ||
                        header.payload_checksum !=
                            ComputeNvmeKvPayloadChecksum(payload_view)) {
                        return {};
                    }
                    NvmeKvManifestMetadata metadata{};
                    std::vector<NvmeKvManifestChunkRecord> chunk_records;
                    if (!ParseNvmeKvManifest(payload_view, metadata,
                                             chunk_records) ||
                        metadata.chunk_count != chunk_records.size()) {
                        return {};
                    }
                    for (size_t chunk_index = 0;
                         chunk_index < chunk_records.size(); ++chunk_index) {
                        if (chunk_records[chunk_index].physical_key !=
                            EncodeNvmeKvChunkPhysicalKey(
                                identity, static_cast<uint32_t>(chunk_index),
                                stored_identity_view.resolved_slot)) {
                            return {};
                        }
                    }
                    logical_payload_size = metadata.logical_payload_size;
                }

                auto [it, inserted] = rebuilt_catalog.emplace(
                    identity.logical_key,
                    CatalogEntry{device_id, physical_key,
                                 stored_identity_view.resolved_slot,
                                 logical_payload_size, ObjectState::COMMITTED});
                if (!inserted) {
                    return {};
                }
                total_size += logical_payload_size;
                total_keys += 1;
                per_device_sizes[device_id] += logical_payload_size;
                per_device_keys[device_id] += 1;
                return {};
            });
        if (!iterate_res) {
            LOG(WARNING) << "NVMe KV iterate failed for device " << device_id
                         << ", error=" << toString(iterate_res.error());
            return iterate_res;
        }
    }

    {
        SharedMutexLocker lock(&mutex_);
        catalog_ = std::move(rebuilt_catalog);
        for (auto& [device_id, runtime] : devices_) {
            runtime.total_size = per_device_sizes[device_id];
            runtime.total_keys = per_device_keys[device_id];
        }
    }
    total_size_.store(total_size, std::memory_order_relaxed);
    total_keys_.store(total_keys, std::memory_order_relaxed);
    return PersistCatalog();
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::PersistCatalog() const {
    std::error_code ec;
    std::filesystem::create_directories(
        std::filesystem::path(file_storage_config_.storage_filepath), ec);
    if (ec) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    std::ofstream out(CatalogPath(), std::ios::trunc);
    if (!out) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    SharedMutexLocker lock(&mutex_, shared_lock);
    for (const auto& [key, entry] : catalog_) {
        out << key << '\t' << entry.device_id << '\t'
            << PhysicalKeyToHex(entry.physical_key) << '\t'
            << entry.physical_key_slot << ' ' << entry.payload_size << ' '
            << static_cast<int>(entry.state) << '\n';
    }
    if (!out.good()) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

uint32_t NvmeKvStorageBackend::EffectiveMaxValueSize() const {
    if (test_max_value_size_override_ != 0) {
        return test_max_value_size_override_;
    }
    SharedMutexLocker lock(&mutex_, shared_lock);
    uint32_t min_max_value_size = UINT32_MAX;
    for (const auto& [_, device] : devices_) {
        if (device.state != DeviceState::ENABLED ||
            device.connector == nullptr) {
            continue;
        }
        min_max_value_size = std::min(
            min_max_value_size,
            device.connector->GetCapabilities().effective_max_value_size);
    }
    if (min_max_value_size == UINT32_MAX) {
        return kMinMaxValueSize;
    }
    return std::max(min_max_value_size, kMinMaxValueSize);
}

uint32_t NvmeKvStorageBackend::InlinePayloadLimit() const {
    const uint32_t max_value_size = EffectiveMaxValueSize();
    if (max_value_size <= sizeof(ObjectHeader)) {
        return 0;
    }
    return max_value_size - sizeof(ObjectHeader);
}

uint32_t NvmeKvStorageBackend::ChunkPayloadLimit() const {
    return InlinePayloadLimit();
}

bool NvmeKvStorageBackend::ShouldStoreInline(size_t payload_size) const {
    return payload_size <= InlinePayloadLimit();
}

void NvmeKvStorageBackend::MarkCorrupted(const std::string& key) {
    SharedMutexLocker lock(&mutex_);
    auto it = catalog_.find(key);
    if (it != catalog_.end()) {
        it->second.state = ObjectState::CORRUPTED;
    }
}

std::vector<std::string> NvmeKvStorageBackend::EnabledDeviceIds() const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    std::vector<std::string> device_ids;
    device_ids.reserve(devices_.size());
    for (const auto& [device_id, runtime] : devices_) {
        if (runtime.state == DeviceState::ENABLED) {
            device_ids.push_back(device_id);
        }
    }
    std::sort(device_ids.begin(), device_ids.end());
    return device_ids;
}

tl::expected<std::string, ErrorCode> NvmeKvStorageBackend::SelectDeviceIdForKey(
    const std::string& key, size_t retry_count) const {
    auto device_ids = EnabledDeviceIds();
    if (device_ids.empty()) {
        SharedMutexLocker lock(&mutex_, shared_lock);
        for (const auto& [_, runtime] : devices_) {
            if (runtime.state == DeviceState::DISABLED_BY_CAPACITY) {
                return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
            }
        }
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const size_t index =
        (std::hash<std::string>{}(key) + retry_count) % device_ids.size();
    return device_ids[index];
}

std::shared_ptr<NvmeKvConnector> NvmeKvStorageBackend::GetConnectorForDeviceId(
    const std::string& device_id) const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return nullptr;
    }
    return it->second.connector;
}

void NvmeKvStorageBackend::RecordDeviceFailure(const std::string& device_id,
                                               const std::string& reason) {
    SharedMutexLocker lock(&mutex_);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return;
    }
    auto& runtime = it->second;
    runtime.consecutive_failures += 1;
    if (!reason.empty()) {
        runtime.provider_last_error = reason;
    }
    if (runtime.consecutive_failures >= kDeviceFailureThreshold) {
        runtime.state = DeviceState::DISABLED_BY_FAILURE;
    }
}

std::optional<std::string> NvmeKvStorageBackend::ResolveDeviceNamespaceId(
    const UUID& device_id) const {
    for (const auto& [ns_id, runtime] : devices_) {
        if (runtime.connector && StableNvmeKvDeviceUuid(ns_id) == device_id) {
            return ns_id;
        }
    }
    return std::nullopt;
}

void NvmeKvStorageBackend::RecordDeviceFull(const std::string& device_id) {
    SharedMutexLocker lock(&mutex_);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return;
    }
    auto& runtime = it->second;
    runtime.state = DeviceState::DISABLED_BY_CAPACITY;
}

void NvmeKvStorageBackend::RecordDeviceSuccess(const std::string& device_id,
                                               int64_t payload_size,
                                               bool new_key_committed) {
    SharedMutexLocker lock(&mutex_);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return;
    }
    auto& runtime = it->second;
    runtime.consecutive_failures = 0;
    runtime.provider_last_error.clear();
    if (runtime.state == DeviceState::DISABLED_BY_FAILURE) {
        runtime.state = DeviceState::ENABLED;
    } else if (runtime.state == DeviceState::DISABLED_BY_CAPACITY &&
               (payload_size > 0 || new_key_committed)) {
        runtime.state = DeviceState::ENABLED;
    }
    runtime.total_size += payload_size;
    if (new_key_committed) {
        runtime.total_keys += 1;
    }
}

std::vector<NvmeKvStorageBackend::NvmeKvDeviceSnapshot>
NvmeKvStorageBackend::GetDeviceSnapshots() const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    std::vector<NvmeKvDeviceSnapshot> snapshots;
    snapshots.reserve(devices_.size());
    for (const auto& [device_id, runtime] : devices_) {
        NvmeKvDeviceSnapshot snapshot{
            .device_id = device_id,
            .state =
                [&runtime]() {
                    switch (runtime.state) {
                        case DeviceState::ENABLED:
                            return std::string("enabled");
                        case DeviceState::DISABLED_BY_CONFIG:
                            return std::string("disabled_by_config");
                        case DeviceState::DISABLED_BY_FAILURE:
                            return std::string("disabled_by_failure");
                        case DeviceState::DISABLED_BY_CAPACITY:
                            return std::string("disabled_by_capacity");
                    }
                    return std::string("unknown");
                }(),
            .consecutive_failures = runtime.consecutive_failures,
            .failure_threshold = kDeviceFailureThreshold,
            .total_size = runtime.total_size,
            .total_keys = runtime.total_keys,
        };
        if (runtime.connector != nullptr) {
            const auto& capabilities = runtime.connector->GetCapabilities();
            const auto& metadata = runtime.connector->GetMetadata();
            snapshot.capacity_total = metadata.capacity_total;
            snapshot.max_key_size = capabilities.max_key_size;
            snapshot.max_value_size = capabilities.max_value_size;
            snapshot.runtime_transfer_limit =
                capabilities.runtime_transfer_limit;
            snapshot.effective_max_value_size =
                capabilities.effective_max_value_size;
            snapshot.queue_depth = capabilities.queue_depth;
            snapshot.backend_type = metadata.backend_type;
            snapshot.uuid = metadata.uuid;
            snapshot.device_serial =
                metadata.uuid.empty() ? device_id : metadata.uuid;
            snapshot.device_path = metadata.device_path;
            snapshot.readable =
                runtime.state != DeviceState::DISABLED_BY_FAILURE;
            snapshot.writable = runtime.state == DeviceState::ENABLED;
            snapshot.health_state = metadata.health_state;
            snapshot.last_error = metadata.last_error.empty()
                                      ? runtime.provider_last_error
                                      : metadata.last_error;
            snapshot.storage_path =
                runtime.connector->GetStoragePath().string();
        }
        snapshots.push_back(std::move(snapshot));
    }
    std::sort(snapshots.begin(), snapshots.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.device_id < rhs.device_id;
              });
    return snapshots;
}

std::vector<StorageDeviceMetadata>
NvmeKvStorageBackend::ListStorageDeviceMetadata() const {
    auto snapshots = GetDeviceSnapshots();
    std::vector<StorageDeviceMetadata> metadata;
    metadata.reserve(snapshots.size());
    for (const auto& snapshot : snapshots) {
        StorageDeviceMetadata item;
        item.identity.kind = StorageDeviceKind::NAMESPACE;
        item.identity.provider = "nvme_kv";
        item.identity.device_id = StableNvmeKvDeviceUuid(snapshot.device_id);
        item.identity.target_id =
            snapshot.uuid.empty() ? snapshot.device_serial : snapshot.uuid;
        item.identity.namespace_id = snapshot.device_id;
        item.identity.disk_id = snapshot.device_path;
        item.identity.node_id = snapshot.storage_path;
        item.health = ToStorageDeviceHealth(snapshot);
        item.readable = snapshot.readable;
        item.writable = snapshot.writable;
        item.schedulable =
            snapshot.writable && item.health == StorageDeviceHealth::HEALTHY;
        item.capacity_total = snapshot.capacity_total;
        item.capacity_used = snapshot.total_size;
        item.capacity_available =
            std::max<int64_t>(0, snapshot.capacity_total - snapshot.total_size);
        item.consecutive_failures = snapshot.consecutive_failures;
        item.last_error = snapshot.last_error;
        item.mount_endpoint = snapshot.device_path;
        item.transport = NvmeKvTransport(snapshot);
        std::ostringstream opaque;
        opaque << "backend_type=" << snapshot.backend_type
               << ";max_key_size=" << snapshot.max_key_size
               << ";max_value_size=" << snapshot.max_value_size
               << ";effective_max_value_size="
               << snapshot.effective_max_value_size
               << ";runtime_transfer_limit=" << snapshot.runtime_transfer_limit
               << ";queue_depth=" << snapshot.queue_depth
               << ";total_keys=" << snapshot.total_keys
               << ";state=" << snapshot.state
               << ";health_state=" << snapshot.health_state
               << ";consecutive_failures=" << snapshot.consecutive_failures
               << ";failure_threshold=" << snapshot.failure_threshold
               << ";device_serial=" << snapshot.device_serial
               << ";namespace_uuid=" << snapshot.uuid;
        item.opaque_provider_metadata = opaque.str();
        metadata.push_back(std::move(item));
    }
    return metadata;
}

tl::expected<StorageDeviceMetadata, ErrorCode>
NvmeKvStorageBackend::ApplyStorageDeviceMetadataUpdate(
    const StorageDeviceMetadataUpdate& update) {
    auto ns_id = ResolveDeviceNamespaceId(update.identity.device_id);
    if (!ns_id.has_value()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    std::optional<bool> enabled;
    if (update.health.has_value()) {
        if (*update.health == StorageDeviceHealth::DISABLED) {
            enabled = false;
        } else if (*update.health == StorageDeviceHealth::HEALTHY) {
            enabled = true;
        }
    }
    if (update.writable.has_value()) enabled = *update.writable;
    if (update.schedulable.has_value()) enabled = *update.schedulable;
    if (enabled.has_value()) {
        SetDeviceEnabled(*ns_id, *enabled);
    }
    return ListStorageDeviceMetadata()[0];  // single-device backend
}

tl::expected<StorageDeviceMetadata, ErrorCode>
NvmeKvStorageBackend::StartStorageDeviceRecovery(const UUID& device_id) {
    SharedMutexLocker lock(&mutex_);
    auto ns_id = ResolveDeviceNamespaceId(device_id);
    if (!ns_id.has_value()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto it = devices_.find(*ns_id);
    if (it == devices_.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (it->second.state != DeviceState::DISABLED_BY_CONFIG) {
        it->second.state = DeviceState::DISABLED_BY_FAILURE;
        it->second.consecutive_failures =
            std::max(it->second.consecutive_failures, kDeviceFailureThreshold);
        it->second.provider_last_error = "recovery_in_progress";
    }
    lock.unlock();
    auto metadata = ListStorageDeviceMetadata();
    for (auto& m : metadata) {
        if (m.identity.device_id == device_id) return m;
    }
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<StorageDeviceMetadata, ErrorCode>
NvmeKvStorageBackend::CompleteStorageDeviceRecovery(const UUID& device_id) {
    auto ns_id = ResolveDeviceNamespaceId(device_id);
    if (!ns_id.has_value()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto refresh_result = RefreshDevices();
    if (!refresh_result.has_value()) {
        return tl::make_unexpected(refresh_result.error());
    }
    auto metadata = ListStorageDeviceMetadata();
    for (auto& m : metadata) {
        if (m.identity.device_id == device_id) return m;
    }
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<StorageDeviceMetadata, ErrorCode>
NvmeKvStorageBackend::FailStorageDeviceRecovery(const UUID& device_id) {
    SharedMutexLocker lock(&mutex_);
    auto ns_id = ResolveDeviceNamespaceId(device_id);
    if (!ns_id.has_value()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto it = devices_.find(*ns_id);
    if (it != devices_.end() &&
        it->second.state != DeviceState::DISABLED_BY_CONFIG) {
        it->second.state = DeviceState::DISABLED_BY_FAILURE;
        it->second.consecutive_failures =
            std::max(it->second.consecutive_failures, kDeviceFailureThreshold);
        it->second.provider_last_error = "recovery_failed";
    }
    lock.unlock();
    auto metadata = ListStorageDeviceMetadata();
    for (auto& m : metadata) {
        if (m.identity.device_id == device_id) return m;
    }
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<StorageDeviceMetadata, ErrorCode>
NvmeKvStorageBackend::RecordStorageDeviceProbeSuccess(const UUID& device_id) {
    return CompleteStorageDeviceRecovery(device_id);
}

tl::expected<StorageDeviceMetadata, ErrorCode>
NvmeKvStorageBackend::RecordStorageDeviceProbeFailure(
    const UUID& device_id, const std::string& reason) {
    auto ns_id = ResolveDeviceNamespaceId(device_id);
    if (!ns_id.has_value()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    RecordDeviceFailure(*ns_id, reason);
    auto metadata = ListStorageDeviceMetadata();
    for (auto& m : metadata) {
        if (m.identity.device_id == device_id) return m;
    }
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<StorageDeviceMetadata, ErrorCode>
NvmeKvStorageBackend::ProbeStorageDevice(const UUID& device_id) {
    auto ns_id = ResolveDeviceNamespaceId(device_id);
    if (!ns_id.has_value()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto connector = GetConnectorForDeviceId(*ns_id);
    if (connector == nullptr) {
        return RecordStorageDeviceProbeFailure(device_id, "connector_missing");
    }
    auto probe_result = connector->Probe();
    if (!probe_result.has_value()) {
        return RecordStorageDeviceProbeFailure(
            device_id, "probe_failed:" + toString(probe_result.error()));
    }
    return RecordStorageDeviceProbeSuccess(device_id);
}

std::optional<NvmeKvStorageBackend::NvmeKvDeviceSnapshot>
NvmeKvStorageBackend::GetDeviceSnapshot(const std::string& device_id) const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return std::nullopt;
    }
    const auto& runtime = it->second;
    std::string state;
    switch (runtime.state) {
        case DeviceState::ENABLED:
            state = "enabled";
            break;
        case DeviceState::DISABLED_BY_CONFIG:
            state = "disabled_by_config";
            break;
        case DeviceState::DISABLED_BY_FAILURE:
            state = "disabled_by_failure";
            break;
        case DeviceState::DISABLED_BY_CAPACITY:
            state = "disabled_by_capacity";
            break;
    }
    NvmeKvDeviceSnapshot snapshot{
        .device_id = device_id,
        .state = std::move(state),
        .consecutive_failures = runtime.consecutive_failures,
        .total_size = runtime.total_size,
        .total_keys = runtime.total_keys,
    };
    if (runtime.connector != nullptr) {
        const auto& capabilities = runtime.connector->GetCapabilities();
        const auto& metadata = runtime.connector->GetMetadata();
        snapshot.capacity_total = metadata.capacity_total;
        snapshot.max_key_size = capabilities.max_key_size;
        snapshot.max_value_size = capabilities.max_value_size;
        snapshot.runtime_transfer_limit = capabilities.runtime_transfer_limit;
        snapshot.effective_max_value_size =
            capabilities.effective_max_value_size;
        snapshot.queue_depth = capabilities.queue_depth;
        snapshot.backend_type = metadata.backend_type;
        snapshot.uuid = metadata.uuid;
        snapshot.device_serial =
            metadata.uuid.empty() ? device_id : metadata.uuid;
        snapshot.device_path = metadata.device_path;
        snapshot.readable = runtime.state != DeviceState::DISABLED_BY_FAILURE;
        snapshot.writable = runtime.state == DeviceState::ENABLED;
        snapshot.health_state = metadata.health_state;
        snapshot.last_error = metadata.last_error.empty()
                                  ? runtime.provider_last_error
                                  : metadata.last_error;
        snapshot.storage_path = runtime.connector->GetStoragePath().string();
    }
    return snapshot;
}

NvmeKvStorageBackend::NvmeKvStatusSnapshot
NvmeKvStorageBackend::GetStatusSnapshot() const {
    NvmeKvStatusSnapshot snapshot;
    snapshot.initialized = initialized_.load(std::memory_order_acquire);
    snapshot.total_size = total_size_.load(std::memory_order_relaxed);
    snapshot.total_keys = total_keys_.load(std::memory_order_relaxed);
    snapshot.effective_max_value_size = EffectiveMaxValueSize();
    snapshot.placement_policy = "hash_mod_enabled_devices";
    snapshot.devices = GetDeviceSnapshots();
    return snapshot;
}

std::vector<bool> NvmeKvStorageBackend::DeleteKeysFromDevice(
    const std::vector<std::string>& keys) {
    std::vector<bool> results(keys.size(), false);
    for (size_t i = 0; i < keys.size(); ++i) {
        CatalogEntry entry;
        {
            SharedMutexLocker lock(&mutex_, shared_lock);
            auto it = catalog_.find(keys[i]);
            if (it == catalog_.end()) {
                continue;
            }
            entry = it->second;
        }
        auto connector = GetConnectorForDeviceId(entry.device_id);
        if (connector == nullptr) {
            continue;
        }
        auto delete_res = connector->Delete(entry.physical_key);
        if (!delete_res && delete_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
            continue;
        }
        {
            SharedMutexLocker lock(&mutex_);
            auto it = catalog_.find(keys[i]);
            if (it != catalog_.end()) {
                total_size_.fetch_sub(it->second.payload_size,
                                      std::memory_order_relaxed);
                total_keys_.fetch_sub(1, std::memory_order_relaxed);
                catalog_.erase(it);
            }
        }
        results[i] = true;
    }
    PersistCatalog();
    return results;
}

std::vector<std::string> NvmeKvStorageBackend::ListKeysForDevice(
    const std::string& device_id) const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    std::vector<std::string> keys;
    for (const auto& [key, entry] : catalog_) {
        if (entry.device_id == device_id &&
            entry.state == ObjectState::COMMITTED) {
            keys.push_back(key);
        }
    }
    return keys;
}

std::pair<int64_t, int64_t> NvmeKvStorageBackend::GetDeviceCapacityUsage(
    const std::string& device_id) const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return {0, 0};
    }
    int64_t capacity = 0;
    if (it->second.connector) {
        capacity = it->second.connector->GetMetadata().capacity_total;
    }
    return {it->second.total_size, capacity};
}

bool NvmeKvStorageBackend::SetDeviceEnabled(const std::string& device_id,
                                            bool enabled) {
    SharedMutexLocker lock(&mutex_);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return false;
    }
    auto& runtime = it->second;
    runtime.state =
        enabled ? DeviceState::ENABLED : DeviceState::DISABLED_BY_CONFIG;
    if (enabled) {
        runtime.consecutive_failures = 0;
    }
    return true;
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::RefreshDevices() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return ReconcileDevices();
}

std::optional<NvmeKvStorageBackend::NvmeKvObjectSnapshot>
NvmeKvStorageBackend::GetObjectSnapshot(const std::string& key) {
    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        auto it = catalog_.find(key);
        if (it != catalog_.end()) {
            const auto& entry = it->second;
            std::string state;
            switch (entry.state) {
                case ObjectState::WRITING:
                    state = "writing";
                    break;
                case ObjectState::COMMITTED:
                    state = "committed";
                    break;
                case ObjectState::DELETING:
                    state = "deleting";
                    break;
                case ObjectState::DELETED:
                    state = "deleted";
                    break;
                case ObjectState::CORRUPTED:
                    state = "corrupted";
                    break;
            }
            return NvmeKvObjectSnapshot{
                .key = key,
                .payload_size = entry.payload_size,
                .state = std::move(state),
                .tenant_id = "",
                .domain_id = "",
                .namespace_id = "",
            };
        }
    }

    auto refresh_res = RefreshCatalogFromDisk();
    if (!refresh_res) {
        return std::nullopt;
    }

    SharedMutexLocker lock(&mutex_, shared_lock);
    auto it = catalog_.find(key);
    if (it == catalog_.end()) {
        return std::nullopt;
    }
    const auto& entry = it->second;
    std::string state;
    switch (entry.state) {
        case ObjectState::WRITING:
            state = "writing";
            break;
        case ObjectState::COMMITTED:
            state = "committed";
            break;
        case ObjectState::DELETING:
            state = "deleting";
            break;
        case ObjectState::DELETED:
            state = "deleted";
            break;
        case ObjectState::CORRUPTED:
            state = "corrupted";
            break;
    }
    return NvmeKvObjectSnapshot{
        .key = key,
        .payload_size = entry.payload_size,
        .state = std::move(state),
        .tenant_id = "",
        .domain_id = "",
        .namespace_id = "",
    };
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::Init() {
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true,
                                              std::memory_order_acq_rel)) {
        LOG(ERROR) << "NVMe KV backend already initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto init_res = InitDevices();
    if (!init_res) {
        initialized_.store(false, std::memory_order_release);
        return init_res;
    }
    total_size_.store(0, std::memory_order_relaxed);
    total_keys_.store(0, std::memory_order_relaxed);
    auto load_catalog_res = LoadCatalog();
    if (!load_catalog_res) {
        initialized_.store(false, std::memory_order_release);
        return load_catalog_res;
    }
    return {};
}

tl::expected<int64_t, ErrorCode> NvmeKvStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    NvmeKvStorageBackend::EvictionHandler eviction_handler) {
    (void)eviction_handler;
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    int64_t batch_total_size = 0;
    int64_t batch_total_keys = 0;
    for (const auto& [key, slices] : batch_object) {
        if (test_failure_predicate_ && test_failure_predicate_(key)) {
            continue;
        }
        if (slices.empty()) {
            continue;
        }
        size_t payload_size = 0;
        for (const auto& slice : slices) {
            payload_size += slice.size;
        }
        if (payload_size >
            static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        batch_total_size += static_cast<int64_t>(payload_size);
        batch_total_keys++;
    }

    if (batch_total_keys == 0) {
        return 0;
    }

    if (total_keys_.load(std::memory_order_relaxed) + batch_total_keys >
        file_storage_config_.total_keys_limit) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    if (total_size_.load(std::memory_order_relaxed) + batch_total_size >
        file_storage_config_.total_size_limit) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }

    auto enable_offloading_res = IsEnableOffloading();
    if (!enable_offloading_res) {
        return tl::make_unexpected(enable_offloading_res.error());
    }
    if (!enable_offloading_res.value()) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }

    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    keys.reserve(batch_object.size());
    metadatas.reserve(batch_object.size());

    for (const auto& [key, slices] : batch_object) {
        if (test_failure_predicate_ && test_failure_predicate_(key)) {
            continue;
        }
        if (slices.empty()) {
            continue;
        }

        std::string payload;
        size_t payload_size = 0;
        for (const auto& slice : slices) {
            payload_size += slice.size;
        }
        payload.reserve(payload_size);
        for (const auto& slice : slices) {
            payload.append(reinterpret_cast<const char*>(slice.ptr),
                           slice.size);
        }
        if (payload_size >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
            LOG(ERROR) << "Payload too large for key: " << key;
            continue;
        }

        auto candidate_device_ids = EnabledDeviceIds();
        if (candidate_device_ids.empty()) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }

        bool key_committed = false;
        bool saw_capacity_full = false;
        const auto erase_writing_entry = [&]() {
            SharedMutexLocker lock(&mutex_);
            auto it = catalog_.find(key);
            if (it != catalog_.end() &&
                it->second.state == ObjectState::WRITING) {
                catalog_.erase(it);
            }
        };
        const auto append_committed_metadata =
            [&](const std::string& committed_device_id,
                const std::string& committed_device_serial,
                const PhysicalKey& committed_physical_key,
                uint32_t committed_slot) {
                keys.push_back(key);
                metadatas.emplace_back(StorageObjectMetadata{
                    .bucket_id = 0,
                    .offset = 0,
                    .key_size = static_cast<int64_t>(key.size()),
                    .data_size = static_cast<int64_t>(payload_size),
                    .transport_endpoint = "",
                });
            };
        for (size_t retry_count = 0; retry_count < candidate_device_ids.size();
             ++retry_count) {
            const std::string device_id = candidate_device_ids[retry_count];
            auto connector = GetConnectorForDeviceId(device_id);
            if (connector == nullptr) {
                continue;
            }
            const bool use_conditional_store =
                connector->GetCapabilities().supports_conditional_store;

            for (uint32_t slot = 0; slot < kNvmeKvMaxPhysicalKeySlots; ++slot) {
                auto write_plan_res = NvmeKvKeyConflictPolicy::BuildWritePlan(
                    NvmeKvKeyConflictPolicy::BuildIdentity(key), payload, slot,
                    EffectiveMaxValueSize());
                if (!write_plan_res) {
                    return tl::make_unexpected(write_plan_res.error());
                }
                auto& write_plan = write_plan_res.value();
                const bool store_inline = write_plan.store_inline;
                const auto& physical_key = write_plan.root_key;
                const auto& root_blob = write_plan.root_blob;
                const auto& chunk_blobs = write_plan.chunk_blobs;

                std::vector<PhysicalKey> written_chunk_keys;
                bool root_written = false;
                const auto cleanup_new_writes = [&]() {
                    if (root_written) {
                        auto delete_res = connector->Delete(physical_key);
                        if (!delete_res &&
                            delete_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
                            LOG(WARNING)
                                << "NVMe KV failed to clean orphan root for "
                                << key << " on device " << device_id
                                << " key=" << PhysicalKeyToHex(physical_key)
                                << " error=" << toString(delete_res.error());
                        }
                        root_written = false;
                    }
                    if (written_chunk_keys.empty()) {
                        return;
                    }
                    for (const auto& chunk_key : written_chunk_keys) {
                        auto delete_res = connector->Delete(chunk_key);
                        if (!delete_res &&
                            delete_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
                            LOG(WARNING)
                                << "NVMe KV failed to clean orphan chunk for "
                                << key << " on device " << device_id
                                << " key=" << PhysicalKeyToHex(chunk_key)
                                << " error=" << toString(delete_res.error());
                        }
                    }
                    written_chunk_keys.clear();
                };
                const auto commit_existing =
                    [&]() -> tl::expected<void, ErrorCode> {
                    std::optional<CatalogEntry> previous_entry;
                    int64_t previous_total_size =
                        total_size_.load(std::memory_order_relaxed);
                    int64_t previous_total_keys =
                        total_keys_.load(std::memory_order_relaxed);
                    DeviceState previous_device_state = DeviceState::ENABLED;
                    uint32_t previous_consecutive_failures = 0;
                    int64_t previous_device_total_size = 0;
                    int64_t previous_device_total_keys = 0;
                    bool have_previous_device_runtime = false;
                    bool inserted_new_key = false;
                    {
                        SharedMutexLocker lock(&mutex_);
                        auto catalog_it = catalog_.find(key);
                        if (catalog_it != catalog_.end()) {
                            previous_entry = catalog_it->second;
                        }
                        auto device_it = devices_.find(device_id);
                        if (device_it != devices_.end()) {
                            have_previous_device_runtime = true;
                            previous_device_state = device_it->second.state;
                            previous_consecutive_failures =
                                device_it->second.consecutive_failures;
                            previous_device_total_size =
                                device_it->second.total_size;
                            previous_device_total_keys =
                                device_it->second.total_keys;
                        }
                        if (catalog_it == catalog_.end()) {
                            catalog_.emplace(
                                key, CatalogEntry{
                                         device_id, physical_key, slot,
                                         static_cast<uint32_t>(payload_size),
                                         ObjectState::COMMITTED});
                            total_keys_.fetch_add(1, std::memory_order_relaxed);
                            total_size_.fetch_add(
                                static_cast<uint32_t>(payload_size),
                                std::memory_order_relaxed);
                            inserted_new_key = true;
                        } else if (catalog_it->second.state !=
                                   ObjectState::COMMITTED) {
                            catalog_it->second.state = ObjectState::COMMITTED;
                            catalog_it->second.payload_size =
                                static_cast<uint32_t>(payload_size);
                            catalog_it->second.physical_key = physical_key;
                            catalog_it->second.physical_key_slot = slot;
                            catalog_it->second.device_id = device_id;
                        }
                    }
                    RecordDeviceSuccess(device_id,
                                        inserted_new_key
                                            ? static_cast<int64_t>(payload_size)
                                            : 0,
                                        inserted_new_key);
                    auto persist_res = PersistCatalog();
                    if (!persist_res) {
                        SharedMutexLocker lock(&mutex_);
                        if (previous_entry.has_value()) {
                            catalog_[key] = *previous_entry;
                        } else {
                            catalog_.erase(key);
                        }
                        if (have_previous_device_runtime) {
                            auto device_it = devices_.find(device_id);
                            if (device_it != devices_.end()) {
                                device_it->second.state = previous_device_state;
                                device_it->second.consecutive_failures =
                                    previous_consecutive_failures;
                                device_it->second.total_size =
                                    previous_device_total_size;
                                device_it->second.total_keys =
                                    previous_device_total_keys;
                            }
                        }
                        total_size_.store(previous_total_size,
                                          std::memory_order_relaxed);
                        total_keys_.store(previous_total_keys,
                                          std::memory_order_relaxed);
                        return tl::make_unexpected(persist_res.error());
                    }
                    append_committed_metadata(device_id, "", physical_key,
                                              slot);
                    key_committed = true;
                    return {};
                };

                const auto resolve_existing_root =
                    [&]() -> tl::expected<bool, ErrorCode> {
                    auto decision_res =
                        NvmeKvKeyConflictPolicy::ResolveExistingObject(
                            *connector, physical_key, root_blob);
                    if (!decision_res) {
                        return tl::make_unexpected(decision_res.error());
                    }
                    if (decision_res.value() ==
                        NvmeKvKeyConflictPolicy::ExistingObjectDecision::
                            kNotFound) {
                        return false;
                    }
                    if (decision_res.value() ==
                        NvmeKvKeyConflictPolicy::ExistingObjectDecision::
                            kDifferentObject) {
                        return false;
                    }
                    auto commit_res = commit_existing();
                    if (!commit_res) {
                        return tl::make_unexpected(commit_res.error());
                    }
                    return true;
                };

                if (!use_conditional_store) {
                    auto existing_res = resolve_existing_root();
                    if (!existing_res) {
                        RecordDeviceFailure(device_id);
                        if (retry_count + 1 == candidate_device_ids.size()) {
                            return tl::make_unexpected(existing_res.error());
                        }
                        break;
                    }
                    if (existing_res.value()) {
                        break;
                    }
                }

                {
                    SharedMutexLocker lock(&mutex_);
                    auto existing_it = catalog_.find(key);
                    if (existing_it != catalog_.end()) {
                        if (existing_it->second.state ==
                            ObjectState::COMMITTED) {
                            LOG(ERROR)
                                << "NVMe KV object already exists for key: "
                                << key;
                            key_committed = true;
                            break;
                        }
                        if (existing_it->second.state ==
                                ObjectState::DELETING ||
                            existing_it->second.state == ObjectState::DELETED ||
                            existing_it->second.state ==
                                ObjectState::CORRUPTED) {
                            catalog_.erase(existing_it);
                        }
                    }
                    catalog_[key] =
                        CatalogEntry{device_id, physical_key, slot,
                                     static_cast<uint32_t>(payload_size),
                                     ObjectState::WRITING};
                }

                bool write_success = true;
                bool slot_collision = false;
                ErrorCode write_error = ErrorCode::OK;
                if (!store_inline) {
                    auto root_existing_res = resolve_existing_root();
                    if (!root_existing_res) {
                        erase_writing_entry();
                        RecordDeviceFailure(device_id);
                        if (retry_count + 1 == candidate_device_ids.size()) {
                            return tl::make_unexpected(
                                root_existing_res.error());
                        }
                        break;
                    }
                    if (root_existing_res.value()) {
                        erase_writing_entry();
                        break;
                    }
                }

                if (store_inline) {
                    auto store_res =
                        connector->Store(physical_key, root_blob,
                                         NvmeKvCommandExecutor::StoreOptions(
                                             use_conditional_store));
                    if (!store_res) {
                        write_success = false;
                        write_error = store_res.error();
                    } else {
                        root_written = true;
                    }
                } else {
                    for (const auto& [chunk_key, chunk_blob] : chunk_blobs) {
                        auto chunk_res =
                            connector->Store(chunk_key, chunk_blob);
                        if (!chunk_res) {
                            if (chunk_res.error() ==
                                ErrorCode::OBJECT_ALREADY_EXISTS) {
                                auto existing_chunk_res =
                                    NvmeKvKeyConflictPolicy::
                                        ResolveExistingObject(
                                            *connector, chunk_key, chunk_blob);
                                if (existing_chunk_res &&
                                    existing_chunk_res.value() ==
                                        NvmeKvKeyConflictPolicy::
                                            ExistingObjectDecision::
                                                kSameObject) {
                                    continue;
                                }
                                if (!existing_chunk_res) {
                                    write_error = existing_chunk_res.error();
                                }
                                slot_collision = true;
                                write_success = false;
                                cleanup_new_writes();
                                break;
                            }
                            write_success = false;
                            write_error = chunk_res.error();
                            cleanup_new_writes();
                            break;
                        }
                        written_chunk_keys.push_back(chunk_key);
                    }
                    if (write_success) {
                        auto store_res = connector->Store(
                            physical_key, root_blob,
                            NvmeKvCommandExecutor::StoreOptions(
                                use_conditional_store));
                        if (!store_res) {
                            write_success = false;
                            write_error = store_res.error();
                            cleanup_new_writes();
                        } else {
                            root_written = true;
                        }
                    }
                }

                if (!write_success &&
                    write_error == ErrorCode::OBJECT_ALREADY_EXISTS) {
                    auto existing_res = resolve_existing_root();
                    if (existing_res && existing_res.value()) {
                        erase_writing_entry();
                        break;
                    }
                    if (!existing_res) {
                        write_error = existing_res.error();
                    } else {
                        slot_collision = true;
                        cleanup_new_writes();
                    }
                }

                if (!write_success) {
                    if (!slot_collision && !store_inline) {
                        cleanup_new_writes();
                    }
                    written_chunk_keys.clear();
                    erase_writing_entry();
                    if (slot_collision) {
                        continue;
                    }
                    if (write_error == ErrorCode::KEYS_ULTRA_LIMIT) {
                        saw_capacity_full = true;
                        RecordDeviceFull(device_id);
                    } else {
                        RecordDeviceFailure(device_id);
                    }
                    if (retry_count + 1 == candidate_device_ids.size()) {
                        return tl::make_unexpected(
                            saw_capacity_full ? ErrorCode::KEYS_ULTRA_LIMIT
                                              : write_error);
                    }
                    break;
                }

                DeviceState previous_device_state = DeviceState::ENABLED;
                uint32_t previous_consecutive_failures = 0;
                int64_t previous_device_total_size = 0;
                int64_t previous_device_total_keys = 0;
                bool have_previous_device_runtime = false;
                const int64_t previous_total_size =
                    total_size_.load(std::memory_order_relaxed);
                const int64_t previous_total_keys =
                    total_keys_.load(std::memory_order_relaxed);
                {
                    SharedMutexLocker lock(&mutex_);
                    auto device_it = devices_.find(device_id);
                    if (device_it != devices_.end()) {
                        have_previous_device_runtime = true;
                        previous_device_state = device_it->second.state;
                        previous_consecutive_failures =
                            device_it->second.consecutive_failures;
                        previous_device_total_size =
                            device_it->second.total_size;
                        previous_device_total_keys =
                            device_it->second.total_keys;
                    }
                    catalog_[key].state = ObjectState::COMMITTED;
                    total_keys_.fetch_add(1, std::memory_order_relaxed);
                    total_size_.fetch_add(static_cast<uint32_t>(payload_size),
                                          std::memory_order_relaxed);
                }
                RecordDeviceSuccess(device_id,
                                    static_cast<int64_t>(payload_size), true);
                auto persist_res = PersistCatalog();
                if (!persist_res) {
                    cleanup_new_writes();
                    SharedMutexLocker lock(&mutex_);
                    catalog_.erase(key);
                    if (have_previous_device_runtime) {
                        auto device_it = devices_.find(device_id);
                        if (device_it != devices_.end()) {
                            device_it->second.state = previous_device_state;
                            device_it->second.consecutive_failures =
                                previous_consecutive_failures;
                            device_it->second.total_size =
                                previous_device_total_size;
                            device_it->second.total_keys =
                                previous_device_total_keys;
                        }
                    }
                    total_size_.store(previous_total_size,
                                      std::memory_order_relaxed);
                    total_keys_.store(previous_total_keys,
                                      std::memory_order_relaxed);
                    return tl::make_unexpected(persist_res.error());
                }
                append_committed_metadata(device_id, "", physical_key, slot);
                key_committed = true;
                break;
            }

            if (key_committed) {
                break;
            }
        }
    }

    if (complete_handler != nullptr && !keys.empty()) {
        auto error_code = complete_handler(keys, metadatas);
        if (error_code != ErrorCode::OK) {
            return tl::make_unexpected(error_code);
        }
    }

    return static_cast<int64_t>(keys.size());
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    struct ReadPlan {
        std::string key;
        Slice dest_slice;
        std::string device_id;
        PhysicalKey physical_key;
        uint32_t physical_key_slot = 0;
        uint32_t expected_size;
    };
    std::vector<ReadPlan> plans;
    plans.reserve(batched_slices.size());

    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        for (const auto& [key, dest_slice] : batched_slices) {
            auto it = catalog_.find(key);
            if (it == catalog_.end() ||
                it->second.state != ObjectState::COMMITTED) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            if (dest_slice.size != it->second.payload_size) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            plans.push_back(ReadPlan{
                key, dest_slice, it->second.device_id, it->second.physical_key,
                it->second.physical_key_slot, it->second.payload_size});
        }
    }

    for (const auto& plan : plans) {
        auto connector = GetConnectorForDeviceId(plan.device_id);
        if (connector == nullptr) {
            RecordDeviceFailure(plan.device_id);
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        auto object_res =
            connector->Retrieve(plan.physical_key, plan.expected_size);
        if (!object_res) {
            RecordDeviceFailure(plan.device_id);
            return tl::make_unexpected(object_res.error());
        }
        const std::string& object_blob = object_res.value();
        ObjectHeader header{};
        std::string_view identity_metadata_view;
        std::string_view payload_view;
        if (!ParseNvmeKvObjectBlob(object_blob, header, identity_metadata_view,
                                   payload_view)) {
            MarkCorrupted(plan.key);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        NvmeKvStoredIdentityView stored_identity_view{};
        if (!ParseNvmeKvStoredIdentity(identity_metadata_view,
                                       stored_identity_view) ||
            stored_identity_view.logical_key.empty() ||
            stored_identity_view.logical_key != plan.key) {
            MarkCorrupted(plan.key);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        NvmeKvObjectIdentity identity =
            NvmeKvKeyConflictPolicy::BuildIdentityFromStoredView(
                stored_identity_view);
        if (test_corruption_predicate_ &&
            test_corruption_predicate_(plan.key)) {
            header.header_checksum ^= 0x1u;
        }
        if (!NvmeKvKeyConflictPolicy::ValidateResolvedRootPlacement(
                identity, stored_identity_view, plan.physical_key) ||
            stored_identity_view.resolved_slot != plan.physical_key_slot) {
            MarkCorrupted(plan.key);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        const auto object_type =
            static_cast<NvmeKvObjectType>(header.object_type);
        const uint32_t expected_identity_size =
            ComputeNvmeKvStoredIdentityMetadataSize(identity);
        if (object_type == NvmeKvObjectType::kInline) {
            if (!ValidateNvmeKvHeader(header, identity, expected_identity_size,
                                      plan.expected_size,
                                      NvmeKvObjectType::kInline) ||
                header.payload_checksum !=
                    ComputeNvmeKvPayloadChecksum(payload_view) ||
                payload_view.size() != plan.dest_slice.size) {
                MarkCorrupted(plan.key);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            std::memcpy(plan.dest_slice.ptr, payload_view.data(),
                        payload_view.size());
            RecordDeviceSuccess(plan.device_id, 0, false);
            continue;
        }

        if (object_type != NvmeKvObjectType::kManifest ||
            !ValidateNvmeKvHeader(header, identity, expected_identity_size,
                                  static_cast<uint32_t>(payload_view.size()),
                                  NvmeKvObjectType::kManifest) ||
            header.payload_checksum !=
                ComputeNvmeKvPayloadChecksum(payload_view)) {
            MarkCorrupted(plan.key);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        NvmeKvManifestMetadata metadata{};
        std::vector<NvmeKvManifestChunkRecord> chunk_records;
        if (!ParseNvmeKvManifest(payload_view, metadata, chunk_records) ||
            metadata.logical_payload_size != plan.expected_size ||
            metadata.chunk_count != chunk_records.size()) {
            MarkCorrupted(plan.key);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        size_t copied = 0;
        for (size_t chunk_index = 0; chunk_index < chunk_records.size();
             ++chunk_index) {
            const auto& chunk_record = chunk_records[chunk_index];
            if (chunk_record.physical_key !=
                EncodeNvmeKvChunkPhysicalKey(
                    identity, static_cast<uint32_t>(chunk_index),
                    stored_identity_view.resolved_slot)) {
                MarkCorrupted(plan.key);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            auto chunk_res = connector->Retrieve(chunk_record.physical_key);
            if (!chunk_res) {
                RecordDeviceFailure(plan.device_id);
                return tl::make_unexpected(chunk_res.error());
            }
            ObjectHeader chunk_header{};
            std::string_view chunk_identity_metadata_view;
            std::string_view chunk_payload;
            if (!ParseNvmeKvObjectBlob(chunk_res.value(), chunk_header,
                                       chunk_identity_metadata_view,
                                       chunk_payload)) {
                MarkCorrupted(plan.key);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            NvmeKvStoredIdentityView chunk_identity_view{};
            if (!ParseNvmeKvStoredIdentity(chunk_identity_metadata_view,
                                           chunk_identity_view)) {
                MarkCorrupted(plan.key);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            auto chunk_identity =
                NvmeKvKeyConflictPolicy::BuildIdentityFromStoredView(
                    chunk_identity_view);
            if (chunk_identity_view.resolved_slot !=
                    stored_identity_view.resolved_slot ||
                chunk_identity_view.resolved_physical_key !=
                    chunk_record.physical_key ||
                EncodeNvmeKvChunkPhysicalKey(
                    chunk_identity, static_cast<uint32_t>(chunk_index),
                    chunk_identity_view.resolved_slot) !=
                    chunk_record.physical_key ||
                chunk_header.payload_checksum !=
                    chunk_record.payload_checksum ||
                chunk_header.payload_checksum !=
                    ComputeNvmeKvPayloadChecksum(chunk_payload) ||
                chunk_payload.size() != chunk_record.payload_size ||
                copied + chunk_payload.size() > plan.dest_slice.size ||
                !ValidateNvmeKvHeader(
                    chunk_header, chunk_identity,
                    ComputeNvmeKvStoredIdentityMetadataSize(chunk_identity),
                    chunk_record.payload_size, NvmeKvObjectType::kChunk)) {
                MarkCorrupted(plan.key);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            std::memcpy(static_cast<char*>(plan.dest_slice.ptr) + copied,
                        chunk_payload.data(), chunk_payload.size());
            copied += chunk_payload.size();
        }

        if (copied != plan.dest_slice.size) {
            MarkCorrupted(plan.key);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        RecordDeviceSuccess(plan.device_id, 0, false);
    }

    return {};
}

tl::expected<bool, ErrorCode> NvmeKvStorageBackend::IsExist(
    const std::string& key) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto it = catalog_.find(key);
    return it != catalog_.end() && it->second.state == ObjectState::COMMITTED;
}

tl::expected<bool, ErrorCode> NvmeKvStorageBackend::IsEnableOffloading() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const auto candidate_device_ids = EnabledDeviceIds();
    if (candidate_device_ids.empty()) {
        SharedMutexLocker lock(&mutex_, shared_lock);
        for (const auto& [_, runtime] : devices_) {
            if (runtime.state == DeviceState::DISABLED_BY_CAPACITY) {
                return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
            }
        }
        return false;
    }
    return total_keys_.load(std::memory_order_relaxed) <
               file_storage_config_.total_keys_limit &&
           total_size_.load(std::memory_order_relaxed) <
               file_storage_config_.total_size_limit;
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        keys.reserve(catalog_.size());
        metadatas.reserve(catalog_.size());
        for (const auto& [key, entry] : catalog_) {
            if (entry.state != ObjectState::COMMITTED) {
                continue;
            }
            auto connector = GetConnectorForDeviceId(entry.device_id);
            const std::string device_serial =
                connector == nullptr || connector->GetMetadata().uuid.empty()
                    ? entry.device_id
                    : connector->GetMetadata().uuid;
            keys.push_back(key);
            metadatas.emplace_back(StorageObjectMetadata{
                .bucket_id = 0,
                .offset = 0,
                .key_size = static_cast<int64_t>(key.size()),
                .data_size = static_cast<int64_t>(entry.payload_size),
                .transport_endpoint = "",
            });
        }
    }

    if (!keys.empty()) {
        auto error_code = handler(keys, metadatas);
        if (error_code != ErrorCode::OK) {
            return tl::make_unexpected(error_code);
        }
    }
    return {};
}

}  // namespace mooncake
