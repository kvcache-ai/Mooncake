#include "nvme_kv_backend.h"

#include "utils.h"
#include "utils/file_util.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <limits>
#include <ranges>
#include <sstream>
#include <string_view>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace mooncake {

namespace {

constexpr char kDefaultDeviceId[] = "default";
// Minimal failure isolation: avoid disabling a device on one transient command
// error, but stop selecting it after repeated failures.
constexpr uint32_t kDeviceFailureThreshold = 3;
constexpr std::string_view kCatalogFormatPrefix = "v2";

std::string EncodeCatalogField(std::string_view value) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned char ch : value) {
        oss << std::setw(2) << static_cast<int>(ch);
    }
    return oss.str();
}

bool DecodeCatalogField(std::string_view encoded, std::string& value) {
    if (encoded.size() % 2 != 0) {
        return false;
    }
    value.clear();
    value.reserve(encoded.size() / 2);
    const auto hex_value = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') {
            return ch - '0';
        }
        if (ch >= 'a' && ch <= 'f') {
            return ch - 'a' + 10;
        }
        if (ch >= 'A' && ch <= 'F') {
            return ch - 'A' + 10;
        }
        return -1;
    };
    for (size_t i = 0; i < encoded.size(); i += 2) {
        const int high = hex_value(encoded[i]);
        const int low = hex_value(encoded[i + 1]);
        if (high < 0 || low < 0) {
            return false;
        }
        value.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

bool IsSafeDeviceId(const std::string& device_id) {
    return !device_id.empty() &&
           std::ranges::all_of(device_id, [](unsigned char ch) {
               return std::isalnum(ch) || ch == '_';
           });
}

std::vector<std::string> ParseConfiguredDeviceIds() {
    const auto configured = GetEnvStringOr("MOONCAKE_NVME_KV_DEVICE_IDS", "");
    if (configured.empty()) {
        return {kDefaultDeviceId};
    }

    const auto device_ids = splitString(configured, ',', true, false);
    if (device_ids.empty()) {
        return {};
    }
    if (!std::ranges::all_of(device_ids, IsSafeDeviceId)) {
        return {};
    }
    return device_ids;
}

uint32_t ParseConfiguredNsid() {
    return GetEnvOr<uint32_t>("MOONCAKE_NVME_KV_NSID", 1);
}

std::string ResolveConfiguredDevicePath(const std::string& device_id) {
    const auto per_device_path = GetEnvStringOr(
        ("MOONCAKE_NVME_KV_DEVICE_PATH_" + device_id).c_str(), "");
    if (!per_device_path.empty()) {
        return per_device_path;
    }
    return GetEnvStringOr("MOONCAKE_NVME_KV_DEVICE_PATH", "");
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
    auto configured_device_ids = ParseConfiguredDeviceIds();
    if (configured_device_ids.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint32_t nsid = ParseConfiguredNsid();
    const auto executor_type =
        GetEnvStringOr("MOONCAKE_NVME_KV_EXECUTOR_TYPE", "ioctl");
    std::sort(configured_device_ids.begin(), configured_device_ids.end());
    configured_device_ids.erase(
        std::unique(configured_device_ids.begin(), configured_device_ids.end()),
        configured_device_ids.end());

    std::unordered_map<std::string, NvmeKvDeviceRuntime> devices;
    devices.reserve(configured_device_ids.size());
    for (const auto& device_id : configured_device_ids) {
        auto device_path = ResolveConfiguredDevicePath(device_id);
        if (device_path.empty()) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>
            capabilities = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        auto executor = CreateNvmeKvExecutor(executor_type, device_path, nsid,
                                             capabilities);
        if (executor == nullptr || !capabilities) {
            return tl::make_unexpected(capabilities ? ErrorCode::INTERNAL_ERROR
                                                    : capabilities.error());
        }
        auto connector =
            std::make_shared<NvmeKvConnector>(device_id, std::move(executor));
        devices.emplace(device_id,
                        NvmeKvDeviceRuntime{device_id, std::move(connector),
                                            DeviceState::ENABLED});
    }

    SharedMutexLocker lock(&mutex_);
    devices_ = std::move(devices);
    return {};
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::LoadCatalog() {
    std::ifstream in(CatalogPath());
    if (!in) {
        return {};
    }

    std::unordered_map<std::string, CatalogEntry> loaded_catalog;
    int64_t total_size = 0;
    int64_t total_keys = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::istringstream iss(line);
        std::string format;
        std::string encoded_key;
        std::string encoded_device_id;
        std::string key;
        std::string device_id;
        std::string physical_key_hex;
        uint32_t payload_size = 0;
        int state_int = 0;
        if (!std::getline(iss, format, '\t')) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (format == kCatalogFormatPrefix) {
            if (!std::getline(iss, encoded_key, '\t') ||
                !std::getline(iss, encoded_device_id, '\t') ||
                !std::getline(iss, physical_key_hex, '\t') ||
                !(iss >> payload_size >> state_int) ||
                !DecodeCatalogField(encoded_key, key) ||
                !DecodeCatalogField(encoded_device_id, device_id)) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
        } else {
            key = std::move(format);
            if (!std::getline(iss, device_id, '\t') ||
                key.find_first_of("\t\n\r") != std::string::npos) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            if (device_id.size() == 32) {
                physical_key_hex = device_id;
                device_id = kDefaultDeviceId;
                if (!(iss >> payload_size >> state_int)) {
                    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
            } else if (!std::getline(iss, physical_key_hex, '\t') ||
                       !(iss >> payload_size >> state_int)) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
        }

        PhysicalKey physical_key{};
        if (!ParseNvmeKvPhysicalKeyHex(physical_key_hex, physical_key)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        auto state = static_cast<ObjectState>(state_int);
        loaded_catalog.emplace(
            key, CatalogEntry{device_id, physical_key, payload_size, state});
        if (state == ObjectState::COMMITTED) {
            total_size += payload_size;
            total_keys += 1;
        }
    }

    SharedMutexLocker lock(&mutex_);
    catalog_ = std::move(loaded_catalog);
    total_size_.store(total_size, std::memory_order_relaxed);
    total_keys_.store(total_keys, std::memory_order_relaxed);
    return {};
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::PersistCatalog() const {
    std::vector<std::tuple<std::string, std::string, PhysicalKey, uint32_t,
                           ObjectState>>
        entries;
    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        entries.reserve(catalog_.size());
        for (const auto& [key, entry] : catalog_) {
            entries.emplace_back(key, entry.device_id, entry.physical_key,
                                 entry.payload_size, entry.state);
        }
    }

    const auto catalog_path = CatalogPath();
    const auto ensure_dir_res =
        FileUtil::EnsureDirExists(catalog_path.parent_path().string());
    if (!ensure_dir_res) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    std::ostringstream content;
    for (const auto& [key, device_id, physical_key, payload_size, state] :
         entries) {
        content << kCatalogFormatPrefix << '\t' << EncodeCatalogField(key)
                << '\t' << EncodeCatalogField(device_id) << '\t'
                << NvmeKvPhysicalKeyToHex(physical_key) << '\t' << payload_size
                << ' ' << static_cast<int>(state) << '\n';
    }

    const auto tmp_path = catalog_path.string() + ".tmp";
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    const std::string serialized = content.str();
    const char* ptr = serialized.data();
    size_t remaining = serialized.size();
    while (remaining > 0) {
        const ssize_t written = ::write(fd, ptr, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            ::close(fd);
            ::unlink(tmp_path.c_str());
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        ptr += written;
        remaining -= static_cast<size_t>(written);
    }

    if (::fsync(fd) != 0) {
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (::close(fd) != 0) {
        ::unlink(tmp_path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    std::error_code ec;
    std::filesystem::rename(tmp_path, catalog_path, ec);
    if (ec) {
        ::unlink(tmp_path.c_str());
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    const std::string parent_dir = catalog_path.parent_path().string();
    const int dir_fd = ::open(parent_dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
        ::fsync(dir_fd);
        ::close(dir_fd);
    }
    return {};
}

uint32_t NvmeKvStorageBackend::EffectiveMaxValueSize() const {
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
        return 0;
    }
    return min_max_value_size;
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

StorageObjectMetadata BuildStorageObjectMetadata(std::string_view key,
                                                 uint32_t payload_size) {
    return StorageObjectMetadata{0, 0, static_cast<int64_t>(key.size()),
                                 static_cast<int64_t>(payload_size), ""};
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

std::shared_ptr<NvmeKvConnector> NvmeKvStorageBackend::GetConnectorForDeviceId(
    const std::string& device_id) const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return nullptr;
    }
    return it->second.connector;
}

void NvmeKvStorageBackend::RecordDeviceFailure(const std::string& device_id) {
    SharedMutexLocker lock(&mutex_);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return;
    }
    auto& runtime = it->second;
    runtime.consecutive_failures += 1;
    if (runtime.consecutive_failures >= kDeviceFailureThreshold) {
        runtime.state = DeviceState::DISABLED_BY_FAILURE;
    }
}

void NvmeKvStorageBackend::RecordDeviceSuccess(const std::string& device_id,
                                               int64_t payload_size,
                                               bool new_key_committed) {
    (void)payload_size;
    (void)new_key_committed;
    SharedMutexLocker lock(&mutex_);
    auto it = devices_.find(device_id);
    if (it == devices_.end()) {
        return;
    }
    auto& runtime = it->second;
    if (runtime.state == DeviceState::ENABLED) {
        runtime.consecutive_failures = 0;
    }
}

void NvmeKvStorageBackend::ReleaseReservation(int64_t payload_size,
                                              int64_t key_count) {
    SharedMutexLocker lock(&mutex_);
    reserved_size_ -= payload_size;
    reserved_keys_ -= key_count;
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
    {
        SharedMutexLocker lock(&mutex_);
        reserved_size_ = 0;
        reserved_keys_ = 0;
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
    std::function<void(const std::vector<std::string>& evicted_keys)>
        eviction_handler) {
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
            if (slice.size > 0 && slice.ptr == nullptr) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
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

    {
        SharedMutexLocker lock(&mutex_);
        if (total_keys_.load(std::memory_order_relaxed) + reserved_keys_ +
                batch_total_keys >
            file_storage_config_.total_keys_limit) {
            return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
        }
        if (total_size_.load(std::memory_order_relaxed) + reserved_size_ +
                batch_total_size >
            file_storage_config_.total_size_limit) {
            return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
        }
        reserved_keys_ += batch_total_keys;
        reserved_size_ += batch_total_size;
    }
    int64_t remaining_reserved_size = batch_total_size;
    int64_t remaining_reserved_keys = batch_total_keys;
    auto release_reservation = [this, &remaining_reserved_size,
                                &remaining_reserved_keys] {
        if (remaining_reserved_size != 0 || remaining_reserved_keys != 0) {
            ReleaseReservation(remaining_reserved_size,
                               remaining_reserved_keys);
        }
    };
    struct ReservationGuard {
        decltype(release_reservation)& release;
        ~ReservationGuard() { release(); }
    } reservation_guard{release_reservation};

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
            if (slice.size > 0 && slice.ptr == nullptr) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            payload_size += slice.size;
        }
        payload.resize(payload_size);
        size_t write_offset = 0;
        for (const auto& slice : slices) {
            if (slice.size == 0) {
                continue;
            }
            std::memcpy(payload.data() + write_offset, slice.ptr, slice.size);
            write_offset += slice.size;
        }

        if (payload_size >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const auto physical_key = EncodeNvmeKvPhysicalKey(key);
        const auto verify_hash = ComputeNvmeKvVerifyHash(key);
        std::string root_value;
        const bool store_inline = ShouldStoreInline(payload_size);

        if (store_inline) {
            ObjectHeader header{
                .magic = ObjectHeader::kMagic,
                .version = ObjectHeader::kVersion,
                .object_type = static_cast<uint32_t>(NvmeKvObjectType::kInline),
                .payload_size = static_cast<uint32_t>(payload_size),
                .verify_hash = verify_hash,
                .payload_checksum = ComputeNvmeKvPayloadChecksum(payload),
                .header_checksum = 0,
            };
            header.header_checksum = ComputeNvmeKvHeaderChecksum(header);
            root_value = BuildNvmeKvObjectValue(header, payload);
        } else {
            const uint32_t chunk_payload_limit = ChunkPayloadLimit();
            if (chunk_payload_limit == 0) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            const size_t chunk_count =
                (payload_size + chunk_payload_limit - 1) / chunk_payload_limit;
            if (chunk_count == 0 ||
                chunk_count >
                    static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            std::vector<NvmeKvManifestChunkRecord> manifest_records;
            manifest_records.reserve(chunk_count);
            for (size_t chunk_index = 0; chunk_index < chunk_count;
                 ++chunk_index) {
                const size_t offset = chunk_index * chunk_payload_limit;
                const size_t chunk_size =
                    std::min(static_cast<size_t>(chunk_payload_limit),
                             payload_size - offset);
                std::string_view chunk_payload(payload.data() + offset,
                                               chunk_size);
                const auto chunk_key =
                    EncodeNvmeKvChunkPhysicalKey(key, chunk_index);
                manifest_records.push_back(NvmeKvManifestChunkRecord{
                    chunk_key, static_cast<uint32_t>(chunk_size),
                    ComputeNvmeKvPayloadChecksum(chunk_payload)});
            }

            const NvmeKvManifestMetadata metadata{
                .logical_payload_size = static_cast<uint32_t>(payload_size),
                .chunk_count = static_cast<uint32_t>(manifest_records.size()),
            };
            const std::string manifest_payload =
                SerializeNvmeKvManifest(metadata, manifest_records);
            if (manifest_payload.size() + sizeof(ObjectHeader) >
                EffectiveMaxValueSize()) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            ObjectHeader manifest_header{
                .magic = ObjectHeader::kMagic,
                .version = ObjectHeader::kVersion,
                .object_type =
                    static_cast<uint32_t>(NvmeKvObjectType::kManifest),
                .payload_size = static_cast<uint32_t>(manifest_payload.size()),
                .verify_hash = verify_hash,
                .payload_checksum =
                    ComputeNvmeKvPayloadChecksum(manifest_payload),
                .header_checksum = 0,
            };
            manifest_header.header_checksum =
                ComputeNvmeKvHeaderChecksum(manifest_header);
            root_value =
                BuildNvmeKvObjectValue(manifest_header, manifest_payload);
        }

        const auto enabled_device_ids = EnabledDeviceIds();
        if (enabled_device_ids.empty()) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }

        bool key_committed = false;
        for (size_t retry_count = 0; retry_count < enabled_device_ids.size();
             ++retry_count) {
            const size_t index = (std::hash<std::string>{}(key) + retry_count) %
                                 enabled_device_ids.size();
            const std::string& device_id = enabled_device_ids[index];
            auto connector = GetConnectorForDeviceId(device_id);
            if (connector == nullptr) {
                continue;
            }

            auto existing_value_res = connector->Retrieve(physical_key);
            if (existing_value_res) {
                if (existing_value_res.value() == root_value) {
                    bool inserted_new_key = false;
                    {
                        SharedMutexLocker lock(&mutex_);
                        auto catalog_it = catalog_.find(key);
                        if (catalog_it == catalog_.end()) {
                            catalog_.emplace(
                                key, CatalogEntry{
                                         device_id, physical_key,
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
                        return tl::make_unexpected(persist_res.error());
                    }
                    keys.push_back(key);
                    metadatas.emplace_back(BuildStorageObjectMetadata(
                        key, static_cast<uint32_t>(payload_size)));
                    key_committed = true;
                    break;
                }
                LOG(ERROR) << "NVMe KV hash collision for key: " << key;
                break;
            }
            if (existing_value_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
                RecordDeviceFailure(device_id);
                continue;
            }

            {
                SharedMutexLocker lock(&mutex_);
                auto existing_it = catalog_.find(key);
                if (existing_it != catalog_.end()) {
                    if (existing_it->second.state == ObjectState::COMMITTED) {
                        LOG(ERROR)
                            << "NVMe KV object already exists for key: " << key;
                        key_committed = true;
                        break;
                    }
                    if (existing_it->second.state == ObjectState::WRITING) {
                        break;
                    }
                    if (existing_it->second.state == ObjectState::CORRUPTED) {
                        catalog_.erase(existing_it);
                    }
                }

                catalog_[key] = CatalogEntry{
                    device_id, physical_key,
                    static_cast<uint32_t>(payload_size), ObjectState::WRITING};
            }

            bool write_success = true;
            ErrorCode write_error = ErrorCode::OK;
            if (store_inline) {
                auto store_res = connector->Store(physical_key, root_value);
                if (!store_res) {
                    write_success = false;
                    write_error = store_res.error();
                }
            } else {
                const uint32_t chunk_payload_limit = ChunkPayloadLimit();
                const size_t chunk_count =
                    (payload_size + chunk_payload_limit - 1) /
                    chunk_payload_limit;
                for (size_t chunk_index = 0; chunk_index < chunk_count;
                     ++chunk_index) {
                    const size_t offset = chunk_index * chunk_payload_limit;
                    const size_t chunk_size =
                        std::min(static_cast<size_t>(chunk_payload_limit),
                                 payload_size - offset);
                    std::string_view chunk_payload(payload.data() + offset,
                                                   chunk_size);
                    ObjectHeader chunk_header{
                        .magic = ObjectHeader::kMagic,
                        .version = ObjectHeader::kVersion,
                        .object_type =
                            static_cast<uint32_t>(NvmeKvObjectType::kChunk),
                        .payload_size = static_cast<uint32_t>(chunk_size),
                        .verify_hash = verify_hash,
                        .payload_checksum =
                            ComputeNvmeKvPayloadChecksum(chunk_payload),
                        .header_checksum = 0,
                    };
                    chunk_header.header_checksum =
                        ComputeNvmeKvHeaderChecksum(chunk_header);
                    auto chunk_value =
                        BuildNvmeKvObjectValue(chunk_header, chunk_payload);
                    auto chunk_res = connector->Store(
                        EncodeNvmeKvChunkPhysicalKey(key, chunk_index),
                        std::move(chunk_value));
                    if (!chunk_res) {
                        write_success = false;
                        write_error = chunk_res.error();
                        break;
                    }
                }

                if (write_success) {
                    auto store_res = connector->Store(physical_key, root_value);
                    if (!store_res) {
                        write_success = false;
                        write_error = store_res.error();
                    }
                }
            }

            if (!write_success) {
                {
                    SharedMutexLocker lock(&mutex_);
                    auto it = catalog_.find(key);
                    if (it != catalog_.end() &&
                        it->second.state == ObjectState::WRITING) {
                        catalog_.erase(it);
                    }
                }
                RecordDeviceFailure(device_id);
                if (retry_count + 1 == enabled_device_ids.size()) {
                    return tl::make_unexpected(write_error);
                }
                continue;
            }

            {
                SharedMutexLocker lock(&mutex_);
                catalog_[key].state = ObjectState::COMMITTED;
                total_keys_.fetch_add(1, std::memory_order_relaxed);
                total_size_.fetch_add(static_cast<uint32_t>(payload_size),
                                      std::memory_order_relaxed);
            }
            RecordDeviceSuccess(device_id, static_cast<int64_t>(payload_size),
                                true);
            auto persist_res = PersistCatalog();
            if (!persist_res) {
                return tl::make_unexpected(persist_res.error());
            }

            keys.push_back(key);
            metadatas.emplace_back(BuildStorageObjectMetadata(
                key, static_cast<uint32_t>(payload_size)));
            key_committed = true;
            break;
        }

        remaining_reserved_size -= static_cast<int64_t>(payload_size);
        remaining_reserved_keys -= 1;
        if (!key_committed) {
            continue;
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
            if (dest_slice.size != it->second.payload_size ||
                (dest_slice.size > 0 && dest_slice.ptr == nullptr)) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            plans.push_back(ReadPlan{key, dest_slice, it->second.device_id,
                                     it->second.physical_key,
                                     it->second.payload_size});
        }
    }

    for (const auto& plan : plans) {
        auto connector = GetConnectorForDeviceId(plan.device_id);
        if (connector == nullptr) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        auto object_res = connector->Retrieve(plan.physical_key);
        if (!object_res) {
            return tl::make_unexpected(object_res.error());
        }
        const std::string& object_value = object_res.value();
        ObjectHeader header{};
        std::string_view payload_view;
        if (!ParseNvmeKvObjectValue(object_value, header, payload_view)) {
            MarkCorrupted(plan.key);
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        const auto object_type =
            static_cast<NvmeKvObjectType>(header.object_type);
        if (object_type == NvmeKvObjectType::kInline) {
            if (!ValidateNvmeKvHeader(header, plan.key, plan.expected_size,
                                      NvmeKvObjectType::kInline)) {
                MarkCorrupted(plan.key);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            if (header.payload_checksum !=
                    ComputeNvmeKvPayloadChecksum(payload_view) ||
                payload_view.size() != plan.dest_slice.size) {
                MarkCorrupted(plan.key);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            std::memcpy(plan.dest_slice.ptr, payload_view.data(),
                        payload_view.size());
            continue;
        }

        if (object_type != NvmeKvObjectType::kManifest ||
            !ValidateNvmeKvHeader(header, plan.key,
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
        for (const auto& chunk_record : chunk_records) {
            auto chunk_res = connector->Retrieve(chunk_record.physical_key);
            if (!chunk_res) {
                return tl::make_unexpected(chunk_res.error());
            }
            ObjectHeader chunk_header{};
            std::string_view chunk_payload;
            if (!ParseNvmeKvObjectValue(chunk_res.value(), chunk_header,
                                        chunk_payload) ||
                !ValidateNvmeKvHeader(chunk_header, plan.key,
                                      chunk_record.payload_size,
                                      NvmeKvObjectType::kChunk) ||
                chunk_header.payload_checksum !=
                    chunk_record.payload_checksum ||
                chunk_header.payload_checksum !=
                    ComputeNvmeKvPayloadChecksum(chunk_payload) ||
                chunk_payload.size() != chunk_record.payload_size ||
                copied + chunk_payload.size() > plan.dest_slice.size) {
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
    return !EnabledDeviceIds().empty() &&
           total_keys_.load(std::memory_order_relaxed) <
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
            keys.push_back(key);
            metadatas.emplace_back(
                BuildStorageObjectMetadata(key, entry.payload_size));
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
