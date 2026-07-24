#include "nvme_kv_connector.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <utility>

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <glog/logging.h>

#include "nvme_kv_executor.h"
#include "storage_backend.h"

namespace mooncake {
namespace {

using DiscoveredDeviceIdentity = NvmeKvConnector::DiscoveredDeviceIdentity;
using DeviceIdentityEnumerator = NvmeKvConnector::DeviceIdentityEnumerator;
using DeviceSelectorType = NvmeKvConnector::DeviceSelectorType;
using ResolvedDeviceSelector = NvmeKvConnector::ResolvedDeviceSelector;

DeviceIdentityEnumerator& TestDeviceIdentityEnumerator() {
    static DeviceIdentityEnumerator enumerator;
    return enumerator;
}

uint32_t ParseU32EnvOr(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    char* end = nullptr;
    errno = 0;
    unsigned long parsed = std::strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed > UINT32_MAX) {
        return fallback;
    }
    return static_cast<uint32_t>(parsed);
}

std::string GetEnvStringOrEmpty(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string() : std::string(value);
}

enum class RuntimeTransport {
    kIoUring,
    kIoctl,
    kLibnvme,
};

tl::expected<RuntimeTransport, ErrorCode> ParseRuntimeTransport() {
    const auto transport = GetEnvStringOrEmpty("MOONCAKE_NVME_KV_TRANSPORT");
    if (transport.empty()) {
#ifdef MOONCAKE_HAVE_NVME_URING_CMD
        return RuntimeTransport::kIoUring;
#else
        return RuntimeTransport::kIoctl;
#endif
    }
    if (transport == "io_uring") {
        return RuntimeTransport::kIoUring;
    }
    if (transport == "ioctl") {
        return RuntimeTransport::kIoctl;
    }
    if (transport == "libnvme") {
        return RuntimeTransport::kLibnvme;
    }
    LOG(ERROR) << "Unknown MOONCAKE_NVME_KV_TRANSPORT: " << transport;
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

std::string TrimString(std::string value) {
    auto not_space = [](unsigned char c) { return !std::isspace(c); };
    value.erase(value.begin(),
                std::find_if(value.begin(), value.end(), not_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(),
                value.end());
    return value;
}

std::string NormalizeStableId(std::string value) {
    value = TrimString(std::move(value));
    std::string normalized;
    normalized.reserve(value.size());
    for (unsigned char c : value) {
        if (c == '-' || c == '_' || c == ':') {
            continue;
        }
        normalized.push_back(static_cast<char>(std::tolower(c)));
    }
    return normalized;
}

bool LooksLikeDevicePath(const std::string& value) {
    return value.rfind("/dev/", 0) == 0;
}

std::string ReadTrimmedFile(const std::filesystem::path& path) {
    std::ifstream in(path);
    if (!in.is_open()) {
        return {};
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return TrimString(ss.str());
}

int64_t QueryFilesystemCapacityBytes(const std::filesystem::path& path) {
    std::error_code ec;
    const auto space_info = std::filesystem::space(path, ec);
    if (ec || space_info.capacity >
                  static_cast<uintmax_t>(std::numeric_limits<int64_t>::max())) {
        return 0;
    }
    return static_cast<int64_t>(space_info.capacity);
}

int64_t QueryBlockDeviceCapacityBytes(const std::string& device_path) {
    const int fd = ::open(device_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return 0;
    }
    uint64_t bytes = 0;
    const int ret = ::ioctl(fd, BLKGETSIZE64, &bytes);
    ::close(fd);
    if (ret != 0 ||
        bytes > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return 0;
    }
    return static_cast<int64_t>(bytes);
}

std::vector<DiscoveredDeviceIdentity> EnumerateSystemNvmeDeviceIdentities() {
    std::vector<DiscoveredDeviceIdentity> devices;
    std::error_code ec;
    for (const auto& entry :
         std::filesystem::directory_iterator("/sys/block", ec)) {
        if (ec) {
            break;
        }
        const auto name = entry.path().filename().string();
        if (name.rfind("nvme", 0) != 0 || name.find('n') == std::string::npos) {
            continue;
        }
        DiscoveredDeviceIdentity identity;
        identity.device_path = "/dev/" + name;
        const auto device_dir = entry.path() / "device";
        for (const char* file_name :
             {"serial", "uuid", "nguid", "eui", "wwid"}) {
            auto value = ReadTrimmedFile(device_dir / file_name);
            if (!value.empty()) {
                identity.stable_ids.push_back(std::move(value));
            }
        }
        if (!identity.stable_ids.empty()) {
            devices.push_back(std::move(identity));
        }
    }
    return devices;
}

std::vector<DiscoveredDeviceIdentity> EnumerateDeviceIdentities() {
    auto& test_enumerator = TestDeviceIdentityEnumerator();
    if (test_enumerator) {
        return test_enumerator();
    }
    return EnumerateSystemNvmeDeviceIdentities();
}

std::optional<std::string> FindStableIdForDevicePath(
    const std::string& device_path) {
    for (const auto& device : EnumerateDeviceIdentities()) {
        if (device.device_path != device_path) {
            continue;
        }
        for (const auto& stable_id : device.stable_ids) {
            if (!NormalizeStableId(stable_id).empty()) {
                return stable_id;
            }
        }
    }
    return std::nullopt;
}

std::optional<std::string> ResolveDevicePathFromStableId(
    const std::string& configured_selector) {
    const auto normalized_selector = NormalizeStableId(configured_selector);
    if (normalized_selector.empty()) {
        return std::nullopt;
    }
    for (const auto& device : EnumerateDeviceIdentities()) {
        for (const auto& stable_id : device.stable_ids) {
            if (NormalizeStableId(stable_id) == normalized_selector) {
                return device.device_path;
            }
        }
    }
    return std::nullopt;
}

std::optional<std::string> ParseDeviceMapSelector(
    const std::string& device_id) {
    const auto mapping = GetEnvStringOrEmpty("MOONCAKE_NVME_KV_DEVICE_MAP");
    if (mapping.empty()) {
        return std::nullopt;
    }
    std::stringstream ss(mapping);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto pos = item.find(':');
        if (pos == std::string::npos) {
            continue;
        }
        if (TrimString(item.substr(0, pos)) == device_id) {
            auto selector = TrimString(item.substr(pos + 1));
            if (!selector.empty()) {
                return selector;
            }
        }
    }
    return std::nullopt;
}

}  // namespace

NvmeKvConnector::NvmeKvConnector(const FileStorageConfig& file_storage_config,
                                 std::string device_id)
    : storage_root_(file_storage_config.storage_filepath),
      device_id_(std::move(device_id)),
      storage_path_(std::filesystem::path(storage_root_) / "nvme_kv_blobs" /
                    device_id_) {}

tl::expected<void, ErrorCode> NvmeKvConnector::Init() {
    if (executor_ != nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto real_res = InitRealExecutor();
    if (real_res) {
        return {};
    }
    if (!ShouldUseStubFallback()) {
        if (metadata_.last_error.empty()) {
            metadata_.last_error = "real executor init failed";
        }
        metadata_.health_state = "error";
        return real_res;
    }
    LOG(WARNING) << "Falling back to stub NVMe KV executor for device "
                 << device_id_;
    return InitStubExecutor();
}

tl::expected<void, ErrorCode> NvmeKvConnector::InitRealExecutor() {
    auto resolved_selector = ResolveDeviceSelector();
    if (!resolved_selector) {
        if (metadata_.last_error.empty()) {
            metadata_.last_error = "failed to resolve NVMe KV device selector";
        }
        return tl::make_unexpected(resolved_selector.error());
    }

    metadata_.device_path = resolved_selector->resolved_device_path;
    metadata_.uuid = resolved_selector->resolved_uuid;
    metadata_.nsid = ParseU32EnvOr("MOONCAKE_NVME_KV_NSID", 1);
    metadata_.queue_depth = ParseU32EnvOr("MOONCAKE_NVME_KV_QUEUE_DEPTH", 32);
    metadata_.capacity_total =
        QueryBlockDeviceCapacityBytes(metadata_.device_path);

    tl::expected<Capabilities, ErrorCode> capabilities =
        tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    const auto runtime_transfer_limit =
        ParseU32EnvOr("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT", 128 * 1024);
    const auto transport = ParseRuntimeTransport();
    if (!transport) {
        return tl::make_unexpected(transport.error());
    }

    std::unique_ptr<NvmeKvCommandExecutor> executor;
    switch (transport.value()) {
        case RuntimeTransport::kIoUring:
            executor = CreateNvmeKvIoUringExecutor(
                device_id_, storage_path_, metadata_.device_path,
                metadata_.nsid, metadata_.queue_depth, runtime_transfer_limit,
                capabilities);
            break;
        case RuntimeTransport::kIoctl:
            executor = CreateNvmeKvIoctlExecutor(
                device_id_, storage_path_, metadata_.device_path,
                metadata_.nsid, metadata_.queue_depth, runtime_transfer_limit,
                capabilities);
            break;
        case RuntimeTransport::kLibnvme:
            executor = CreateNvmeKvLibnvmeExecutor(
                device_id_, storage_path_, metadata_.device_path,
                metadata_.nsid, metadata_.queue_depth, runtime_transfer_limit,
                capabilities);
            break;
    }
    if (executor == nullptr || !capabilities) {
        return tl::make_unexpected(capabilities ? ErrorCode::INTERNAL_ERROR
                                                : capabilities.error());
    }

    executor_ = std::move(executor);
    metadata_.backend_type = executor_->GetBackendType();
    metadata_.health_state = "healthy";
    metadata_.last_error.clear();
    return {};
}

tl::expected<void, ErrorCode> NvmeKvConnector::InitStubExecutor() {
    std::error_code ec;
    std::filesystem::create_directories(storage_path_, ec);
    if (ec) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    executor_ = CreateNvmeKvStubExecutor(storage_path_);
    metadata_.backend_type = executor_->GetBackendType();
    metadata_.device_path = storage_path_.string();
    metadata_.uuid = device_id_;
    metadata_.nsid = 0;
    metadata_.queue_depth = executor_->GetCapabilities().queue_depth;
    metadata_.capacity_total = QueryFilesystemCapacityBytes(storage_path_);
    metadata_.health_state = "stub";
    metadata_.last_error.clear();
    return {};
}

tl::expected<ResolvedDeviceSelector, ErrorCode>
NvmeKvConnector::ResolveDeviceSelector() {
    const auto force_stub = GetEnvStringOrEmpty("MOONCAKE_NVME_KV_DRIVER");
    if (force_stub == "stub") {
        metadata_.last_error =
            "NVMe KV real device resolution disabled by stub driver";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    std::optional<std::pair<std::string, DeviceSelectorType>> selector;
    const std::string uuid_key = "MOONCAKE_NVME_KV_DEVICE_UUID_" + device_id_;
    auto direct_uuid = TrimString(GetEnvStringOrEmpty(uuid_key.c_str()));
    if (!direct_uuid.empty()) {
        selector = std::make_pair(direct_uuid, DeviceSelectorType::kUuid);
    }
    if (!selector.has_value()) {
        const std::string path_key =
            "MOONCAKE_NVME_KV_DEVICE_PATH_" + device_id_;
        auto direct_path = TrimString(GetEnvStringOrEmpty(path_key.c_str()));
        if (!direct_path.empty()) {
            selector = std::make_pair(direct_path, DeviceSelectorType::kPath);
        }
    }
    if (!selector.has_value()) {
        auto mapped_selector = ParseDeviceMapSelector(device_id_);
        if (mapped_selector.has_value()) {
            selector = std::make_pair(*mapped_selector,
                                      LooksLikeDevicePath(*mapped_selector)
                                          ? DeviceSelectorType::kPath
                                          : DeviceSelectorType::kUuid);
        }
    }
    if (!selector.has_value()) {
        auto global_uuid =
            TrimString(GetEnvStringOrEmpty("MOONCAKE_NVME_KV_DEVICE_UUID"));
        if (!global_uuid.empty()) {
            selector = std::make_pair(global_uuid, DeviceSelectorType::kUuid);
        }
    }
    if (!selector.has_value()) {
        auto global_path =
            TrimString(GetEnvStringOrEmpty("MOONCAKE_NVME_KV_DEVICE_PATH"));
        if (!global_path.empty()) {
            selector = std::make_pair(global_path, DeviceSelectorType::kPath);
        }
    }
    if (!selector.has_value()) {
        metadata_.last_error =
            "no NVMe KV device selector configured for device " + device_id_;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    ResolvedDeviceSelector resolved;
    resolved.configured_selector = selector->first;
    resolved.selector_type = selector->second;
    if (resolved.selector_type == DeviceSelectorType::kPath) {
        resolved.resolved_device_path = resolved.configured_selector;
        auto stable_id =
            FindStableIdForDevicePath(resolved.resolved_device_path);
        resolved.resolved_uuid =
            stable_id.value_or(resolved.resolved_device_path);
        return resolved;
    }

    auto resolved_path =
        ResolveDevicePathFromStableId(resolved.configured_selector);
    if (!resolved_path.has_value()) {
        metadata_.last_error =
            "configured NVMe KV device UUID could not be resolved: " +
            resolved.configured_selector;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    resolved.resolved_device_path = *resolved_path;
    auto stable_id = FindStableIdForDevicePath(resolved.resolved_device_path);
    resolved.resolved_uuid = stable_id.value_or(resolved.configured_selector);
    return resolved;
}

void NvmeKvConnector::SetTestDeviceIdentityEnumerator(
    DeviceIdentityEnumerator enumerator) {
    TestDeviceIdentityEnumerator() = std::move(enumerator);
}

void NvmeKvConnector::ClearTestDeviceIdentityEnumerator() {
    TestDeviceIdentityEnumerator() = nullptr;
}

bool NvmeKvConnector::ShouldUseStubFallback() const {
    const auto driver = GetEnvStringOrEmpty("MOONCAKE_NVME_KV_DRIVER");
    if (driver == "stub") {
        return true;
    }
    if (driver == "real") {
        return false;
    }
    const auto allow_fallback =
        GetEnvStringOrEmpty("MOONCAKE_NVME_KV_ALLOW_STUB_FALLBACK");
    return allow_fallback.empty() || allow_fallback == "1" ||
           allow_fallback == "true";
}

tl::expected<void, ErrorCode> NvmeKvConnector::Store(
    const PhysicalKey& key, std::string value,
    NvmeKvCommandExecutor::StoreOptions options) {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto result = executor_->Store(key, std::move(value), options);
    if (!result && result.error() != ErrorCode::OBJECT_ALREADY_EXISTS) {
        metadata_.last_error = "store failed: " + toString(result.error());
        if (result.error() == ErrorCode::KEYS_ULTRA_LIMIT) {
            if (metadata_.health_state.empty() ||
                metadata_.health_state == "error") {
                metadata_.health_state = "healthy";
            }
        } else {
            metadata_.health_state = "error";
        }
    }
    return result;
}

tl::expected<std::string, ErrorCode> NvmeKvConnector::Retrieve(
    const PhysicalKey& key, uint32_t size_hint) const {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Retrieve(key, size_hint);
}

tl::expected<void, ErrorCode> NvmeKvConnector::Delete(const PhysicalKey& key) {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto result = executor_->Delete(key);
    if (!result && result.error() != ErrorCode::OBJECT_NOT_FOUND) {
        metadata_.last_error = "delete failed: " + toString(result.error());
        metadata_.health_state = "error";
    }
    return result;
}

tl::expected<void, ErrorCode> NvmeKvConnector::Iterate(
    const std::function<tl::expected<void, ErrorCode>(const PhysicalKey& key)>&
        visitor) const {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Iterate(visitor);
}

tl::expected<void, ErrorCode> NvmeKvConnector::Probe() {
    if (executor_ == nullptr) {
        metadata_.last_error =
            "probe failed: " + toString(ErrorCode::INTERNAL_ERROR);
        metadata_.health_state = "error";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    bool reached_first_key = false;
    auto result = executor_->Iterate([&reached_first_key](const PhysicalKey&)
                                         -> tl::expected<void, ErrorCode> {
        reached_first_key = true;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    });
    if (reached_first_key) {
        result = {};
    }
    if (!result.has_value()) {
        metadata_.last_error = "probe failed: " + toString(result.error());
        metadata_.health_state = "error";
        return result;
    }
    metadata_.last_error.clear();
    metadata_.health_state =
        metadata_.backend_type == "stub" ? "stub" : "healthy";
    return {};
}

const NvmeKvConnector::Capabilities& NvmeKvConnector::GetCapabilities() const {
    static const Capabilities kDefaultCapabilities{};
    if (executor_ == nullptr) {
        return kDefaultCapabilities;
    }
    return executor_->GetCapabilities();
}

}  // namespace mooncake
