#include "tenant_quota_policy_store.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <thread>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#endif
#include "types.h"

namespace mooncake {
namespace {

bool IsValidTenantQuotaName(const std::string& name) {
    return IsValidTenantId(name);
}

std::string ErrnoMessage(const std::string& action, const std::string& path) {
    return action + " '" + path + "' failed: " + std::strerror(errno);
}

#ifdef STORE_USE_ETCD
tl::expected<std::string, std::string> NormalizeClusterIdForEtcdKey(
    const std::string& cluster_id) {
    std::string normalized =
        cluster_id.empty() ? std::string(DEFAULT_CLUSTER_ID) : cluster_id;
    while (!normalized.empty() && normalized.back() == '/') {
        normalized.pop_back();
    }
    if (normalized.empty()) {
        normalized = DEFAULT_CLUSTER_ID;
    }
    if (!IsValidClusterIdComponent(normalized)) {
        return tl::make_unexpected("invalid tenant quota etcd cluster_id '" +
                                   cluster_id + "'");
    }
    return normalized;
}

tl::expected<std::string, std::string> BuildTenantQuotaEtcdKey(
    const std::string& cluster_id) {
    auto normalized = NormalizeClusterIdForEtcdKey(cluster_id);
    if (!normalized) {
        return tl::make_unexpected(normalized.error());
    }
    return "mooncake-store/" + normalized.value() + "/tenant_quota_policy";
}
#endif

tl::expected<void, std::string> WriteAll(int fd, const std::string& content,
                                         const std::string& path) {
    const char* data = content.data();
    size_t remaining = content.size();
    while (remaining > 0) {
        ssize_t written = ::write(fd, data, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return tl::make_unexpected(ErrnoMessage("write", path));
        }
        if (written == 0) {
            return tl::make_unexpected("write '" + path + "' made no progress");
        }
        data += written;
        remaining -= static_cast<size_t>(written);
    }
    return {};
}

tl::expected<void, std::string> FsyncDirectory(const std::string& path) {
    std::filesystem::path dir = std::filesystem::path(path).parent_path();
    if (dir.empty()) {
        dir = ".";
    }
    int fd = ::open(dir.string().c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return tl::make_unexpected(
            ErrnoMessage("open directory", dir.string()));
    }
    auto close_fd = [&] { ::close(fd); };
    if (::fsync(fd) != 0) {
        std::string error = ErrnoMessage("fsync directory", dir.string());
        close_fd();
        return tl::make_unexpected(error);
    }
    close_fd();
    return {};
}

std::string MakeTempPath(const std::string& path) {
    const auto now =
        std::chrono::steady_clock::now().time_since_epoch().count();
    std::ostringstream oss;
    oss << path << ".tmp." << ::getpid() << "."
        << std::hash<std::thread::id>{}(std::this_thread::get_id()) << "."
        << now;
    return oss.str();
}

std::string QuoteYamlDoubleQuotedScalar(const std::string& value) {
    std::ostringstream out;
    out << '"';
    for (unsigned char c : value) {
        switch (c) {
            case '\\':
                out << "\\\\";
                break;
            case '"':
                out << "\\\"";
                break;
            case '\0':
                out << "\\0";
                break;
            case '\a':
                out << "\\a";
                break;
            case '\b':
                out << "\\b";
                break;
            case '\t':
                out << "\\t";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\v':
                out << "\\v";
                break;
            case '\f':
                out << "\\f";
                break;
            case '\r':
                out << "\\r";
                break;
            default:
                if (c < 0x20 || c == 0x7f) {
                    out << "\\x" << std::uppercase << std::hex << std::setw(2)
                        << std::setfill('0') << static_cast<int>(c) << std::dec
                        << std::nouppercase << std::setfill(' ');
                } else {
                    out << static_cast<char>(c);
                }
                break;
        }
    }
    out << '"';
    return out.str();
}

}  // namespace

tl::expected<uint64_t, std::string> ParseTenantQuotaBytes(
    const std::string& value) {
    if (value.empty()) {
        return tl::make_unexpected("quota must not be empty");
    }

    size_t digits = 0;
    while (digits < value.size() && value[digits] >= '0' &&
           value[digits] <= '9') {
        ++digits;
    }
    if (digits == 0) {
        return tl::make_unexpected("quota must start with an integer");
    }

    uint64_t number = 0;
    for (size_t i = 0; i < digits; ++i) {
        const uint64_t digit = static_cast<uint64_t>(value[i] - '0');
        if (number > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
            return tl::make_unexpected("quota integer overflows uint64");
        }
        number = number * 10 + digit;
    }
    if (number == 0) {
        return tl::make_unexpected("quota must be positive");
    }

    const std::string unit = value.substr(digits);
    uint64_t multiplier = 1;
    if (unit.empty() || unit == "B") {
        multiplier = 1;
    } else if (unit == "KB") {
        multiplier = 1024ULL;
    } else if (unit == "MB") {
        multiplier = 1024ULL * 1024ULL;
    } else if (unit == "GB") {
        multiplier = 1024ULL * 1024ULL * 1024ULL;
    } else if (unit == "TB") {
        multiplier = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
    } else {
        return tl::make_unexpected("unsupported quota unit '" + unit + "'");
    }

    if (number > std::numeric_limits<uint64_t>::max() / multiplier) {
        return tl::make_unexpected("quota byte value overflows uint64");
    }
    return number * multiplier;
}

tl::expected<TenantQuotaPolicySnapshot, std::string> ParseTenantQuotaPolicyYaml(
    const std::string& yaml) {
    YAML::Node root;
    try {
        root = YAML::Load(yaml);
    } catch (const YAML::Exception& e) {
        return tl::make_unexpected(std::string("invalid YAML: ") + e.what());
    }

    if (!root || !root.IsMap()) {
        return tl::make_unexpected("tenant quota policy must be a YAML map");
    }
    const auto version_node = root["version"];
    if (!version_node || !version_node.IsScalar()) {
        return tl::make_unexpected("tenant quota policy version is required");
    }
    int version = 0;
    try {
        version = version_node.as<int>();
    } catch (const YAML::Exception& e) {
        return tl::make_unexpected(std::string("invalid version: ") + e.what());
    }
    if (version != 1) {
        return tl::make_unexpected("unsupported tenant quota policy version: " +
                                   std::to_string(version));
    }

    const auto tenants_node = root["tenants"];
    if (!tenants_node || !tenants_node.IsSequence()) {
        return tl::make_unexpected("tenants must be a YAML sequence");
    }

    TenantQuotaPolicySnapshot snapshot;
    for (size_t i = 0; i < tenants_node.size(); ++i) {
        const auto entry = tenants_node[i];
        if (!entry || !entry.IsMap()) {
            return tl::make_unexpected("tenant entry must be a YAML map");
        }
        const auto name_node = entry["name"];
        const auto quota_node = entry["quota"];
        if (!name_node || !name_node.IsScalar()) {
            return tl::make_unexpected("tenant name is required");
        }
        if (!quota_node || !quota_node.IsScalar()) {
            return tl::make_unexpected("tenant quota is required");
        }

        std::string name;
        std::string quota;
        try {
            name = name_node.as<std::string>();
            quota = quota_node.as<std::string>();
        } catch (const YAML::Exception& e) {
            return tl::make_unexpected(std::string("invalid tenant entry: ") +
                                       e.what());
        }
        if (!IsValidTenantQuotaName(name)) {
            return tl::make_unexpected("invalid tenant name '" + name + "'");
        }
        name = NormalizeTenantId(name);
        if (!IsValidTenantQuotaName(name)) {
            return tl::make_unexpected("invalid tenant name '" + name + "'");
        }
        if (snapshot.tenant_quotas.contains(name)) {
            return tl::make_unexpected("duplicate tenant name '" + name + "'");
        }

        auto quota_bytes = ParseTenantQuotaBytes(quota);
        if (!quota_bytes) {
            return tl::make_unexpected("invalid quota for tenant '" + name +
                                       "': " + quota_bytes.error());
        }
        snapshot.tenant_quotas.emplace(std::move(name), quota_bytes.value());
    }

    return snapshot;
}

std::string FormatTenantQuotaPolicyYaml(
    const TenantQuotaPolicySnapshot& snapshot) {
    std::ostringstream out;
    out << "version: 1\n\n";
    if (snapshot.tenant_quotas.empty()) {
        out << "tenants: []\n";
        return out.str();
    }
    out << "tenants:\n";
    for (const auto& [tenant_id, quota] : snapshot.tenant_quotas) {
        out << "  - name: " << QuoteYamlDoubleQuotedScalar(tenant_id) << "\n";
        out << "    quota: " << quota << "\n";
    }
    return out.str();
}

YamlTenantQuotaPolicyStore::YamlTenantQuotaPolicyStore(std::string path)
    : path_(std::move(path)) {}

tl::expected<TenantQuotaPolicySnapshot, std::string>
YamlTenantQuotaPolicyStore::Load() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ifstream input(path_);
    if (!input.is_open()) {
        return tl::make_unexpected("failed to open tenant quota policy file '" +
                                   path_ + "'");
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (input.bad()) {
        return tl::make_unexpected("failed to read tenant quota policy file '" +
                                   path_ + "'");
    }
    return ParseTenantQuotaPolicyYaml(buffer.str());
}

tl::expected<void, std::string> YamlTenantQuotaPolicyStore::Save(
    const TenantQuotaPolicySnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string content = FormatTenantQuotaPolicyYaml(snapshot);
    const std::string tmp_path = MakeTempPath(path_);

    int fd = ::open(tmp_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        return tl::make_unexpected(ErrnoMessage("open", tmp_path));
    }

    auto cleanup = [&] {
        ::close(fd);
        ::unlink(tmp_path.c_str());
    };

    auto write_result = WriteAll(fd, content, tmp_path);
    if (!write_result) {
        cleanup();
        return tl::make_unexpected(write_result.error());
    }
    if (::fsync(fd) != 0) {
        std::string error = ErrnoMessage("fsync", tmp_path);
        cleanup();
        return tl::make_unexpected(error);
    }
    if (::close(fd) != 0) {
        std::string error = ErrnoMessage("close", tmp_path);
        ::unlink(tmp_path.c_str());
        return tl::make_unexpected(error);
    }
    fd = -1;

    if (::rename(tmp_path.c_str(), path_.c_str()) != 0) {
        std::string error = ErrnoMessage("rename", path_);
        ::unlink(tmp_path.c_str());
        return tl::make_unexpected(error);
    }

    auto fsync_result = FsyncDirectory(path_);
    if (!fsync_result) {
        LOG(WARNING) << "failed to fsync tenant quota policy directory after "
                        "rename: "
                     << fsync_result.error();
    }
    return {};
}

#ifdef STORE_USE_ETCD
EtcdTenantQuotaPolicyStore::EtcdTenantQuotaPolicyStore(
    const std::string& endpoints, const std::string& cluster_id) {
    auto key = BuildTenantQuotaEtcdKey(cluster_id);
    if (!key) {
        throw std::invalid_argument(key.error());
    }
    if (endpoints.empty()) {
        throw std::invalid_argument(
            "tenant quota etcd connector requires a non-empty uri");
    }
    ErrorCode connect_error = EtcdHelper::ConnectToEtcdStoreClient(endpoints);
    if (connect_error != ErrorCode::OK) {
        if (connect_error == ErrorCode::INVALID_PARAMS) {
            throw std::runtime_error(
                "failed to connect tenant quota etcd store: "
                "tenant_quota_connector_uri must match the already connected "
                "store etcd endpoints used by HA/oplog");
        }
        throw std::runtime_error("failed to connect tenant quota etcd store: " +
                                 toString(connect_error));
    }
    key_ = std::move(key.value());
}

tl::expected<TenantQuotaPolicySnapshot, std::string>
EtcdTenantQuotaPolicyStore::Load() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string content;
    EtcdRevisionId revision_id = 0;
    ErrorCode error =
        EtcdHelper::Get(key_.c_str(), key_.size(), content, revision_id);
    if (error != ErrorCode::OK) {
        return tl::make_unexpected(
            "failed to load tenant quota policy from "
            "etcd key '" +
            key_ + "': " + toString(error));
    }
    return ParseTenantQuotaPolicyYaml(content);
}

tl::expected<void, std::string> EtcdTenantQuotaPolicyStore::Save(
    const TenantQuotaPolicySnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string content = FormatTenantQuotaPolicyYaml(snapshot);
    ErrorCode error = EtcdHelper::Put(key_.c_str(), key_.size(),
                                      content.c_str(), content.size());
    if (error != ErrorCode::OK) {
        return tl::make_unexpected(
            "failed to save tenant quota policy to "
            "etcd key '" +
            key_ + "': " + toString(error));
    }
    return {};
}
#endif

tl::expected<std::unique_ptr<TenantQuotaPolicyStore>, std::string>
CreateTenantQuotaPolicyStore(const std::string& type, const std::string& uri,
                             const std::string& cluster_id) {
    if (type == "file") {
        if (uri.empty()) {
            return tl::make_unexpected(
                "tenant quota file connector requires a non-empty uri");
        }
        return std::make_unique<YamlTenantQuotaPolicyStore>(uri);
    }
    if (type == "etcd") {
#ifdef STORE_USE_ETCD
        try {
            return std::make_unique<EtcdTenantQuotaPolicyStore>(uri,
                                                                cluster_id);
        } catch (const std::exception& e) {
            return tl::make_unexpected(e.what());
        }
#else
        return tl::make_unexpected(
            "tenant quota etcd connector requires STORE_USE_ETCD");
#endif
    }
    return tl::make_unexpected("unsupported tenant quota connector type '" +
                               type + "'");
}

}  // namespace mooncake
