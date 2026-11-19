// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_peer_directory.h"
#include "random.h"
#include "tenant_id.h"
#include <json/json.h>
#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace mooncake::ssd_offload {
namespace {
constexpr size_t kMaxPeers = 64, kMaxDocumentBytes = 16384;
class File {
   public:
    explicit File(int fd) : fd_(fd) {}
    ~File() {
        if (fd_ >= 0) close(fd_);
    }
    int get() const { return fd_; }
    File(File&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    File(const File&) = delete;
    File& operator=(const File&) = delete;

   private:
    int fd_;
};
struct DirectoryClose {
    void operator()(DIR* directory) const noexcept { closedir(directory); }
};
bool Private(const struct stat& info, bool directory) {
    return (directory ? S_ISDIR(info.st_mode) : S_ISREG(info.st_mode)) &&
           info.st_uid == geteuid() && !(info.st_mode & 0077);
}
std::string Name(PeerIdentity id) {
    std::ostringstream out;
    out << "ssd-peer-" << std::hex << std::setfill('0') << std::setw(16)
        << id.client_id_high << '-' << std::setw(16) << id.client_id_low << '-'
        << std::setw(16) << id.boot_epoch << ".json";
    return out.str();
}
bool String(const std::string& value, size_t max = 4096) {
    return !value.empty() && value.size() <= max &&
           value.find('\0') == std::string::npos;
}
bool Valid(const SsdPeerDirectoryEntry& entry) {
    const auto& fixed = entry.fixed;
    const auto& route = entry.route;
    in_addr v4{};
    in6_addr v6{};
    return (fixed.identity.client_id_high || fixed.identity.client_id_low) &&
           fixed.identity.boot_epoch && fixed.credential != WriteCredential{} &&
           TenantId(fixed.tenant_id).IsValid() &&
           String(fixed.tenant_id, 256) && String(fixed.transport_endpoint) &&
           route.owner == fixed.identity &&
           route.tenant_id == fixed.tenant_id &&
           route.transport_endpoint == fixed.transport_endpoint &&
           String(route.legacy_rpc_endpoint) &&
           String(route.control_host, INET6_ADDRSTRLEN) && route.control_port &&
           (inet_pton(AF_INET, route.control_host.c_str(), &v4) == 1 ||
            inet_pton(AF_INET6, route.control_host.c_str(), &v6) == 1) &&
           (entry.protocol == "tcp" || entry.protocol == "rdma") &&
           route.capability.protocol_version == 1 &&
           route.capability.owner == fixed.identity &&
           route.capability.max_batch_objects &&
           route.capability.max_batch_bytes && route.capability.max_fragments &&
           route.capability.max_inflight_bytes &&
           route.capability.preferred_chunk_bytes &&
           route.capability.supported_transports ==
               std::vector<std::string>{entry.protocol};
}
std::string Encode(const SsdPeerDirectoryEntry& entry) {
    Json::Value value;
    value["version"] = 1;
    value["client_id_high"] = Json::UInt64(entry.fixed.identity.client_id_high);
    value["client_id_low"] = Json::UInt64(entry.fixed.identity.client_id_low);
    value["boot_epoch"] = Json::UInt64(entry.fixed.identity.boot_epoch);
    value["credential_high"] = Json::UInt64(entry.fixed.credential.high);
    value["credential_low"] = Json::UInt64(entry.fixed.credential.low);
    value["tenant"] = entry.fixed.tenant_id;
    value["transport_endpoint"] = entry.fixed.transport_endpoint;
    value["legacy_endpoint"] = entry.route.legacy_rpc_endpoint;
    value["control_host"] = entry.route.control_host;
    value["control_port"] = entry.route.control_port;
    value["protocol"] = entry.protocol;
    value["can_read_ssd"] = entry.readable;
    value["max_batch_objects"] =
        Json::UInt64(entry.route.capability.max_batch_objects);
    auto& cap = value["capability"];
    cap["features"] = Json::UInt64(entry.route.capability.features);
    cap["max_batch_objects"] =
        Json::UInt64(entry.route.capability.max_batch_objects);
    cap["max_batch_bytes"] =
        Json::UInt64(entry.route.capability.max_batch_bytes);
    cap["max_fragments"] = Json::UInt64(entry.route.capability.max_fragments);
    cap["max_inflight_bytes"] =
        Json::UInt64(entry.route.capability.max_inflight_bytes);
    cap["preferred_chunk_bytes"] =
        Json::UInt64(entry.route.capability.preferred_chunk_bytes);
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, value);
}
std::optional<SsdPeerDirectoryEntry> Decode(const std::string& bytes) {
    Json::Value value;
    Json::CharReaderBuilder builder;
    builder["rejectDupKeys"] = true;
    std::unique_ptr<Json::CharReader> parser(builder.newCharReader());
    std::string errors;
    if (!parser->parse(bytes.data(), bytes.data() + bytes.size(), &value,
                       &errors) ||
        !value["version"].isUInt() || value["version"].asUInt() != 1)
        return {};
    for (const auto* field :
         {"client_id_high", "client_id_low", "boot_epoch", "credential_high",
          "credential_low", "control_port"})
        if (!value[field].isUInt64()) return {};
    for (const auto* field : {"tenant", "transport_endpoint", "legacy_endpoint",
                              "control_host", "protocol"})
        if (!value[field].isString()) return {};
    if (!value["can_read_ssd"].isBool() ||
        value["control_port"].asUInt64() > UINT16_MAX)
        return {};
    uint64_t max_objects = 64;
    if (value.isMember("max_batch_objects")) {
        if (!value["max_batch_objects"].isUInt64() ||
            !value["max_batch_objects"].asUInt64() ||
            value["max_batch_objects"].asUInt64() > 65536)
            return {};
        max_objects = value["max_batch_objects"].asUInt64();
    }
    const auto& cap = value["capability"];
    for (const auto* field :
         {"features", "max_batch_objects", "max_batch_bytes", "max_fragments",
          "max_inflight_bytes", "preferred_chunk_bytes"})
        if (!cap[field].isUInt64()) return {};
    SsdPeerDirectoryEntry entry;
    entry.fixed = {
        {value["client_id_high"].asUInt64(), value["client_id_low"].asUInt64(),
         value["boot_epoch"].asUInt64()},
        value["tenant"].asString(),
        value["transport_endpoint"].asString(),
        {value["credential_high"].asUInt64(),
         value["credential_low"].asUInt64()}};
    entry.protocol = value["protocol"].asString();
    entry.readable = value["can_read_ssd"].asBool();
    entry.route = {entry.fixed.identity,
                   entry.fixed.tenant_id,
                   value["legacy_endpoint"].asString(),
                   value["control_host"].asString(),
                   uint16_t(value["control_port"].asUInt64()),
                   entry.fixed.transport_endpoint,
                   {1,
                    entry.fixed.identity,
                    cap["features"].asUInt64(),
                    max_objects,
                    cap["max_batch_bytes"].asUInt64(),
                    cap["max_fragments"].asUInt64(),
                    cap["max_inflight_bytes"].asUInt64(),
                    cap["preferred_chunk_bytes"].asUInt64(),
                    {entry.protocol}}};
    return Valid(entry) ? std::optional(std::move(entry)) : std::nullopt;
}
}  // namespace
struct SsdPeerDirectory::Impl {
    explicit Impl(File fd) : directory(std::move(fd)) {}
    File directory;
    mutable std::mutex mutex;
    std::string publication;
    dev_t device{};
    ino_t inode{};
    bool Secure() const {
        struct stat info{};
        return fstat(directory.get(), &info) == 0 && Private(info, true);
    }
    LookupResult Lookup(
        const std::function<bool(const SsdPeerDirectoryEntry&)>& match) const {
        if (!Secure()) return {DestinationError::kUnauthorized, {}};
        // A new open file description avoids reusing the previous scan offset.
        int fd =
            openat(directory.get(), ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) return {DestinationError::kTransportError, {}};
        DIR* raw = fdopendir(fd);
        if (!raw) {
            close(fd);
            return {DestinationError::kTransportError, {}};
        }
        std::unique_ptr<DIR, DirectoryClose> list(raw);
        LookupResult result;
        size_t count = 0;
        errno = 0;
        while (auto* item = readdir(list.get())) {
            const std::string name = item->d_name;
            if (!name.starts_with("ssd-peer-") || !name.ends_with(".json"))
                continue;
            if (++count > kMaxPeers) return {DestinationError::kLimit, {}};
            File file(openat(directory.get(), name.c_str(),
                             O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
            if (file.get() < 0) {
                if (errno == ENOENT) {
                    errno = 0;
                    continue;
                }  // Concurrent clean close.
                return {DestinationError::kUnauthorized, {}};
            }
            struct stat info{};
            if (fstat(file.get(), &info) != 0 || !Private(info, false))
                return {DestinationError::kUnauthorized, {}};
            if (info.st_size <= 0 || uint64_t(info.st_size) > kMaxDocumentBytes)
                return {DestinationError::kInvalidArgument, {}};
            std::string bytes;
            char buffer[4096];
            for (;;) {
                const auto got = read(file.get(), buffer, sizeof(buffer));
                if (got < 0 && errno == EINTR) continue;
                if (got < 0) return {DestinationError::kTransportError, {}};
                if (!got) break;
                bytes.append(buffer, size_t(got));
                if (bytes.size() > kMaxDocumentBytes)
                    return {DestinationError::kInvalidArgument, {}};
            }
            auto entry = Decode(bytes);
            if (!entry || Name(entry->fixed.identity) != name)
                return {DestinationError::kInvalidArgument, {}};
            if (match(*entry)) {
                if (result.peer) return {DestinationError::kDuplicate, {}};
                result = {DestinationError::kOk, std::move(entry)};
            }
            errno = 0;
        }
        if (errno) return {DestinationError::kTransportError, {}};
        return result;
    }
};
SsdPeerDirectory::SsdPeerDirectory(std::string path) {
    // A trailing slash makes POSIX open follow the preceding symlink even
    // with O_NOFOLLOW. Remove it before checking the final path component.
    while (path.size() > 1 && path.back() == '/') path.pop_back();
    if (!String(path))
        throw std::invalid_argument("Invalid SSD peer directory path");
    File fd(
        open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    struct stat info{};
    if (fd.get() < 0 || fstat(fd.get(), &info) != 0 || !Private(info, true))
        throw std::invalid_argument(
            "SSD peer directory must be private and owned by this UID");
    // Allocation happens before File ownership moves into Impl. If it fails,
    // the local guard still closes the already opened directory descriptor.
    impl_ = std::make_unique<Impl>(std::move(fd));
}
SsdPeerDirectory::~SsdPeerDirectory() { Unpublish(); }
DestinationError SsdPeerDirectory::Publish(const SsdPeerDirectoryEntry& entry) {
    if (!Valid(entry)) return DestinationError::kInvalidArgument;
    const auto bytes = Encode(entry);
    if (bytes.size() > kMaxDocumentBytes) return DestinationError::kLimit;
    std::lock_guard lock(impl_->mutex);
    if (!impl_->Secure()) return DestinationError::kUnauthorized;
    if (!impl_->publication.empty()) return DestinationError::kDuplicate;
    uint64_t nonce{};
    if (!secureRandomBytes(std::as_writable_bytes(std::span(&nonce, 1))))
        return DestinationError::kTransportError;
    auto name = Name(entry.fixed.identity);
    const auto temporary = ".ssd-peer-" + std::to_string(nonce);
    File file(openat(impl_->directory.get(), temporary.c_str(),
                     O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                     0600));
    if (file.get() < 0) return DestinationError::kTransportError;
    auto cleanup = [&] {
        unlinkat(impl_->directory.get(), temporary.c_str(), 0);
    };
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto done =
            write(file.get(), bytes.data() + offset, bytes.size() - offset);
        if (done < 0 && errno == EINTR) continue;
        if (done <= 0) {
            cleanup();
            return DestinationError::kTransportError;
        }
        offset += size_t(done);
    }
    struct stat info{};
    if (fchmod(file.get(), 0600) != 0 || fsync(file.get()) != 0 ||
        fstat(file.get(), &info) != 0) {
        cleanup();
        return DestinationError::kTransportError;
    }
    // linkat atomically publishes the complete file and never overwrites an
    // existing UUID/epoch. The temporary link is not a discovery document.
    if (linkat(impl_->directory.get(), temporary.c_str(),
               impl_->directory.get(), name.c_str(), 0) != 0) {
        const auto error = errno == EEXIST ? DestinationError::kDuplicate
                                           : DestinationError::kTransportError;
        cleanup();
        return error;
    }
    // Never allocate after publication: a thrown string copy would leave a
    // discoverable secret record that Unpublish cannot identify or clean up.
    impl_->publication.swap(name);
    impl_->device = info.st_dev;
    impl_->inode = info.st_ino;
    cleanup();
    return DestinationError::kOk;
}
SsdPeerDirectory::LookupResult SsdPeerDirectory::LookupLegacy(
    const std::string& endpoint, const std::string& tenant,
    const std::string& protocol) const {
    std::lock_guard lock(impl_->mutex);
    return impl_->Lookup([&](const auto& entry) {
        return entry.readable && entry.route.legacy_rpc_endpoint == endpoint &&
               entry.fixed.tenant_id == tenant && entry.protocol == protocol;
    });
}
SsdPeerDirectory::LookupResult SsdPeerDirectory::LookupRequester(
    PeerIdentity identity, const std::string& tenant,
    const std::string& protocol) const {
    std::lock_guard lock(impl_->mutex);
    return impl_->Lookup([&](const auto& entry) {
        return entry.fixed.identity == identity &&
               entry.fixed.tenant_id == tenant && entry.protocol == protocol;
    });
}
DestinationError SsdPeerDirectory::Unpublish() {
    std::lock_guard lock(impl_->mutex);
    if (impl_->publication.empty()) return DestinationError::kOk;
    struct stat info{};
    if (fstatat(impl_->directory.get(), impl_->publication.c_str(), &info,
                AT_SYMLINK_NOFOLLOW) != 0) {
        if (errno != ENOENT) return DestinationError::kTransportError;
    } else {
        if (info.st_dev != impl_->device || info.st_ino != impl_->inode)
            return DestinationError::kDuplicate;
        if (unlinkat(impl_->directory.get(), impl_->publication.c_str(), 0) !=
            0)
            return DestinationError::kTransportError;
    }
    impl_->publication.clear();
    return DestinationError::kOk;
}
}  // namespace mooncake::ssd_offload
