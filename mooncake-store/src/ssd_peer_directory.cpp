// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_peer_directory.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <json/json.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <iomanip>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "random.h"
#include "tenant_id.h"

namespace mooncake::ssd_offload {
namespace {

constexpr size_t kMaxPeers = 256;
constexpr size_t kMaxRetainedDeadLeftovers = 64;
constexpr size_t kMaxDocumentBytes = 16384;
constexpr uint64_t kMaxBatchObjects = 65536;

class File {
   public:
    explicit File(int fd) : fd_(fd) {}
    ~File() {
        if (fd_ >= 0) close(fd_);
    }
    int get() const { return fd_; }
    void reset() {
        if (fd_ >= 0) {
            close(fd_);
            fd_ = -1;
        }
    }
    File(File&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    File& operator=(File&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
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

std::string Stem(PeerIdentity id) {
    std::ostringstream out;
    out << "ssd-peer-" << std::hex << std::setfill('0') << std::setw(16)
        << id.client_id_high << '-' << std::setw(16) << id.client_id_low << '-'
        << std::setw(16) << id.boot_epoch;
    return out.str();
}

std::string Name(PeerIdentity id) { return Stem(id) + ".json"; }

std::string LockName(PeerIdentity id) { return "." + Stem(id) + ".lock"; }

bool String(const std::string& value, size_t max = 4096) {
    return !value.empty() && value.size() <= max &&
           value.find('\0') == std::string::npos;
}

bool NonWildcardNumericHost(const std::string& host) {
    return IsNonWildcardNumericHost(host);
}

bool Valid(const SsdPeerDirectoryEntry& entry) {
    const auto& fixed = entry.fixed;
    const auto& route = entry.route;
    return (fixed.identity.client_id_high || fixed.identity.client_id_low) &&
           fixed.identity.boot_epoch && fixed.credential != WriteCredential{} &&
           TenantId(fixed.tenant_id).IsValid() &&
           String(fixed.tenant_id, 256) && String(fixed.transport_endpoint) &&
           route.owner == fixed.identity &&
           route.tenant_id == fixed.tenant_id &&
           route.transport_endpoint == fixed.transport_endpoint &&
           String(route.legacy_rpc_endpoint) &&
           String(route.control_host, INET6_ADDRSTRLEN) &&
           (!entry.readable || route.control_port != 0) &&
           NonWildcardNumericHost(route.control_host) &&
           entry.protocol == "tcp" && route.capability.protocol_version == 1 &&
           route.capability.owner == fixed.identity &&
           route.capability.max_batch_objects > 0 &&
           route.capability.max_batch_objects <= kMaxBatchObjects &&
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
        !value.isObject() || !value["version"].isUInt() ||
        value["version"].asUInt() != 1)
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
    const auto& cap = value["capability"];
    if (!cap.isObject()) return {};
    for (const auto* field :
         {"features", "max_batch_objects", "max_batch_bytes", "max_fragments",
          "max_inflight_bytes", "preferred_chunk_bytes"})
        if (!cap[field].isUInt64()) return {};

    uint64_t max_objects = cap["max_batch_objects"].asUInt64();
    if (max_objects == 0 || max_objects > kMaxBatchObjects) return {};
    if (value.isMember("max_batch_objects")) {
        if (!value["max_batch_objects"].isUInt64() ||
            value["max_batch_objects"].asUInt64() != max_objects)
            return {};
    }

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

// Returns true if the sidecar lock exists and is NOT currently held by a live
// process (i.e. crash leftover after SIGKILL/OOM).
bool IsDeadCrashLeftover(int dir_fd, PeerIdentity id,
                         const std::string& self_lock_name) {
    const std::string lock_name = LockName(id);
    if (!self_lock_name.empty() && lock_name == self_lock_name) return false;
    File lock_file(openat(dir_fd, lock_name.c_str(),
                          O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC));
    if (lock_file.get() < 0) {
        // No sidecar lock file (e.g. legacy publisher): treat as live.
        return false;
    }
    if (flock(lock_file.get(), LOCK_EX | LOCK_NB) == 0) {
        flock(lock_file.get(), LOCK_UN);
        return true;
    }
    return false;
}

}  // namespace

struct SsdPeerDirectory::Impl {
    explicit Impl(File fd) : directory(std::move(fd)) {}
    File directory;
    mutable std::shared_mutex mutex;
    std::string publication;
    std::string lock_file_name;
    File lock_fd{-1};
    dev_t device{};
    ino_t inode{};

    bool Secure() const {
        struct stat info{};
        return fstat(directory.get(), &info) == 0 && Private(info, true);
    }

    LookupResult ReadEntryFile(const std::string& name,
                               bool skip_dead_leftovers) const {
        File file(openat(directory.get(), name.c_str(),
                         O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        if (file.get() < 0) {
            if (errno == ENOENT) return {DestinationError::kNotFound, {}};
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
        if (skip_dead_leftovers &&
            IsDeadCrashLeftover(directory.get(), entry->fixed.identity,
                                lock_file_name)) {
            return {DestinationError::kNotFound, {}};
        }
        return {DestinationError::kOk, std::move(entry)};
    }

    void GarbageCollectDeadLeftoversLocked() const {
        int fd =
            openat(directory.get(), ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) return;
        DIR* raw = fdopendir(fd);
        if (!raw) {
            close(fd);
            return;
        }
        std::unique_ptr<DIR, DirectoryClose> list(raw);
        struct DeadEntry {
            time_t mtime;
            std::string json_name;
            std::string lock_name;
        };
        std::vector<DeadEntry> dead;
        while (auto* item = readdir(list.get())) {
            const std::string name = item->d_name;
            if (!name.starts_with("ssd-peer-") || !name.ends_with(".json"))
                continue;
            if (name == publication) continue;
            const std::string stem = name.substr(0, name.size() - 5);
            const std::string lock_name = "." + stem + ".lock";
            if (!lock_file_name.empty() && lock_name == lock_file_name)
                continue;
            File lock_file(
                openat(directory.get(), lock_name.c_str(),
                       O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC));
            if (lock_file.get() < 0) continue;
            if (flock(lock_file.get(), LOCK_EX | LOCK_NB) != 0) continue;
            struct stat info{};
            time_t mtime = 0;
            if (fstatat(directory.get(), name.c_str(), &info,
                        AT_SYMLINK_NOFOLLOW) == 0) {
                mtime = info.st_mtime;
            }
            flock(lock_file.get(), LOCK_UN);
            dead.push_back({mtime, name, lock_name});
        }
        if (dead.size() <= kMaxRetainedDeadLeftovers) return;
        std::sort(dead.begin(), dead.end(),
                  [](const DeadEntry& a, const DeadEntry& b) {
                      return a.mtime < b.mtime;
                  });
        const size_t to_remove = dead.size() - kMaxRetainedDeadLeftovers;
        for (size_t i = 0; i < to_remove; ++i) {
            File lock_file(
                openat(directory.get(), dead[i].lock_name.c_str(),
                       O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC));
            if (lock_file.get() < 0) continue;
            if (flock(lock_file.get(), LOCK_EX | LOCK_NB) != 0) continue;
            (void)unlinkat(directory.get(), dead[i].json_name.c_str(), 0);
            (void)unlinkat(directory.get(), dead[i].lock_name.c_str(), 0);
            flock(lock_file.get(), LOCK_UN);
        }
    }

    LookupResult Lookup(
        const std::function<bool(const SsdPeerDirectoryEntry&)>& match,
        bool skip_dead_leftovers) const {
        if (!Secure()) return {DestinationError::kUnauthorized, {}};
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
        std::optional<DestinationError> first_file_error;
        size_t live_count = 0;
        errno = 0;
        while (auto* item = readdir(list.get())) {
            const std::string name = item->d_name;
            if (!name.starts_with("ssd-peer-") || !name.ends_with(".json"))
                continue;
            auto loaded = ReadEntryFile(name, skip_dead_leftovers);
            if (loaded.error == DestinationError::kNotFound) {
                errno = 0;
                continue;
            }
            if (loaded.error != DestinationError::kOk || !loaded.peer) {
                // Skip isolated malformed/insecure files so one bad record does
                // not poison discovery of healthy peers; fail closed with the
                // recorded error if no healthy matching peer is found.
                if (!first_file_error) first_file_error = loaded.error;
                LOG(WARNING)
                    << "Skipping invalid SSD peer directory entry '" << name
                    << "' error=" << static_cast<int>(loaded.error);
                errno = 0;
                continue;
            }
            if (++live_count > kMaxPeers) return {DestinationError::kLimit, {}};
            if (match(*loaded.peer)) {
                if (result.peer) return {DestinationError::kDuplicate, {}};
                result = {DestinationError::kOk, std::move(loaded.peer)};
            }
            errno = 0;
        }
        if (errno) return {DestinationError::kTransportError, {}};
        if (!result.peer && first_file_error) return {*first_file_error, {}};
        return result;
    }
};

bool IsNonWildcardNumericHost(const std::string& host) {
    in_addr v4{};
    if (inet_pton(AF_INET, host.c_str(), &v4) == 1) {
        return v4.s_addr != htonl(INADDR_ANY);
    }
    in6_addr v6{};
    if (inet_pton(AF_INET6, host.c_str(), &v6) == 1) {
        if (IN6_IS_ADDR_UNSPECIFIED(&v6)) return false;
        if (IN6_IS_ADDR_V4MAPPED(&v6) && v6.s6_addr[12] == 0 &&
            v6.s6_addr[13] == 0 && v6.s6_addr[14] == 0 && v6.s6_addr[15] == 0) {
            return false;
        }
        return true;
    }
    return false;
}

std::string EncodeSsdPeerDocument(const SsdPeerDirectoryEntry& entry) {
    return Encode(entry);
}

std::optional<SsdPeerDirectoryEntry> DecodeSsdPeerDocument(
    const std::string& bytes) {
    if (bytes.size() > kMaxDocumentBytes) return std::nullopt;
    return Decode(bytes);
}

SsdPeerDirectory::SsdPeerDirectory(std::string path) {
    while (path.size() > 1 && path.back() == '/') path.pop_back();
    if (!String(path))
        throw std::invalid_argument("Invalid SSD peer directory path");
    File fd(
        open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    struct stat info{};
    if (fd.get() < 0 || fstat(fd.get(), &info) != 0 || !Private(info, true))
        throw std::invalid_argument(
            "SSD peer directory must be private and owned by this UID");
    impl_ = std::make_unique<Impl>(std::move(fd));
}

SsdPeerDirectory::~SsdPeerDirectory() { Unpublish(); }

DestinationError SsdPeerDirectory::Publish(const SsdPeerDirectoryEntry& entry) {
    if (!Valid(entry)) return DestinationError::kInvalidArgument;
    const auto bytes = Encode(entry);
    if (bytes.size() > kMaxDocumentBytes) return DestinationError::kLimit;
    std::unique_lock lock(impl_->mutex);
    if (!impl_->Secure()) return DestinationError::kUnauthorized;
    if (!impl_->publication.empty()) return DestinationError::kDuplicate;

    // Exclusively create and flock our epoch's sidecar lock file before
    // publishing the discovery record so dead crash leftovers can be
    // distinguished from live peers and an existing lock file is never
    // clobbered or unlinked on duplicate publication.
    auto lock_name = LockName(entry.fixed.identity);
    File liveness_lock(
        openat(impl_->directory.get(), lock_name.c_str(),
               O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
    if (liveness_lock.get() < 0) {
        return errno == EEXIST ? DestinationError::kDuplicate
                               : DestinationError::kTransportError;
    }
    if (fchmod(liveness_lock.get(), 0600) != 0 ||
        flock(liveness_lock.get(), LOCK_EX | LOCK_NB) != 0) {
        unlinkat(impl_->directory.get(), lock_name.c_str(), 0);
        return DestinationError::kTransportError;
    }

    uint64_t nonce{};
    if (!secureRandomBytes(std::as_writable_bytes(std::span(&nonce, 1)))) {
        unlinkat(impl_->directory.get(), lock_name.c_str(), 0);
        return DestinationError::kTransportError;
    }
    auto name = Name(entry.fixed.identity);
    const auto temporary = ".ssd-peer-" + std::to_string(nonce);
    File file(openat(impl_->directory.get(), temporary.c_str(),
                     O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                     0600));
    if (file.get() < 0) {
        unlinkat(impl_->directory.get(), lock_name.c_str(), 0);
        return DestinationError::kTransportError;
    }
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
            unlinkat(impl_->directory.get(), lock_name.c_str(), 0);
            return DestinationError::kTransportError;
        }
        offset += size_t(done);
    }
    struct stat info{};
    if (fchmod(file.get(), 0600) != 0 || fsync(file.get()) != 0 ||
        fstat(file.get(), &info) != 0) {
        cleanup();
        unlinkat(impl_->directory.get(), lock_name.c_str(), 0);
        return DestinationError::kTransportError;
    }
    if (linkat(impl_->directory.get(), temporary.c_str(),
               impl_->directory.get(), name.c_str(), 0) != 0) {
        const auto error = errno == EEXIST ? DestinationError::kDuplicate
                                           : DestinationError::kTransportError;
        cleanup();
        unlinkat(impl_->directory.get(), lock_name.c_str(), 0);
        return error;
    }
    impl_->publication.swap(name);
    impl_->lock_file_name.swap(lock_name);
    impl_->lock_fd = std::move(liveness_lock);
    impl_->device = info.st_dev;
    impl_->inode = info.st_ino;
    cleanup();
    impl_->GarbageCollectDeadLeftoversLocked();
    return DestinationError::kOk;
}

SsdPeerDirectory::LookupResult SsdPeerDirectory::LookupLegacy(
    const std::string& endpoint, const std::string& tenant,
    const std::string& protocol) const {
    std::shared_lock lock(impl_->mutex);
    return impl_->Lookup(
        [&](const auto& entry) {
            return entry.readable &&
                   entry.route.legacy_rpc_endpoint == endpoint &&
                   entry.fixed.tenant_id == tenant &&
                   entry.protocol == protocol;
        },
        /*skip_dead_leftovers=*/true);
}

SsdPeerDirectory::LookupResult SsdPeerDirectory::LookupRequester(
    PeerIdentity identity, const std::string& tenant,
    const std::string& protocol) const {
    if (!(identity.client_id_high || identity.client_id_low) ||
        !identity.boot_epoch) {
        return {DestinationError::kNotFound, {}};
    }
    std::shared_lock lock(impl_->mutex);
    if (!impl_->Secure()) return {DestinationError::kUnauthorized, {}};
    // O(1) direct open by canonical identity filename: avoids O(N) directory
    // scans and never fails with kLimit after accumulated crash leftovers.
    auto loaded = impl_->ReadEntryFile(Name(identity),
                                       /*skip_dead_leftovers=*/false);
    if (loaded.error != DestinationError::kOk || !loaded.peer) {
        return {loaded.error, {}};
    }
    if (loaded.peer->fixed.identity != identity ||
        loaded.peer->fixed.tenant_id != tenant ||
        loaded.peer->protocol != protocol) {
        return {DestinationError::kNotFound, {}};
    }
    return loaded;
}

DestinationError SsdPeerDirectory::Unpublish() {
    std::unique_lock lock(impl_->mutex);
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
    if (!impl_->lock_file_name.empty()) {
        unlinkat(impl_->directory.get(), impl_->lock_file_name.c_str(), 0);
        impl_->lock_file_name.clear();
    }
    impl_->lock_fd.reset();
    impl_->publication.clear();
    return DestinationError::kOk;
}

}  // namespace mooncake::ssd_offload
