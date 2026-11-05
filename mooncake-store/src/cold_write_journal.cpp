#include "cold_write_journal.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <filesystem>
#include <string_view>

#include <openssl/sha.h>
#include <ylt/struct_pack.hpp>

namespace mooncake {
namespace {

constexpr uint32_t kRecordVersion = 1;
constexpr size_t kMaxRecordBytes = 1024 * 1024;

struct TokenPayload {
    uint32_t version{kRecordVersion};
    std::string storage_key;
    ColdWriteRequest request;
};
YLT_REFL(TokenPayload, version, storage_key, request);

class FileDescriptor {
   public:
    explicit FileDescriptor(int fd) : fd_(fd) {}
    ~FileDescriptor() {
        if (fd_ >= 0) close(fd_);
    }
    int get() const { return fd_; }

   private:
    int fd_;
};

std::string Digest(std::string_view value) {
    std::array<unsigned char, SHA256_DIGEST_LENGTH> bytes{};
    if (!SHA256(reinterpret_cast<const unsigned char*>(value.data()),
                value.size(), bytes.data()))
        return {};
    static constexpr char hex[] = "0123456789abcdef";
    std::string result(bytes.size() * 2, '0');
    for (size_t i = 0; i < bytes.size(); ++i) {
        result[2 * i] = hex[bytes[i] >> 4];
        result[2 * i + 1] = hex[bytes[i] & 15];
    }
    return result;
}

bool SyncDirectory(const std::string& directory) {
    FileDescriptor fd(open(directory.c_str(), O_RDONLY | O_DIRECTORY));
    return fd.get() >= 0 && fsync(fd.get()) == 0;
}

tl::expected<std::string, ErrorCode> ReadFile(const std::string& path) {
    FileDescriptor fd(open(path.c_str(), O_RDONLY));
    if (fd.get() < 0) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    struct stat st{};
    if (fstat(fd.get(), &st) != 0 || st.st_size <= 0 ||
        st.st_size > static_cast<off_t>(kMaxRecordBytes)) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    std::string bytes(static_cast<size_t>(st.st_size), '\0');
    size_t done = 0;
    while (done < bytes.size()) {
        auto n = read(fd.get(), bytes.data() + done, bytes.size() - done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        done += n;
    }
    return bytes;
}

tl::expected<TokenPayload, ErrorCode> Decode(const std::string& bytes) {
    constexpr size_t kDigestBytes = SHA256_DIGEST_LENGTH * 2;
    if (bytes.size() <= kDigestBytes)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    std::string_view payload(bytes.data(), bytes.size() - kDigestBytes);
    if (Digest(payload) != std::string_view(bytes).substr(payload.size())) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    TokenPayload record;
    if (struct_pack::deserialize_to(record, payload) != struct_pack::errc::ok ||
        record.version != kRecordVersion || record.storage_key.empty() ||
        struct_pack::serialize<std::string>(record) != payload) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return record;
}

}  // namespace

ColdWriteJournal::ColdWriteJournal(std::string storage_root)
    : storage_root_(std::move(storage_root)),
      directory_((std::filesystem::path(storage_root_) / "cold_write_journal")
                     .string()) {}

ColdWriteJournal::~ColdWriteJournal() {
    if (lock_fd_ >= 0) close(lock_fd_);
}

tl::expected<void, ErrorCode> ColdWriteJournal::Init() {
    std::lock_guard lock(mutex_);
    if (initialized_) return {};
    std::error_code ec;
    std::filesystem::create_directories(directory_, ec);
    if (ec || !SyncDirectory(storage_root_)) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (lock_fd_ < 0) {
        lock_fd_ =
            open((directory_ + "/.lock").c_str(), O_CREAT | O_RDWR, 0600);
        if (lock_fd_ < 0 || flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
            if (lock_fd_ >= 0) close(lock_fd_);
            lock_fd_ = -1;
            return tl::make_unexpected(ErrorCode::FILE_LOCK_FAIL);
        }
    }
    auto records = ListLocked();
    if (!records) return tl::make_unexpected(records.error());
    initialized_ = true;
    return {};
}

tl::expected<std::unordered_map<std::string, ColdWriteRequest>, ErrorCode>
ColdWriteJournal::ListLocked() {
    std::unordered_map<std::string, ColdWriteRequest> result;
    std::error_code ec;
    for (auto it = std::filesystem::directory_iterator(directory_, ec);
         !ec && it != std::filesystem::directory_iterator(); it.increment(ec)) {
        const auto& path = it->path();
        if (path.filename() == ".lock" || path.extension() == ".tmp") continue;
        if (!it->is_regular_file(ec) || ec || path.extension() != ".token") {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        auto bytes = ReadFile(path.string());
        if (!bytes) return tl::make_unexpected(bytes.error());
        auto record = Decode(*bytes);
        if (!record ||
            path.filename() != Digest(record->storage_key) + ".token") {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (!result.emplace(record->storage_key, record->request).second) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    if (ec) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return result;
}

tl::expected<std::unordered_map<std::string, ColdWriteRequest>, ErrorCode>
ColdWriteJournal::List() {
    std::lock_guard lock(mutex_);
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    return ListLocked();
}

tl::expected<void, ErrorCode> ColdWriteJournal::Put(
    const std::string& storage_key, const ColdWriteRequest& request) {
    std::lock_guard lock(mutex_);
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (storage_key.empty() || storage_key.size() > kMaxRecordBytes ||
        request.key.size() > kMaxRecordBytes ||
        request.tenant.size() > kMaxRecordBytes) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    TokenPayload record{kRecordVersion, storage_key, request};
    auto bytes = struct_pack::serialize<std::string>(record);
    const auto digest = Digest(bytes);
    bytes += digest;
    if (bytes.size() > kMaxRecordBytes || digest.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto path = directory_ + "/" + Digest(storage_key) + ".token";
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    if (exists) {
        auto previous = ReadFile(path);
        if (!previous || !Decode(*previous)) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (*previous != bytes)
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        return SyncDirectory(directory_)
                   ? tl::expected<void, ErrorCode>{}
                   : tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    const auto temporary = path + ".tmp";
    FileDescriptor fd(
        open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600));
    if (fd.get() < 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    size_t done = 0;
    while (done < bytes.size()) {
        auto n = write(fd.get(), bytes.data() + done, bytes.size() - done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        done += n;
    }
    if (fsync(fd.get()) != 0 || rename(temporary.c_str(), path.c_str()) != 0 ||
        !SyncDirectory(directory_)) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<void, ErrorCode> ColdWriteJournal::Remove(
    const std::string& storage_key) {
    std::lock_guard lock(mutex_);
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (storage_key.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const auto path = directory_ + "/" + Digest(storage_key) + ".token";
    if (unlink(path.c_str()) != 0 && errno != ENOENT) {
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (!SyncDirectory(directory_))
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

}  // namespace mooncake
