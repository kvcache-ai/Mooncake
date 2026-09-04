#include "storage/local/log_structured/storage_directory.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <filesystem>
#include <limits>
#include <string_view>
#include <type_traits>

#include "crc32c.h"
#include "random.h"

namespace mooncake::logstructured {
namespace {

constexpr uint32_t kIdentityMagic = 0x494C434D;
constexpr uint16_t kIdentityVersion = 1;
constexpr uint64_t kIdentityCommitMagic = 0x54494D4D4F434449ULL;
constexpr size_t kIdentitySize = 40;
constexpr size_t kMagicOffset = 0;
constexpr size_t kVersionOffset = 4;
constexpr size_t kHighOffset = 8;
constexpr size_t kLowOffset = 16;
constexpr size_t kChecksumOffset = 24;
constexpr size_t kChecksumInputSize = kChecksumOffset;
constexpr size_t kCommitMagicOffset = 32;

template <typename T>
void WriteLittleEndian(char* output, T value) {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned unsigned_value = static_cast<Unsigned>(value);
    for (size_t i = 0; i < sizeof(T); ++i) {
        output[i] = static_cast<char>((unsigned_value >> (i * 8)) & 0xff);
    }
}

template <typename T>
T ReadLittleEndian(const char* input) {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<Unsigned>(static_cast<unsigned char>(input[i]))
                 << (i * 8);
    }
    return static_cast<T>(value);
}

bool WriteAll(int fd, const char* data, size_t length) {
    size_t written = 0;
    while (written < length) {
        const ssize_t result = write(fd, data + written, length - written);
        if (result < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (result == 0) return false;
        written += static_cast<size_t>(result);
    }
    return true;
}

bool ReadAll(int fd, char* data, size_t length) {
    size_t read_bytes = 0;
    while (read_bytes < length) {
        const ssize_t result = read(fd, data + read_bytes, length - read_bytes);
        if (result < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (result == 0) return false;
        read_bytes += static_cast<size_t>(result);
    }
    return true;
}

bool DirectoryHasUserData(const std::string& root_path) {
    std::error_code error;
    for (const auto& entry :
         std::filesystem::directory_iterator(root_path, error)) {
        if (error) return true;
        const std::string name = entry.path().filename().string();
        if (name != "LOCK" && name != "IDENTITY.tmp") return true;
    }
    return error.operator bool();
}

tl::expected<StorageIdentity, StorageDirectoryError> ReadIdentity(
    const std::string& path) {
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::unexpected(StorageDirectoryError::kIoError);
    std::array<char, kIdentitySize> bytes{};
    const bool read = ReadAll(fd, bytes.data(), bytes.size());
    char extra = 0;
    const ssize_t extra_read = ::read(fd, &extra, 1);
    close(fd);
    if (!read || extra_read != 0 ||
        ReadLittleEndian<uint32_t>(bytes.data() + kMagicOffset) !=
            kIdentityMagic ||
        ReadLittleEndian<uint16_t>(bytes.data() + kVersionOffset) !=
            kIdentityVersion ||
        Crc32cValue(bytes.data(), kChecksumInputSize) !=
            ReadLittleEndian<uint32_t>(bytes.data() + kChecksumOffset) ||
        ReadLittleEndian<uint64_t>(bytes.data() + kCommitMagicOffset) !=
            kIdentityCommitMagic) {
        return tl::unexpected(StorageDirectoryError::kCorruptIdentity);
    }
    StorageIdentity identity{
        .high = ReadLittleEndian<uint64_t>(bytes.data() + kHighOffset),
        .low = ReadLittleEndian<uint64_t>(bytes.data() + kLowOffset)};
    if (identity == StorageIdentity{}) {
        return tl::unexpected(StorageDirectoryError::kCorruptIdentity);
    }
    return identity;
}

tl::expected<StorageIdentity, StorageDirectoryError> CreateIdentity(
    const std::string& root_path) {
    StorageIdentity identity{.high = randomUniform<uint64_t>(
                                 0, std::numeric_limits<uint64_t>::max()),
                             .low = randomUniform<uint64_t>(
                                 0, std::numeric_limits<uint64_t>::max())};
    if (identity == StorageIdentity{}) identity.low = 1;

    std::array<char, kIdentitySize> bytes{};
    WriteLittleEndian<uint32_t>(bytes.data() + kMagicOffset, kIdentityMagic);
    WriteLittleEndian<uint16_t>(bytes.data() + kVersionOffset,
                                kIdentityVersion);
    WriteLittleEndian<uint64_t>(bytes.data() + kHighOffset, identity.high);
    WriteLittleEndian<uint64_t>(bytes.data() + kLowOffset, identity.low);
    WriteLittleEndian<uint32_t>(bytes.data() + kChecksumOffset,
                                Crc32cValue(bytes.data(), kChecksumInputSize));
    WriteLittleEndian<uint64_t>(bytes.data() + kCommitMagicOffset,
                                kIdentityCommitMagic);

    const std::string temporary_path = root_path + "/IDENTITY.tmp";
    const std::string final_path = root_path + "/IDENTITY";
    const int fd = open(temporary_path.c_str(),
                        O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0644);
    if (fd < 0) return tl::unexpected(StorageDirectoryError::kIoError);
    const bool data_written = WriteAll(fd, bytes.data(), bytes.size());
    const bool data_synced = data_written && fsync(fd) == 0;
    const int close_result = close(fd);
    if (!data_synced || close_result != 0) {
        const int saved_errno = errno;
        unlink(temporary_path.c_str());
        errno = saved_errno;
        return tl::unexpected(StorageDirectoryError::kIoError);
    }
    if (rename(temporary_path.c_str(), final_path.c_str()) != 0) {
        unlink(temporary_path.c_str());
        return tl::unexpected(StorageDirectoryError::kIoError);
    }
    const int directory_fd =
        open(root_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory_fd < 0) {
        return tl::unexpected(StorageDirectoryError::kIoError);
    }
    const int sync_result = fsync(directory_fd);
    close(directory_fd);
    if (sync_result != 0) {
        return tl::unexpected(StorageDirectoryError::kIoError);
    }
    return identity;
}

}  // namespace

StorageDirectory::StorageDirectory(std::string root_path, int lock_fd,
                                   StorageIdentity identity)
    : root_path_(std::move(root_path)),
      lock_fd_(lock_fd),
      identity_(identity) {}

StorageDirectory::~StorageDirectory() {
    if (lock_fd_ >= 0) {
        flock(lock_fd_, LOCK_UN);
        close(lock_fd_);
    }
}

tl::expected<std::unique_ptr<StorageDirectory>, StorageDirectoryError>
StorageDirectory::Open(std::string root_path) {
    if (root_path.empty()) {
        return tl::unexpected(StorageDirectoryError::kInvalidArgument);
    }
    std::error_code error;
    std::filesystem::create_directories(root_path, error);
    if (error) return tl::unexpected(StorageDirectoryError::kIoError);

    const std::string lock_path = root_path + "/LOCK";
    const int lock_fd =
        open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (lock_fd < 0) return tl::unexpected(StorageDirectoryError::kIoError);
    if (flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
        const auto result = errno == EWOULDBLOCK
                                ? StorageDirectoryError::kAlreadyMounted
                                : StorageDirectoryError::kIoError;
        close(lock_fd);
        return tl::unexpected(result);
    }

    const std::string identity_path = root_path + "/IDENTITY";
    tl::expected<StorageIdentity, StorageDirectoryError> identity =
        tl::unexpected(StorageDirectoryError::kIoError);
    if (std::filesystem::exists(identity_path, error)) {
        if (!error) identity = ReadIdentity(identity_path);
    } else if (!error && !DirectoryHasUserData(root_path)) {
        identity = CreateIdentity(root_path);
    } else if (!error) {
        identity = tl::unexpected(StorageDirectoryError::kUnrecognizedFormat);
    }
    if (!identity) {
        flock(lock_fd, LOCK_UN);
        close(lock_fd);
        return tl::unexpected(identity.error());
    }
    return std::unique_ptr<StorageDirectory>(
        new StorageDirectory(std::move(root_path), lock_fd, identity.value()));
}

}  // namespace mooncake::logstructured
