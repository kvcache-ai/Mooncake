#include "storage/local/log_structured/metadata.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>
#include <type_traits>

#include <ylt/struct_pack.hpp>

#include "crc32c.h"

namespace mooncake::logstructured {
namespace {

constexpr uint32_t kMetadataMagic = 0x444D434D;
constexpr uint16_t kMetadataVersion = 1;
constexpr uint64_t kMetadataCommitMagic = 0x54494D4D4F434D4DULL;
constexpr size_t kHeaderSize = 24;
constexpr size_t kFooterSize = 8;
constexpr size_t kMagicOffset = 0;
constexpr size_t kVersionOffset = 4;
constexpr size_t kKindOffset = 6;
constexpr size_t kPayloadLengthOffset = 8;
constexpr size_t kPayloadChecksumOffset = 16;
constexpr size_t kHeaderChecksumOffset = 20;
constexpr size_t kHeaderChecksumInputSize = kHeaderChecksumOffset;

enum class ArtifactKind : uint16_t {
    kCheckpoint = 1,
    kManifest = 2,
};

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

tl::expected<void, MetadataError> SyncDirectory(const std::string& path) {
    const int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return tl::unexpected(MetadataError::kIoError);
    const int result = fsync(fd);
    const int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    if (result != 0) return tl::unexpected(MetadataError::kIoError);
    return {};
}

bool IsSafeFilename(std::string_view filename) {
    return !filename.empty() && filename != "." && filename != ".." &&
           filename.find('/') == std::string_view::npos;
}

bool IsNumberedArtifact(std::string_view filename, std::string_view prefix) {
    if (!filename.starts_with(prefix) || filename.size() == prefix.size()) {
        return false;
    }
    filename.remove_prefix(prefix.size());
    return std::all_of(filename.begin(), filename.end(),
                       [](char value) { return value >= '0' && value <= '9'; });
}

std::string NumberedFilename(std::string_view prefix, uint64_t generation) {
    std::ostringstream stream;
    stream << prefix << '-' << std::setw(20) << std::setfill('0') << generation;
    return stream.str();
}

template <typename T>
std::string EncodeArtifact(ArtifactKind kind, const T& value) {
    auto payload = struct_pack::serialize<std::vector<char>>(value);
    std::string encoded(kHeaderSize + payload.size() + kFooterSize, '\0');
    WriteLittleEndian<uint32_t>(encoded.data() + kMagicOffset, kMetadataMagic);
    WriteLittleEndian<uint16_t>(encoded.data() + kVersionOffset,
                                kMetadataVersion);
    WriteLittleEndian<uint16_t>(encoded.data() + kKindOffset,
                                static_cast<uint16_t>(kind));
    WriteLittleEndian<uint64_t>(encoded.data() + kPayloadLengthOffset,
                                payload.size());
    WriteLittleEndian<uint32_t>(encoded.data() + kPayloadChecksumOffset,
                                Crc32cValue(payload.data(), payload.size()));
    WriteLittleEndian<uint32_t>(
        encoded.data() + kHeaderChecksumOffset,
        Crc32cValue(encoded.data(), kHeaderChecksumInputSize));
    std::copy(payload.begin(), payload.end(), encoded.begin() + kHeaderSize);
    WriteLittleEndian<uint64_t>(encoded.data() + encoded.size() - kFooterSize,
                                kMetadataCommitMagic);
    return encoded;
}

template <typename T>
tl::expected<T, MetadataError> DecodeArtifact(std::span<const char> encoded,
                                              ArtifactKind expected_kind) {
    if (encoded.size() < kHeaderSize + kFooterSize ||
        ReadLittleEndian<uint32_t>(encoded.data() + kMagicOffset) !=
            kMetadataMagic ||
        ReadLittleEndian<uint16_t>(encoded.data() + kVersionOffset) !=
            kMetadataVersion ||
        ReadLittleEndian<uint16_t>(encoded.data() + kKindOffset) !=
            static_cast<uint16_t>(expected_kind) ||
        Crc32cValue(encoded.data(), kHeaderChecksumInputSize) !=
            ReadLittleEndian<uint32_t>(encoded.data() +
                                       kHeaderChecksumOffset)) {
        return tl::unexpected(MetadataError::kCorruptData);
    }
    const uint64_t payload_length =
        ReadLittleEndian<uint64_t>(encoded.data() + kPayloadLengthOffset);
    if (payload_length != encoded.size() - kHeaderSize - kFooterSize ||
        ReadLittleEndian<uint64_t>(encoded.data() + encoded.size() -
                                   kFooterSize) != kMetadataCommitMagic ||
        Crc32cValue(encoded.data() + kHeaderSize, payload_length) !=
            ReadLittleEndian<uint32_t>(encoded.data() +
                                       kPayloadChecksumOffset)) {
        return tl::unexpected(MetadataError::kCorruptData);
    }
    T result;
    std::span<const char> payload(encoded.data() + kHeaderSize,
                                  static_cast<size_t>(payload_length));
    if (struct_pack::deserialize_to(result, payload) != struct_pack::errc::ok) {
        return tl::unexpected(MetadataError::kCorruptData);
    }
    return result;
}

tl::expected<std::string, MetadataError> ReadFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary | std::ios::ate);
    if (!input.is_open()) return tl::unexpected(MetadataError::kNotFound);
    const auto end = input.tellg();
    if (end < 0 ||
        static_cast<uint64_t>(end) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return tl::unexpected(MetadataError::kIoError);
    }
    std::string contents(static_cast<size_t>(end), '\0');
    input.seekg(0);
    if (!contents.empty() &&
        !input.read(contents.data(),
                    static_cast<std::streamsize>(contents.size()))) {
        return tl::unexpected(MetadataError::kIoError);
    }
    return contents;
}

tl::expected<void, MetadataError> AtomicWrite(const std::string& root_path,
                                              const std::string& filename,
                                              std::string_view contents) {
    if (!IsSafeFilename(filename)) {
        return tl::unexpected(MetadataError::kInvalidArgument);
    }
    const std::string final_path = root_path + "/" + filename;
    const std::string temporary_path = final_path + ".tmp";
    const int fd = open(temporary_path.c_str(),
                        O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0644);
    if (fd < 0) return tl::unexpected(MetadataError::kIoError);
    bool success = WriteAll(fd, contents.data(), contents.size()) &&
                   fsync(fd) == 0 && close(fd) == 0;
    if (!success) {
        const int saved_errno = errno;
        close(fd);
        unlink(temporary_path.c_str());
        errno = saved_errno;
        return tl::unexpected(MetadataError::kIoError);
    }
    if (rename(temporary_path.c_str(), final_path.c_str()) != 0) {
        unlink(temporary_path.c_str());
        return tl::unexpected(MetadataError::kIoError);
    }
    return SyncDirectory(root_path);
}

}  // namespace

tl::expected<std::string, MetadataError> WriteCheckpoint(
    const std::string& root_path, uint64_t generation,
    const CheckpointState& checkpoint) {
    if (checkpoint.format_version != 1) {
        return tl::unexpected(MetadataError::kInvalidArgument);
    }
    const std::string filename = NumberedFilename("CHECKPOINT", generation);
    auto written =
        AtomicWrite(root_path, filename,
                    EncodeArtifact(ArtifactKind::kCheckpoint, checkpoint));
    if (!written) return tl::unexpected(written.error());
    return filename;
}

tl::expected<CheckpointState, MetadataError> LoadCheckpoint(
    const std::string& root_path, const std::string& checkpoint_file) {
    if (!IsSafeFilename(checkpoint_file)) {
        return tl::unexpected(MetadataError::kInvalidArgument);
    }
    auto contents = ReadFile(root_path + "/" + checkpoint_file);
    if (!contents) return tl::unexpected(contents.error());
    auto checkpoint = DecodeArtifact<CheckpointState>(
        std::span<const char>(contents->data(), contents->size()),
        ArtifactKind::kCheckpoint);
    if (!checkpoint || checkpoint->format_version != 1) {
        return tl::unexpected(MetadataError::kCorruptData);
    }
    return checkpoint.value();
}

tl::expected<void, MetadataError> PublishManifest(
    const std::string& root_path, const ManifestState& manifest,
    std::function<bool()> fail_before_current) {
    if (manifest.format_version != 1 || manifest.generation == 0 ||
        !IsSafeFilename(manifest.checkpoint_file) ||
        !IsSafeFilename(manifest.wal_file)) {
        return tl::unexpected(MetadataError::kInvalidArgument);
    }
    const std::string manifest_file =
        NumberedFilename("MANIFEST", manifest.generation);
    auto written =
        AtomicWrite(root_path, manifest_file,
                    EncodeArtifact(ArtifactKind::kManifest, manifest));
    if (!written) return written;
    if (fail_before_current && fail_before_current()) {
        return tl::unexpected(MetadataError::kIoError);
    }
    return AtomicWrite(root_path, "CURRENT", manifest_file + "\n");
}

tl::expected<ManifestState, MetadataError> LoadCurrentManifest(
    const std::string& root_path) {
    auto current = ReadFile(root_path + "/CURRENT");
    if (!current) return tl::unexpected(current.error());
    while (!current->empty() &&
           (current->back() == '\n' || current->back() == '\r')) {
        current->pop_back();
    }
    if (!IsSafeFilename(*current) ||
        !std::string_view(*current).starts_with("MANIFEST-")) {
        return tl::unexpected(MetadataError::kCorruptData);
    }
    auto contents = ReadFile(root_path + "/" + *current);
    if (!contents) return tl::unexpected(contents.error());
    auto manifest = DecodeArtifact<ManifestState>(
        std::span<const char>(contents->data(), contents->size()),
        ArtifactKind::kManifest);
    if (!manifest || manifest->format_version != 1) {
        return tl::unexpected(MetadataError::kCorruptData);
    }
    return manifest.value();
}

tl::expected<void, MetadataError> CleanupMetadataArtifacts(
    const std::string& root_path, uint64_t current_generation,
    const std::string& current_wal_file) {
    if (!IsSafeFilename(current_wal_file)) {
        return tl::unexpected(MetadataError::kInvalidArgument);
    }
    const std::string current_manifest =
        current_generation == 0
            ? std::string{}
            : NumberedFilename("MANIFEST", current_generation);
    const std::string current_checkpoint =
        current_generation == 0
            ? std::string{}
            : NumberedFilename("CHECKPOINT", current_generation);
    std::error_code error;
    bool removed_any = false;
    for (const auto& entry :
         std::filesystem::directory_iterator(root_path, error)) {
        if (error) return tl::unexpected(MetadataError::kIoError);
        if (!entry.is_regular_file(error)) {
            error.clear();
            continue;
        }
        const std::string filename = entry.path().filename().string();
        const bool temporary = filename.ends_with(".tmp");
        std::string_view stable_name = filename;
        if (temporary && stable_name.ends_with(".tmp")) {
            stable_name.remove_suffix(4);
        }
        const bool managed = IsNumberedArtifact(stable_name, "MANIFEST-") ||
                             IsNumberedArtifact(stable_name, "CHECKPOINT-") ||
                             IsNumberedArtifact(stable_name, "WAL-") ||
                             filename == "CURRENT.tmp";
        if (!managed || filename == "CURRENT" || filename == current_manifest ||
            filename == current_checkpoint || filename == current_wal_file) {
            continue;
        }
        if (unlink(entry.path().c_str()) != 0 && errno != ENOENT) {
            return tl::unexpected(MetadataError::kIoError);
        }
        removed_any = true;
    }
    if (!removed_any) return {};
    return SyncDirectory(root_path);
}

tl::expected<void, MetadataError> RemoveFileDurably(
    const std::string& root_path, const std::string& filename) {
    if (!IsSafeFilename(filename)) {
        return tl::unexpected(MetadataError::kInvalidArgument);
    }
    if (unlink((root_path + "/" + filename).c_str()) != 0 && errno != ENOENT) {
        return tl::unexpected(MetadataError::kIoError);
    }
    return SyncDirectory(root_path);
}

}  // namespace mooncake::logstructured
