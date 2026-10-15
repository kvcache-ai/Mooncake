#include "handoff/handoff_lifecycle.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "crc32c.h"

namespace mooncake::handoff {

namespace {

namespace fs = std::filesystem;

constexpr char kReadyHeader[] = "MOONCAKE-HANDOFF-READY";
constexpr uint32_t kReadyVersion = 1;
constexpr char kReadyTmp[] = "READY.tmp";

std::string Errno(const std::string& what, const fs::path& path) {
    return what + " " + path.string() + ": " + std::strerror(errno);
}

bool FsyncPath(const fs::path& path, bool directory) {
    const int fd =
        ::open(path.c_str(),
               (directory ? O_RDONLY | O_DIRECTORY : O_RDONLY) | O_CLOEXEC);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

// Writes `data` to `path` and fsyncs the file (not the directory).
bool WriteFileDurable(const fs::path& path, const std::string& data) {
    const int fd =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return false;
    const char* p = data.data();
    size_t left = data.size();
    while (left > 0) {
        const ssize_t n = ::write(fd, p, left);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            ::close(fd);
            return false;
        }
        p += n;
        left -= static_cast<size_t>(n);
    }
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

bool ReadSmallFile(const fs::path& path, std::string* out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::ostringstream ss;
    ss << in.rdbuf();
    *out = ss.str();
    return true;
}

bool EndsWith(const std::string& s, std::string_view suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// Removes `name` under root if it belongs to the layout and is not `keep`.
// Returns false on an error other than "does not exist".
bool RemoveLayoutEntries(const fs::path& root, const std::string& keep,
                         bool markers, std::string* error) {
    std::error_code ec;
    std::vector<fs::path> victims;
    for (auto it = fs::directory_iterator(root, ec);
         !ec && it != fs::directory_iterator(); it.increment(ec)) {
        const std::string name = it->path().filename().string();
        std::error_code type_ec;
        const bool snapshot_dir =
            name.rfind(kSnapshotPrefix, 0) == 0 && it->is_directory(type_ec);
        const bool marker =
            name == kReadyMarker || name == kClaimedMarker || name == kReadyTmp;
        if ((snapshot_dir && name != keep) || (markers && marker)) {
            victims.push_back(it->path());
        }
    }
    if (ec) {
        *error = "list " + root.string() + ": " + ec.message();
        return false;
    }
    // Markers first: once READY is gone nothing here is restorable anymore.
    std::stable_partition(victims.begin(), victims.end(), [](const auto& p) {
        std::error_code type_ec;
        return !fs::is_directory(p, type_ec);
    });
    for (const auto& path : victims) {
        fs::remove_all(path, ec);
        if (ec) {
            *error = "remove " + path.string() + ": " + ec.message();
            return false;
        }
    }
    return true;
}

}  // namespace

// ------------------------------------------------------------------ marker

std::string EncodeReadyMarker(const ReadyMarker& marker) {
    std::ostringstream body;
    body << "snapshot=" << marker.snapshot << "\n"
         << "created_at_ms=" << marker.created_at_ms << "\n"
         << "objects=" << marker.objects << "\n"
         << "bytes=" << marker.bytes << "\n"
         << "disk_replicas=" << marker.disk_replicas << "\n"
         << "disk_only_objects=" << marker.disk_only_objects << "\n";
    const std::string b = body.str();
    std::ostringstream out;
    out << kReadyHeader << " " << kReadyVersion << " "
        << Crc32cValue(b.data(), b.size()) << " " << b.size() << "\n"
        << b;
    return out.str();
}

tl::expected<ReadyMarker, std::string> DecodeReadyMarker(
    std::string_view file) {
    const auto nl = file.find('\n');
    if (nl == std::string_view::npos) {
        return tl::make_unexpected("marker: no header");
    }
    std::istringstream header{std::string(file.substr(0, nl))};
    std::string magic;
    uint32_t version = 0, crc = 0;
    uint64_t size = 0;
    if (!(header >> magic >> version >> crc >> size) || magic != kReadyHeader) {
        return tl::make_unexpected("marker: bad header");
    }
    if (version != kReadyVersion) {
        return tl::make_unexpected("marker: unknown version");
    }
    const std::string_view body = file.substr(nl + 1);
    if (body.size() != size || Crc32cValue(body.data(), body.size()) != crc) {
        return tl::make_unexpected("marker: size or crc mismatch");
    }
    ReadyMarker marker;
    std::istringstream lines{std::string(body)};
    std::string line;
    while (std::getline(lines, line)) {
        const auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        const std::string key = line.substr(0, eq);
        const std::string value = line.substr(eq + 1);
        try {
            if (key == "snapshot") {
                marker.snapshot = value;
            } else if (key == "created_at_ms") {
                marker.created_at_ms = std::stoll(value);
            } else if (key == "objects") {
                marker.objects = std::stoull(value);
            } else if (key == "bytes") {
                marker.bytes = std::stoull(value);
            } else if (key == "disk_replicas") {
                marker.disk_replicas = std::stoull(value);
            } else if (key == "disk_only_objects") {
                marker.disk_only_objects = std::stoull(value);
            }
        } catch (const std::exception&) {
            return tl::make_unexpected("marker: bad value for " + key);
        }
    }
    if (!IsSnapshotName(marker.snapshot)) {
        return tl::make_unexpected("marker: bad snapshot name");
    }
    return marker;
}

bool IsSnapshotName(const std::string& name) {
    return name.size() > std::strlen(kSnapshotPrefix) &&
           name.rfind(kSnapshotPrefix, 0) == 0 &&
           name.find('/') == std::string::npos && name != "." && name != ".." &&
           !EndsWith(name, kTmpSuffix);
}

std::string NewSnapshotName(int64_t now_ms) {
    return std::string(kSnapshotPrefix) + std::to_string(now_ms) + "-" +
           std::to_string(::getpid());
}

// -------------------------------------------------------------------- lock

HandoffDirLock::~HandoffDirLock() {
    if (fd_ >= 0) ::close(fd_);
}

bool HandoffDirLock::Acquire(const std::string& root, int64_t wait_ms,
                             std::string* error) {
    std::error_code ec;
    fs::create_directories(root, ec);
    if (ec) {
        *error = "create " + root + ": " + ec.message();
        return false;
    }
    const fs::path path = fs::path(root) / kLockFile;
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        *error = Errno("open", path);
        return false;
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_ms);
    for (;;) {
        if (::flock(fd, LOCK_EX | LOCK_NB) == 0) {
            fd_ = fd;
            return true;
        }
        if (errno != EWOULDBLOCK && errno != EINTR) {
            *error = Errno("flock", path);
            ::close(fd);
            return false;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            *error = "another process holds " + path.string();
            ::close(fd);
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

tl::expected<void, std::string> WritePoison(const std::string& root) {
    const fs::path path = fs::path(root) / kPoisonFile;
    if (!WriteFileDurable(path, std::to_string(::getpid()) + "\n") ||
        !FsyncPath(root, /*directory=*/true)) {
        return tl::make_unexpected(Errno("write", path));
    }
    return {};
}

bool PoisonExists(const std::string& root) {
    return ProbePath((fs::path(root) / kPoisonFile).string()) !=
           PathState::kAbsent;
}

PathState ProbePath(const std::string& path) {
    std::error_code ec;
    const auto status = fs::symlink_status(path, ec);
    if (ec) {
        return ec == std::errc::no_such_file_or_directory ? PathState::kAbsent
                                                          : PathState::kUnknown;
    }
    return status.type() == fs::file_type::not_found ? PathState::kAbsent
                                                     : PathState::kPresent;
}

std::optional<ReadyMarker> PeekReadyMarker(const std::string& root) {
    std::string text;
    if (!ReadSmallFile(fs::path(root) / kReadyMarker, &text)) {
        return std::nullopt;
    }
    auto marker = DecodeReadyMarker(text);
    if (!marker) return std::nullopt;
    return *marker;
}

// ------------------------------------------------------------------ writer

tl::expected<void, std::string> ResetHandoffDir(const std::string& root) {
    std::error_code ec;
    fs::create_directories(root, ec);
    if (ec) {
        return tl::make_unexpected("create " + root + ": " + ec.message());
    }
    std::string error;
    if (!RemoveLayoutEntries(root, /*keep=*/"", /*markers=*/true, &error)) {
        return tl::make_unexpected(error);
    }
    if (!FsyncPath(root, /*directory=*/true)) {
        return tl::make_unexpected(Errno("fsync", root));
    }
    return {};
}

tl::expected<void, std::string> PublishSnapshot(const std::string& root,
                                                const std::string& name,
                                                const ReadyMarker& marker) {
    if (!IsSnapshotName(name) || marker.snapshot != name) {
        return tl::make_unexpected("bad snapshot name " + name);
    }
    const fs::path dir(root);
    std::error_code ec;
    fs::rename(dir / (name + kTmpSuffix), dir / name, ec);
    if (ec) {
        return tl::make_unexpected("rename " + name + ": " + ec.message());
    }
    if (!FsyncPath(dir, /*directory=*/true)) {
        return tl::make_unexpected(Errno("fsync", dir));
    }
    if (!WriteFileDurable(dir / kReadyTmp, EncodeReadyMarker(marker))) {
        return tl::make_unexpected(Errno("write", dir / kReadyTmp));
    }
    fs::rename(dir / kReadyTmp, dir / kReadyMarker, ec);
    if (ec) {
        return tl::make_unexpected("rename READY: " + ec.message());
    }
    if (!FsyncPath(dir, /*directory=*/true)) {
        return tl::make_unexpected(Errno("fsync", dir));
    }
    return {};
}

// ----------------------------------------------------------------- startup

const char* ToString(ClaimState state) {
    switch (state) {
        case ClaimState::kNone:
            return "none";
        case ClaimState::kClaimed:
            return "claimed";
        case ClaimState::kAbortedPrevious:
            return "aborted_previous";
        case ClaimState::kPoisoned:
            return "poisoned";
        case ClaimState::kInvalid:
            return "invalid";
        case ClaimState::kError:
            return "error";
    }
    return "unknown";
}

ClaimResult ClaimHandoffSnapshot(const std::string& root) {
    ClaimResult result;
    const fs::path dir(root);
    std::error_code ec;
    std::string error;
    const auto probe = [&dir](const char* name) {
        return ProbePath((dir / name).string());
    };
    // After a failed discard: a READY may survive unless it is proven gone
    // and that removal is durable.
    const auto fail = [&](std::string detail, bool fsynced) {
        result.state = ClaimState::kError;
        result.detail = std::move(detail);
        result.ready_may_remain =
            !fsynced || probe(kReadyMarker) != PathState::kAbsent;
    };
    auto discard_all = [&](ClaimState state, std::string detail) {
        result.state = state;
        result.detail = std::move(detail);
        if (!RemoveLayoutEntries(dir, "", /*markers=*/true, &error)) {
            fail(result.detail + "; " + error, /*fsynced=*/false);
            return;
        }
        // POISON goes last: until here a crash leaves it in place.
        fs::remove(dir / kPoisonFile, ec);
        if (!FsyncPath(dir, /*directory=*/true)) {
            // The removal of READY may not survive a crash.
            fail(result.detail + "; " + Errno("fsync", dir), /*fsynced=*/false);
        }
    };

    // A stat error is never read as "absent": POISON or CLAIMED of unknown
    // state discard everything, READY of unknown state is an error.
    if (probe(kPoisonFile) != PathState::kAbsent) {
        discard_all(ClaimState::kPoisoned,
                    "another master served while the lock was held (or "
                    "POISON could not be checked)");
        return result;
    }
    if (probe(kClaimedMarker) != PathState::kAbsent) {
        discard_all(ClaimState::kAbortedPrevious,
                    "CLAIMED found: the previous restore attempt did not "
                    "finish (or CLAIMED could not be checked)");
        return result;
    }
    const PathState ready = probe(kReadyMarker);
    if (ready == PathState::kUnknown) {
        fail("cannot check " + (dir / kReadyMarker).string(),
             /*fsynced=*/true);
        return result;
    }
    if (ready == PathState::kAbsent) {
        // Nothing to restore; drop half-written leftovers. A failure here
        // leaves nothing restorable behind.
        if (!RemoveLayoutEntries(dir, "", /*markers=*/true, &error)) {
            result.state = ClaimState::kError;
            result.detail = error;
        }
        return result;
    }
    // The claim: from here on this snapshot gets no second attempt.
    fs::rename(dir / kReadyMarker, dir / kClaimedMarker, ec);
    if (ec || !FsyncPath(dir, /*directory=*/true)) {
        discard_all(ClaimState::kError,
                    "claim READY: " + (ec ? ec.message() : "fsync failed"));
        // ready_may_remain stays false only if everything was removed and
        // fsynced: then serving cold is safe although the claim failed.
        return result;
    }
    std::string text;
    if (!ReadSmallFile(dir / kClaimedMarker, &text)) {
        discard_all(ClaimState::kInvalid, "read CLAIMED failed");
        return result;
    }
    auto marker = DecodeReadyMarker(text);
    if (!marker) {
        discard_all(ClaimState::kInvalid, marker.error());
        return result;
    }
    if (!fs::is_directory(dir / marker->snapshot, ec)) {
        discard_all(ClaimState::kInvalid,
                    "snapshot directory " + marker->snapshot + " missing");
        return result;
    }
    // Leftovers other than the claimed snapshot can never be restored.
    if (!RemoveLayoutEntries(dir, marker->snapshot, /*markers=*/false,
                             &error)) {
        LOG(WARNING) << "handoff: cleanup of " << root << " failed: " << error;
    }
    fs::remove(dir / kReadyTmp, ec);
    result.state = ClaimState::kClaimed;
    result.marker = *marker;
    result.snapshot_dir = (dir / marker->snapshot).string();
    return result;
}

void ReleaseClaim(const std::string& root, const ClaimResult& claim) {
    const fs::path dir(root);
    std::error_code ec;
    if (claim.state == ClaimState::kClaimed && !claim.snapshot_dir.empty()) {
        fs::remove_all(claim.snapshot_dir, ec);
        if (ec) {
            LOG(WARNING) << "handoff: remove " << claim.snapshot_dir
                         << " failed: " << ec.message();
        }
    }
    fs::remove(dir / kClaimedMarker, ec);
    (void)FsyncPath(dir, /*directory=*/true);
}

void WriteLastShutdown(const std::string& root, const std::string& json) {
    const fs::path path = fs::path(root) / kLastShutdownFile;
    const fs::path tmp =
        fs::path(root) / (std::string(kLastShutdownFile) + kTmpSuffix);
    std::error_code ec;
    if (WriteFileDurable(tmp, json + "\n")) {
        fs::rename(tmp, path, ec);
        (void)FsyncPath(root, /*directory=*/true);
    }
}

std::string ReadLastShutdown(const std::string& root) {
    std::string text;
    if (!ReadSmallFile(fs::path(root) / kLastShutdownFile, &text)) return "";
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
        text.pop_back();
    }
    return text;
}

}  // namespace mooncake::handoff
