#include "traffic/file_sink.h"

#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <system_error>
#include <vector>

#include <glog/logging.h>

#include "crc32c.h"
#include "traffic/traffic_format.h"

namespace mooncake::traffic {
namespace {

// writev rejects more than IOV_MAX segments, so a block is written in slices of
// that size instead of letting a large iovec become a write failure.
constexpr int kMaxIovCount = IOV_MAX;

int SyncFile(int fd) {
#if defined(__linux__)
    return ::fdatasync(fd);
#else
    return ::fsync(fd);
#endif
}

bool WriteAll(int fd, const void* data, std::size_t size) {
    const char* cursor = static_cast<const char*>(data);
    while (size > 0) {
        const ssize_t written = ::write(fd, cursor, size);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (written == 0) {
            return false;
        }
        cursor += written;
        size -= static_cast<std::size_t>(written);
    }
    return true;
}

// Two sessions started in the same millisecond in the same process must stay
// distinguishable, so the id carries the start time, the pid, and a per-process
// sequence. The file format is little-endian (traffic_format.h), so copying
// these fields writes the same bytes on every platform Mooncake builds on.
std::array<std::uint8_t, 16> MakeSessionId(std::uint64_t start_ms,
                                           std::uint64_t sequence) {
    const std::uint32_t pid = static_cast<std::uint32_t>(::getpid());
    const std::uint32_t seq = static_cast<std::uint32_t>(sequence);
    std::array<std::uint8_t, 16> id{};
    std::memcpy(id.data(), &start_ms, sizeof(start_ms));
    std::memcpy(id.data() + sizeof(start_ms), &pid, sizeof(pid));
    std::memcpy(id.data() + sizeof(start_ms) + sizeof(pid), &seq, sizeof(seq));
    return id;
}

std::string MakeSessionDir(const std::string& output_dir,
                           std::uint64_t start_ms) {
    std::error_code ec;
    const bool created = std::filesystem::create_directories(output_dir, ec);
    if (ec) {
        LOG(ERROR) << "traffic: create output directory failed: "
                   << ec.message();
        return {};
    }
    if (created && ::chmod(output_dir.c_str(), 0700) != 0) {
        LOG(ERROR) << "traffic: chmod output directory failed: " << output_dir;
        return {};
    }
    const std::string base = output_dir + "/session-" +
                             std::to_string(start_ms) + "-" +
                             std::to_string(::getpid());
    // A session only accepts a directory it created itself. Checking whether
    // the name exists and then creating it lets two sinks that start in the
    // same millisecond pick the same path and overwrite each other's first
    // segment, so the create call decides instead and a taken name moves to the
    // next suffix.
    for (int suffix = 0; suffix < 1024; ++suffix) {
        const std::string candidate =
            suffix == 0 ? base : base + "-" + std::to_string(suffix);
        if (!std::filesystem::create_directory(candidate, ec)) {
            if (ec) {
                LOG(ERROR) << "traffic: create session directory failed: "
                           << ec.message();
                return {};
            }
            continue;
        }
        if (::chmod(candidate.c_str(), 0700) != 0) {
            LOG(ERROR) << "traffic: chmod session directory failed: "
                       << candidate;
            return {};
        }
        return candidate;
    }
    LOG(ERROR) << "traffic: cannot find a free session directory under "
               << output_dir;
    return {};
}

}  // namespace

FileSink::FileSink(std::uint64_t segment_bytes, std::uint64_t segment_seconds,
                   std::uint64_t max_total_bytes, RedactionMode redaction)
    : segment_bytes_(segment_bytes),
      segment_seconds_(segment_seconds),
      max_total_bytes_(max_total_bytes),
      redaction_(redaction) {}

FileSink::~FileSink() { Close(); }

bool FileSink::OpenSession(const std::string& output_dir,
                           std::uint64_t start_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    if (fd_ >= 0 && !CloseSegment(true)) {
        // The previous segment could not be finalized; keep failed() true so
        // the caller learns that its last records did not close cleanly.
        return false;
    }
    failed_ = false;
    quota_reached_ = false;
    iovec_scratch_.clear();
    iovec_scratch_.reserve(1024);
    segment_seq_ = 0;
    block_count_ = 0;
    record_count_ = 0;
    record_bytes_ = 0;
    bytes_written_ = 0;
    segments_ = 0;
    segment_start_ms_ = start_ms;
    session_dir_ = MakeSessionDir(output_dir, start_ms);
    if (session_dir_.empty()) {
        failed_ = true;
        return false;
    }
    static std::atomic<std::uint64_t> session_sequence{0};
    session_id_ = MakeSessionId(
        start_ms, session_sequence.fetch_add(1, std::memory_order_relaxed));
    return OpenNextSegment();
}

bool FileSink::FitsQuota(std::uint64_t extra) const {
    return max_total_bytes_ == 0 || bytes_written_ + extra <= max_total_bytes_;
}

void FileSink::StopAtQuota() {
    LOG(WARNING) << "traffic: session reached max_total_bytes ("
                 << max_total_bytes_ << " bytes); recording stops";
    quota_reached_.store(true, std::memory_order_relaxed);
}

bool FileSink::OpenNextSegment() {
    if (!FitsQuota(kTrafficFileHeaderSize + kTrafficFooterSize)) {
        StopAtQuota();
        return false;
    }
    if (segment_seq_ != 0) {
        segment_start_ms_ = TrafficNowMs();
    }
    segment_started_at_ = std::chrono::steady_clock::now();
    char name[64];
    std::snprintf(name, sizeof(name), "segment-%06llu.mcr",
                  static_cast<unsigned long long>(segment_seq_));
    segment_path_ = session_dir_ + "/" + name;
    fd_ = ::open(segment_path_.c_str(),
                 O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd_ < 0) {
        LOG(ERROR) << "traffic: open segment failed: " << segment_path_;
        failed_ = true;
        return false;
    }

    TrafficFileHeader header;
    header.segment_seq = segment_seq_;
    header.start_ms = segment_start_ms_;
    header.redaction = static_cast<std::uint8_t>(redaction_);
    std::memcpy(header.session_id, session_id_.data(), session_id_.size());
    if (!WriteAll(fd_, &header, sizeof(header))) {
        LOG(ERROR) << "traffic: write segment header failed: " << segment_path_;
        failed_ = true;
        ::close(fd_);
        fd_ = -1;
        // A segment without its header cannot be read, so leave no file behind.
        ::unlink(segment_path_.c_str());
        return false;
    }
    segment_bytes_written_ = sizeof(header);
    block_count_ = 0;
    record_count_ = 0;
    record_bytes_ = 0;
    bytes_written_ += sizeof(header);
    ++segments_;
    return true;
}

bool FileSink::WriteRecords(const struct iovec* iov, int iov_count,
                            std::uint64_t bytes, std::uint32_t records) {
    std::lock_guard<std::mutex> lock(mu_);
    // A session stopped by its size limit is a normal end, so it is checked
    // before the failure state and before the session-open check.
    if (quota_reached_.load(std::memory_order_relaxed)) {
        return false;
    }
    if (failed_) {
        LOG(ERROR) << "traffic: cannot write records after a write failure";
        return false;
    }
    if (fd_ < 0) {
        LOG(ERROR) << "traffic: cannot write records before a session is open";
        failed_ = true;
        return false;
    }
    if (iov == nullptr || iov_count <= 0 || bytes == 0 || records == 0) {
        LOG(ERROR) << "traffic: write records called with an empty block";
        failed_ = true;
        return false;
    }
    std::uint64_t iov_bytes = 0;
    for (int i = 0; i < iov_count; ++i) {
        iov_bytes += iov[i].iov_len;
    }
    if (iov_bytes != bytes) {
        LOG(ERROR) << "traffic: write records length mismatch: iovec holds "
                   << iov_bytes << " bytes but the caller reported " << bytes;
        failed_ = true;
        return false;
    }
    if (bytes > kMaxBlockBytes) {
        LOG(ERROR) << "traffic: block of " << bytes << " bytes exceeds the "
                   << kMaxBlockBytes << " byte block limit";
        failed_ = true;
        return false;
    }
    if (static_cast<std::uint64_t>(records) * kTrafficRecordHeaderSize >
        bytes) {
        LOG(ERROR) << "traffic: " << records << " records do not fit in "
                   << bytes << " bytes";
        failed_ = true;
        return false;
    }
    const std::uint64_t block_total =
        kTrafficBlockHeaderSize + static_cast<std::uint64_t>(bytes);
    const bool rotate_by_bytes =
        segment_bytes_ > 0 && block_count_ > 0 &&
        segment_bytes_written_ + block_total + kTrafficFooterSize >
            segment_bytes_;
    // The age is compared in whole seconds on a monotonic clock: a backwards
    // wall clock step cannot wrap the elapsed time, and a huge segment_seconds
    // cannot overflow a millisecond product before the comparison.
    const auto elapsed_seconds = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - segment_started_at_)
            .count());
    const bool rotate_by_time = segment_seconds_ > 0 && block_count_ > 0 &&
                                elapsed_seconds >= segment_seconds_;
    const bool rotate = rotate_by_bytes || rotate_by_time;
    // A rotation spends the old footer and a whole new segment before this
    // block, so the size limit is checked against that full cost; checking only
    // the block lets the session overshoot max_total_bytes by the new header.
    const std::uint64_t needed =
        block_total + kTrafficFooterSize +
        (rotate ? kTrafficFileHeaderSize + kTrafficFooterSize : 0);
    if (!FitsQuota(needed)) {
        StopAtQuota();
        return false;
    }
    if (rotate && (!CloseSegment(false) || !OpenNextSegment())) {
        return false;
    }

    mooncake::Crc32c crc;
    for (int i = 0; i < iov_count; ++i) {
        crc.Extend(iov[i].iov_base, static_cast<std::size_t>(iov[i].iov_len));
    }
    TrafficBlockHeader header;
    header.block_bytes = static_cast<std::uint32_t>(bytes);
    header.record_count = records;
    header.crc32 = crc.Final();
    // Header and records leave in one writev. Two calls would leave a complete
    // header with no payload when the process dies between them, and the reader
    // cannot parse that block.
    iovec_scratch_.resize(static_cast<std::size_t>(iov_count) + 1);
    iovec_scratch_[0].iov_base = &header;
    iovec_scratch_[0].iov_len = sizeof(header);
    std::copy(iov, iov + iov_count, iovec_scratch_.begin() + 1);
    int remaining = iov_count + 1;
    struct iovec* cursor = iovec_scratch_.data();
    while (remaining > 0) {
        const int batch = std::min(remaining, kMaxIovCount);
        const ssize_t written = ::writev(fd_, cursor, batch);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            LOG(ERROR) << "traffic: write records failed: " << segment_path_;
            failed_ = true;
            return false;
        }
        std::size_t left = static_cast<std::size_t>(written);
        while (remaining > 0 && left >= cursor->iov_len) {
            left -= cursor->iov_len;
            ++cursor;
            --remaining;
        }
        if (remaining > 0 && left > 0) {
            cursor->iov_base = static_cast<char*>(cursor->iov_base) + left;
            cursor->iov_len -= left;
        }
    }
    segment_bytes_written_ += block_total;
    bytes_written_ += block_total;
    ++block_count_;
    record_count_ += records;
    record_bytes_ += bytes;
    return true;
}

bool FileSink::CloseSegment(bool sync) {
    if (fd_ < 0) {
        return true;
    }
    bool ok = true;
    // A failed write can leave part of a block after the last complete one.
    // Segments end on a block boundary so a reader can parse the blocks that
    // did reach the disk, so the partial block is cut off before the footer.
    if (failed_ &&
        (::ftruncate(fd_, static_cast<off_t>(segment_bytes_written_)) != 0 ||
         ::lseek(fd_, 0, SEEK_END) < 0)) {
        LOG(ERROR) << "traffic: truncate failed segment failed: "
                   << segment_path_;
        failed_ = true;
        ok = false;
    }
    TrafficFooter footer;
    footer.block_count = static_cast<std::uint32_t>(block_count_);
    footer.record_count = record_count_;
    footer.record_bytes = record_bytes_;
    footer.start_ms = segment_start_ms_;
    footer.end_ms = TrafficNowMs();
    if (!WriteAll(fd_, &footer, sizeof(footer))) {
        failed_ = true;
        ok = false;
    } else {
        bytes_written_ += sizeof(footer);
    }
    if (sync && SyncFile(fd_) != 0) {
        failed_ = true;
        ok = false;
    }
    if (::close(fd_) != 0) {
        failed_ = true;
        ok = false;
    }
    fd_ = -1;
    ++segment_seq_;
    return ok;
}

void FileSink::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (fd_ >= 0) {
        CloseSegment(true);
    }
}

}  // namespace mooncake::traffic
