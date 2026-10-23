#pragma once

// On-disk life cycle of shutdown handoff snapshots.
//
// Layout of --handoff_dir (node-local; one master process uses it at a time):
//
//   LOCK            flock()ed by the master for its whole life. A master that
//                   cannot take it neither restores nor writes snapshots.
//   POISON          written by a master that could not take LOCK, i.e. that
//                   serves next to the lock owner. A READY that coexists with
//                   POISON is never restored; only a lock owner removes it, at
//                   startup.
//   snap-<id>.tmp/  snapshot being written; never restored, removed by the
//                   next writer or the next startup.
//   snap-<id>/      complete snapshot: every file fsynced, renamed from
//                   snap-<id>.tmp, parent fsynced.
//   READY           marker naming a complete snapshot. Written last
//                   (READY.tmp -> fsync -> rename -> fsync dir).
//   CLAIMED         READY renamed by a starting master before it allocates
//                   anything, so each snapshot gets at most one restore
//                   attempt. Removed with the snapshot after the attempt; a
//                   CLAIMED found at startup means the previous attempt died.
//   last_shutdown   one JSON line with the outcome of the last shutdown
//                   (best effort, for logs and metrics of the next master).
//
// State machine of one snapshot:
//   (none) -> snap-<id>.tmp -> snap-<id> -> READY -> CLAIMED -> (removed)
// Any interruption before READY leaves nothing restorable. Any interruption
// after CLAIMED makes the next start a cold start.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

namespace mooncake::handoff {

inline constexpr char kLockFile[] = "LOCK";
inline constexpr char kPoisonFile[] = "POISON";
inline constexpr char kReadyMarker[] = "READY";
inline constexpr char kClaimedMarker[] = "CLAIMED";
inline constexpr char kLastShutdownFile[] = "last_shutdown";
inline constexpr char kSnapshotPrefix[] = "snap-";
inline constexpr char kTmpSuffix[] = ".tmp";

struct ReadyMarker {
    std::string snapshot;  // directory name, "snap-<id>"
    int64_t created_at_ms{0};
    // Objects in the snapshot; the restore admission charges them before
    // anything is read.
    uint64_t objects{0};
    uint64_t bytes{0};
    // Objects with a LOCAL_DISK replica (schema 2; absent in older markers
    // and then 0). Used by the restore admission.
    uint64_t disk_replicas{0};
    // Objects without a memory replica (absent in older markers and then 0,
    // which makes the restore admission over-estimate, never under-).
    uint64_t disk_only_objects{0};
};

// "MOONCAKE-HANDOFF-READY 1 <crc32c of body> <body bytes>\n<body>", body is
// "key=value" lines.
std::string EncodeReadyMarker(const ReadyMarker& marker);
tl::expected<ReadyMarker, std::string> DecodeReadyMarker(std::string_view file);

// True for "snap-<id>" names without path separators.
bool IsSnapshotName(const std::string& name);
std::string NewSnapshotName(int64_t now_ms);

// Exclusive flock() on <root>/LOCK, held until destruction or process exit.
class HandoffDirLock {
   public:
    HandoffDirLock() = default;
    ~HandoffDirLock();
    HandoffDirLock(const HandoffDirLock&) = delete;
    HandoffDirLock& operator=(const HandoffDirLock&) = delete;

    // Creates `root` if needed and tries to lock it for up to wait_ms.
    // Returns false (with a reason) if another process holds the lock or the
    // directory is unusable.
    bool Acquire(const std::string& root, int64_t wait_ms, std::string* error);
    bool held() const { return fd_ >= 0; }

   private:
    int fd_{-1};
};

// Writes <root>/POISON durably (a master that serves without the lock).
tl::expected<void, std::string> WritePoison(const std::string& root);
// True if POISON exists or its state cannot be determined (a stat error is
// never read as "absent").
bool PoisonExists(const std::string& root);

// Result of a stat that tells "does not exist" apart from an I/O error.
enum class PathState { kAbsent, kPresent, kUnknown };
PathState ProbePath(const std::string& path);

// The READY marker, if it exists and can be read and decoded. Used to know
// when a READY that cannot be removed becomes too old to be restored.
std::optional<ReadyMarker> PeekReadyMarker(const std::string& root);

// Writer side. Removes every READY/CLAIMED marker, READY.tmp and snapshot
// directory (complete or not) from `root`, then fsyncs it. Keeps LOCK,
// POISON and last_shutdown. Only names of this layout are touched.
tl::expected<void, std::string> ResetHandoffDir(const std::string& root);

// Writer side. <root>/<name>.tmp must hold a complete, fsynced snapshot.
// Renames it to <root>/<name>, fsyncs root, then writes READY durably.
tl::expected<void, std::string> PublishSnapshot(const std::string& root,
                                                const std::string& name,
                                                const ReadyMarker& marker);

enum class ClaimState {
    kNone,             // nothing to restore
    kClaimed,          // READY renamed to CLAIMED; snapshot_dir is set
    kAbortedPrevious,  // CLAIMED found: a previous restore attempt died
    kPoisoned,         // POISON found: snapshot discarded
    kInvalid,          // marker unreadable or names no complete snapshot
    kError,            // I/O error while claiming
};
const char* ToString(ClaimState state);

struct ClaimResult {
    ClaimState state{ClaimState::kNone};
    std::string snapshot_dir;  // absolute path when kClaimed
    ReadyMarker marker;
    std::string detail;
    // kError only: a restorable READY may still be on disk (it could not be
    // claimed nor removed, its state is unknown, or the directory could not
    // be fsynced after removing it). A master must not serve next to it.
    bool ready_may_remain{false};
};

// Startup side; the caller must hold the HandoffDirLock. Must run before the
// master allocates or restores anything. Renames READY to CLAIMED (fsync) and
// removes leftovers that cannot be restored anymore: *.tmp directories,
// READY.tmp, snapshot directories not named by the claimed marker, and on
// kAbortedPrevious / kPoisoned / kInvalid everything (POISON included).
ClaimResult ClaimHandoffSnapshot(const std::string& root);

// Removes the claimed snapshot directory and CLAIMED after the restore
// attempt (success or failure) or when the claim is discarded.
void ReleaseClaim(const std::string& root, const ClaimResult& claim);

// last_shutdown: one JSON line, written with fsync but never required.
void WriteLastShutdown(const std::string& root, const std::string& json);
std::string ReadLastShutdown(const std::string& root);

}  // namespace mooncake::handoff
