#pragma once

// Memory and disk admission for the shutdown handoff.
//
// Shutdown (before the export):
//   limit    = min(memory.max, memory.high)      (MemTotal if unlimited)
//   headroom = limit - memory.current            (page cache counts as used)
//   safety   = max(512 MiB, 3% of limit)
//   per_wkr  = 16 MiB + ceil(objects / 1024) * 1350 B
//                     + ceil(disk / 1024) * 200 B
//              (one shard's StandbyObjectEntry copies + encoded chunk;
//              disk = objects with a LOCAL_DISK replica, schema 2)
//   base     = 64 MiB                            (segments, chunk index, ...)
//   W        = min(max_workers, cpus, floor((headroom - safety - base) /
//                                           per_wkr))
//   skip if W == 0 ("memory_headroom"), if the free space of the handoff
//   file system is below 1.3 * (objects * 200 B + disk * 100 B + 16 MiB)
//   ("disk_space"), or if the predicted restore need (below) exceeds what
//   the next master can spend ("restore_memory"; it would start empty
//   anyway). Before the write the split of `disk` (objects with a LOCAL_DISK
//   replica) into disk-only objects and extra disk replicas is unknown, so
//   the cheapest split is assumed (lower bound); after the write the exact
//   counts are checked again and the snapshot is not published if they do
//   not fit. The snapshot always includes L4: there is no memory-only
//   fallback.
//
// Restore (after the claim, before anything is allocated):
//   raw   = memory_objects     * bytes_per_memory_object
//           + extra_disk       * bytes_per_extra_disk_replica
//           + disk_only        * bytes_per_disk_only_object
//   need  = raw * (100 + margin_percent) / 100
//   memory_objects = objects with a memory replica = objects - disk_only,
//   extra_disk     = LOCAL_DISK replicas of those    = disk - disk_only.
//   skip if need > headroom ("memory_headroom") -> cold start.
//   The per-item costs are the measured peak RSS (VmHWM) per item of
//   restores at scale, rounded up; the margin is the only safety term.
//
// All inputs are read through injectable file paths so tests can feed fake
// cgroup files.

#include <cstddef>
#include <cstdint>
#include <string>

#include <ylt/util/tl/expected.hpp>

namespace mooncake::handoff {

struct CgroupPaths {
    // cgroup v2 files; v1 fallbacks are used when memory_max is missing.
    std::string memory_max;
    std::string memory_high;
    std::string memory_current;
    std::string memory_stat;
    std::string cpu_max;
    std::string v1_limit;  // memory.limit_in_bytes
    std::string v1_usage;  // memory.usage_in_bytes
    std::string meminfo;   // /proc/meminfo, for unlimited cgroups

    // Paths under `cgroup_root` (default "/sys/fs/cgroup").
    static CgroupPaths Under(const std::string& cgroup_root,
                             const std::string& meminfo = "/proc/meminfo");
};

struct MemorySnapshot {
    uint64_t limit{0};    // effective limit used for admission
    bool limited{false};  // false: limit is MemTotal
    uint64_t current{0};
    uint64_t anon{0};
    uint64_t file{0};
    uint64_t file_dirty{0};
    uint64_t file_writeback{0};
    uint64_t inactive_file{0};
    std::string source;  // "cgroup_v2" | "cgroup_v1" | "meminfo"
    uint64_t Headroom() const { return limit > current ? limit - current : 0; }
    std::string ToJson() const;
};

tl::expected<MemorySnapshot, std::string> ReadMemorySnapshot(
    const CgroupPaths& paths);

// CPUs this process may use: sched_getaffinity, capped by cpu.max quota.
size_t AvailableCpus(const CgroupPaths& paths);

// Free bytes for unprivileged writes on the file system holding `path`.
uint64_t FreeDiskBytes(const std::string& path);

uint64_t AdmissionSafetyBytes(uint64_t limit);
// What the next master can spend on a restore: the limit minus the startup
// admission's view of a fresh process (memory.current of an empty master),
// approximated by the safety term. The shutdown admission compares the
// predicted restore need against this, so a snapshot it writes is not
// refused at startup for lack of headroom.
uint64_t RestoreBudgetBytes(uint64_t limit);

// Restore memory model. Defaults: measured VmHWM per item (incl. the empty
// master) at 25M / 32M memory objects, case O (24M extra disk replicas) and
// case D (24M disk-only objects), rounded up to a multiple of 8.
struct RestoreCostModel {
    // An object with at least one memory replica (and its memory replica).
    uint64_t bytes_per_memory_object{1144};
    // A LOCAL_DISK replica of an object that also has a memory replica.
    uint64_t bytes_per_extra_disk_replica{184};
    // An object whose only replica is a LOCAL_DISK replica.
    uint64_t bytes_per_disk_only_object{904};
    // One explicit safety margin on the total.
    uint32_t margin_percent{15};
};

struct RestoreCounts {
    uint64_t objects{0};        // all objects in the snapshot
    uint64_t disk_replicas{0};  // objects with a LOCAL_DISK replica
    // Objects without a memory replica. Unknown (e.g. a marker written
    // before this field existed) is 0, which charges every disk object as
    // a memory object plus an extra disk replica: an over-estimate.
    uint64_t disk_only_objects{0};
};

// need = raw * (100 + margin) / 100, rounded up (see the header comment).
// disk_only_objects is clamped to min(objects, disk_replicas).
uint64_t RestoreNeedBytes(const RestoreCounts& counts,
                          const RestoreCostModel& model);

// Smallest need over every possible disk-only split of `disk_replicas`
// (used before the write, when the split is not known yet).
uint64_t RestoreNeedLowerBound(uint64_t objects, uint64_t disk_replicas,
                               const RestoreCostModel& model);

struct ShutdownAdmissionInput {
    MemorySnapshot memory;
    uint64_t objects{0};
    uint64_t local_disk_objects{0};
    uint64_t disk_free_bytes{0};
    RestoreCostModel restore_cost;
    size_t max_workers{8};
    size_t cpus{1};
};

struct ShutdownAdmission {
    bool admit{false};
    size_t workers{0};
    std::string reason;  // empty when admitted
    uint64_t headroom{0};
    uint64_t safety{0};
    uint64_t per_worker{0};
    uint64_t base{0};
    uint64_t disk_needed{0};
    // Predicted restore need (lower bound over the disk-only split, incl.
    // the margin); the exact need is checked after the write.
    uint64_t restore_needed{0};
    std::string ToJson() const;
};

ShutdownAdmission DecideShutdownAdmission(const ShutdownAdmissionInput& in);

struct RestoreAdmission {
    bool admit{false};
    uint64_t needed{0};
    uint64_t headroom{0};
    std::string ToJson() const;
};

RestoreAdmission DecideRestoreAdmission(const MemorySnapshot& memory,
                                        const RestoreCounts& counts,
                                        const RestoreCostModel& model);

}  // namespace mooncake::handoff
