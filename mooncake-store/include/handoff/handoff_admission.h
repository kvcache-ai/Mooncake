#pragma once

// Memory and disk admission for the shutdown handoff.
//
// Shutdown (before the export):
//   limit    = min(memory.max, memory.high)      (MemTotal if unlimited)
//   headroom = limit - memory.current            (page cache counts as used)
//   safety   = max(512 MiB, 3% of limit)
//   per_wkr  = 16 MiB + ceil(objects / 1024) * 1350 B
//              (one shard's StandbyObjectEntry copies + encoded chunk)
//   base     = 64 MiB                            (segments, chunk index, ...)
//   W        = min(max_workers, cpus, floor((headroom - safety - base) /
//                                           per_wkr))
//   skip if W == 0 ("memory_headroom"), if the free space of the handoff
//   file system is below 1.3 * (objects * 200 B + 16 MiB) ("disk_space"), or
//   if the predicted restore need (below) exceeds what the next master can
//   spend ("restore_memory"; it would start empty anyway).
//
// Restore (after the claim, before anything is allocated):
//   need  = objects * bytes_per_object * (100 + margin_percent) / 100
//   skip if need > headroom ("memory_headroom") -> cold start.
//   bytes_per_object is the measured peak RSS (VmHWM) per object of a
//   restore at 25M objects, rounded up; the margin is the only safety term.
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

struct RestoreCostModel {
    // Peak RSS per restored object (incl. its memory replicas and the empty
    // master), measured at 25M objects and rounded up.
    uint64_t bytes_per_object{1144};
    // One explicit safety margin on the total.
    uint32_t margin_percent{15};
};

// need = objects * bytes_per_object * (100 + margin) / 100, rounded up.
uint64_t RestoreNeedBytes(uint64_t objects, const RestoreCostModel& model);

struct ShutdownAdmissionInput {
    MemorySnapshot memory;
    uint64_t objects{0};
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
    // Predicted restore need of every object (incl. the margin); the need
    // of the objects actually written is checked again after the write.
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
                                        uint64_t objects,
                                        const RestoreCostModel& model);

}  // namespace mooncake::handoff
