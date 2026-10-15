// Tests for the shutdown handoff life cycle: marker state machine,
// directory lock and poison, admission control, watchdog, shutdown/startup
// ordering, configuration checks, and one shutdown-to-startup round trip.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "crc32c.h"
#include "handoff/handoff_admission.h"
#include "handoff/handoff_lifecycle.h"
#include "handoff/handoff_runtime.h"
#include "handoff/handoff_writer.h"
#include "master_service.h"

namespace mooncake::test {
namespace {

namespace fs = std::filesystem;
using namespace mooncake::handoff;

constexpr uint64_t kGiB = 1024ULL * 1024 * 1024;
constexpr uint64_t kMiB = 1024ULL * 1024;

class TempDir {
   public:
    TempDir() {
        std::string pattern =
            (fs::temp_directory_path() / "handoff-life-XXXXXX").string();
        path_ = mkdtemp(pattern.data());
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    const std::string& path() const { return path_; }
    std::string operator/(const std::string& name) const {
        return path_ + "/" + name;
    }

   private:
    std::string path_;
};

void WriteText(const std::string& path, const std::string& text) {
    fs::create_directories(fs::path(path).parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << text;
}

std::string ReadText(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

// Creates <root>/<name>.tmp with one file and publishes it.
void MakePublished(const std::string& root, const std::string& name,
                   int64_t created_at_ms, uint64_t objects = 7) {
    WriteText(root + "/" + name + kTmpSuffix + "/data", "x");
    auto published =
        PublishSnapshot(root, name, {name, created_at_ms, objects, 1});
    ASSERT_TRUE(published.has_value()) << published.error();
}

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// ------------------------------------------------------------------ marker

TEST(HandoffMarkerTest, RoundTripAndCorruption) {
    ReadyMarker marker{"snap-123-4", 123, 25000000, 4500000000ULL};
    const std::string text = EncodeReadyMarker(marker);
    auto decoded = DecodeReadyMarker(text);
    ASSERT_TRUE(decoded.has_value()) << decoded.error();
    EXPECT_EQ(decoded->snapshot, "snap-123-4");
    EXPECT_EQ(decoded->created_at_ms, 123);
    EXPECT_EQ(decoded->objects, 25000000u);
    EXPECT_EQ(decoded->bytes, 4500000000ULL);

    std::string flipped = text;
    flipped[flipped.size() - 2] ^= 1;
    EXPECT_FALSE(DecodeReadyMarker(flipped).has_value());
    EXPECT_FALSE(DecodeReadyMarker(text.substr(0, text.size() - 3)));
    EXPECT_FALSE(DecodeReadyMarker("garbage"));
    // Names that could escape the directory are rejected.
    EXPECT_FALSE(DecodeReadyMarker(EncodeReadyMarker({"../x", 1, 1, 1})));
    EXPECT_FALSE(
        DecodeReadyMarker(EncodeReadyMarker({"snap-a/../../b", 1, 1, 1})));
    EXPECT_FALSE(DecodeReadyMarker(EncodeReadyMarker({"snap-a.tmp", 1, 1, 1})));
}
// ----------------------------------------------------------- state machine

TEST(HandoffStateMachineTest, PublishClaimReleaseIsOneShot) {
    TempDir root;
    MakePublished(root.path(), "snap-1-1", 1000);
    EXPECT_TRUE(fs::exists(root / "READY"));
    EXPECT_TRUE(fs::is_directory(root / "snap-1-1"));
    EXPECT_FALSE(fs::exists(root / "snap-1-1.tmp"));

    ClaimResult claim = ClaimHandoffSnapshot(root.path());
    ASSERT_EQ(claim.state, ClaimState::kClaimed) << claim.detail;
    EXPECT_EQ(claim.snapshot_dir, root / "snap-1-1");
    EXPECT_EQ(claim.marker.objects, 7u);
    EXPECT_FALSE(fs::exists(root / "READY"));
    EXPECT_TRUE(fs::exists(root / "CLAIMED"));

    // The process dies during the restore: CLAIMED stays. The next start
    // must not try again.
    ClaimResult again = ClaimHandoffSnapshot(root.path());
    EXPECT_EQ(again.state, ClaimState::kAbortedPrevious);
    EXPECT_FALSE(fs::exists(root / "CLAIMED"));
    EXPECT_FALSE(fs::exists(root / "snap-1-1"));
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
}

TEST(HandoffStateMachineTest, ReleaseRemovesSnapshotAndClaim) {
    TempDir root;
    MakePublished(root.path(), "snap-2-1", 1000);
    ClaimResult claim = ClaimHandoffSnapshot(root.path());
    ASSERT_EQ(claim.state, ClaimState::kClaimed);
    ReleaseClaim(root.path(), claim);
    EXPECT_FALSE(fs::exists(root / "CLAIMED"));
    EXPECT_FALSE(fs::exists(root / "snap-2-1"));
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
}

TEST(HandoffStateMachineTest, PartialWritesAreNeverClaimed) {
    TempDir root;
    // Killed while writing chunks, while writing READY.tmp, or after the
    // rename but before READY.
    WriteText(root / "snap-3-1.tmp/objects/0.bin", "partial");
    WriteText(root / "READY.tmp", "half");
    WriteText(root / "snap-3-2/handoff_manifest", "complete but unmarked");
    WriteText(root / "unrelated.txt", "keep");
    WriteText(root / "last_shutdown", "{}");
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
    EXPECT_FALSE(fs::exists(root / "snap-3-1.tmp"));
    EXPECT_FALSE(fs::exists(root / "READY.tmp"));
    EXPECT_FALSE(fs::exists(root / "snap-3-2"));
    EXPECT_TRUE(fs::exists(root / "unrelated.txt"));
    EXPECT_TRUE(fs::exists(root / "last_shutdown"));
}

TEST(HandoffStateMachineTest, ClaimKeepsOnlyTheMarkedSnapshot) {
    TempDir root;
    WriteText(root / "snap-4-0/old", "older complete snapshot");
    WriteText(root / "snap-4-2.tmp/x", "tmp");
    MakePublished(root.path(), "snap-4-1", 1000);
    ClaimResult claim = ClaimHandoffSnapshot(root.path());
    ASSERT_EQ(claim.state, ClaimState::kClaimed);
    EXPECT_TRUE(fs::exists(root / "snap-4-1"));
    EXPECT_FALSE(fs::exists(root / "snap-4-0"));
    EXPECT_FALSE(fs::exists(root / "snap-4-2.tmp"));
}

TEST(HandoffStateMachineTest, InvalidMarkerIsDiscarded) {
    TempDir root;
    WriteText(root / "READY", "MOONCAKE-HANDOFF-READY 1 0 3\nabc");
    WriteText(root / "snap-5-1/x", "x");
    ClaimResult claim = ClaimHandoffSnapshot(root.path());
    EXPECT_EQ(claim.state, ClaimState::kInvalid);
    EXPECT_FALSE(fs::exists(root / "CLAIMED"));
    EXPECT_FALSE(fs::exists(root / "snap-5-1"));

    // A valid marker naming a directory that does not exist.
    WriteText(root / "READY", EncodeReadyMarker({"snap-5-2", 1, 1, 1}));
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kInvalid);
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
}

TEST(HandoffStateMachineTest, ResetKeepsLockPoisonAndStats) {
    TempDir root;
    MakePublished(root.path(), "snap-6-1", 1000);
    WriteText(root / "CLAIMED", "x");
    WriteText(root / "snap-6-2.tmp/x", "x");
    WriteText(root / "LOCK", "");
    WriteText(root / "POISON", "1");
    WriteText(root / "last_shutdown", "{}");
    ASSERT_TRUE(ResetHandoffDir(root.path()).has_value());
    EXPECT_FALSE(fs::exists(root / "READY"));
    EXPECT_FALSE(fs::exists(root / "CLAIMED"));
    EXPECT_FALSE(fs::exists(root / "snap-6-1"));
    EXPECT_FALSE(fs::exists(root / "snap-6-2.tmp"));
    EXPECT_TRUE(fs::exists(root / "LOCK"));
    EXPECT_TRUE(fs::exists(root / "POISON"));
    EXPECT_TRUE(fs::exists(root / "last_shutdown"));
}

TEST(HandoffStateMachineTest, PoisonDiscardsReadyAndIsRemovedByOwner) {
    TempDir root;
    MakePublished(root.path(), "snap-7-1", 1000);
    ASSERT_TRUE(WritePoison(root.path()).has_value());
    ClaimResult claim = ClaimHandoffSnapshot(root.path());
    EXPECT_EQ(claim.state, ClaimState::kPoisoned);
    EXPECT_FALSE(fs::exists(root / "READY"));
    EXPECT_FALSE(fs::exists(root / "snap-7-1"));
    EXPECT_FALSE(PoisonExists(root.path()));
}

TEST(HandoffLockTest, SecondHolderIsRefused) {
    TempDir root;
    auto first = std::make_unique<HandoffDirLock>();
    std::string error;
    ASSERT_TRUE(first->Acquire(root.path(), 0, &error)) << error;
    HandoffDirLock second;
    const auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(second.Acquire(root.path(), 300, &error));
    EXPECT_GE(std::chrono::steady_clock::now() - start,
              std::chrono::milliseconds(250));
    EXPECT_FALSE(second.held());
    // flock is tied to the open file description: closing it releases it.
    first.reset();
    HandoffDirLock third;
    EXPECT_TRUE(third.Acquire(root.path(), 0, &error)) << error;
}

// --------------------------------------------------------------- admission

class FakeCgroup {
   public:
    FakeCgroup() : paths_(CgroupPaths::Under(dir_.path(), dir_ / "meminfo")) {
        WriteText(dir_ / "meminfo",
                  "MemTotal:       2097152 kB\nMemAvailable:   1048576 kB\n");
    }
    void Set(const std::string& name, const std::string& value) {
        WriteText(dir_ / name, value + "\n");
    }
    const CgroupPaths& paths() const { return paths_; }

   private:
    TempDir dir_;
    CgroupPaths paths_;
};

TEST(HandoffAdmissionTest, ReadsCgroupV2) {
    FakeCgroup cg;
    cg.Set("memory.max", std::to_string(80 * kGiB));
    cg.Set("memory.high", "max");
    cg.Set("memory.current", std::to_string(30 * kGiB));
    WriteText(cg.paths().memory_stat,
              "anon 28000000000\nfile 2000000000\nfile_dirty 1000\n"
              "file_writeback 2000\ninactive_file 1500000000\n");
    auto snap = ReadMemorySnapshot(cg.paths());
    ASSERT_TRUE(snap.has_value()) << snap.error();
    EXPECT_EQ(snap->source, "cgroup_v2");
    EXPECT_TRUE(snap->limited);
    EXPECT_EQ(snap->limit, 80 * kGiB);
    EXPECT_EQ(snap->current, 30 * kGiB);
    EXPECT_EQ(snap->anon, 28000000000ULL);
    EXPECT_EQ(snap->file_dirty, 1000u);
    EXPECT_EQ(snap->inactive_file, 1500000000ULL);
    EXPECT_EQ(snap->Headroom(), 50 * kGiB);

    // memory.high below memory.max wins.
    cg.Set("memory.high", std::to_string(60 * kGiB));
    EXPECT_EQ(ReadMemorySnapshot(cg.paths())->limit, 60 * kGiB);
    // Unlimited: fall back to MemTotal (2 GiB in the fake meminfo).
    cg.Set("memory.max", "max");
    cg.Set("memory.high", "max");
    cg.Set("memory.current", std::to_string(1 * kGiB));
    snap = ReadMemorySnapshot(cg.paths());
    ASSERT_TRUE(snap.has_value());
    EXPECT_FALSE(snap->limited);
    EXPECT_EQ(snap->limit, 2 * kGiB);
    // memory.current unreadable -> error, never a guess.
    fs::remove(cg.paths().memory_current);
    cg.Set("memory.max", std::to_string(80 * kGiB));
    EXPECT_FALSE(ReadMemorySnapshot(cg.paths()).has_value());
}

TEST(HandoffAdmissionTest, FallsBackToV1AndMeminfo) {
    FakeCgroup cg;
    WriteText(cg.paths().v1_limit, std::to_string(kGiB) + "\n");
    WriteText(cg.paths().v1_usage, std::to_string(256 * kMiB) + "\n");
    auto snap = ReadMemorySnapshot(cg.paths());
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->source, "cgroup_v1");
    EXPECT_EQ(snap->limit, kGiB);
    EXPECT_EQ(snap->current, 256 * kMiB);

    FakeCgroup bare;
    snap = ReadMemorySnapshot(bare.paths());
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->source, "meminfo");
    EXPECT_EQ(snap->limit, 2 * kGiB);
    EXPECT_EQ(snap->current, kGiB);
}

TEST(HandoffAdmissionTest, CpuQuotaCapsWorkers) {
    FakeCgroup cg;
    cg.Set("cpu.max", "250000 100000");
    EXPECT_LE(AvailableCpus(cg.paths()), 3u);
    EXPECT_GE(AvailableCpus(cg.paths()), 1u);
}

MemorySnapshot Memory(uint64_t limit, uint64_t current) {
    MemorySnapshot m;
    m.limit = limit;
    m.limited = true;
    m.current = current;
    m.source = "test";
    return m;
}

TEST(HandoffAdmissionTest, ShutdownWorkersFollowHeadroom) {
    ShutdownAdmissionInput in;
    in.objects = 25000000;
    in.max_workers = 8;
    in.cpus = 14;
    in.disk_free_bytes = 100 * kGiB;

    // 80 GiB pod, 30 GiB used: plenty -> max_workers.
    in.memory = Memory(80 * kGiB, 30 * kGiB);
    ShutdownAdmission a = DecideShutdownAdmission(in);
    EXPECT_TRUE(a.admit);
    EXPECT_EQ(a.workers, 8u);
    // per worker = 16 MiB + ceil(25e6 / 1024) * 1350 B
    EXPECT_EQ(a.per_worker, 16 * kMiB + 24415ULL * 1350);
    EXPECT_EQ(a.safety, static_cast<uint64_t>(80.0 * kGiB * 0.03));

    // Headroom for exactly 3 workers above safety + base.
    in.memory = Memory(80 * kGiB,
                       80 * kGiB - (a.safety + a.base + 3 * a.per_worker + 1));
    a = DecideShutdownAdmission(in);
    EXPECT_TRUE(a.admit);
    EXPECT_EQ(a.workers, 3u);

    // Less than one worker -> skip, cold start next time.
    in.memory = Memory(80 * kGiB, 80 * kGiB - a.safety);
    a = DecideShutdownAdmission(in);
    EXPECT_FALSE(a.admit);
    EXPECT_EQ(a.reason, "memory_headroom");
    EXPECT_EQ(a.workers, 0u);

    // CPU bound.
    in.memory = Memory(80 * kGiB, 1 * kGiB);
    in.cpus = 2;
    EXPECT_EQ(DecideShutdownAdmission(in).workers, 2u);

    // Disk: 25M objects need 1.3 * (5e9 + 16 MiB).
    in.disk_free_bytes = 5 * kGiB;
    a = DecideShutdownAdmission(in);
    EXPECT_FALSE(a.admit);
    EXPECT_EQ(a.reason, "disk_space");
}

// Raw model (margin 0) and the default model with its 15 % margin.
TEST(HandoffAdmissionTest, RestoreModelNeverUnderEstimatesMeasuredRuns) {
    // Peak RSS (VmHWM) of real restores at 25M and 32M objects.
    struct Case {
        const char* name;
        uint64_t objects, hwm_kib;
    };
    const Case cases[] = {
        {"25M", 25000000, 27582136},
        {"32M", 32000000, 35665740},
    };
    RestoreCostModel raw;
    raw.margin_percent = 0;
    const RestoreCostModel model;  // defaults
    for (const Case& c : cases) {
        const uint64_t measured = c.hwm_kib * 1024;
        const uint64_t raw_need = RestoreNeedBytes(c.objects, raw);
        const uint64_t need = RestoreNeedBytes(c.objects, model);
        // Even without the margin the per-object cost covers the measurement.
        EXPECT_GE(raw_need, measured) << c.name;
        EXPECT_EQ(need, (raw_need * 115 + 99) / 100) << c.name;
        // The over-estimate is the margin plus rounding, not more.
        EXPECT_LT(static_cast<double>(need) / static_cast<double>(measured),
                  1.17)
            << c.name;
    }
}

TEST(HandoffAdmissionTest, RestoreMarginAndCostsAreConfigurable) {
    RestoreCostModel model;
    model.margin_percent = 0;
    EXPECT_EQ(RestoreNeedBytes(1000, model), 1000ULL * 1144);
    model.margin_percent = 30;
    model.bytes_per_object = 1000;
    EXPECT_EQ(RestoreNeedBytes(1000, model), 1000ULL * 1000 * 130 / 100);
    // 25M objects at the default model: 30.6 GiB.
    EXPECT_TRUE(DecideRestoreAdmission(Memory(32 * kGiB, 1 * kGiB), 25000000,
                                       RestoreCostModel{})
                    .admit);
    EXPECT_FALSE(DecideRestoreAdmission(Memory(31 * kGiB, 1 * kGiB), 25000000,
                                        RestoreCostModel{})
                     .admit);
}

TEST(HandoffAdmissionTest, ShutdownPredictsTheRestore) {
    ShutdownAdmissionInput in;
    in.objects = 25000000;
    in.max_workers = 8;
    in.cpus = 14;
    in.disk_free_bytes = 100 * kGiB;
    in.memory = Memory(80 * kGiB, 30 * kGiB);
    ShutdownAdmission a = DecideShutdownAdmission(in);
    EXPECT_TRUE(a.admit) << a.ToJson();
    EXPECT_EQ(a.restore_needed, RestoreNeedBytes(25000000, RestoreCostModel{}));
    // 25M objects (30.6 GiB) do not fit a 30 GiB pod: skip before writing
    // anything (cold start); trimming cannot help.
    in.memory = Memory(30 * kGiB, 10 * kGiB);
    a = DecideShutdownAdmission(in);
    EXPECT_FALSE(a.admit);
    EXPECT_EQ(a.reason, "restore_memory");
}

// ---------------------------------------------------------------- watchdog

TEST(HandoffWatchdogTest, FiresOnceAfterDeadline) {
    std::atomic<int> fired{0};
    const auto start = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point at;
    {
        Watchdog dog(std::chrono::milliseconds(200), [&] {
            at = std::chrono::steady_clock::now();
            ++fired;
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        EXPECT_TRUE(dog.fired());
    }
    EXPECT_EQ(fired.load(), 1);
    EXPECT_GE(at - start, std::chrono::milliseconds(200));
}

TEST(HandoffWatchdogTest, CancelPreventsFiring) {
    std::atomic<int> fired{0};
    {
        Watchdog dog(std::chrono::milliseconds(200), [&] { ++fired; });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        dog.Cancel();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        EXPECT_FALSE(dog.fired());
    }
    EXPECT_EQ(fired.load(), 0);
}

// ----------------------------------------------------- shutdown sequencing

struct FakeShutdown {
    std::vector<std::string> events;
    MemorySnapshot memory = Memory(80 * kGiB, 10 * kGiB);
    uint64_t objects = 1000;
    uint64_t disk = 100 * kGiB;
    bool write_fails = false;
    std::chrono::milliseconds write_delay{0};
    size_t workers_seen = 0;

    ShutdownSteps Steps() {
        ShutdownSteps s;
        s.stop_serving = [this] { events.push_back("stop_serving"); };
        s.quiesce = [this] { events.push_back("quiesce"); };
        s.trim = [this] { events.push_back("trim"); };
        s.read_memory = [this]() -> tl::expected<MemorySnapshot, std::string> {
            events.push_back("read_memory");
            return memory;
        };
        s.object_count = [this] {
            events.push_back("object_count");
            return objects;
        };
        s.disk_free = [this](const std::string&) {
            events.push_back("disk_free");
            return disk;
        };
        s.write = [this](const std::string& dir, size_t workers)
            -> tl::expected<ShutdownWriteResult, std::string> {
            events.push_back("write");
            workers_seen = workers;
            std::this_thread::sleep_for(write_delay);
            if (write_fails) return tl::make_unexpected("disk on fire");
            WriteText(dir + "/handoff_manifest", "m");
            return ShutdownWriteResult{objects, 4096, "{}"};
        };
        return s;
    }
};

TEST(HandoffShutdownTest, OrderAndPublishedSnapshot) {
    TempDir root;
    WriteText(root / "READY", "stale marker from an older master");
    FakeShutdown fake;
    std::atomic<bool> published{false};
    ShutdownReport report = RunHandoffShutdown(
        fake.Steps(), {root.path(), /*owner=*/true, 4, 14}, &published);
    EXPECT_EQ(report.result, "ok") << report.detail;
    EXPECT_TRUE(published.load());
    // Plenty of headroom: no malloc_trim.
    const std::vector<std::string> expected{"stop_serving", "quiesce",
                                            "object_count", "disk_free",
                                            "read_memory",  "write"};
    EXPECT_EQ(fake.events, expected);
    EXPECT_EQ(fake.workers_seen, 4u);
    ClaimResult claim = ClaimHandoffSnapshot(root.path());
    ASSERT_EQ(claim.state, ClaimState::kClaimed) << claim.detail;
    EXPECT_EQ(claim.marker.snapshot, report.snapshot);
    EXPECT_EQ(claim.marker.objects, 1000u);
    EXPECT_TRUE(fs::exists(claim.snapshot_dir + "/handoff_manifest"));
    EXPECT_NE(ReadLastShutdown(root.path()).find("\"result\":\"ok\""),
              std::string::npos);
}

TEST(HandoffShutdownTest, TrimOnlyWhenShortOfWorkers) {
    TempDir root;
    FakeShutdown fake;
    // Room for 2 of the 4 wanted workers before the trim; the trim frees
    // enough for all 4.
    const uint64_t safety = AdmissionSafetyBytes(80 * kGiB);
    const uint64_t per_worker = 16 * kMiB + 1 * 1350;
    fake.memory = Memory(80 * kGiB,
                         80 * kGiB - (safety + 64 * kMiB + 2 * per_worker + 1));
    ShutdownSteps steps = fake.Steps();
    steps.trim = [&] {
        fake.events.push_back("trim");
        fake.memory = Memory(80 * kGiB, 1 * kGiB);
    };
    ShutdownReport report =
        RunHandoffShutdown(steps, {root.path(), true, 4, 14});
    EXPECT_EQ(report.result, "ok") << report.detail;
    const std::vector<std::string> expected{
        "stop_serving", "quiesce", "object_count", "disk_free",
        "read_memory",  "trim",    "read_memory",  "write"};
    EXPECT_EQ(fake.events, expected);
    EXPECT_EQ(fake.workers_seen, 4u);
}

TEST(HandoffShutdownTest, NotOwnerStopsButWritesNothing) {
    TempDir root;
    FakeShutdown fake;
    ShutdownReport report =
        RunHandoffShutdown(fake.Steps(), {root.path(), /*owner=*/false, 4, 14});
    EXPECT_EQ(report.result, "skipped_not_owner");
    const std::vector<std::string> expected{"stop_serving", "quiesce"};
    EXPECT_EQ(fake.events, expected);
    EXPECT_FALSE(fs::exists(root / "READY"));
    EXPECT_FALSE(fs::exists(root / "last_shutdown"));
}

TEST(HandoffShutdownTest, NoHeadroomSkipsAndRemovesOlderSnapshots) {
    TempDir root;
    MakePublished(root.path(), "snap-8-1", 1000);
    FakeShutdown fake;
    fake.memory = Memory(8 * kGiB, 8 * kGiB - 100 * kMiB);
    ShutdownReport report =
        RunHandoffShutdown(fake.Steps(), {root.path(), true, 4, 14});
    EXPECT_EQ(report.result, "skipped_memory_headroom");
    EXPECT_EQ(std::count(fake.events.begin(), fake.events.end(), "write"), 0);
    EXPECT_FALSE(fs::exists(root / "READY"));
    EXPECT_FALSE(fs::exists(root / "snap-8-1"));
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
}

TEST(HandoffShutdownTest, WriteErrorLeavesNothingRestorable) {
    TempDir root;
    FakeShutdown fake;
    fake.write_fails = true;
    ShutdownReport report =
        RunHandoffShutdown(fake.Steps(), {root.path(), true, 4, 14});
    EXPECT_EQ(report.result, "error");
    EXPECT_FALSE(fs::exists(root / "READY"));
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
}

TEST(HandoffShutdownTest, PoisonBeforeOrDuringWriteBlocksPublish) {
    TempDir root;
    {
        FakeShutdown fake;
        ASSERT_TRUE(WritePoison(root.path()).has_value());
        ShutdownReport report =
            RunHandoffShutdown(fake.Steps(), {root.path(), true, 4, 14});
        EXPECT_EQ(report.result, "skipped_poisoned");
        EXPECT_EQ(std::count(fake.events.begin(), fake.events.end(), "write"),
                  0);
        fs::remove(root / "POISON");
    }
    {
        FakeShutdown fake;
        ShutdownSteps steps = fake.Steps();
        steps.before_publish = [&] { (void)WritePoison(root.path()); };
        ShutdownReport report =
            RunHandoffShutdown(steps, {root.path(), true, 4, 14});
        EXPECT_EQ(report.result, "skipped_poisoned");
        EXPECT_FALSE(fs::exists(root / "READY"));
    }
}

TEST(HandoffShutdownTest, WatchdogFiresBeforeReadyExists) {
    TempDir root;
    FakeShutdown fake;
    fake.write_delay = std::chrono::milliseconds(600);
    std::atomic<bool> ready_at_fire{true};
    std::atomic<bool> published{false};
    {
        // In the master the action records "deadline" and calls _exit().
        Watchdog dog(std::chrono::milliseconds(200), [&] {
            ready_at_fire = fs::exists(root / "READY") || published.load();
        });
        RunHandoffShutdown(fake.Steps(), {root.path(), true, 4, 14},
                           &published);
        EXPECT_TRUE(dog.fired());
    }
    EXPECT_FALSE(ready_at_fire.load());
}

TEST(HandoffShutdownTest, RestoreThatWouldNotFitIsSkipped) {
    TempDir root;
    FakeShutdown fake;
    // 20M objects need 24.5 GiB > 16 GiB.
    fake.objects = 20000000;
    fake.memory = Memory(16 * kGiB, 1 * kGiB);
    ShutdownReport report =
        RunHandoffShutdown(fake.Steps(), {root.path(), true, 4, 14});
    EXPECT_EQ(report.result, "skipped_restore_memory");
    // No write, no trim (trimming cannot help), nothing to claim.
    EXPECT_EQ(std::count(fake.events.begin(), fake.events.end(), "write"), 0);
    EXPECT_EQ(std::count(fake.events.begin(), fake.events.end(), "trim"), 0);
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
    const std::string last = ReadLastShutdown(root.path());
    EXPECT_NE(last.find("\"result\":\"skipped_restore_memory\""),
              std::string::npos)
        << last;
}

TEST(HandoffShutdownTest, RestoreNeedOfTheWrittenSnapshotIsCheckedAgain) {
    // The admission counted 1000 objects; the write reports far more (the
    // count grew between the two, which a quiesced master never does). A
    // snapshot the next master could not restore is not published.
    TempDir root;
    FakeShutdown fake;
    fake.memory = Memory(16 * kGiB, 1 * kGiB);
    ShutdownSteps steps = fake.Steps();
    steps.write = [](const std::string& dir,
                     size_t) -> tl::expected<ShutdownWriteResult, std::string> {
        WriteText(dir + "/handoff_manifest", "m");
        return ShutdownWriteResult{20000000, 4096, "{}"};
    };
    ShutdownReport report =
        RunHandoffShutdown(steps, {root.path(), true, 4, 14});
    EXPECT_EQ(report.result, "skipped_restore_memory") << report.ToJson();
    EXPECT_EQ(report.restore_needed,
              RestoreNeedBytes(20000000, RestoreCostModel{}));
    EXPECT_EQ(ClaimHandoffSnapshot(root.path()).state, ClaimState::kNone);
    for (const auto& entry : fs::directory_iterator(root.path())) {
        EXPECT_EQ(entry.path().filename().string().rfind(kSnapshotPrefix, 0),
                  std::string::npos)
            << "left behind: " << entry.path();
    }
}

// ------------------------------------------------------ startup sequencing

struct FakeStartup {
    int restores = 0;
    std::string dir_seen;
    bool fail = false;
    MemorySnapshot memory = Memory(80 * kGiB, 1 * kGiB);
    StartupSteps Steps() {
        StartupSteps s;
        s.read_memory = [this]() -> tl::expected<MemorySnapshot, std::string> {
            return memory;
        };
        s.restore = [this](
                        const std::string& dir,
                        size_t) -> tl::expected<RestoreSummary, std::string> {
            ++restores;
            dir_seen = dir;
            EXPECT_TRUE(fs::exists(dir));
            if (fail) return tl::make_unexpected("chunk crc mismatch");
            return RestoreSummary{7, 2, "{}"};
        };
        return s;
    }
};

StartupOptions Options(const std::string& root) {
    StartupOptions o;
    o.root = root;
    o.owner = true;
    o.max_age_ms = 600000;
    return o;
}

TEST(HandoffStartupTest, RestoresOnceAndCleansUp) {
    TempDir root;
    MakePublished(root.path(), "snap-9-1", NowMs());
    FakeStartup fake;
    StartupReport report =
        RunHandoffStartup(fake.Steps(), Options(root.path()));
    EXPECT_EQ(report.result, "ok") << report.detail;
    EXPECT_EQ(fake.restores, 1);
    EXPECT_EQ(fake.dir_seen, root / "snap-9-1");
    EXPECT_FALSE(fs::exists(root / "CLAIMED"));
    EXPECT_FALSE(fs::exists(root / "snap-9-1"));
    report = RunHandoffStartup(fake.Steps(), Options(root.path()));
    EXPECT_EQ(report.result, "none");
    EXPECT_EQ(fake.restores, 1);
}

TEST(HandoffStartupTest, FailedRestoreIsNotRetried) {
    TempDir root;
    MakePublished(root.path(), "snap-10-1", NowMs());
    FakeStartup fake;
    fake.fail = true;
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), Options(root.path())).result,
              "error");
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), Options(root.path())).result,
              "none");
    EXPECT_EQ(fake.restores, 1);
}

TEST(HandoffStartupTest, StaleDisabledAndNoHeadroomAreDiscarded) {
    TempDir root;
    FakeStartup fake;
    MakePublished(root.path(), "snap-11-1", NowMs() - 700000);
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), Options(root.path())).result,
              "stale");
    MakePublished(root.path(), "snap-11-2", NowMs() + 60000);
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), Options(root.path())).result,
              "stale");  // clock went backwards: refuse
    MakePublished(root.path(), "snap-11-3", NowMs());
    StartupOptions disabled = Options(root.path());
    disabled.restore_enabled = false;
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), disabled).result, "disabled");
    MakePublished(root.path(), "snap-11-4", NowMs(), 25000000);
    fake.memory = Memory(16 * kGiB, 1 * kGiB);
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), Options(root.path())).result,
              "memory_headroom");
    EXPECT_EQ(fake.restores, 0);
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), Options(root.path())).result,
              "none");
    for (const auto& entry : fs::directory_iterator(root.path())) {
        ADD_FAILURE() << "left behind: " << entry.path();
    }
}

TEST(HandoffStartupTest, MasterWithoutLockPoisonsTheDirectory) {
    TempDir root;
    FakeStartup fake;
    StartupOptions not_owner = Options(root.path());
    not_owner.owner = false;
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), not_owner).result, "not_owner");
    EXPECT_TRUE(PoisonExists(root.path()));
    // The lock owner publishes afterwards (it does not see POISON in time).
    MakePublished(root.path(), "snap-12-1", NowMs());
    EXPECT_EQ(RunHandoffStartup(fake.Steps(), Options(root.path())).result,
              "poisoned");
    EXPECT_EQ(fake.restores, 0);
    EXPECT_FALSE(PoisonExists(root.path()));
}

// ------------------------------------- real MasterService: quiesce + E2E

MasterServiceConfig ShortTtlConfig() {
    return MasterServiceConfig::builder()
        .set_memory_allocator(BufferAllocatorType::OFFSET)
        .set_eviction_ratio(0.0)
        .set_eviction_high_watermark_ratio(1.0)
        .set_client_live_ttl_sec(1)
        .set_client_suspicion_ttl_sec(1)
        .build();
}

Segment MakeSegment(int index, uint64_t size = 64 * kMiB) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "10.1.0." + std::to_string(10 + index) + ":15651";
    segment.base = 0x200000000000ULL + index * 0x10000000000ULL;
    segment.size = size;
    segment.te_endpoint = segment.name;
    segment.protocol = "tcp";
    segment.host_id = "host-" + std::to_string(index);
    return segment;
}

void PutObject(MasterService& service, const UUID& client,
               const Segment& segment, const std::string& key,
               bool end = true) {
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = segment.name;
    auto started =
        service.PutStart(client, key, TenantId::Default(), 64 * 1024, config);
    ASSERT_TRUE(started.has_value()) << toString(started.error());
    if (end) {
        ASSERT_TRUE(
            service
                .PutEnd(client, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
    }
}

size_t MountedSegments(MasterService& service) {
    auto segments = service.GetAllSegments();
    return segments ? segments->size() : 0;
}
TEST(HandoffEndToEndTest, ShutdownThenStartupRestoresTheIndex) {
    TempDir root;
    std::vector<Segment> segments{MakeSegment(0), MakeSegment(1)};
    std::vector<UUID> clients{generate_uuid(), generate_uuid()};
    uint64_t objects_before = 0;
    {
        MasterService old_master(
            MasterServiceConfig::builder()
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .set_eviction_ratio(0.0)
                .set_eviction_high_watermark_ratio(1.0)
                .build());
        for (int s = 0; s < 2; ++s) {
            ASSERT_TRUE(
                old_master.MountSegment(segments[s], clients[s]).has_value());
            for (int i = 0; i < 50; ++i) {
                PutObject(old_master, clients[s], segments[s],
                          "k-" + std::to_string(s) + "-" + std::to_string(i));
            }
        }
        PutObject(old_master, clients[0], segments[0], "in-flight",
                  /*end=*/false);
        objects_before = old_master.HandoffObjectCount();
        ASSERT_EQ(objects_before, 101u);

        ShutdownSteps steps;
        std::vector<std::string> order;
        steps.stop_serving = [&] { order.push_back("stop"); };
        steps.quiesce = [&] {
            order.push_back("quiesce");
            old_master.QuiesceBackgroundWork();
        };
        steps.read_memory = [] {
            return tl::expected<MemorySnapshot, std::string>(
                Memory(80 * kGiB, 1 * kGiB));
        };
        steps.object_count = [&] { return old_master.HandoffObjectCount(); };
        steps.disk_free = [](const std::string& p) { return FreeDiskBytes(p); };
        steps.write = [&](const std::string& dir, size_t workers)
            -> tl::expected<ShutdownWriteResult, std::string> {
            order.push_back("write");
            EXPECT_TRUE(old_master.BackgroundWorkQuiesced());
            HandoffWriteOptions options;
            options.dir = dir;
            options.workers = workers;
            auto written = WriteHandoffSnapshot(old_master, options);
            if (!written) return tl::make_unexpected(written.error());
            return ShutdownWriteResult{written->stats.objects,
                                       written->stats.total_bytes,
                                       written->ToJson()};
        };
        ShutdownReport report =
            RunHandoffShutdown(steps, {root.path(), true, 3, 4});
        ASSERT_EQ(report.result, "ok") << report.detail;
        EXPECT_EQ(report.objects, 100u);  // the in-flight put is left out
        const std::vector<std::string> expected{"stop", "quiesce", "write"};
        EXPECT_EQ(order, expected);
    }

    MasterService new_master(
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .build());
    StartupSteps steps;
    steps.read_memory = [] {
        return tl::expected<MemorySnapshot, std::string>(
            Memory(80 * kGiB, 1 * kGiB));
    };
    steps.restore =
        [&](const std::string& dir,
            size_t workers) -> tl::expected<RestoreSummary, std::string> {
        auto restored = new_master.RestoreFromHandoff(dir, {workers});
        if (!restored) return tl::make_unexpected(restored.error());
        return RestoreSummary{restored->objects, restored->segments,
                              restored->ToJson()};
    };
    StartupReport report = RunHandoffStartup(steps, Options(root.path()));
    ASSERT_EQ(report.result, "ok") << report.detail;
    EXPECT_EQ(report.objects, 100u);
    EXPECT_EQ(report.segments, 2u);
    EXPECT_EQ(new_master.HandoffObjectCount(), 100u);
    EXPECT_EQ(new_master.HandoffPendingSegmentCount(), 2u);
    // The owner remounts with the same identity -> readable again.
    ASSERT_TRUE(
        new_master.ReMountSegment({segments[0]}, clients[0]).has_value());
    EXPECT_TRUE(
        new_master.GetReplicaList("k-0-3", TenantId::Default()).has_value());
    // One shot: nothing left to restore.
    EXPECT_EQ(RunHandoffStartup(steps, Options(root.path())).result, "none");
}
// ---- Never serve next to a READY that could not be claimed or removed;
// never serve without POISON as a non-owner.

// A "handoff dir" that is a regular file: every stat below it fails with
// ENOTDIR (state unknown), every write fails. Turning it back into a
// directory models a disk that recovers.
struct BrokenDir {
    TempDir parent;
    std::string root = parent / "handoff";
    BrokenDir() { WriteText(root, "not a directory"); }
    void Repair() const {
        fs::remove(root);
        fs::create_directories(root);
    }
};

TEST(HandoffStartupTest, UnknownStateIsNeverReadAsAbsent) {
    BrokenDir broken;
    EXPECT_EQ(ProbePath(broken.root + "/READY"), PathState::kUnknown);
    EXPECT_TRUE(PoisonExists(broken.root));
    const ClaimResult claim = ClaimHandoffSnapshot(broken.root);
    EXPECT_EQ(claim.state, ClaimState::kError);
    EXPECT_TRUE(claim.ready_may_remain) << claim.detail;

    TempDir ok;
    EXPECT_EQ(ProbePath(ok / "READY"), PathState::kAbsent);
    EXPECT_FALSE(PoisonExists(ok.path()));
    MakePublished(ok.path(), "snap-20-1", NowMs());
    EXPECT_EQ(ProbePath(ok / "READY"), PathState::kPresent);
    EXPECT_EQ(StartupOptions{}.max_age_ms, 120000);
}

TEST(HandoffStartupTest, ClaimFailureNeverServesWhileReadyMayRemain) {
    BrokenDir broken;
    FakeStartup fake;
    StartupSteps steps = fake.Steps();
    int waits = 0;
    steps.wait_retry = [&](int attempt) {
        EXPECT_EQ(attempt, waits + 1);
        ++waits;
        return waits < 3;  // asked to shut down during the third wait
    };
    StartupReport report = RunHandoffStartup(steps, Options(broken.root));
    EXPECT_EQ(report.result, "error");
    EXPECT_FALSE(report.serve) << report.detail;
    EXPECT_EQ(waits, 3);
    EXPECT_EQ(report.attempts, 3u);
    EXPECT_EQ(fake.restores, 0);
}

TEST(HandoffStartupTest, ClaimIsRetriedUntilTheDiskRecovers) {
    BrokenDir broken;
    FakeStartup fake;
    StartupSteps steps = fake.Steps();
    steps.wait_retry = [&](int attempt) {
        if (attempt == 2) {
            // The disk recovers and holds a fresh snapshot.
            broken.Repair();
            MakePublished(broken.root, "snap-21-1", NowMs());
        }
        return true;
    };
    StartupReport report = RunHandoffStartup(steps, Options(broken.root));
    EXPECT_EQ(report.result, "ok") << report.detail;
    EXPECT_TRUE(report.serve);
    EXPECT_EQ(report.attempts, 3u);
    EXPECT_EQ(fake.restores, 1);
    EXPECT_EQ(ProbePath(broken.root + "/READY"), PathState::kAbsent);
}

TEST(HandoffStartupTest, UnremovableReadyIsWaitedOutUntilStale) {
    BrokenDir broken;
    FakeStartup fake;
    StartupSteps steps = fake.Steps();
    int waits = 0;
    steps.wait_retry = [&](int) {
        ++waits;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        return true;
    };
    // max_age 0: a READY written before the wait started (its marker is
    // unreadable here) is refused by every master 1 s after the wait began.
    StartupOptions options = Options(broken.root);
    options.max_age_ms = 0;
    const auto start = std::chrono::steady_clock::now();
    StartupReport report = RunHandoffStartup(steps, options);
    EXPECT_EQ(report.result, "error");
    EXPECT_TRUE(report.serve) << report.detail;
    EXPECT_GE(std::chrono::steady_clock::now() - start,
              std::chrono::milliseconds(1000));
    EXPECT_GE(waits, 3);
    EXPECT_EQ(fake.restores, 0);
}

TEST(HandoffStartupTest, NonOwnerServesOnlyAfterPoisonIsWritten) {
    {
        BrokenDir broken;
        FakeStartup fake;
        StartupSteps steps = fake.Steps();
        steps.wait_retry = [](int attempt) { return attempt < 2; };
        StartupOptions not_owner = Options(broken.root);
        not_owner.owner = false;
        StartupReport report = RunHandoffStartup(steps, not_owner);
        EXPECT_EQ(report.result, "error");
        EXPECT_FALSE(report.serve);
        EXPECT_EQ(report.attempts, 2u);
    }
    {
        BrokenDir broken;
        FakeStartup fake;
        StartupSteps steps = fake.Steps();
        steps.wait_retry = [&](int) {
            broken.Repair();
            return true;
        };
        StartupOptions not_owner = Options(broken.root);
        not_owner.owner = false;
        StartupReport report = RunHandoffStartup(steps, not_owner);
        EXPECT_EQ(report.result, "not_owner");
        EXPECT_TRUE(report.serve);
        EXPECT_EQ(report.attempts, 2u);
        EXPECT_EQ(ProbePath(broken.root + "/POISON"), PathState::kPresent);
    }
}

TEST(HandoffShutdownTest, PoisonOfUnknownStateBlocksThePublish) {
    BrokenDir broken;
    FakeShutdown fake;
    ShutdownReport report =
        RunHandoffShutdown(fake.Steps(), {broken.root, true, 4, 14});
    EXPECT_EQ(report.result, "skipped_poisoned");
    EXPECT_EQ(std::count(fake.events.begin(), fake.events.end(), "write"), 0);
}

// ------------------------------------------------------------------ config

TEST(HandoffConfigTest, RefusesHaDfsAndOtherAllocators) {
    EXPECT_FALSE(ValidateHandoffConfig(false, false, "offset").has_value());
    // HA owns the index through the OpLog and the standby: one authority per
    // state, so the handoff cannot be switched on next to it.
    auto ha = ValidateHandoffConfig(true, false, "offset");
    ASSERT_TRUE(ha.has_value());
    EXPECT_NE(ha->find("enable_ha"), std::string::npos) << *ha;
    EXPECT_TRUE(ValidateHandoffConfig(false, true, "offset").has_value());
    EXPECT_TRUE(ValidateHandoffConfig(false, false, "cachelib").has_value());
}

}  // namespace
}  // namespace mooncake::test
