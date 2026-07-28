// Reproduction test for the snapshot_mutex_ <-> segment_mutex_ ABBA deadlock.
//
// MasterService documents its canonical lock hierarchy in master_service.h:
//
//     1. client_mutex_
//     2. tenant_quota_policy_mutex_
//     3. snapshot_mutex_
//     4. metadata_shards_[i].mutex
//     5. tenant_quota_shards_[i].mutex
//     6. segment_mutex_
//
// i.e. snapshot_mutex_ (#3) must ALWAYS be acquired before segment_mutex_ (#6).
// Nearly every path obeys this (UnmountSegment, MountSegment, ReMountSegment,
// GracefulUnmountSegment, ClientMonitorFunc, ClearInvalidHandles all take
// snapshot first). ApplySnapshotState violates it:
//
//     master_service.cpp (ApplySnapshotState):
//         ScopedSegmentAccess segment_access =
//             segment_manager_.getSegmentAccess();          // #6 segment
//             UNIQUE
//         ...
//         for (...) UnmountSegment(segment.id, client_id);  // -> #3 snapshot
//         SHARED
//
// That is segment(#6) -> snapshot(#3), the inverse of the documented order, and
// it forms a deterministic ABBA with any snapshot->segment path, e.g.
// GracefulUnmountSegment (snapshot UNIQUE -> segment UNIQUE):
//
//     Thread A (GracefulUnmountSegment order): holds snapshot(U), wants
//     segment(U) Thread B (ApplySnapshotState order):     holds segment(U),
//     wants snapshot(S)
//
//     A's segment(U) is blocked by B's segment(U).
//     B's snapshot(S) is blocked by A's snapshot(U)  (a unique lock blocks
//     readers).
//     => both threads wedge forever.
//
// This is the same deadlock family as commit 299e95e7 ("Fix ABBA deadlock
// between GracefulUnmountScheduler and snapshot_mutex_"). In production it
// surfaces as the master freezing (metadata counters stop moving, gRPC port
// hangs) while the separate metrics endpoint keeps `up` == 1.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace mooncake::test {

// Friend of MasterService (see master_service.h) so the scenario member
// functions below can drive the REAL private snapshot_mutex_ and the
// SegmentManager's segment_mutex_ (via the public getSegmentAccess() RAII
// accessor) in the exact orders the production code paths use.
//
// NOTE: the private-member access must happen in member functions of this
// class (which are friends), NOT inside lambdas (lambdas are distinct types
// and do not inherit friend access).
class MasterServiceDeadlockTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (!google::IsGoogleLoggingInitialized()) {
            google::InitGoogleLogging("MasterServiceDeadlockTest");
            FLAGS_logtostderr = true;
        }
    }

    // Path A — GracefulUnmountSegment order: snapshot(U) -> segment(U).
    // Path B — ApplySnapshotState order:     segment(U) -> snapshot(S).
    // The two threads wedge in an ABBA. Does not return on buggy code.
    void RunInvertedOrderScenario() {
        MasterService service(MasterServiceConfig::builder().build());

        // Cross-wait so both threads HOLD their first lock before either
        // requests its second — makes the ABBA deterministic, not a race.
        std::atomic<bool> a_holds_snapshot{false};
        std::atomic<bool> b_holds_segment{false};

        std::thread path_a([&] {
            std::unique_lock<std::shared_mutex> snapshot_lock(
                service.snapshot_mutex_);  // #3 snapshot UNIQUE
            a_holds_snapshot.store(true);
            while (!b_holds_segment.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            auto segment_lock =
                service.segment_manager_.getSegmentAccess();  // #6 -> BLOCKS
            (void)segment_lock;
        });

        std::thread path_b([&] {
            auto segment_lock = service.segment_manager_
                                    .getSegmentAccess();  // #6 segment UNIQUE
            b_holds_segment.store(true);
            while (!a_holds_snapshot.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            std::shared_lock<std::shared_mutex> snapshot_lock(
                service.snapshot_mutex_);  // #3 snapshot SHARED -> BLOCKS
            (void)snapshot_lock;
        });

        path_a.join();  // never returns once the ABBA forms
        path_b.join();
    }

    // Control: Path B fixed to the documented order (snapshot #3 BEFORE segment
    // #6). Both threads finish.
    void RunCanonicalOrderScenario() {
        MasterService service(MasterServiceConfig::builder().build());

        std::thread path_a([&] {
            std::unique_lock<std::shared_mutex> snapshot_lock(
                service.snapshot_mutex_);  // #3 first (canonical)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto segment_lock =
                service.segment_manager_.getSegmentAccess();  // #6 then
            (void)segment_lock;
        });

        std::thread path_b([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            std::shared_lock<std::shared_mutex> snapshot_lock(
                service.snapshot_mutex_);  // #3 first (canonical)
            auto segment_lock =
                service.segment_manager_.getSegmentAccess();  // #6 then
            (void)segment_lock;
        });

        path_a.join();
        path_b.join();
    }

    // Run `scenario` in a forked child and report whether it FINISHED within
    // `timeout`. The deadlock scenario intentionally wedges two threads, so it
    // cannot run in the gtest process directly (it would hang the whole suite).
    // Forking isolates the hang: if the child is still alive when the deadline
    // passes, the ABBA reproduced and we SIGKILL the child. Returns true iff
    // the child exited in time (no deadlock); false iff it was killed
    // (deadlock).
    template <typename Scenario>
    bool FinishesWithin(Scenario scenario, std::chrono::seconds timeout) {
        ::fflush(nullptr);
        pid_t pid = ::fork();
        if (pid == 0) {
            // Child: only the forking thread exists here, so constructing the
            // service inside `scenario` starts its background threads fresh
            // (no inherited threads).
            scenario();
            ::_exit(0);  // reached only if scenario() did not deadlock
        }
        if (pid == -1) {
            ADD_FAILURE() << "fork failed: " << strerror(errno);
            return true;  // don't mask the real error
        }

        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            int status = 0;
            pid_t r = ::waitpid(pid, &status, WNOHANG);
            if (r == pid) {
                return true;  // child finished -> no deadlock
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ::kill(pid, SIGKILL);
        int status = 0;
        ::waitpid(pid, &status, 0);
        return false;  // child wedged -> deadlock reproduced
    }
};

// Hazard proof: driving the two lock orders that the (pre-fix)
// ApplySnapshotState combined with GracefulUnmountSegment —
// snapshot(U)->segment(U) vs segment(U)->snapshot(S) — ALWAYS deadlocks. This
// test constructs the inverted order directly (it does not call
// ApplySnapshotState), so it is expected to deadlock regardless of production
// code; it documents WHY the lock order matters and guards against someone
// "fixing" the control test below by weakening the lock primitives. Pair with
// CanonicalOrderDoesNotDeadlock, which proves the order ApplySnapshotState uses
// after the fix is safe.
TEST_F(MasterServiceDeadlockTest, ReproducesSnapshotSegmentABBA) {
    const bool finished = FinishesWithin([this] { RunInvertedOrderScenario(); },
                                         std::chrono::seconds(5));

    EXPECT_FALSE(finished)
        << "The inverted snapshot_mutex_(#3) <-> segment_mutex_(#6) order is "
           "expected to deadlock. If this no longer reproduces, the lock "
           "primitives/semantics changed and both this test and its control "
           "need revisiting.";
}

// Control: when Path B follows the documented order (snapshot #3 BEFORE segment
// #6), the same two threads complete. Isolates the lock ORDER as the bug.
TEST_F(MasterServiceDeadlockTest, CanonicalOrderDoesNotDeadlock) {
    const bool finished = FinishesWithin(
        [this] { RunCanonicalOrderScenario(); }, std::chrono::seconds(5));

    EXPECT_TRUE(finished)
        << "Canonical snapshot(#3)->segment(#6) order must not deadlock.";
}

}  // namespace mooncake::test
