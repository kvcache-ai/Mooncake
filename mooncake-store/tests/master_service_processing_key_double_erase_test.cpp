// Regression test for the invalid-replica cleanup that runs at the head of a
// read-write operation: it must tear one publication down at most once, and it
// must not crash the process.
//
// The chain: MountSegment a segment, PutStart a key without PutEnd (routing
// the entry with is_processing set and one incomplete replica), then
// PrepareUnmountSegment so that replica's memory handle goes invalid, then
// PutEnd, which resolves the entry, drops the invalid replica and tears the
// object down exactly once.
//
// The unmount must not use MasterService::UnmountSegment: it runs
// ClearInvalidHandles() internally, which would erase the crafted object
// through the sweep before PutEnd could exercise the accessor's own cleanup.
//
// The child asserts EraseMetadata claimed ObjectEntry::State::is_torn_down,
// the processing marker is cleared and the route slot is gone, and reports
// that through its exit code: a forked child turns a crash into a test failure.

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <sys/wait.h>
#include <unistd.h>

namespace mooncake::test {

class MasterServiceProcessingKeyDoubleEraseTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceProcessingKeyDoubleEraseTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kSegmentBase = 0x300000000;
    static constexpr size_t kSegmentSize = 16 * 1024 * 1024;

    // Exit codes used by the child to report what it observed.
    static constexpr int kExitOk = 0;               // torn down, no crash
    static constexpr int kExitMountFailed = 2;      // scenario setup broken
    static constexpr int kExitPutStartFailed = 3;   // scenario setup broken
    static constexpr int kExitUnmountFailed = 4;    // scenario setup broken
    static constexpr int kExitPutEndAnswer = 5;     // PutEnd answered wrong
    static constexpr int kExitNotTornDown = 6;      // teardown flag unclaimed
    static constexpr int kExitStillProcessing = 7;  // marker outlived teardown
    static constexpr int kExitStillRouted = 8;      // slot outlived teardown

    // Builds the crafted state and fires the trigger; only returns once the
    // cleanup has claimed the teardown, so a double teardown dies here.
    void RunIncidentScenario() {
        MasterService service(MasterServiceConfig::builder().build());

        // 1. Ghost client mounts a segment.
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "ghost_segment";
        segment.base = kSegmentBase;
        segment.size = kSegmentSize;
        segment.te_endpoint = segment.name;
        const UUID client_id = generate_uuid();
        if (!service.MountSegment(segment, client_id).has_value()) {
            ::_exit(kExitMountFailed);
        }

        // 2. PutStart a key onto the segment and never complete it: the entry
        //    is routed with is_processing set and one PROCESSING replica, which
        //    is the state a client that died mid-put leaves behind.
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment.name;
        const std::string key = "orphan_processing_key";
        if (!service.PutStart(client_id, key, TenantId::Default(), 1024, config)
                 .has_value()) {
            ::_exit(kExitPutStartFailed);
        }

        // Hold the publication across its teardown, so the flag the cleanup
        // claims can still be read once the route slot is gone.
        auto entry = MasterServiceTestPeer::FindObject(
            service,
            MasterServiceTestPeer::ObjectIdentity{TenantId::Default(), key});
        if (entry == nullptr) {
            ::_exit(kExitPutStartFailed);
        }

        // 3. Ghost client expires: the segment allocator is destroyed,
        //    invalidating the replica's memory handle (weak_ptr expires). No
        //    ClearInvalidHandles sweep here (see the file header).
        size_t metrics_dec_capacity = 0;
        {
            auto segment_access = MasterServiceTestPeer::SegmentManager(service)
                                      .getSegmentAccess();
            if (segment_access.PrepareUnmountSegment(
                    segment.id, metrics_dec_capacity) != ErrorCode::OK) {
                ::_exit(kExitUnmountFailed);
            }
        }

        // 4. Trigger: the read-write access drops the invalid replica, finds
        //    the entry invalid and tears it down. The callback never runs, so
        //    PutEnd answers the way every other path reports an absent key.
        const auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                            ReplicaType::MEMORY);
        if (put_end.has_value() ||
            put_end.error() != ErrorCode::OBJECT_NOT_FOUND) {
            ::_exit(kExitPutEndAnswer);
        }

        const bool torn_down = entry->WithSharedAccess(
            [](const ObjectMetadata&, const ObjectEntry::State& state) {
                return state.is_torn_down;
            });
        if (!torn_down) {
            ::_exit(kExitNotTornDown);
        }
        const bool still_processing = entry->WithSharedAccess(
            [](const ObjectMetadata&, const ObjectEntry::State& state) {
                return state.is_processing;
            });
        if (still_processing) {
            ::_exit(kExitStillProcessing);
        }
        if (MasterServiceTestPeer::FindObject(
                service, MasterServiceTestPeer::ObjectIdentity{
                             TenantId::Default(), key}) != nullptr) {
            ::_exit(kExitStillRouted);
        }

        // A repeat of the trigger resolves nothing now and stays harmless.
        (void)service.PutEnd(client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY);

        ::_exit(kExitOk);
    }
};

// The chain above must reach the end without crashing, with the object torn
// down exactly once.
TEST_F(MasterServiceProcessingKeyDoubleEraseTest,
       AccessorCleanupAfterSegmentUnmountDoesNotCrash) {
    ::fflush(nullptr);
    pid_t pid = ::fork();
    ASSERT_NE(pid, -1) << "fork failed: " << strerror(errno);
    if (pid == 0) {
        RunIncidentScenario();
        ::_exit(kExitOk);  // unreachable (RunIncidentScenario exits itself)
    }

    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);

    if (WIFSIGNALED(status)) {
        FAIL() << "invalid-replica cleanup crashed the service: child died "
                  "with signal "
               << WTERMSIG(status)
               << (WTERMSIG(status) == SIGSEGV ? " (SIGSEGV)" : "")
               << ". One publication must be torn down at most once; "
                  "ObjectEntry::State::is_torn_down is what claims it.";
    }
    ASSERT_TRUE(WIFEXITED(status)) << "child did not exit normally";
    EXPECT_EQ(WEXITSTATUS(status), kExitOk)
        << "scenario failed (exit " << WEXITSTATUS(status)
        << ": 2=MountSegment, 3=PutStart, 4=PrepareUnmountSegment, "
           "5=PutEnd did not report the object as absent, "
           "6=teardown flag never claimed, 7=processing marker outlived the "
           "teardown, 8=route slot outlived the teardown)";
}

}  // namespace mooncake::test
