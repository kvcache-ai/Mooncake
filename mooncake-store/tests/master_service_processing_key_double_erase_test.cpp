// Regression test for a MetadataAccessorRW double-erase use-after-free (prod
// incident 2026-08-03: mooncake_master segfaulted every ~5 minutes after a
// snapshot restore, always at the same instruction inside
// std::unordered_set<std::string>::erase(const_iterator) — the bucket-chain
// walk dereferencing the chain-end nullptr).
//
// The MetadataAccessorRW constructor erases an object whose replicas have all
// turned invalid. While the processing marker was a key in a per-tenant
// processing set, the constructor erased the object (EraseMetadata also
// dropped the key from the set) and then erased the key again through an
// iterator taken before, which by then pointed at a freed node. libstdc++
// re-read the cached hash from the freed node, ran off the end of the bucket
// chain and dereferenced nullptr -> SIGSEGV (fault address 0x0, exactly as
// observed in the prod kernel logs).
//
// The marker now lives on the object's own record and goes with it, so there
// is nothing left to erase twice. The scenario stays as a regression test for
// the accessor's cleanup path.
//
// Production trigger chain reproduced here (public API + test peer):
//   1. MountSegment                     (a "ghost" client mounts a segment)
//   2. PutStart without PutEnd          (the object stays processing with an
//                                        incomplete replica on that segment)
//   3. PrepareUnmountSegment            (ghost client expires; the replica's
//                                        allocator weak_ptr expires, so
//                                        has_invalid_mem_handle() == true)
//   4. PutEnd (or any op constructing   (ctor cleanup: erases invalid replicas
//      MetadataAccessorRW for the key)   -> !IsValid() -> Erase())
//
// Two scenario details matter for a deterministic repro:
//   * Step 3 must NOT use MasterService::UnmountSegment: it internally runs
//     ClearInvalidHandles(), which would erase the crafted object through the
//     SAFE path (EraseMetadata) before step 4 can hit the buggy accessor path.
//     Production had the same window: the expiry thread unmounts the ghost
//     segment and only THEN slowly sweeps 23M keys in ClearInvalidHandles —
//     any RPC landing in that window hits the buggy accessor cleanup first.
//   * A second live key ON THE SAME METADATA SHARD must keep the TenantState
//     non-empty. Otherwise MaybeEraseEmptyTenant() erases the tenant and
//     nulls tenant_state_, masking the bug (the buggy branch is guarded by
//     tenant_state_ != nullptr). Production tenants hold millions of keys,
//     so the buggy branch always executed.
//
// On the buggy code step 4 segfaults; the forked-child assertion below turns
// that into a clean test failure. After the fix the child exits 0 (PutEnd
// simply reports OBJECT_NOT_FOUND) and the test passes.

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

    // Exit codes used by the child to report how far it got.
    static constexpr int kExitOk = 0;              // reached past the trigger
    static constexpr int kExitMountFailed = 2;     // scenario setup broken
    static constexpr int kExitPutStartFailed = 3;  // scenario setup broken
    static constexpr int kExitUnmountFailed = 4;   // scenario setup broken

    // Friend access: find a key that routes to the SAME metadata shard as
    // `key` (getShardIndex hashes tenant+key, so a naive second key
    // lands in a different shard's TenantState and cannot keep THIS shard's
    // tenant non-empty).
    std::string FindKeyOnSameShard(MasterService& service,
                                   const std::string& key) {
        const size_t target = MasterServiceTestPeer(service).getShardIndex(
            TenantId::Default(), key);
        for (int i = 0; i < 100000; ++i) {
            std::string candidate = key + "_keepalive_" + std::to_string(i);
            if (MasterServiceTestPeer(service).getShardIndex(
                    TenantId::Default(), candidate) == target) {
                return candidate;
            }
        }
        return key + "_keepalive_fallback";
    }

    // Builds the incident state and fires the trigger. Only returns on
    // fixed code; on buggy code it dies with SIGSEGV inside the
    // MetadataAccessorRW constructor invoked by PutEnd.
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

        // 2. PutStart a key onto the segment and never complete it — the key
        //    object stays processing (client "died" mid-put).
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment.name;
        const std::string key = "orphan_processing_key";
        if (!service.PutStart(client_id, key, TenantId::Default(), 1024, config)
                 .has_value()) {
            ::_exit(kExitPutStartFailed);
        }

        // 2b. A second, completed key on the SAME shard keeps the TenantState
        //     non-empty in step 4 (see file header for why this is required).
        const std::string keepalive_key = FindKeyOnSameShard(service, key);
        if (!service
                 .PutStart(client_id, keepalive_key, TenantId::Default(), 1024,
                           config)
                 .has_value() ||
            !service
                 .PutEnd(client_id, keepalive_key, TenantId::Default(),
                         ReplicaType::MEMORY)
                 .has_value()) {
            ::_exit(kExitPutStartFailed);
        }

        // 3. Ghost client expires: the segment allocator is destroyed,
        //    invalidating the replica's memory handle (weak_ptr expires).
        //    No ClearInvalidHandles sweep here (see file header).
        size_t metrics_dec_capacity = 0;
        {
            auto segment_access = MasterServiceTestPeer::SegmentManager(service)
                                      .getSegmentAccess();
            if (segment_access.PrepareUnmountSegment(
                    segment.id, metrics_dec_capacity) != ErrorCode::OK) {
                ::_exit(kExitUnmountFailed);
            }
        }

        // 4. Trigger: PutEnd constructs MetadataAccessorRW(service, key).
        //    The ctor erases the invalid replica, finds !IsValid() and
        //    erases the object, processing marker included.
        (void)service.PutEnd(client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY);
        ::_exit(kExitOk);  // only reachable on fixed code
    }
};

// Regression assertion: the incident scenario must complete without crashing.
// On the current buggy code the forked child dies with SIGSEGV (this is the
// reproduction); after the fix it exits kExitOk and the test passes.
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
        FAIL() << "MetadataAccessorRW cleanup crashed: child died with signal "
               << WTERMSIG(status)
               << (WTERMSIG(status) == SIGSEGV ? " (SIGSEGV)" : "")
               << " while erasing a processing object left with only invalid "
                  "replicas.";
    }
    ASSERT_TRUE(WIFEXITED(status)) << "child did not exit normally";
    EXPECT_EQ(WEXITSTATUS(status), kExitOk)
        << "scenario setup failed (exit " << WEXITSTATUS(status)
        << ": 2=MountSegment, 3=PutStart, 4=UnmountSegment)";
}

}  // namespace mooncake::test
