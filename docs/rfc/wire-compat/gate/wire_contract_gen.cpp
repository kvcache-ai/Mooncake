// wire_contract_gen.cpp
//
// Wire-contract golden generator for the Mooncake Store master coro_rpc surface.
//
// This is the FACTUAL backbone of the wire-compat CI gate proposed in
// WIRE-COMPAT-RFC.md (a complement to upstream issue #1920). It emits, for every
// WrappedMasterService coro_rpc handler, the two integers that together define
// the on-wire identity of an RPC:
//
//   * function-id   = coro_rpc dispatch key
//                   = struct_pack::MD5::MD5Hash32(qualified-function-name)
//                     where the name is the fully-qualified
//                     "mooncake::WrappedMasterService::<Method>" string produced
//                     by coro_rpc::get_func_name<&Method>()
//                     (ylt/util/utils.hpp: consteval func_id()).
//
//   * arg-type-code = struct_pack type code of the serialized argument tuple
//                   = struct_pack::MD5::MD5Hash32(type-layout-literal) & 0xFFFFFFFE
//                     (ylt/struct_pack/type_calculate.hpp:515 get_types_code_impl).
//
// Both are computed here with the REAL yalantinglibs implementation, not a
// re-implementation. The function-id is fully self-contained (it depends only on
// the qualified name). The arg-type-codes below are computed over layout mirrors
// that are pinned to the v0.3.11.post1 wire layout and are proven faithful by the
// self-check in main(): they reproduce the type codes already established in the
// #2288 tenant-identity investigation
//   (ExistKey  bare<string>            = 0x9dcffa76,
//    PutStart  v0.3.11 4-arg tuple     = 0xfad0c534,
//    PutStart  + bare trailing string  = 0x22f8edba   <- the #2288 break,
//    PutStart  + compatible<string>    = 0xfad0c534   <- the fix).
//
// The PRODUCTION gate (wire_contract_test.cpp) instead #includes the real
// types.h / replica.h and computes the same codes over the actual production
// structs, so it tracks real layout drift. This standalone generator exists so
// the gate can be built and demonstrated without the full Mooncake build, and so
// the golden file can be regenerated and diffed in code review.
//
// Build:
//   g++ -std=c++20 -I<ylt-install>/include wire_contract_gen.cpp -o wire_contract_gen
// Run:
//   ./wire_contract_gen            # prints the golden table to stdout
//   ./wire_contract_gen --selfcheck  # verifies the #2288 reference codes, rc!=0 on mismatch

#include <ylt/util/function_name.h>
#include <ylt/struct_pack.hpp>
#include <ylt/struct_pack/md5_constexpr.hpp>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

// --- Real coro_rpc function-id, reproduced verbatim from ylt/util/utils.hpp ---
// (utils.hpp pulls async_simple transitively via other headers, so we restate the
//  three-line consteval here. It calls the REAL get_func_name + REAL MD5Hash32.)
namespace coro_rpc_gate {
template <auto func>
consteval std::uint32_t func_id() {
    constexpr auto name = coro_rpc::get_func_name<func>();
    return struct_pack::MD5::MD5Hash32Constexpr(name.data(), name.length());
}
}  // namespace coro_rpc_gate

// --- Wire-layout mirrors (pinned to v0.3.11.post1) -------------------------
// These mirror the on-wire layout of the argument types. They are intentionally
// frozen copies: the production gate uses the real types, but the wire CONTRACT
// is the frozen layout, so freezing the mirror here is correct for the golden.
namespace wire {

using UUID = std::pair<std::uint64_t, std::uint64_t>;

// allocator.h ReplicaType: a plain enum class over the default underlying type.
enum class ReplicaType { MEMORY, DISK, LOCAL_DISK, NOF_SSD, ALL };

// replica.h ReplicateConfig as serialized at the CURRENT v0.3.11.post1 HEAD
// (commit e9c6107). NOTE: this is a 7-field layout. The #2288-era investigation
// recorded the PutStart code 0xfad0c534, which corresponds to the EARLIER 3-field
// layout (replica_num, with_soft_pin, preferred_segment) -- captured below as
// ReplicateConfigV2288. The fact that these two produce DIFFERENT type-codes
//   3-field -> 0xfad0c534      (#2288-era)
//   7-field -> 0x2b2f212e      (current HEAD)
// is itself a real, silent PutStart wire-code shift introduced by field additions
// (e.g. NoF metadata, #2143) -- exactly the class of break this gate exists to
// catch. The golden table emits the CURRENT (7-field) code; the self-check
// validates the engine against the recorded #2288 reference using the 3-field
// layout that produced it.
struct ReplicateConfig {
    std::size_t replica_num{1};
    std::size_t nof_replica_num{0};
    bool with_soft_pin{false};
    bool with_hard_pin{false};
    std::vector<std::string> preferred_segments{};
    std::string preferred_segment{};
    std::vector<std::string> preferred_nof_segments{};
};

// Frozen 3-field layout as serialized at the time of the #2288 investigation.
// Used only by the self-check to prove the codec engine is faithful to the
// recorded reference codes.
struct ReplicateConfigV2288 {
    std::size_t replica_num{1};
    bool with_soft_pin{false};
    std::string preferred_segment{};
};

using compat_string = struct_pack::compatible<std::string>;

template <typename... T>
std::uint32_t arg_type_code() {
    return struct_pack::get_type_code<T...>();
}

}  // namespace wire

// --- Stub of the real service for faithful function-id computation ----------
// Only the qualified NAME of each member matters for the function-id, so these
// stubs reproduce the exact dispatch keys of the production handlers. The bodies
// are never defined; we only take their addresses in a consteval context.
namespace mooncake {
struct WrappedMasterService {
    bool ExistKey(const std::string&);
    int BatchQueryIp(const std::vector<wire::UUID>&);
    int BatchReplicaClear(const std::vector<std::string>&, const wire::UUID&,
                          const std::string&);
    int GetReplicaListByRegex(const std::string&);
    int GetReplicaList(const std::string&);
    int BatchGetReplicaList(const std::vector<std::string>&);
    int PutStart(const wire::UUID&, const std::string&, std::uint64_t,
                 const wire::ReplicateConfig&);
    int PutEnd(const wire::UUID&, const std::string&, wire::ReplicaType);
    int PutRevoke(const wire::UUID&, const std::string&, wire::ReplicaType);
    int BatchPutStart(const wire::UUID&, const std::vector<std::string>&,
                      const std::vector<std::uint64_t>&,
                      const wire::ReplicateConfig&);
    int BatchPutEnd(const wire::UUID&, const std::vector<std::string>&,
                    wire::ReplicaType);
    int BatchPutRevoke(const wire::UUID&, const std::vector<std::string>&,
                       wire::ReplicaType);
    int UpsertStart(const wire::UUID&, const std::string&, std::uint64_t,
                    const wire::ReplicateConfig&);
    int UpsertEnd(const wire::UUID&, const std::string&, wire::ReplicaType);
    int UpsertRevoke(const wire::UUID&, const std::string&, wire::ReplicaType);
    int BatchUpsertStart(const wire::UUID&, const std::vector<std::string>&,
                         const std::vector<std::uint64_t>&,
                         const wire::ReplicateConfig&);
    int BatchUpsertEnd(const wire::UUID&, const std::vector<std::string>&);
    int BatchUpsertRevoke(const wire::UUID&, const std::vector<std::string>&);
    int Remove(const std::string&, bool);
    int RemoveByRegex(const std::string&, bool);
    int RemoveAll(bool);
    int BatchRemove(const std::vector<std::string>&, bool);
    int MountSegment(int, const wire::UUID&);
    int MountNoFSegment(int, const wire::UUID&);
    int ReMountSegment(int, const wire::UUID&);
    int ReMountNoFSegment(int, const wire::UUID&);
    int UnmountSegment(const wire::UUID&, const wire::UUID&);
    int GracefulUnmountSegment(const wire::UUID&, const wire::UUID&,
                               std::uint64_t);
    int UnmountNoFSegment(const wire::UUID&, const wire::UUID&);
    int GetAllNoFSegments();
    int GetNoFSegmentsByName(const std::string&);
    int Ping(const wire::UUID&);
    int GetFsdir();
    int QuerySegmentStatus(const std::string&);
    int QuerySegmentStatusById(const wire::UUID&);
    int GetStorageConfig();
    int BatchExistKey(const std::vector<std::string>&);
    int ServiceReady();
    int MountLocalDiskSegment(const wire::UUID&, bool);
    int OffloadObjectHeartbeat(const wire::UUID&, bool);
    int ReportSsdCapacity(const wire::UUID&, std::int64_t);
    int NotifyOffloadSuccess(const wire::UUID&, const std::vector<std::string>&,
                             int);
    int PromotionObjectHeartbeat(const wire::UUID&);
    int PromotionAllocStart(const wire::UUID&, const std::string&,
                            std::uint64_t, const std::vector<std::string>&);
    int NotifyPromotionSuccess(const wire::UUID&, const std::string&);
    int NotifyPromotionFailure(const wire::UUID&, const std::string&);
    int CopyStart(const wire::UUID&, const std::string&, const std::string&,
                  const std::vector<std::string>&);
    int CopyEnd(const wire::UUID&, const std::string&);
    int CopyRevoke(const wire::UUID&, const std::string&);
    int MoveStart(const wire::UUID&, const std::string&, const std::string&,
                  const std::string&);
    int MoveEnd(const wire::UUID&, const std::string&);
    int MoveRevoke(const wire::UUID&, const std::string&);
    int EvictDiskReplica(const wire::UUID&, const std::string&,
                         wire::ReplicaType);
    int BatchEvictDiskReplica(const wire::UUID&, const std::vector<std::string>&,
                              wire::ReplicaType);
    int CreateCopyTask(const std::string&, const std::vector<std::string>&);
    int CreateMoveTask(const std::string&, const std::string&,
                       const std::string&);
    int QueryTask(const wire::UUID&);
    int FetchTasks(const wire::UUID&, std::size_t);
    int MarkTaskToComplete(const wire::UUID&, int);
};
}  // namespace mooncake

namespace mc = mooncake;

struct Row {
    const char* name;
    std::uint32_t function_id;
};

// Every handler registered in rpc_service.cpp RegisterRpcService(), in registration
// order. Function-ids are REAL coro_rpc dispatch keys.
static const std::vector<Row> kFunctionIds = {
#define FID(M) {#M, coro_rpc_gate::func_id<&mc::WrappedMasterService::M>()}
    FID(ExistKey),          FID(BatchQueryIp),
    FID(BatchReplicaClear), FID(GetReplicaListByRegex),
    FID(GetReplicaList),    FID(BatchGetReplicaList),
    FID(PutStart),          FID(PutEnd),
    FID(PutRevoke),         FID(BatchPutStart),
    FID(BatchPutEnd),       FID(BatchPutRevoke),
    FID(UpsertStart),       FID(UpsertEnd),
    FID(UpsertRevoke),      FID(BatchUpsertStart),
    FID(BatchUpsertEnd),    FID(BatchUpsertRevoke),
    FID(Remove),            FID(RemoveByRegex),
    FID(RemoveAll),         FID(BatchRemove),
    FID(MountSegment),      FID(MountNoFSegment),
    FID(ReMountSegment),    FID(ReMountNoFSegment),
    FID(UnmountSegment),    FID(GracefulUnmountSegment),
    FID(UnmountNoFSegment), FID(GetAllNoFSegments),
    FID(GetNoFSegmentsByName), FID(Ping),
    FID(GetFsdir),          FID(QuerySegmentStatus),
    FID(QuerySegmentStatusById), FID(GetStorageConfig),
    FID(BatchExistKey),     FID(ServiceReady),
    FID(MountLocalDiskSegment), FID(OffloadObjectHeartbeat),
    FID(ReportSsdCapacity), FID(NotifyOffloadSuccess),
    FID(PromotionObjectHeartbeat), FID(PromotionAllocStart),
    FID(NotifyPromotionSuccess), FID(NotifyPromotionFailure),
    FID(CopyStart),         FID(CopyEnd),
    FID(CopyRevoke),        FID(MoveStart),
    FID(MoveEnd),           FID(MoveRevoke),
    FID(EvictDiskReplica),  FID(BatchEvictDiskReplica),
    FID(CreateCopyTask),    FID(CreateMoveTask),
    FID(QueryTask),         FID(FetchTasks),
    FID(MarkTaskToComplete),
#undef FID
};

// Arg-tuple type-codes for handlers whose argument types are fully self-contained
// here (primitives / string / vector<string> / UUID / ReplicaType /
// ReplicateConfig). Handlers whose arguments use heavier production structs
// (Segment, *Request, *Response) are covered by the in-build production gate and
// are intentionally omitted from this standalone golden (marked OMITTED there).
struct TcRow {
    const char* name;
    std::uint32_t arg_type_code;
};

static std::vector<TcRow> argTypeCodes() {
    using namespace wire;
    return {
        {"ExistKey", arg_type_code<std::string>()},
        {"BatchQueryIp", arg_type_code<std::vector<UUID>>()},
        {"BatchReplicaClear",
         arg_type_code<std::vector<std::string>, UUID, std::string>()},
        {"GetReplicaListByRegex", arg_type_code<std::string>()},
        {"GetReplicaList", arg_type_code<std::string>()},
        {"BatchGetReplicaList", arg_type_code<std::vector<std::string>>()},
        {"PutStart",
         arg_type_code<UUID, std::string, std::uint64_t, ReplicateConfig>()},
        {"PutEnd", arg_type_code<UUID, std::string, ReplicaType>()},
        {"PutRevoke", arg_type_code<UUID, std::string, ReplicaType>()},
        {"BatchPutStart",
         arg_type_code<UUID, std::vector<std::string>,
                       std::vector<std::uint64_t>, ReplicateConfig>()},
        {"BatchPutEnd",
         arg_type_code<UUID, std::vector<std::string>, ReplicaType>()},
        {"BatchPutRevoke",
         arg_type_code<UUID, std::vector<std::string>, ReplicaType>()},
        {"UpsertStart",
         arg_type_code<UUID, std::string, std::uint64_t, ReplicateConfig>()},
        {"UpsertEnd", arg_type_code<UUID, std::string, ReplicaType>()},
        {"UpsertRevoke", arg_type_code<UUID, std::string, ReplicaType>()},
        {"BatchUpsertEnd",
         arg_type_code<UUID, std::vector<std::string>>()},
        {"BatchUpsertRevoke",
         arg_type_code<UUID, std::vector<std::string>>()},
        {"Remove", arg_type_code<std::string, bool>()},
        {"RemoveByRegex", arg_type_code<std::string, bool>()},
        {"RemoveAll", arg_type_code<bool>()},
        {"BatchRemove", arg_type_code<std::vector<std::string>, bool>()},
        {"UnmountSegment", arg_type_code<UUID, UUID>()},
        {"GracefulUnmountSegment",
         arg_type_code<UUID, UUID, std::uint64_t>()},
        {"UnmountNoFSegment", arg_type_code<UUID, UUID>()},
        {"GetNoFSegmentsByName", arg_type_code<std::string>()},
        {"Ping", arg_type_code<UUID>()},
        {"QuerySegmentStatus", arg_type_code<std::string>()},
        {"QuerySegmentStatusById", arg_type_code<UUID>()},
        {"BatchExistKey", arg_type_code<std::vector<std::string>>()},
        {"MountLocalDiskSegment", arg_type_code<UUID, bool>()},
        {"OffloadObjectHeartbeat", arg_type_code<UUID, bool>()},
        {"ReportSsdCapacity", arg_type_code<UUID, std::int64_t>()},
        {"PromotionObjectHeartbeat", arg_type_code<UUID>()},
        {"PromotionAllocStart",
         arg_type_code<UUID, std::string, std::uint64_t,
                       std::vector<std::string>>()},
        {"NotifyPromotionSuccess", arg_type_code<UUID, std::string>()},
        {"NotifyPromotionFailure", arg_type_code<UUID, std::string>()},
        {"CopyStart",
         arg_type_code<UUID, std::string, std::string,
                       std::vector<std::string>>()},
        {"CopyEnd", arg_type_code<UUID, std::string>()},
        {"CopyRevoke", arg_type_code<UUID, std::string>()},
        {"MoveStart",
         arg_type_code<UUID, std::string, std::string, std::string>()},
        {"MoveEnd", arg_type_code<UUID, std::string>()},
        {"MoveRevoke", arg_type_code<UUID, std::string>()},
        {"EvictDiskReplica",
         arg_type_code<UUID, std::string, ReplicaType>()},
        {"BatchEvictDiskReplica",
         arg_type_code<UUID, std::vector<std::string>, ReplicaType>()},
        {"CreateCopyTask",
         arg_type_code<std::string, std::vector<std::string>>()},
        {"CreateMoveTask",
         arg_type_code<std::string, std::string, std::string>()},
        {"QueryTask", arg_type_code<UUID>()},
        {"FetchTasks", arg_type_code<UUID, std::size_t>()},
    };
}

static int selfcheck() {
    using namespace wire;
    struct Case {
        const char* what;
        std::uint32_t got;
        std::uint32_t want;
    };
    const Case cases[] = {
        {"ExistKey bare<string>", arg_type_code<std::string>(), 0x9dcffa76u},
        {"PutStart #2288-era 4-arg (3-field RC)",
         arg_type_code<UUID, std::string, std::uint64_t, ReplicateConfigV2288>(),
         0xfad0c534u},
        {"PutStart + bare trailing string (#2288 break)",
         arg_type_code<UUID, std::string, std::uint64_t, ReplicateConfigV2288,
                       std::string>(),
         0x22f8edbau},
        {"PutStart + compatible<string> (the fix)",
         arg_type_code<UUID, std::string, std::uint64_t, ReplicateConfigV2288,
                       compat_string>(),
         0xfad0c534u},
    };
    int rc = 0;
    for (const auto& c : cases) {
        bool ok = (c.got == c.want);
        std::printf("[selfcheck] %-50s got=0x%08x want=0x%08x %s\n", c.what,
                    c.got, c.want, ok ? "OK" : "FAIL");
        if (!ok) rc = 1;
    }
    return rc;
}

int main(int argc, char** argv) {
    if (argc > 1 && std::strcmp(argv[1], "--selfcheck") == 0) {
        return selfcheck();
    }

    auto tc = argTypeCodes();
    std::unordered_map<std::string, std::uint32_t> tcmap;
    for (const auto& r : tc) tcmap[r.name] = r.arg_type_code;

    std::printf("# Mooncake Store master wire-contract golden\n");
    std::printf("# source: rpc_service.cpp RegisterRpcService (v0.3.11.post1)\n");
    std::printf("# function_id = MD5Hash32(\"mooncake::WrappedMasterService::<M>\")\n");
    std::printf("# arg_type_code = struct_pack::get_type_code<args...> (& 0xFFFFFFFE); OMITTED = covered by in-build gate\n");
    std::printf("# columns: name function_id arg_type_code\n");
    for (const auto& r : kFunctionIds) {
        auto it = tcmap.find(r.name);
        if (it != tcmap.end()) {
            std::printf("%-26s 0x%08x 0x%08x\n", r.name, r.function_id,
                        it->second);
        } else {
            std::printf("%-26s 0x%08x OMITTED\n", r.name, r.function_id);
        }
    }
    return 0;
}
