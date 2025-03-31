#pragma once
#include <cstdint>
#include <ylt/reflection/user_reflect_macro.hpp>

#include "master_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
namespace mooncake {

struct GetReplicaListResponse {
    std::vector<Replica::Descriptor> replica_list;
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(GetReplicaListResponse, replica_list, error_code)

struct PutStartResponse {
    std::vector<Replica::Descriptor> replica_list;
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(PutStartResponse, replica_list, error_code)

struct PutEndResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(PutEndResponse, error_code)
struct PutRevokeResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(PutRevokeResponse, error_code)

struct RemoveResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(RemoveResponse, error_code)
struct MountSegmentResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(MountSegmentResponse, error_code)

struct UnmountSegmentResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(UnmountSegmentResponse, error_code)

class WrappedMasterService {
   public:
    WrappedMasterService(bool enable_gc) : master_service_(enable_gc) {}

    GetReplicaListResponse GetReplicaList(const std::string& key) {
        ScopedVLogTimer timer(1, "GetReplicaList");
        timer.LogRequest("key=", key);

        GetReplicaListResponse response;
        response.error_code =
            master_service_.GetReplicaList(key, response.replica_list);
        timer.LogResponseJson(response);
        return response;
    }

    PutStartResponse PutStart(const std::string& key, uint64_t value_length,
                              const std::vector<uint64_t>& slice_lengths,
                              const ReplicateConfig& config) {
        ScopedVLogTimer timer(1, "PutStart");
        timer.LogRequest("key=", key, ", value_length=", value_length,
                         ", slice_lengths=", slice_lengths.size());

        PutStartResponse response;
        response.error_code = master_service_.PutStart(
            key, value_length, slice_lengths, config, response.replica_list);
        timer.LogResponseJson(response);
        return response;
    }

    PutEndResponse PutEnd(const std::string& key) {
        ScopedVLogTimer timer(1, "PutEnd");
        timer.LogRequest("key=", key);

        PutEndResponse response;
        response.error_code = master_service_.PutEnd(key);
        timer.LogResponseJson(response);
        return response;
    }

    PutRevokeResponse PutRevoke(const std::string& key) {
        ScopedVLogTimer timer(1, "PutRevoke");
        timer.LogRequest("key=", key);

        PutRevokeResponse response;
        response.error_code = master_service_.PutRevoke(key);
        timer.LogResponseJson(response);
        return response;
    }

    RemoveResponse Remove(const std::string& key) {
        ScopedVLogTimer timer(1, "Remove");
        timer.LogRequest("key=", key);

        RemoveResponse response;
        response.error_code = master_service_.Remove(key);
        timer.LogResponseJson(response);
        return response;
    }

    MountSegmentResponse MountSegment(uint64_t buffer, uint64_t size,
                                      const std::string& segment_name) {
        ScopedVLogTimer timer(1, "MountSegment");
        timer.LogRequest("buffer=", buffer, ", size=", size,
                         ", segment_name=", segment_name);

        MountSegmentResponse response;
        response.error_code =
            master_service_.MountSegment(buffer, size, segment_name);
        timer.LogResponseJson(response);
        return response;
    }

    UnmountSegmentResponse UnmountSegment(const std::string& segment_name) {
        ScopedVLogTimer timer(1, "UnmountSegment");
        timer.LogRequest("segment_name=", segment_name);

        UnmountSegmentResponse response;
        response.error_code = master_service_.UnmountSegment(segment_name);
        timer.LogResponseJson(response);
        return response;
    }

   private:
    MasterService master_service_;
};

}  // namespace mooncake
