#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "master.grpc.pb.h"
#include "master.pb.h"
#include "master_service.h"
#include "types.h"

// Define command line flags
DEFINE_int32(port, 50051, "Port for master service to listen on");
DEFINE_int32(max_threads, 4, "Maximum number of threads to use");
DEFINE_bool(enable_gc, false, "Enable garbage collection");

namespace mooncake {

// Helper function to check if a port is available
bool isPortAvailable(int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        LOG(ERROR) << "Failed to create socket for port check";
        return false;
    }

    // Allow reuse of local addresses
    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        LOG(WARNING) << "Failed to set socket options";
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int result = bind(sockfd, (struct sockaddr*)&addr, sizeof(addr));
    close(sockfd);

    return result == 0;
}

// Convert internal ReplicaInfo to proto ReplicaInfo
void ConvertToProtoReplicaInfo(const ReplicaInfo& internal_info,
                               mooncake_store::ReplicaInfo* proto_info) {
    // Convert status
    switch (internal_info.status) {
        case ReplicaStatus::UNDEFINED:
            proto_info->set_status(
                mooncake_store::ReplicaInfo_ReplicaStatus_UNDEFINED);
            break;
        case ReplicaStatus::INITIALIZED:
            proto_info->set_status(
                mooncake_store::ReplicaInfo_ReplicaStatus_INITIALIZED);
            break;
        case ReplicaStatus::COMPLETE:
            proto_info->set_status(
                mooncake_store::ReplicaInfo_ReplicaStatus_COMPLETE);
            break;
        case ReplicaStatus::REMOVED:
            proto_info->set_status(
                mooncake_store::ReplicaInfo_ReplicaStatus_REMOVED);
            break;
        case ReplicaStatus::FAILED:
            proto_info->set_status(
                mooncake_store::ReplicaInfo_ReplicaStatus_FAILED);
            break;
        case ReplicaStatus::PROCESSING:
            proto_info->set_status(
                mooncake_store::ReplicaInfo_ReplicaStatus_PROCESSING);
            break;
    }

    // Convert handles
    for (const auto& internal_handle : internal_info.handles) {
        auto proto_handle = proto_info->add_handles();
        proto_handle->set_segment_name(internal_handle->segment_name);
        proto_handle->set_size(internal_handle->size);
        // Convert void* buffer to uint64_t for protobuf
        proto_handle->set_buffer(
            reinterpret_cast<uint64_t>(internal_handle->buffer));

        // Convert buffer status
        switch (internal_handle->status) {
            case BufStatus::INIT:
                proto_handle->set_status(
                    mooncake_store::BufHandle_BufStatus_INIT);
                break;
            case BufStatus::COMPLETE:
                proto_handle->set_status(
                    mooncake_store::BufHandle_BufStatus_COMPLETE);
                break;
            case BufStatus::FAILED:
                proto_handle->set_status(
                    mooncake_store::BufHandle_BufStatus_FAILED);
                break;
            case BufStatus::UNREGISTERED:
                proto_handle->set_status(
                    mooncake_store::BufHandle_BufStatus_UNREGISTERED);
                break;
        }
    }
}

// Convert proto BufHandle to internal BufHandle
std::shared_ptr<BufHandle> ConvertFromProtoBufHandle(
    const mooncake_store::BufHandle& proto_handle,
    std::shared_ptr<BufferAllocator> allocator) {
    // Convert uint64_t back to void* for buffer
    void* buffer = reinterpret_cast<void*>(proto_handle.buffer());

    // Create BufHandle using constructor
    auto handle = std::make_shared<BufHandle>(
        allocator, proto_handle.segment_name(), proto_handle.size(), buffer);

    // Set status
    switch (proto_handle.status()) {
        case mooncake_store::BufHandle_BufStatus_INIT:
            handle->status = BufStatus::INIT;
            break;
        case mooncake_store::BufHandle_BufStatus_COMPLETE:
            handle->status = BufStatus::COMPLETE;
            break;
        case mooncake_store::BufHandle_BufStatus_FAILED:
            handle->status = BufStatus::FAILED;
            break;
        case mooncake_store::BufHandle_BufStatus_UNREGISTERED:
            handle->status = BufStatus::UNREGISTERED;
            break;
    }

    return handle;
}

class MasterServiceImpl final : public mooncake_store::MasterService::Service {
   public:
    explicit MasterServiceImpl(std::shared_ptr<MasterService> service)
        : master_service_(service) {}

    grpc::Status GetReplicaList(
        grpc::ServerContext* context,
        const mooncake_store::GetReplicaListRequest* request,
        mooncake_store::GetReplicaListResponse* response) override {
        std::vector<ReplicaInfo> replica_list;
        ErrorCode error_code =
            master_service_->GetReplicaList(request->key(), replica_list);

        response->set_status_code(toInt(error_code));
        if (error_code == ErrorCode::OK) {
            for (const auto& replica : replica_list) {
                auto proto_replica = response->add_replica_list();
                ConvertToProtoReplicaInfo(replica, proto_replica);
            }
        }
        return grpc::Status::OK;
    }

    grpc::Status PutStart(grpc::ServerContext* context,
                          const mooncake_store::PutStartRequest* request,
                          mooncake_store::PutStartResponse* response) override {
        std::vector<ReplicaInfo> replica_list;
        ReplicateConfig config;
        config.replica_num = request->config().replica_num();
        // Convert slice_lengths from repeated field to vector
        std::vector<uint64_t> slice_lengths;
        for (const auto& length : request->slice_lengths()) {
            slice_lengths.push_back(length);
        }

        ErrorCode error_code =
            master_service_->PutStart(request->key(), request->value_length(),
                                      slice_lengths, config, replica_list);

        response->set_status_code(toInt(error_code));
        if (error_code == ErrorCode::OK) {
            for (const auto& replica : replica_list) {
                auto proto_replica = response->add_replica_list();
                ConvertToProtoReplicaInfo(replica, proto_replica);
            }
        }
        return grpc::Status::OK;
    }

    grpc::Status PutEnd(grpc::ServerContext* context,
                        const mooncake_store::PutEndRequest* request,
                        mooncake_store::PutEndResponse* response) override {
        ErrorCode error_code = master_service_->PutEnd(request->key());
        response->set_status_code(toInt(error_code));
        return grpc::Status::OK;
    }

    grpc::Status PutRevoke(
        grpc::ServerContext* context,
        const mooncake_store::PutRevokeRequest* request,
        mooncake_store::PutRevokeResponse* response) override {
        ErrorCode error_code = master_service_->PutRevoke(request->key());
        response->set_status_code(toInt(error_code));
        return grpc::Status::OK;
    }

    grpc::Status Remove(grpc::ServerContext* context,
                        const mooncake_store::RemoveRequest* request,
                        mooncake_store::RemoveResponse* response) override {
        ErrorCode error_code = master_service_->Remove(request->key());
        response->set_status_code(toInt(error_code));
        return grpc::Status::OK;
    }

    grpc::Status MountSegment(
        grpc::ServerContext* context,
        const mooncake_store::MountSegmentRequest* request,
        mooncake_store::MountSegmentResponse* response) override {
        ErrorCode error_code = master_service_->MountSegment(
            request->buffer(), request->size(), request->segment_name());
        response->set_status_code(toInt(error_code));
        return grpc::Status::OK;
    }

    grpc::Status UnmountSegment(
        grpc::ServerContext* context,
        const mooncake_store::UnmountSegmentRequest* request,
        mooncake_store::UnmountSegmentResponse* response) override {
        ErrorCode error_code =
            master_service_->UnmountSegment(request->segment_name());
        response->set_status_code(toInt(error_code));
        return grpc::Status::OK;
    }

   private:
    std::shared_ptr<MasterService> master_service_;
};

}  // namespace mooncake

int main(int argc, char* argv[]) {
    // Initialize gflags
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;  // Log to stderr for development

    LOG(INFO) << "Starting Mooncake Master Service";
    LOG(INFO) << "Port: " << FLAGS_port;
    LOG(INFO) << "Max threads: " << FLAGS_max_threads;

    try {
        // Check if the port is available before starting server
        if (!mooncake::isPortAvailable(FLAGS_port)) {
            LOG(ERROR) << "Port " << FLAGS_port << " is already in use. "
                       << "Please specify a different port using --port flag.";
            return 1;
        }

        // Create master service instance with GC flag
        auto master_service =
            std::make_shared<mooncake::MasterService>(FLAGS_enable_gc);

        LOG(INFO) << "Garbage collection: "
                  << (FLAGS_enable_gc ? "enabled" : "disabled");

        // Initialize gRPC server
        std::string server_address = "0.0.0.0:" + std::to_string(FLAGS_port);
        grpc::ServerBuilder builder;

        // Add listening port
        builder.AddListeningPort(server_address,
                                 grpc::InsecureServerCredentials());

        // Register service
        mooncake::MasterServiceImpl service(master_service);
        builder.RegisterService(&service);

        // Set thread pool size
        builder.SetSyncServerOption(
            grpc::ServerBuilder::SyncServerOption::NUM_CQS, FLAGS_max_threads);

        // Build and start server
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        if (!server) {
            LOG(ERROR) << "Failed to start server on port " << FLAGS_port;
            return 1;
        }

        LOG(INFO) << "Master service listening on " << server_address;

        // Wait for server to shutdown
        server->Wait();

    } catch (const std::exception& e) {
        LOG(ERROR) << "Error in master service: " << e.what();
        return 1;
    }

    return 0;
}
