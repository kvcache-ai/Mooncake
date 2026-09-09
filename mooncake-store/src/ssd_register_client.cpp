// Mooncake NoF SSD register/unregister client.
#include "ssd_register_client.h"
#include <algorithm>
#include <cctype>
#include <cstdlib>

namespace mooncake {

// Normalize and validate the MC_NOF_TRTYPE environment variable.
//
// Behaviour:
//   - None / empty / whitespace-only -> default to "RDMA"
//   - Trims surrounding whitespace from the env value
//   - Uppercases the result so callers can compare against "RDMA"/"TCP"
//   - Anything that is not "RDMA" or "TCP" after normalization is replaced
//     with "RDMA" and a warning is logged.
//
// The previous implementation validated the raw value before normalizing,
// which silently dropped common lower-case spellings like "tcp" and made
// set_register and set_unregister_by_endpoint disagree on the endpoint
// string they built.
static std::string NormalizeTrType(const char *env) {
    std::string trtype = env ? env : "RDMA";

    // Trim surrounding whitespace so "tcp " / " tcp" behave as "tcp".
    auto not_space = [](unsigned char c) { return !std::isspace(c); };
    trtype.erase(trtype.begin(),
                 std::find_if(trtype.begin(), trtype.end(), not_space));
    trtype.erase(std::find_if(trtype.rbegin(), trtype.rend(), not_space).base(),
                 trtype.end());

    if (trtype.empty()) {
        return "RDMA";
    }

    // Case-fold to upper so callers can compare against "RDMA"/"TCP".
    std::transform(
        trtype.begin(), trtype.end(), trtype.begin(),
        [](unsigned char c) { return static_cast<char>(std::toupper(c)); });

    if (trtype != "RDMA" && trtype != "TCP") {
        LOG(WARNING) << "Invalid MC_NOF_TRTYPE=\"" << (env ? env : "")
                     << "\", fallback to RDMA";
        trtype = "RDMA";
    }
    return trtype;
}

NoFRegisterClient::NoFRegisterClient()
    : master_client_(generate_uuid(), nullptr) {}

NoFRegisterClient::~NoFRegisterClient() = default;

int NoFRegisterClient::set_register(const std::string &nqn, size_t nsid,
                                    const std::string &traddr, size_t trsvcid,
                                    uintptr_t base, size_t size,
                                    const std::string &master_server_addr) {
    LOG(INFO) << "Registering SSD: nqn=" << nqn << ",nsid=" << nsid
              << ",traddr=" << traddr << ",trsvcid=" << trsvcid
              << ",master=" << master_server_addr << ",base=" << base
              << ",size=" << size;

    auto err = master_client_.Connect(master_server_addr);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master";
        return OPERATION_FAILED;
    }

    const char *trtype_env = std::getenv("MC_NOF_TRTYPE");
    std::string trtype = NormalizeTrType(trtype_env);

    std::string te_endpoint = "traddr:" + traddr +
                              " trsvcid:" + std::to_string(trsvcid) +
                              " subnqn:" + nqn + " trtype:" + trtype +
                              " adrfam:IPv4 ns:" + std::to_string(nsid);

    NoFSegment segment;
    segment.base = base;
    segment.size = size;
    segment.id = generate_uuid();
    segment.name = te_endpoint;
    segment.te_endpoint = te_endpoint;
    auto mount_result = master_client_.MountNoFSegment(segment);
    if (!mount_result) {
        LOG(ERROR) << "mount_segment_to_master_failed ";
        return OPERATION_FAILED;
    }

    return OPERATION_OK;
}

int NoFRegisterClient::set_unregister_by_endpoint(
    const std::string &nqn, size_t nsid, const std::string &traddr,
    size_t trsvcid, const std::string &master_server_addr) {
    LOG(INFO) << "Unregistering SSD by endpoint: nqn=" << nqn
              << ",nsid=" << nsid << ",traddr=" << traddr
              << ",trsvcid=" << trsvcid << ",master=" << master_server_addr;

    // Connect to master server
    auto err = master_client_.Connect(master_server_addr);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master: " << static_cast<int>(err);
        return OPERATION_FAILED;
    }

    const char *trtype_env = std::getenv("MC_NOF_TRTYPE");
    std::string trtype = NormalizeTrType(trtype_env);

    // Build the te_endpoint string to match registered segments
    std::string te_endpoint = "traddr:" + traddr +
                              " trsvcid:" + std::to_string(trsvcid) +
                              " subnqn:" + nqn + " trtype:" + trtype +
                              " adrfam:IPv4 ns:" + std::to_string(nsid);

    LOG(INFO) << "Built te_endpoint: " << te_endpoint;

    auto matching_segments_result =
        master_client_.GetNoFSegmentsByName(te_endpoint);
    if (!matching_segments_result) {
        LOG(ERROR) << "Failed to get NoF segments by name: "
                   << static_cast<int>(matching_segments_result.error());
        return OPERATION_FAILED;
    }

    std::vector<NoFSegmentOwnerInfo> matching_segments =
        matching_segments_result.value();
    LOG(INFO) << "Retrieved " << matching_segments.size()
              << " mounted NoF segments for te_endpoint";
    if (matching_segments.empty()) {
        LOG(ERROR) << "No segment found for te_endpoint: " << te_endpoint;
        return OPERATION_FAILED;
    }

    // Unmount all matching segments
    bool all_unmounted = true;
    for (const auto &segment : matching_segments) {
        LOG(INFO) << "Found matching segment: id=" << segment.segment_id
                  << ", owner_client_id=" << segment.client_id;
        MasterClient owner_master_client(segment.client_id, nullptr);
        err = owner_master_client.Connect(master_server_addr);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect owner master client for segment "
                       << segment.segment_id << ": " << static_cast<int>(err);
            all_unmounted = false;
            continue;
        }

        auto unmount_result =
            owner_master_client.UnmountNoFSegment(segment.segment_id);
        if (!unmount_result) {
            LOG(ERROR) << "Failed to unmount segment " << segment.segment_id
                       << ": " << static_cast<int>(unmount_result.error());
            all_unmounted = false;
        } else {
            LOG(INFO) << "Successfully unmounted segment " << segment.segment_id
                      << ", owner_client_id=" << segment.client_id;
        }
    }

    return all_unmounted ? OPERATION_OK : OPERATION_FAILED;
}

}  // namespace mooncake
