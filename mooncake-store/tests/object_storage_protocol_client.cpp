// Interactive RPC driver for object_storage_e2e.py. Preserve one writer UUID
// and old completion token while the real Master is killed and restarted.
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "master_client.h"

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    mooncake::MasterClient client(mooncake::generate_uuid());
    std::optional<mooncake::ObjectWriteAttempt> old, current;
    uint64_t generation = 0;
    std::string command, argument;
    std::optional<mooncake::WeightRevisionMetadata> importing;
    const std::string key = "reused-attempt";
    while (std::cin >> command) {
        mooncake::ErrorCode error = mooncake::ErrorCode::OK;
        if (command == "connect") {
            std::cin >> argument;
            error = client.Connect(argument);
            if (error == mooncake::ErrorCode::OK) {
                auto mounted = client.ReMountSegment({});
                if (!mounted) error = mounted.error();
            }
        } else if (command == "begin_import") {
            ++generation;
            std::vector<mooncake::WeightAffinityMappingEntry> layout{
                {generation == 1 ? "import-aborted" : "import-admitted", 4096,
                 "unit3-affinity"}};
            auto result = client.BeginWeightImport(
                {.identity = {"default", "unit3", "weights", "revision1",
                              generation},
                 .payload_group_id = {},
                 .expected_payload_count = 1,
                 .expected_logical_bytes = 4096,
                 .policy =
                     mooncake::WeightStoragePolicy{
                         .preferred_residency =
                             mooncake::WeightResidencyState::HOT,
                         .migration_mode =
                             mooncake::WeightMigrationMode::MANUAL},
                 .affinity_summary =
                     {1, mooncake::ComputeWeightAffinityMappingSha256(layout)},
                 .payload_layout = layout});
            if (!result)
                error = result.error();
            else if (!*result) {
                std::cerr << "BeginWeightImport rejected: "
                          << static_cast<int>(result->error()) << std::endl;
                return 4;
            } else
                importing = **result;
        } else if (command == "abort_import" && importing) {
            auto result = client.AbortWeightImport(
                {importing->identity, importing->metadata_generation});
            if (!result)
                error = result.error();
            else if (!*result)
                return 5;
        } else if ((command == "start" || command == "start_memory")) {
            mooncake::ReplicateConfig config;
            config.os_replica_num = command == "start" ? 1 : 0;
            auto result = client.PutStart(key, {4096}, config);
            if (!result)
                error = result.error();
            else {
                for (const auto& replica : *result) {
                    if (replica.is_object_storage_replica())
                        current = mooncake::ObjectWriteAttempt{
                            replica.id,
                            replica.get_object_storage_descriptor().object_key};
                    else if (replica.object_write_attempt.has_value())
                        current = replica.object_write_attempt.value();
                }
                if (!current) return 2;
                if (!old) old = current;
            }
        } else if (command == "not_started" && current) {
            auto result = client.FinishObjectStorageUpload(
                current->token, mooncake::ObjectUploadOutcome::NOT_STARTED);
            if (!result) error = result.error();
        } else if (command == "revoke" && current) {
            auto result = client.RevokeObjectStorageWrite(key, *current);
            if (!result) error = result.error();
        } else if (command == "partial_end" && current) {
            auto result = client.PutEnd({key, std::nullopt, current},
                                        mooncake::ReplicaType::MEMORY);
            if (!result) error = result.error();
        } else if (command == "old_end" && old) {
            auto result = client.PutEnd({key, std::nullopt, old},
                                        mooncake::ReplicaType::ALL);
            if (!result) error = result.error();
        } else if (command == "old_revoke" && old) {
            auto result = client.RevokeObjectStorageWrite(key, *old);
            if (!result) error = result.error();
        } else if (command == "old_finished" && old) {
            auto result = client.FinishObjectStorageUpload(
                old->token, mooncake::ObjectUploadOutcome::COMPLETE);
            if (!result) error = result.error();
        } else {
            return 3;
        }
        std::cout << static_cast<int>(error);
        if ((command == "start" || command == "start_memory") &&
            error == mooncake::ErrorCode::OK)
            std::cout << ' ' << current->replica_id << ' ' << current->token;
        if (command == "begin_import" && importing)
            std::cout << ' ' << importing->manifest.payload_group_id;
        std::cout << std::endl;
    }
    return 0;
}
