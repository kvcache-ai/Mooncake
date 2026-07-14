#include <gflags/gflags.h>
#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_store.h"
#include "tools/oplog_batch_auditor.h"

DEFINE_string(endpoints, "127.0.0.1:2379", "Etcd endpoints");
DEFINE_string(cluster_id, "", "OpLog cluster ID");
DEFINE_bool(json, false, "Emit JSON to stdout");
DEFINE_uint64(max_batches, 1000000,
              "Maximum namespace keys to audit or batches to dump");
DEFINE_uint64(timeout_sec, 30, "Wait timeout in seconds");
DEFINE_uint64(last_seq, 0, "Wait for at least this durable sequence");
DEFINE_uint64(batch_id, 0, "Wait for at least this durable batch ID");
DEFINE_uint64(from_batch, 1, "First batch ID to dump");
DEFINE_uint64(to_batch, 0, "Last batch ID to dump; zero means no limit");
DEFINE_bool(entries, false, "Include entry details in dump output");

namespace mooncake {
namespace {

void PrintUsage(const char* program) {
    std::cerr << "Usage: " << program
              << " <verify|summary|wait|dump> --cluster_id=ID "
                 "[--endpoints=HOST:PORT]\n";
}

void PrintTextReport(const OpLogAuditReport& report) {
    std::cout << "cluster=" << report.cluster_id
              << " ok=" << (report.ok ? "true" : "false")
              << " legacy_max_seq=" << report.legacy_max_seq;
    if (report.durable_prefix.has_value()) {
        std::cout << " durable_batch=" << report.durable_prefix->batch_id
                  << " durable_seq=" << report.durable_prefix->last_seq;
    } else {
        std::cout << " durable_prefix=missing";
    }
    std::cout << " batches=" << report.batch_count
              << " entries=" << report.entry_count << '\n';
    for (const auto& warning : report.warnings) {
        std::cout << "warning: " << warning << '\n';
    }
    for (const auto& error : report.errors) {
        std::cout << "error: " << error << '\n';
    }
    if (report.truncated_errors) {
        std::cout << "error: additional errors truncated\n";
    }
}

int RunAudit(const std::string& command, const std::string& cluster_id,
             HaKvBackend& backend) {
    OpLogNamespaceRead input;
    const ErrorCode err = ReadOpLogNamespace(
        cluster_id, backend, static_cast<size_t>(FLAGS_max_batches), &input);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to read OpLog namespace: " << toString(err);
        return 1;
    }
    auto report = AuditOpLogNamespace(cluster_id, input.kvs,
                                      static_cast<size_t>(FLAGS_max_batches));
    if (input.truncated) {
        report.ok = false;
        if (report.errors.size() < 100) {
            report.errors.push_back("namespace key count exceeds audit limit");
        } else {
            report.truncated_errors = true;
        }
    }
    if (FLAGS_json) {
        std::cout << OpLogAuditReportToJson(report) << '\n';
    } else {
        PrintTextReport(report);
    }
    return command == "verify" && !report.ok ? 3 : 0;
}

int RunWait(const std::string& cluster_id, HaKvBackend& backend) {
    OpLogBatchStorage storage(cluster_id, backend);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_timeout_sec);
    DurablePrefix prefix;
    while (std::chrono::steady_clock::now() < deadline) {
        const ErrorCode err = storage.ReadDurablePrefix(prefix);
        if (err == ErrorCode::OK) {
            if (prefix.last_seq >= FLAGS_last_seq &&
                prefix.batch_id >= FLAGS_batch_id) {
                return RunAudit("verify", cluster_id, backend);
            }
        } else if (err != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(ERROR) << "Failed to read durable prefix: " << toString(err);
            return 1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOG(ERROR) << "Timed out waiting for durable prefix: last_seq>="
               << FLAGS_last_seq << ", batch_id>=" << FLAGS_batch_id;
    return 4;
}

int RunDump(const std::string& cluster_id, HaKvBackend& backend) {
    const auto requested =
        ComputeOpLogDumpLimit(FLAGS_from_batch, FLAGS_to_batch,
                              static_cast<size_t>(FLAGS_max_batches));
    if (!requested.has_value()) {
        LOG(ERROR) << "Invalid dump batch range or range exceeds "
                      "--max_batches";
        return 1;
    }

    OpLogBatchStorage storage(cluster_id, backend);
    std::vector<OpLogBatchRecord> batches;
    const ErrorCode err =
        storage.ReadBatchesAfter(FLAGS_from_batch - 1, *requested, batches);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to read batches: " << toString(err);
        return 1;
    }

    Json::Value root(Json::arrayValue);
    for (const auto& batch : batches) {
        if (FLAGS_to_batch != 0 && batch.batch_id > FLAGS_to_batch) {
            break;
        }
        Json::Value item;
        item["batch_id"] = Json::UInt64(batch.batch_id);
        item["first_seq"] = Json::UInt64(batch.first_seq);
        item["last_seq"] = Json::UInt64(batch.last_seq);
        item["entry_count"] = Json::UInt64(batch.entries.size());
        item["checksum"] = batch.checksum;
        if (FLAGS_entries) {
            item["entries"] = Json::arrayValue;
            for (const auto& entry : batch.entries) {
                Json::Value encoded_entry;
                encoded_entry["sequence_id"] = Json::UInt64(entry.sequence_id);
                encoded_entry["op_type"] =
                    static_cast<Json::UInt>(entry.op_type);
                encoded_entry["tenant_id"] = entry.tenant_id;
                encoded_entry["object_key"] = entry.object_key;
                encoded_entry["payload_bytes"] =
                    Json::UInt64(entry.payload.size());
                item["entries"].append(std::move(encoded_entry));
            }
        }
        root.append(std::move(item));
    }

    Json::StreamWriterBuilder builder;
    builder["indentation"] = FLAGS_json ? "" : "  ";
    std::cout << Json::writeString(builder, root) << '\n';
    return 0;
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    gflags::SetUsageMessage(
        "Inspect and verify Mooncake batch-record OpLog history");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (argc != 2 || FLAGS_cluster_id.empty()) {
        mooncake::PrintUsage(argv[0]);
        return 1;
    }
    const std::string command = argv[1];
    if (command != "verify" && command != "summary" && command != "wait" &&
        command != "dump") {
        mooncake::PrintUsage(argv[0]);
        return 1;
    }
    if (FLAGS_max_batches == 0 ||
        FLAGS_max_batches >= std::numeric_limits<size_t>::max()) {
        LOG(ERROR) << "max_batches must fit in size_t and be greater than zero";
        return 1;
    }

    std::string cluster_id = FLAGS_cluster_id;
    if (!mooncake::NormalizeAndValidateClusterId(cluster_id) ||
        cluster_id.empty()) {
        LOG(ERROR) << "Invalid cluster_id";
        return 1;
    }
    const auto err =
        mooncake::EtcdHelper::ConnectToEtcdStoreClient(FLAGS_endpoints);
    if (err != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd: " << mooncake::toString(err);
        return 1;
    }

    mooncake::EtcdHaKvBackend backend;
    if (command == "wait") {
        return mooncake::RunWait(cluster_id, backend);
    }
    if (command == "dump") {
        return mooncake::RunDump(cluster_id, backend);
    }
    return mooncake::RunAudit(command, cluster_id, backend);
}
