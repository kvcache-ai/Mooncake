// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_peer_directory.h"

#include <gtest/gtest.h>
#include <json/json.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <vector>

namespace mooncake::ssd_offload {
namespace {

class SsdPeerDirectoryTest : public ::testing::Test {
   protected:
    std::filesystem::path path;

    void SetUp() override {
        char name[] = "/tmp/mooncake-ssd-peers-XXXXXX";
        ASSERT_NE(mkdtemp(name), nullptr);
        path = name;
        ASSERT_EQ(chmod(path.c_str(), 0700), 0);
    }

    void TearDown() override { std::filesystem::remove_all(path); }

    SsdPeerDirectoryEntry Entry(uint64_t id = 1, uint64_t boot = 3) {
        SsdFixedL2Peer fixed{{id, 2, boot}, "default", "requester-te", {7, 8}};
        return {fixed,
                {fixed.identity,
                 fixed.tenant_id,
                 "owner:100",
                 "127.0.0.1",
                 1200,
                 fixed.transport_endpoint,
                 {1, fixed.identity, 15, 64, 1024, 64, 1024, 1024, {"tcp"}}},
                "tcp",
                true};
    }

    std::filesystem::path Record() {
        for (const auto& item : std::filesystem::directory_iterator(path))
            if (item.path().extension() == ".json") return item.path();
        return {};
    }
};

TEST_F(SsdPeerDirectoryTest, AutomaticallyPublishedPeerHasPrivateTrustedRoute) {
    SsdPeerDirectory writer(path), reader(path);
    auto entry = Entry();
    ASSERT_EQ(writer.Publish(entry), DestinationError::kOk);
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_EQ(found.error, DestinationError::kOk);
    ASSERT_TRUE(found.peer);
    EXPECT_EQ(found.peer->fixed.identity, entry.fixed.identity);
    EXPECT_EQ(found.peer->fixed.credential, entry.fixed.credential);
    EXPECT_EQ(found.peer->route.control_port, 1200);
    struct stat info{};
    ASSERT_EQ(stat(Record().c_str(), &info), 0);
    EXPECT_EQ(info.st_mode & 0777, 0600);
}

TEST_F(SsdPeerDirectoryTest, LookupRequiresMatchingIdentityTenantAndProtocol) {
    SsdPeerDirectory writer(path), reader(path);
    ASSERT_EQ(writer.Publish(Entry()), DestinationError::kOk);
    EXPECT_EQ(reader.LookupRequester({1, 2, 99}, "default", "tcp").error,
              DestinationError::kNotFound);
    EXPECT_EQ(reader.LookupRequester({1, 2, 3}, "foreign", "tcp").error,
              DestinationError::kNotFound);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "rdma").error,
              DestinationError::kNotFound);
    EXPECT_EQ(reader.LookupRequester({1, 2, 3}, "default", "tcp").error,
              DestinationError::kOk);
}

TEST_F(SsdPeerDirectoryTest, PublishesActualBatchCapabilityAndRejectsOverflow) {
    SsdPeerDirectory writer(path), reader(path);
    auto bad = Entry();
    bad.route.capability.max_batch_objects = 65537;
    EXPECT_EQ(writer.Publish(bad), DestinationError::kInvalidArgument);

    auto entry = Entry();
    entry.route.capability.max_batch_objects = 512;
    ASSERT_EQ(writer.Publish(entry), DestinationError::kOk);
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_TRUE(found.peer);
    EXPECT_EQ(found.peer->route.capability.max_batch_objects, 512u);
}

TEST_F(SsdPeerDirectoryTest, RejectsWildcardControlHost) {
    SsdPeerDirectory writer(path);
    auto entry = Entry();
    entry.route.control_host = "0.0.0.0";
    EXPECT_EQ(writer.Publish(entry), DestinationError::kInvalidArgument);
    entry.route.control_host = "::";
    EXPECT_EQ(writer.Publish(entry), DestinationError::kInvalidArgument);
}

TEST_F(SsdPeerDirectoryTest, UnsafeDirectoryAndSymlinkAreRejected) {
    ASSERT_EQ(chmod(path.c_str(), 0750), 0);
    EXPECT_THROW(SsdPeerDirectory{path}, std::invalid_argument);
    ASSERT_EQ(chmod(path.c_str(), 0700), 0);
    auto link = path.string() + "-link";
    ASSERT_EQ(symlink(path.c_str(), link.c_str()), 0);
    EXPECT_THROW(SsdPeerDirectory{link}, std::invalid_argument);
    std::filesystem::remove(link);
}

TEST_F(SsdPeerDirectoryTest,
       UnsafeRecordPermissionsAndNonObjectJsonFailClosed) {
    SsdPeerDirectory writer(path), reader(path);
    ASSERT_EQ(writer.Publish(Entry()), DestinationError::kOk);
    const auto file = Record();
    ASSERT_EQ(chmod(file.c_str(), 0640), 0);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
              DestinationError::kUnauthorized);
    ASSERT_EQ(chmod(file.c_str(), 0600), 0);

    // Non-object JSON must return kInvalidArgument instead of throwing
    // Json::LogicError.
    for (const char* bad_json :
         {"[]", "123", "\"hello\"", "{\"version\":1,\"capability\":[]}"}) {
        std::ofstream(file, std::ios::trunc) << bad_json;
        EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
                  DestinationError::kInvalidArgument);
    }
}

TEST_F(SsdPeerDirectoryTest, LiveConflictingLegacyRoutesFailClosed) {
    SsdPeerDirectory first(path), second(path), reader(path);
    ASSERT_EQ(first.Publish(Entry(1, 3)), DestinationError::kOk);
    ASSERT_EQ(second.Publish(Entry(2, 4)), DestinationError::kOk);
    EXPECT_EQ(reader.LookupLegacy("owner:100", "default", "tcp").error,
              DestinationError::kDuplicate);
}

TEST_F(SsdPeerDirectoryTest, CrashLeftoverDoesNotPoisonRestartedOwner) {
    // Simulate a publisher process killed by SIGKILL without running
    // Unpublish(): child publishes epoch 3 and exits via _exit(0), releasing
    // its flock while leaving ssd-peer-*.json behind.
    const pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        SsdPeerDirectory crashed(path);
        auto rc = crashed.Publish(Entry(1, 3));
        _exit(rc == DestinationError::kOk ? 0 : 1);
    }
    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    // Restarted owner publishes epoch 4 for the same legacy_rpc_endpoint.
    SsdPeerDirectory restarted(path), reader(path);
    ASSERT_EQ(restarted.Publish(Entry(1, 4)), DestinationError::kOk);

    // LookupLegacy skips the dead crash-leftover and resolves live epoch 4.
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_EQ(found.error, DestinationError::kOk);
    ASSERT_TRUE(found.peer);
    EXPECT_EQ(found.peer->fixed.identity.boot_epoch, 4u);

    // LookupRequester can still locate epoch 3's immutable record for in-flight
    // recovery if needed.
    auto old_req = reader.LookupRequester({1, 2, 3}, "default", "tcp");
    EXPECT_EQ(old_req.error, DestinationError::kOk);
}

TEST_F(SsdPeerDirectoryTest, PureRequesterPublishesWithoutControlPort) {
    SsdPeerDirectory publisher(path);
    auto entry = Entry(42, 7);
    entry.readable = false;
    entry.route.control_port = 0;
    entry.route.capability.features = 0;
    ASSERT_EQ(publisher.Publish(entry), DestinationError::kOk);

    SsdPeerDirectory reader(path);
    auto req = reader.LookupRequester(entry.fixed.identity, "default", "tcp");
    ASSERT_EQ(req.error, DestinationError::kOk);
    ASSERT_TRUE(req.peer.has_value());
    EXPECT_FALSE(req.peer->readable);
    EXPECT_EQ(req.peer->route.control_port, 0u);
    // Pure requesters must never be selected as readable SSD owners.
    auto owner = reader.LookupLegacy("owner:100", "default", "tcp");
    EXPECT_EQ(owner.error, DestinationError::kNotFound);
    EXPECT_EQ(publisher.Unpublish(), DestinationError::kOk);
}

TEST_F(SsdPeerDirectoryTest,
       SingleCorruptFileDoesNotPoisonHealthyPeerAndOmittedTopLevelCapHonored) {
    SsdPeerDirectory writer(path), reader(path);
    auto entry = Entry(7, 11);
    entry.route.capability.max_batch_objects = 384;
    ASSERT_EQ(writer.Publish(entry), DestinationError::kOk);

    // Drop an unrelated corrupt peer file alongside the healthy publisher.
    const auto corrupt =
        path /
        "ssd-peer-00000000000000ff-00000000000000ff-0000000000000001.json";
    {
        std::ofstream out(corrupt, std::ios::trunc);
        out << "{invalid json";
    }
    ASSERT_EQ(chmod(corrupt.c_str(), 0600), 0);

    // Healthy peer lookup must still succeed despite the corrupt sibling file.
    auto found = reader.LookupLegacy("owner:100", "default", "tcp");
    ASSERT_EQ(found.error, DestinationError::kOk);
    ASSERT_TRUE(found.peer.has_value());
    EXPECT_EQ(found.peer->fixed.identity, entry.fixed.identity);
    EXPECT_EQ(found.peer->route.capability.max_batch_objects, 384u);

    // Canonical codec honors capability.max_batch_objects when top-level
    // max_batch_objects is omitted.
    std::string encoded = EncodeSsdPeerDocument(entry);
    Json::Value doc;
    Json::CharReaderBuilder rb;
    std::unique_ptr<Json::CharReader> parser(rb.newCharReader());
    std::string errs;
    ASSERT_TRUE(parser->parse(encoded.data(), encoded.data() + encoded.size(),
                              &doc, &errs));
    doc.removeMember("max_batch_objects");
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    auto decoded = DecodeSsdPeerDocument(Json::writeString(wb, doc));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->route.capability.max_batch_objects, 384u);
}

TEST_F(SsdPeerDirectoryTest,
       LookupRequesterSucceedsInO1Beyond256CrashLeftovers) {
    // Seed 260 dead crash leftovers (unlocked .lock + .json files).
    for (uint64_t epoch = 1; epoch <= 260; ++epoch) {
        auto stale = Entry(50, epoch);
        std::ostringstream stem;
        stem << "ssd-peer-" << std::hex << std::setfill('0') << std::setw(16)
             << stale.fixed.identity.client_id_high << '-' << std::setw(16)
             << stale.fixed.identity.client_id_low << '-' << std::setw(16)
             << stale.fixed.identity.boot_epoch;
        const auto json_path = path / (stem.str() + ".json");
        const auto lock_path = path / ("." + stem.str() + ".lock");
        std::ofstream(json_path, std::ios::trunc)
            << EncodeSsdPeerDocument(stale);
        ASSERT_EQ(chmod(json_path.c_str(), 0600), 0);
        std::ofstream(lock_path, std::ios::trunc);
        ASSERT_EQ(chmod(lock_path.c_str(), 0600), 0);
    }

    SsdPeerDirectory live(path), reader(path);
    auto current = Entry(50, 261);
    ASSERT_EQ(live.Publish(current), DestinationError::kOk);

    // O(1) LookupRequester finds live peer even with >256 prior crashes.
    auto req = reader.LookupRequester(current.fixed.identity, "default", "tcp");
    ASSERT_EQ(req.error, DestinationError::kOk);
    ASSERT_TRUE(req.peer.has_value());
    EXPECT_EQ(req.peer->fixed.identity.boot_epoch, 261u);

    // Publish GC'd dead crash leftovers down to the bounded retention window.
    size_t json_count = 0;
    for (const auto& item : std::filesystem::directory_iterator(path)) {
        if (item.path().extension() == ".json") ++json_count;
    }
    EXPECT_LE(json_count, 65u);
}

TEST_F(SsdPeerDirectoryTest,
       RejectsIpv4MappedWildcardAndDuplicatePublishPreservesSidecarLock) {
    EXPECT_FALSE(IsNonWildcardNumericHost("::ffff:0.0.0.0"));
    EXPECT_TRUE(IsNonWildcardNumericHost("::ffff:127.0.0.1"));

    // Seed an existing dead crash leftover (.json + unlocked .lock).
    auto stale = Entry(77, 1);
    std::ostringstream stem;
    stem << "ssd-peer-" << std::hex << std::setfill('0') << std::setw(16)
         << stale.fixed.identity.client_id_high << '-' << std::setw(16)
         << stale.fixed.identity.client_id_low << '-' << std::setw(16)
         << stale.fixed.identity.boot_epoch;
    const auto json_path = path / (stem.str() + ".json");
    const auto lock_path = path / ("." + stem.str() + ".lock");
    std::ofstream(json_path, std::ios::trunc) << EncodeSsdPeerDocument(stale);
    ASSERT_EQ(chmod(json_path.c_str(), 0600), 0);
    std::ofstream(lock_path, std::ios::trunc);
    ASSERT_EQ(chmod(lock_path.c_str(), 0600), 0);

    // Duplicate Publish() with the same identity/epoch must fail closed with
    // kDuplicate WITHOUT unlinking the sidecar .lock file.
    SsdPeerDirectory dup_pub(path);
    EXPECT_EQ(dup_pub.Publish(stale), DestinationError::kDuplicate);
    EXPECT_TRUE(std::filesystem::exists(lock_path));
}

}  // namespace
}  // namespace mooncake::ssd_offload
