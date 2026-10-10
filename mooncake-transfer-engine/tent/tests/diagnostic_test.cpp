// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/diagnostics/diagnostic.h"

#include <gtest/gtest.h>

#include <string>

namespace mooncake::tent::diagnostics {
namespace {

TEST(DiagnosticSchemaTest, JsonUsesStableSchemaAndOutcomeStrings) {
    DiagnosticSnapshot snapshot;
    snapshot.command = "check-config";
    snapshot.build.version = "test-version";
    snapshot.build.commit = "deadbeef";
    snapshot.checks = {
        {"pass", CheckOutcome::kPass, "passed", {}, {}},
        {"warn", CheckOutcome::kWarn, "warning", {}, {"inspect it"}},
        {"fail", CheckOutcome::kFail, "failed", {}, {"fix it"}},
        {"skip", CheckOutcome::kSkip, "unavailable", {}, {}},
    };

    const auto json = toJson(snapshot);
    EXPECT_EQ(json.at("schema_version"), 1);
    EXPECT_EQ(json.at("command"), "check-config");
    EXPECT_EQ(json.at("checks").at(0).at("outcome"), "pass");
    EXPECT_EQ(json.at("checks").at(1).at("outcome"), "warn");
    EXPECT_EQ(json.at("checks").at(2).at("outcome"), "fail");
    EXPECT_EQ(json.at("checks").at(3).at("outcome"), "skip");
    EXPECT_EQ(json.at("checks").at(2).at("remediation").at(0), "fix it");
}

TEST(DiagnosticSchemaTest, UnavailableEvidenceIsNotRenderedAsZero) {
    DiagnosticSnapshot snapshot;
    snapshot.sections.push_back(
        {"topology",
         "partial topology",
         {{"linux", "nic.speed_bps", SanitizedValue::Unavailable()}}});

    const auto json = toJson(snapshot);
    const auto& evidence = json.at("sections").at(0).at("evidence").at(0);
    EXPECT_FALSE(evidence.at("available"));
    EXPECT_FALSE(evidence.contains("observed"));
    EXPECT_NE(renderText(snapshot).find("unavailable"), std::string::npos);
}

TEST(DiagnosticRedactionTest, SecretsAreAlwaysExcluded) {
    for (const auto mode : {RedactionMode::kDefault, RedactionMode::kStrict,
                            RedactionMode::kNone}) {
        Redactor redactor(mode);
        const auto secret = redactor.sanitize("auth_token", "sensitive");
        const auto address = redactor.sanitize("buffer", "0x1234",
                                               ValueSensitivity::kRawAddress);
        const auto rkey = redactor.sanitize("rdma_rkey", 12345);
        EXPECT_EQ(secret.value, "[redacted]");
        EXPECT_EQ(address.value, "[redacted]");
        EXPECT_EQ(rkey.value, "[redacted]");
        EXPECT_TRUE(secret.redacted);
        EXPECT_EQ(redactor.metadata().fields_redacted, 3);
    }
}

TEST(DiagnosticRedactionTest, StrictModeCorrelatesWithinOneSnapshotOnly) {
    Redactor redactor(RedactionMode::kStrict);
    const auto first =
        redactor.sanitize("hostname", "node-a", ValueSensitivity::kDeployment);
    const auto repeated =
        redactor.sanitize("hostname", "node-a", ValueSensitivity::kDeployment);
    const auto second =
        redactor.sanitize("hostname", "node-b", ValueSensitivity::kDeployment);

    EXPECT_EQ(first.value, repeated.value);
    EXPECT_NE(first.value, second.value);
    EXPECT_NE(first.value, "node-a");
    EXPECT_EQ(redactor.metadata().mode, RedactionMode::kStrict);
    EXPECT_EQ(redactor.metadata().fields_redacted, 3);
}

TEST(DiagnosticRedactionTest, TextSecretsAreRedactedInEveryMode) {
    const std::string text =
        "MC_REDIS_PASSWORD=abc url redis://u:p@h:6379/0 "
        "Authorization: Bearer x rkey=12345 lkey 0x99 "
        "addr=0x7f0069fa6000 {\"redis_password\": \"hunter2\"} peer 10.1.2.3";
    for (const auto mode : {RedactionMode::kDefault, RedactionMode::kStrict,
                            RedactionMode::kNone}) {
        Redactor redactor(mode);
        const std::string sanitized = redactor.sanitizeText(text);
        for (const char* secret : {"=abc", "u:p@", "Bearer x", "12345", "0x99",
                                   "0x7f0069fa6000", "hunter2"}) {
            EXPECT_EQ(sanitized.find(secret), std::string::npos)
                << secret << " in " << sanitized;
        }
        EXPECT_NE(sanitized.find("MC_REDIS_PASSWORD=[redacted]"),
                  std::string::npos);
        EXPECT_NE(sanitized.find("redis://[redacted]@h:6379/0"),
                  std::string::npos);
        EXPECT_NE(sanitized.find("\"redis_password\": \"[redacted]\""),
                  std::string::npos);
        const bool strict = mode == RedactionMode::kStrict;
        EXPECT_EQ(sanitized.find("10.1.2.3") == std::string::npos, strict);
        EXPECT_EQ(redactor.metadata().fields_redacted, strict ? 8 : 7);
    }
}

TEST(DiagnosticRedactionTest, StrictTextUsesTheFieldPseudonyms) {
    Redactor redactor(RedactionMode::kStrict);
    const auto field =
        redactor.sanitize("ip", "192.0.2.10", ValueSensitivity::kDeployment);
    const std::string text = redactor.sanitizeText(
        "peer 192.0.2.10 gid fe80::1 again "
        "192.0.2.10 other 192.0.2.11");
    const std::string pseudonym = field.value.get<std::string>();
    EXPECT_EQ(text.find("192.0.2.1"), std::string::npos) << text;
    EXPECT_EQ(text.find("fe80::1"), std::string::npos) << text;
    EXPECT_EQ(text.find(pseudonym), text.find("peer ") + 5) << text;
    EXPECT_NE(text.find(pseudonym, text.find("again")), std::string::npos);
    EXPECT_EQ(text.find(pseudonym, text.find("other")), std::string::npos);
    EXPECT_EQ(redactor.metadata().fields_redacted, 5);
}

TEST(DiagnosticRedactionTest, StrictTextReplacesKnownValuesAsWholeWords) {
    Redactor redactor(RedactionMode::kStrict);
    const auto host =
        redactor.sanitize("hostname", "node-a", ValueSensitivity::kDeployment);
    const auto netdev =
        redactor.sanitize("netdev", "eth1", ValueSensitivity::kDeployment);
    const std::string text =
        redactor.sanitizeText("node-a eth1 eth10 node-a1 xnode-a (eth1)");
    EXPECT_EQ(text, host.value.get<std::string>() + " " +
                        netdev.value.get<std::string>() +
                        " eth10 node-a1 xnode-a (" +
                        netdev.value.get<std::string>() + ")");
}

TEST(DiagnosticRedactionTest, StrictTextFindsAddressShapes) {
    const std::string addresses[] = {
        "10.0.0.1",
        "198.51.100.7",
        "fe80::1a2b:3cff:fe4d:5e6f",
        "00:00:00:00:00:00:00:00:00:00:ff:ff:c6:33:64:07",
        "0a:1b:2c:3d:4e:5f",
        "0a1b:2c3d:4e5f:6a7b",
        "::ffff:203.0.113.9",
        "10.9.8.7",
    };
    Redactor redactor(RedactionMode::kStrict);
    const std::string text = redactor.sanitizeText(
        "addr:10.0.0.1 seg 198.51.100.7:15321 gid:fe80::1a2b:3cff:fe4d:5e6f "
        "raw 00:00:00:00:00:00:00:00:00:00:ff:ff:c6:33:64:07 mac "
        "0a:1b:2c:3d:4e:5f guid 0a1b:2c3d:4e5f:6a7b mapped "
        "[::ffff:203.0.113.9] end 10.9.8.7.");
    for (const auto& address : addresses) {
        EXPECT_EQ(text.find(address), std::string::npos)
            << address << " in " << text;
    }
    EXPECT_NE(text.find(":15321 "), std::string::npos) << text;
    EXPECT_EQ(text.back(), '.');
    EXPECT_EQ(redactor.metadata().fields_redacted, 8);
}

TEST(DiagnosticRedactionTest, StrictTextKeepsNonAddresses) {
    Redactor redactor(RedactionMode::kStrict);
    const std::string text =
        "std::string I1011 05:17:20.123456 file.cpp:123] mlx5_2:4096 "
        "version 1.2.3 cafe deadbeef :: a::b GID[3] port 1: mtu 4096";
    EXPECT_EQ(redactor.sanitizeText(text), text);
    EXPECT_EQ(redactor.metadata().fields_redacted, 0);
}

TEST(DiagnosticRedactionTest, NonStrictTextKeepsDeploymentValues) {
    for (const auto mode : {RedactionMode::kDefault, RedactionMode::kNone}) {
        Redactor redactor(mode);
        redactor.sanitize("hostname", "node-a", ValueSensitivity::kDeployment);
        const std::string text = "node-a 198.51.100.7 fe80::1";
        EXPECT_EQ(redactor.sanitizeText(text), text);
        EXPECT_EQ(redactor.metadata().fields_redacted, 0);
    }
}

TEST(DiagnosticRendererTest, TextAndJsonContainTheSameCheckFacts) {
    DiagnosticSnapshot snapshot;
    snapshot.command = "version";
    snapshot.checks.push_back({"build.info.available",
                               CheckOutcome::kPass,
                               "build information is available",
                               {},
                               {}});

    const std::string text = renderText(snapshot);
    const std::string json = renderJson(snapshot);
    EXPECT_NE(text.find("build.info.available"), std::string::npos);
    EXPECT_NE(text.find("build information is available"), std::string::npos);
    EXPECT_NE(json.find("build.info.available"), std::string::npos);
    EXPECT_NE(json.find("build information is available"), std::string::npos);
}

TEST(DiagnosticExitCodeTest, FailuresAndStrictWarningsReturnOne) {
    DiagnosticSnapshot snapshot;
    EXPECT_EQ(diagnosticExitCode(snapshot), 0);

    snapshot.checks.push_back(
        {"warning", CheckOutcome::kWarn, "warning", {}, {}});
    EXPECT_EQ(diagnosticExitCode(snapshot), 0);
    EXPECT_EQ(diagnosticExitCode(snapshot, true), 1);

    snapshot.checks.push_back(
        {"failure", CheckOutcome::kFail, "failure", {}, {}});
    EXPECT_EQ(diagnosticExitCode(snapshot), 1);
}

TEST(DiagnosticVersionTest, SnapshotIsOfflineBuildMetadata) {
    const auto snapshot = makeVersionSnapshot();
    EXPECT_EQ(snapshot.schema_version, kDiagnosticSchemaVersion);
    EXPECT_EQ(snapshot.command, "version");
    EXPECT_FALSE(snapshot.build.version.empty());
    EXPECT_FALSE(snapshot.build.commit.empty());
    EXPECT_TRUE(snapshot.build.features.at("tent"));
    ASSERT_EQ(snapshot.checks.size(), 1);
    EXPECT_EQ(snapshot.checks.front().outcome, CheckOutcome::kPass);
}

}  // namespace
}  // namespace mooncake::tent::diagnostics
