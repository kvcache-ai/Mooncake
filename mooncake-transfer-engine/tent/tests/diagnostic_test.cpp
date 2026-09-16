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
