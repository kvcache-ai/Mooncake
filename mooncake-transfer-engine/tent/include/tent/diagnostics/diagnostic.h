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

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent::diagnostics {

inline constexpr std::uint32_t kDiagnosticSchemaVersion = 1;

// Checks with a fixed exit-code meaning. A failed collector check means a
// required collector process or service was unavailable, crashed or timed
// out; a failed internal check means the tool itself failed.
inline constexpr std::string_view kCollectorProcessCheckId = "engine.process";
inline constexpr std::string_view kCollectorMissingCheckId =
    "tool.probe_missing";
inline constexpr std::string_view kInternalErrorCheckId = "tool.internal";

enum class CheckOutcome { kPass, kWarn, kFail, kSkip };
enum class RedactionMode { kDefault, kStrict, kNone };
enum class ValueSensitivity { kPublic, kDeployment, kSecret, kRawAddress };

struct SanitizedValue {
    nlohmann::json value;
    bool available = true;
    bool redacted = false;

    static SanitizedValue Unavailable();
};

struct DiagnosticEvidence {
    std::string source;
    std::string field;
    SanitizedValue observed;
};

struct DiagnosticCheck {
    std::string id;
    CheckOutcome outcome = CheckOutcome::kSkip;
    std::string summary;
    std::vector<DiagnosticEvidence> evidence;
    std::vector<std::string> remediation;
};

struct DiagnosticSection {
    std::string id;
    std::string summary;
    std::vector<DiagnosticEvidence> evidence;
};

struct BuildInfo {
    std::string version;
    std::string commit;
    std::string build_type;
    std::string compiler;
    std::map<std::string, bool> features;
};

struct RedactionMetadata {
    RedactionMode mode = RedactionMode::kDefault;
    std::uint64_t fields_redacted = 0;
};

struct DiagnosticSnapshot {
    std::uint32_t schema_version = kDiagnosticSchemaVersion;
    std::string command;
    BuildInfo build;
    std::vector<DiagnosticSection> sections;
    std::vector<DiagnosticCheck> checks;
    RedactionMetadata redaction;
};

class Redactor {
   public:
    explicit Redactor(RedactionMode mode = RedactionMode::kDefault);

    SanitizedValue sanitize(
        std::string_view field, nlohmann::json value,
        ValueSensitivity sensitivity = ValueSensitivity::kPublic);

    // Sanitizes free text such as log lines and error messages. Credential
    // assignments, URL user information, authorization values, rkey/lkey
    // values and raw addresses are replaced in every mode. In strict mode,
    // deployment strings already passed to sanitize() and any remaining
    // IPv4, IPv6, GID, MAC or GUID token are replaced by the same pseudonyms
    // sanitize() uses. Every replacement counts as one redacted field.
    std::string sanitizeText(std::string_view text);

    RedactionMetadata metadata() const;

   private:
    static bool isAlwaysSensitiveField(std::string_view field);
    std::string strictPseudonym(const nlohmann::json& value);
    std::string redactSecretText(std::string text);
    std::string replaceKnownDeploymentText(const std::string& text);
    std::string replaceAddressTokens(const std::string& text);
    std::string redactAddressRun(const std::string& run, bool start_bounded,
                                 bool end_bounded);

    RedactionMode mode_;
    std::uint64_t fields_redacted_ = 0;
    std::unordered_map<std::string, std::string> pseudonyms_;
    std::vector<std::string> deployment_strings_;
};

std::string_view toString(CheckOutcome outcome);
std::string_view toString(RedactionMode mode);

nlohmann::json toJson(const DiagnosticSnapshot& snapshot);
std::string renderJson(const DiagnosticSnapshot& snapshot, int indent = 2);
std::string renderText(const DiagnosticSnapshot& snapshot);

// Returns 4 if an internal check failed, otherwise 3 if a collector check
// failed, otherwise 1 if any check failed (or warned, with
// warnings_are_errors), otherwise 0. Exit code 2 is left to the command-line
// parser for usage errors.
int diagnosticExitCode(const DiagnosticSnapshot& snapshot,
                       bool warnings_are_errors = false);

// Builds an offline-only snapshot from compile-time information. It does not
// initialize a TransferEngine, transport, metadata client, or device runtime.
DiagnosticSnapshot makeVersionSnapshot();

}  // namespace mooncake::tent::diagnostics
