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
    RedactionMetadata metadata() const;

   private:
    static bool isAlwaysSensitiveField(std::string_view field);
    std::string strictPseudonym(const nlohmann::json& value);

    RedactionMode mode_;
    std::uint64_t fields_redacted_ = 0;
    std::unordered_map<std::string, std::string> pseudonyms_;
};

std::string_view toString(CheckOutcome outcome);
std::string_view toString(RedactionMode mode);

nlohmann::json toJson(const DiagnosticSnapshot& snapshot);
std::string renderJson(const DiagnosticSnapshot& snapshot, int indent = 2);
std::string renderText(const DiagnosticSnapshot& snapshot);
int diagnosticExitCode(const DiagnosticSnapshot& snapshot,
                       bool warnings_are_errors = false);

// Builds an offline-only snapshot from compile-time information. It does not
// initialize a TransferEngine, transport, metadata client, or device runtime.
DiagnosticSnapshot makeVersionSnapshot();

}  // namespace mooncake::tent::diagnostics
