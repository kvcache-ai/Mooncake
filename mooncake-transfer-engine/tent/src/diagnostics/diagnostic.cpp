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

#include <algorithm>
#include <cctype>
#include <iterator>
#include <sstream>
#include <utility>

namespace mooncake::tent::diagnostics {
namespace {

#ifndef TENT_BUILD_VERSION
#define TENT_BUILD_VERSION "unknown"
#endif
#ifndef TENT_GIT_COMMIT
#define TENT_GIT_COMMIT "unknown"
#endif
#ifndef TENT_BUILD_TYPE
#define TENT_BUILD_TYPE "unknown"
#endif
#ifndef TENT_COMPILER_ID
#define TENT_COMPILER_ID "unknown"
#endif
#ifndef TENT_COMPILER_VERSION
#define TENT_COMPILER_VERSION "unknown"
#endif

nlohmann::json evidenceToJson(const DiagnosticEvidence& evidence) {
    nlohmann::json result{{"source", evidence.source},
                          {"field", evidence.field},
                          {"available", evidence.observed.available}};
    if (evidence.observed.available) {
        result["observed"] = evidence.observed.value;
    }
    if (evidence.observed.redacted) {
        result["redacted"] = true;
    }
    return result;
}

std::string lowercase(std::string_view text) {
    std::string result(text);
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

std::string valueForText(const SanitizedValue& value) {
    if (!value.available) {
        return "unavailable";
    }
    if (value.value.is_string()) {
        return value.value.get<std::string>();
    }
    return value.value.dump();
}

}  // namespace

SanitizedValue SanitizedValue::Unavailable() {
    return SanitizedValue{nullptr, false, false};
}

Redactor::Redactor(RedactionMode mode) : mode_(mode) {}

SanitizedValue Redactor::sanitize(std::string_view field, nlohmann::json value,
                                  ValueSensitivity sensitivity) {
    const bool always_redact = sensitivity == ValueSensitivity::kSecret ||
                               sensitivity == ValueSensitivity::kRawAddress ||
                               isAlwaysSensitiveField(field);
    if (always_redact) {
        ++fields_redacted_;
        return SanitizedValue{"[redacted]", true, true};
    }
    if (mode_ == RedactionMode::kStrict &&
        sensitivity == ValueSensitivity::kDeployment) {
        ++fields_redacted_;
        return SanitizedValue{strictPseudonym(value), true, true};
    }
    return SanitizedValue{std::move(value), true, false};
}

RedactionMetadata Redactor::metadata() const {
    return RedactionMetadata{mode_, fields_redacted_};
}

bool Redactor::isAlwaysSensitiveField(std::string_view field) {
    const std::string normalized = lowercase(field);
    static constexpr std::string_view kSensitiveFields[] = {
        "token", "password", "credential",  "secret",     "private_key",
        "rkey",  "lkey",     "raw_address", "ipc_handle", "payload"};
    return std::any_of(std::begin(kSensitiveFields), std::end(kSensitiveFields),
                       [&normalized](std::string_view candidate) {
                           return normalized.find(candidate) !=
                                  std::string::npos;
                       });
}

std::string Redactor::strictPseudonym(const nlohmann::json& value) {
    const std::string key = value.dump();
    const auto found = pseudonyms_.find(key);
    if (found != pseudonyms_.end()) {
        return found->second;
    }
    const std::string pseudonym =
        "entity-" + std::to_string(pseudonyms_.size() + 1);
    pseudonyms_.emplace(key, pseudonym);
    return pseudonym;
}

std::string_view toString(CheckOutcome outcome) {
    switch (outcome) {
        case CheckOutcome::kPass:
            return "pass";
        case CheckOutcome::kWarn:
            return "warn";
        case CheckOutcome::kFail:
            return "fail";
        case CheckOutcome::kSkip:
            return "skip";
    }
    return "skip";
}

std::string_view toString(RedactionMode mode) {
    switch (mode) {
        case RedactionMode::kDefault:
            return "default";
        case RedactionMode::kStrict:
            return "strict";
        case RedactionMode::kNone:
            return "none";
    }
    return "default";
}

nlohmann::json toJson(const DiagnosticSnapshot& snapshot) {
    nlohmann::json result{
        {"schema_version", snapshot.schema_version},
        {"command", snapshot.command},
        {"build",
         {{"version", snapshot.build.version},
          {"commit", snapshot.build.commit},
          {"build_type", snapshot.build.build_type},
          {"compiler", snapshot.build.compiler},
          {"features", snapshot.build.features}}},
        {"sections", nlohmann::json::array()},
        {"checks", nlohmann::json::array()},
        {"redaction",
         {{"mode", toString(snapshot.redaction.mode)},
          {"fields_redacted", snapshot.redaction.fields_redacted}}}};

    for (const auto& section : snapshot.sections) {
        nlohmann::json serialized{{"id", section.id},
                                  {"summary", section.summary},
                                  {"evidence", nlohmann::json::array()}};
        for (const auto& evidence : section.evidence) {
            serialized["evidence"].push_back(evidenceToJson(evidence));
        }
        result["sections"].push_back(std::move(serialized));
    }

    for (const auto& check : snapshot.checks) {
        nlohmann::json serialized{{"id", check.id},
                                  {"outcome", toString(check.outcome)},
                                  {"summary", check.summary},
                                  {"evidence", nlohmann::json::array()},
                                  {"remediation", check.remediation}};
        for (const auto& evidence : check.evidence) {
            serialized["evidence"].push_back(evidenceToJson(evidence));
        }
        result["checks"].push_back(std::move(serialized));
    }
    return result;
}

std::string renderJson(const DiagnosticSnapshot& snapshot, int indent) {
    return toJson(snapshot).dump(indent);
}

std::string renderText(const DiagnosticSnapshot& snapshot) {
    std::ostringstream output;
    output << "TENT diagnostics schema: " << snapshot.schema_version << '\n';
    output << "Command: " << snapshot.command << '\n';
    output << "Version: " << snapshot.build.version << '\n';
    output << "Commit: " << snapshot.build.commit << '\n';
    output << "Build type: " << snapshot.build.build_type << '\n';
    output << "Compiler: " << snapshot.build.compiler << '\n';
    output << "Features:";
    for (const auto& [feature, enabled] : snapshot.build.features) {
        output << ' ' << feature << '=' << (enabled ? "enabled" : "disabled");
    }
    output << '\n';

    for (const auto& section : snapshot.sections) {
        output << '\n' << section.id << ": " << section.summary << '\n';
        for (const auto& evidence : section.evidence) {
            output << "  " << evidence.field << " = "
                   << valueForText(evidence.observed);
            if (evidence.observed.redacted) {
                output << " (redacted)";
            }
            output << " [" << evidence.source << "]\n";
        }
    }

    if (!snapshot.checks.empty()) {
        output << "\nChecks:\n";
        for (const auto& check : snapshot.checks) {
            output << "  [" << toString(check.outcome) << "] " << check.id
                   << ": " << check.summary << '\n';
            for (const auto& remediation : check.remediation) {
                output << "    remediation: " << remediation << '\n';
            }
        }
    }
    output << "Redaction: " << toString(snapshot.redaction.mode) << " ("
           << snapshot.redaction.fields_redacted << " fields redacted)\n";
    return output.str();
}

int diagnosticExitCode(const DiagnosticSnapshot& snapshot,
                       bool warnings_are_errors) {
    for (const auto& check : snapshot.checks) {
        if (check.outcome == CheckOutcome::kFail ||
            (warnings_are_errors && check.outcome == CheckOutcome::kWarn)) {
            return 1;
        }
    }
    return 0;
}

DiagnosticSnapshot makeVersionSnapshot() {
    DiagnosticSnapshot snapshot;
    snapshot.command = "version";
    snapshot.build.version = TENT_BUILD_VERSION;
    snapshot.build.commit = TENT_GIT_COMMIT;
    snapshot.build.build_type = TENT_BUILD_TYPE;
    snapshot.build.compiler =
        std::string(TENT_COMPILER_ID) + " " + TENT_COMPILER_VERSION;
    snapshot.build.features = {
        {"tent", true},
#ifdef TENT_FEATURE_ASCEND
        {"ascend", true},
#else
        {"ascend", false},
#endif
#ifdef TENT_FEATURE_ASCEND_DIRECT
        {"ascend_direct", true},
#else
        {"ascend_direct", false},
#endif
#ifdef TENT_FEATURE_CUDA
        {"cuda", true},
#else
        {"cuda", false},
#endif
#ifdef TENT_FEATURE_HIP
        {"hip", true},
#else
        {"hip", false},
#endif
#ifdef TENT_FEATURE_RDMA
        {"rdma", true},
#else
        {"rdma", false},
#endif
#ifdef TENT_FEATURE_SUNRISE
        {"sunrise", true},
#else
        {"sunrise", false},
#endif
#ifdef TENT_FEATURE_TPU
        {"tpu", true},
#else
        {"tpu", false},
#endif
#ifdef TENT_FEATURE_URING
        {"uring", true},
#else
        {"uring", false},
#endif
    };
    snapshot.checks.push_back({"build.info.available",
                               CheckOutcome::kPass,
                               "compile-time build information is available",
                               {},
                               {}});
    return snapshot;
}

}  // namespace mooncake::tent::diagnostics
