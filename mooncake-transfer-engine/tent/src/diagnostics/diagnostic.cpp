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

#include <arpa/inet.h>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <ostream>
#include <regex>
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

constexpr std::string_view kRedactedText = "[redacted]";
constexpr std::size_t kMinDeploymentTextLength = 3;

bool isWordChar(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

bool isAddressChar(char c) {
    return std::isxdigit(static_cast<unsigned char>(c)) || c == ':' || c == '.';
}

bool isHexGroups(std::string_view token, std::size_t group_length,
                 std::size_t min_groups, std::size_t max_groups) {
    std::size_t groups = 0;
    std::size_t begin = 0;
    while (begin <= token.size()) {
        const std::size_t end = std::min(token.find(':', begin), token.size());
        const std::string_view group = token.substr(begin, end - begin);
        if (group.size() != group_length ||
            !std::all_of(group.begin(), group.end(), [](char c) {
                return std::isxdigit(static_cast<unsigned char>(c));
            })) {
            return false;
        }
        ++groups;
        begin = end + 1;
    }
    return groups >= min_groups && groups <= max_groups;
}

bool isIpv4(const std::string& token) {
    in_addr address{};
    return inet_pton(AF_INET, token.c_str(), &address) == 1;
}

// Requires a decimal digit so scope-like text such as "a::b" is not taken for
// an address.
bool isIpv6(const std::string& token) {
    in6_addr address{};
    return token.find(':') != std::string::npos &&
           std::any_of(token.begin(), token.end(),
                       [](unsigned char c) { return std::isdigit(c); }) &&
           inet_pton(AF_INET6, token.c_str(), &address) == 1;
}

// GIDs and MACs are colon-separated byte pairs (16 and 6 or 8 groups), node
// GUIDs are four groups of four hex digits.
bool isHardwareAddress(std::string_view token) {
    return isHexGroups(token, 2, 6, 6) || isHexGroups(token, 2, 8, 8) ||
           isHexGroups(token, 2, 16, 16) || isHexGroups(token, 4, 4, 4);
}

bool isAddressToken(const std::string& token) {
    const bool has_digit =
        std::any_of(token.begin(), token.end(),
                    [](unsigned char c) { return std::isxdigit(c); });
    return has_digit &&
           (isIpv4(token) || isIpv6(token) || isHardwareAddress(token));
}

// Replaces capture group `group` of every match with "[redacted]", keeping
// surrounding quotes. Values that are already redacted are left alone.
std::string redactGroup(const std::string& text, const std::regex& pattern,
                        std::size_t group, std::uint64_t& replacements) {
    std::string result;
    std::size_t copied = 0;
    for (auto it = std::sregex_iterator(text.begin(), text.end(), pattern);
         it != std::sregex_iterator(); ++it) {
        const std::smatch& match = *it;
        if (!match[group].matched || match.length(group) == 0) {
            continue;
        }
        std::string value = match.str(group);
        const bool quoted = value.size() >= 2 &&
                            (value.front() == '"' || value.front() == '\'') &&
                            value.back() == value.front();
        const std::string_view inner =
            quoted ? std::string_view(value).substr(1, value.size() - 2)
                   : std::string_view(value);
        if (inner == kRedactedText) {
            continue;
        }
        const auto position = static_cast<std::size_t>(match.position(group));
        result.append(text, copied, position - copied);
        if (quoted) {
            result.push_back(value.front());
            result.append(kRedactedText);
            result.push_back(value.front());
        } else {
            result.append(kRedactedText);
        }
        copied = position + value.size();
        ++replacements;
    }
    result.append(text, copied, std::string::npos);
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

void writeEvidenceLine(std::ostream& output, std::string_view indent,
                       const DiagnosticEvidence& evidence) {
    output << indent << evidence.field << " = "
           << valueForText(evidence.observed);
    if (evidence.observed.redacted) {
        output << " (redacted)";
    }
    output << " [" << evidence.source << "]\n";
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
        if (value.is_string()) {
            const auto& text = value.get_ref<const std::string&>();
            if (text.size() >= kMinDeploymentTextLength &&
                std::find(deployment_strings_.begin(),
                          deployment_strings_.end(),
                          text) == deployment_strings_.end()) {
                deployment_strings_.push_back(text);
            }
        }
        return SanitizedValue{strictPseudonym(value), true, true};
    }
    return SanitizedValue{std::move(value), true, false};
}

std::string Redactor::sanitizeText(std::string_view text) {
    std::string result = redactSecretText(std::string(text));
    if (mode_ == RedactionMode::kStrict) {
        result = replaceKnownDeploymentText(result);
        result = replaceAddressTokens(result);
    }
    return result;
}

std::string Redactor::redactSecretText(std::string text) {
    using std::regex_constants::ECMAScript;
    using std::regex_constants::icase;
    static const std::regex kUrlUserInfo(
        R"(([a-z][a-z0-9+.\-]*://)([^\s/@]+)@)", ECMAScript | icase);
    static const std::regex kAuthorization(
        R"((authorization["']?\s*[:=]\s*["']?)((?:bearer|basic|digest|token)\s+)?([^\s"',;]+))",
        ECMAScript | icase);
    static const std::regex kBearer(R"(\b(bearer|basic)\s+([^\s"',;]+))",
                                    ECMAScript | icase);
    static const std::regex kSecretAssignment(
        R"((["']?[a-z0-9_.\-]*(?:password|passwd|token|secret|credential|private_key|access_key|api_key|apikey|rkey|lkey)[a-z0-9_.\-]*["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s"',;&}\]]+))",
        ECMAScript | icase);
    static const std::regex kKeyValue(R"(\b([rl]key)\s+(0x[0-9a-f]+|[0-9]+))",
                                      ECMAScript | icase);
    static const std::regex kRawAddress(R"(\b(0x[0-9a-f]{8,})\b)",
                                        ECMAScript | icase);

    text = redactGroup(text, kUrlUserInfo, 2, fields_redacted_);
    text = redactGroup(text, kAuthorization, 3, fields_redacted_);
    text = redactGroup(text, kBearer, 2, fields_redacted_);
    text = redactGroup(text, kSecretAssignment, 2, fields_redacted_);
    text = redactGroup(text, kKeyValue, 2, fields_redacted_);
    return redactGroup(text, kRawAddress, 1, fields_redacted_);
}

std::string Redactor::replaceKnownDeploymentText(const std::string& text) {
    std::vector<std::string> known = deployment_strings_;
    std::stable_sort(known.begin(), known.end(),
                     [](const std::string& lhs, const std::string& rhs) {
                         return lhs.size() > rhs.size();
                     });
    std::string result = text;
    for (const auto& value : known) {
        std::size_t position = 0;
        while ((position = result.find(value, position)) != std::string::npos) {
            const std::size_t end = position + value.size();
            const bool bounded =
                (position == 0 || !isWordChar(result[position - 1])) &&
                (end == result.size() || !isWordChar(result[end]));
            if (!bounded) {
                position = end;
                continue;
            }
            const std::string pseudonym = strictPseudonym(value);
            result.replace(position, value.size(), pseudonym);
            position += pseudonym.size();
            ++fields_redacted_;
        }
    }
    return result;
}

std::string Redactor::replaceAddressTokens(const std::string& text) {
    std::string result;
    std::size_t index = 0;
    while (index < text.size()) {
        if (!isAddressChar(text[index])) {
            result.push_back(text[index++]);
            continue;
        }
        std::size_t end = index;
        while (end < text.size() && isAddressChar(text[end])) {
            ++end;
        }
        const bool start_bounded = index == 0 || !isWordChar(text[index - 1]);
        const bool end_bounded = end == text.size() || !isWordChar(text[end]);
        result.append(redactAddressRun(text.substr(index, end - index),
                                       start_bounded, end_bounded));
        index = end;
    }
    return result;
}

std::string Redactor::redactAddressRun(const std::string& run,
                                       bool start_bounded, bool end_bounded) {
    if (run.empty()) {
        return run;
    }
    // Dots at either end are punctuation ("reached 10.0.0.1."), not part of
    // an address.
    std::size_t first = 0;
    std::size_t last = run.size();
    while (first < last && run[first] == '.') {
        ++first;
    }
    while (last > first && run[last - 1] == '.') {
        --last;
    }
    if (first > 0 || last < run.size()) {
        return run.substr(0, first) +
               redactAddressRun(run.substr(first, last - first),
                                start_bounded || first > 0,
                                end_bounded || last < run.size()) +
               run.substr(last);
    }
    if (start_bounded && end_bounded && isAddressToken(run)) {
        ++fields_redacted_;
        return strictPseudonym(run);
    }
    // A single colon can separate an address from a label ("gid:fe80::1") or
    // a port ("10.0.0.1:15000"); "::" always belongs to an IPv6 address.
    for (std::size_t i = 0; i < run.size(); ++i) {
        const bool single_colon = run[i] == ':' &&
                                  (i == 0 || run[i - 1] != ':') &&
                                  (i + 1 == run.size() || run[i + 1] != ':');
        if (single_colon) {
            return redactAddressRun(run.substr(0, i), start_bounded, true) +
                   ":" + redactAddressRun(run.substr(i + 1), true, end_bounded);
        }
    }
    return run;
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
            writeEvidenceLine(output, "  ", evidence);
        }
    }

    if (!snapshot.checks.empty()) {
        output << "\nChecks:\n";
        for (const auto& check : snapshot.checks) {
            output << "  [" << toString(check.outcome) << "] " << check.id
                   << ": " << check.summary << '\n';
            // Evidence locates a problem (node, device, link, cause), so it
            // is shown for every check that did not pass.
            if (check.outcome != CheckOutcome::kPass) {
                for (const auto& evidence : check.evidence) {
                    writeEvidenceLine(output, "    ", evidence);
                }
            }
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
    int code = 0;
    for (const auto& check : snapshot.checks) {
        if (check.outcome == CheckOutcome::kFail) {
            if (check.id == kInternalErrorCheckId) {
                return 4;
            }
            const bool collector = check.id == kCollectorProcessCheckId ||
                                   check.id == kCollectorMissingCheckId;
            code = std::max(code, collector ? 3 : 1);
        } else if (warnings_are_errors &&
                   check.outcome == CheckOutcome::kWarn) {
            code = std::max(code, 1);
        }
    }
    return code;
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
