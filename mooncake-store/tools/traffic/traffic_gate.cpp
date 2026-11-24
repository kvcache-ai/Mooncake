#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

#include <json/json.h>

namespace {

struct Options {
    std::string baseline;
    std::string current;
    double max_rps_drop_pct = 5.0;
    double max_p99_increase_pct = 10.0;
};

void PrintUsage() {
    std::printf(
        "usage: traffic_gate --baseline REPORT --current REPORT\n"
        "                    [--max-rps-drop-pct N]\n"
        "                    [--max-p99-increase-pct N]\n"
        "\n"
        "Compares two traffic_load JSON reports from the same plan. Exits 0\n"
        "when the current report is within both tolerances and has no RPC or\n"
        "transport errors, 1 when the gate fails, and 2 on bad input.\n");
}

bool ParseDouble(const std::string& text, double* value) {
    char* end = nullptr;
    errno = 0;
    const double parsed = std::strtod(text.c_str(), &end);
    if (errno != 0 || end == text.c_str() || *end != '\0' || parsed < 0) {
        return false;
    }
    *value = parsed;
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        const auto take = [&](std::string* out) {
            if (i + 1 >= argc) {
                return false;
            }
            *out = argv[++i];
            return true;
        };
        std::string value;
        if (arg == "--baseline") {
            if (!take(&options->baseline)) {
                return false;
            }
        } else if (arg == "--current") {
            if (!take(&options->current)) {
                return false;
            }
        } else if (arg == "--max-rps-drop-pct") {
            if (!take(&value) ||
                !ParseDouble(value, &options->max_rps_drop_pct)) {
                return false;
            }
        } else if (arg == "--max-p99-increase-pct") {
            if (!take(&value) ||
                !ParseDouble(value, &options->max_p99_increase_pct)) {
                return false;
            }
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage();
            std::exit(0);
        } else {
            std::fprintf(stderr, "unknown argument: %s\n", arg.c_str());
            return false;
        }
    }
    return !options->baseline.empty() && !options->current.empty();
}

bool LoadReport(const std::string& path, Json::Value* report,
                std::string* error) {
    std::ifstream in(path);
    if (!in) {
        *error = "cannot read " + path;
        return false;
    }
    Json::CharReaderBuilder builder;
    std::string parse_error;
    if (!Json::parseFromStream(builder, in, report, &parse_error)) {
        *error = "cannot parse " + path + ": " + parse_error;
        return false;
    }
    if (!report->isObject()) {
        *error = path + " must contain a JSON object";
        return false;
    }
    return true;
}

bool NumberField(const Json::Value& report, const char* name, double* value,
                 std::string* error) {
    const Json::Value& field = report[name];
    if (!field.isNumeric()) {
        *error = std::string("missing numeric field: ") + name;
        return false;
    }
    *value = field.asDouble();
    return true;
}

bool IntField(const Json::Value& report, const char* name, int* value,
              std::string* error) {
    const Json::Value& field = report[name];
    if (!field.isInt()) {
        *error = std::string("missing integer field: ") + name;
        return false;
    }
    *value = field.asInt();
    return true;
}

bool BoolField(const Json::Value& report, const char* name, bool* value,
               std::string* error) {
    const Json::Value& field = report[name];
    if (!field.isBool()) {
        *error = std::string("missing boolean field: ") + name;
        return false;
    }
    *value = field.asBool();
    return true;
}

bool SamePlan(const Json::Value& baseline, const Json::Value& current,
              std::string* error) {
    double baseline_rate = 0;
    double current_rate = 0;
    bool baseline_saturate = false;
    bool current_saturate = false;
    int baseline_connections = 0;
    int current_connections = 0;
    double baseline_duration = 0;
    double current_duration = 0;
    double baseline_warmup = 0;
    double current_warmup = 0;
    if (!NumberField(baseline, "rate_target", &baseline_rate, error) ||
        !NumberField(current, "rate_target", &current_rate, error) ||
        !BoolField(baseline, "saturate", &baseline_saturate, error) ||
        !BoolField(current, "saturate", &current_saturate, error) ||
        !IntField(baseline, "connections", &baseline_connections, error) ||
        !IntField(current, "connections", &current_connections, error) ||
        !NumberField(baseline, "duration_s", &baseline_duration, error) ||
        !NumberField(current, "duration_s", &current_duration, error) ||
        !NumberField(baseline, "warmup_s", &baseline_warmup, error) ||
        !NumberField(current, "warmup_s", &current_warmup, error)) {
        return false;
    }
    if (baseline_saturate != current_saturate ||
        std::fabs(baseline_rate - current_rate) > 1e-6 ||
        baseline_connections != current_connections ||
        std::fabs(baseline_duration - current_duration) > 1e-6 ||
        std::fabs(baseline_warmup - current_warmup) > 1e-6) {
        *error = "reports were produced by different plans";
        return false;
    }
    return true;
}

int Fail(const std::string& message) {
    std::fprintf(stderr, "%s\n", message.c_str());
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        PrintUsage();
        return 2;
    }

    Json::Value baseline;
    Json::Value current;
    std::string error;
    if (!LoadReport(options.baseline, &baseline, &error) ||
        !LoadReport(options.current, &current, &error)) {
        return Fail(error);
    }
    if (!SamePlan(baseline, current, &error)) {
        return Fail(error);
    }

    double baseline_rps = 0;
    double current_rps = 0;
    double baseline_p99 = 0;
    double current_p99 = 0;
    if (!NumberField(baseline, "achieved_rps", &baseline_rps, &error) ||
        !NumberField(current, "achieved_rps", &current_rps, &error)) {
        return Fail(error);
    }
    if (!NumberField(baseline["service_us"], "p99", &baseline_p99, &error) ||
        !NumberField(current["service_us"], "p99", &current_p99, &error)) {
        return Fail(error);
    }
    if (baseline_rps <= 0 || baseline_p99 <= 0) {
        return Fail("baseline report has no usable throughput or latency");
    }

    const double rps_drop_pct =
        (baseline_rps - current_rps) / baseline_rps * 100.0;
    const double p99_increase_pct =
        (current_p99 - baseline_p99) / baseline_p99 * 100.0;

    const Json::Value& rpc_errors = current["rpc_errors"];
    const Json::Value& transport_errors = current["transport_errors"];
    if (!rpc_errors.isUInt64() || !transport_errors.isUInt64()) {
        return Fail("current report is missing error counters");
    }
    const bool errors_ok =
        rpc_errors.asUInt64() == 0 && transport_errors.asUInt64() == 0;
    const bool rps_ok = rps_drop_pct <= options.max_rps_drop_pct;
    const bool p99_ok = p99_increase_pct <= options.max_p99_increase_pct;
    const bool passed = errors_ok && rps_ok && p99_ok;

    std::printf("{\n");
    std::printf("  \"passed\": %s,\n", passed ? "true" : "false");
    std::printf("  \"baseline\": \"%s\",\n", options.baseline.c_str());
    std::printf("  \"current\": \"%s\",\n", options.current.c_str());
    std::printf("  \"rps_drop_pct\": %.3f,\n", rps_drop_pct);
    std::printf("  \"p99_increase_pct\": %.3f,\n", p99_increase_pct);
    std::printf("  \"max_rps_drop_pct\": %.3f,\n", options.max_rps_drop_pct);
    std::printf("  \"max_p99_increase_pct\": %.3f,\n",
                options.max_p99_increase_pct);
    std::printf("  \"rps_ok\": %s,\n", rps_ok ? "true" : "false");
    std::printf("  \"p99_ok\": %s,\n", p99_ok ? "true" : "false");
    std::printf("  \"errors_ok\": %s\n", errors_ok ? "true" : "false");
    std::printf("}\n");
    return passed ? 0 : 1;
}
