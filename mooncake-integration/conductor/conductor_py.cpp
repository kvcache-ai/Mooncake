#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <type_traits>

#include "conductor/client/conductor_client.h"

namespace py = pybind11;

namespace {

// tl::expected<T, ErrorCode> → {"ret": int, <payload_key>: converted}
// ret is a direct static_cast<int>(code), matching the C++ numeric values.
py::dict HitToDict(
    const mooncake::conductor::prefixindex::CacheHitResult& hit) {
    py::dict d;
    d["longest_matched"] = hit.longest_match_tokens;
    py::dict dp;
    for (const auto& [rank, tokens] : hit.dp) dp[py::int_(rank)] = tokens;
    d["dp"] = std::move(dp);
    py::dict rank_matches;
    for (const auto& [rank, m] : hit.rank_matches) {
        py::dict rm;
        rm["npu"] = m.npu;
        rm["cpu_local"] = m.cpu_local;
        rm["cpu_share"] = m.cpu_share;
        rm["disk"] = m.disk;
        rank_matches[py::int_(rank)] = std::move(rm);
    }
    d["rank_matches"] = std::move(rank_matches);
    d["npu"] = hit.npu;
    d["cpu_local"] = hit.cpu_local;
    d["cpu_share"] = hit.cpu_share;
    d["disk"] = hit.disk;
    return d;
}

py::dict HashProfileToDict(
    const mooncake::conductor::common::ResolvedHashProfile& profile) {
    // The 5-key shape matches HTTP PackHashProfile and is shared by
    // /services and /global_view.
    py::dict hp;
    hp["strategy"] = profile.strategy;
    hp["algorithm"] = profile.algorithm;
    hp["python_hash_seed"] = profile.python_hash_seed;
    hp["root_digest"] = profile.root_digest;
    hp["index_projection"] = profile.index_projection;
    return hp;
}

py::dict ServiceToDict(const mooncake::conductor::common::ServiceConfig& svc) {
    py::dict d;
    // Key names follow the /services wire contract (PackServiceConfig's
    // export style).
    d["Endpoint"] = svc.endpoint;
    d["ReplayEndpoint"] = svc.replay_endpoint;
    d["Type"] = std::string(
        mooncake::conductor::common::PublisherKindName(svc.publisher_kind));
    d["ModelName"] = svc.model_name;
    d["LoraName"] = svc.lora_name;
    d["TenantID"] = svc.tenant_id;
    d["InstanceID"] = svc.instance_id;
    d["BlockSize"] = svc.block_size;
    d["DPRank"] = svc.dp_rank;
    d["CacheGroup"] = svc.cache_group ? py::cast(*svc.cache_group) : py::none();
    d["HashProfile"] = HashProfileToDict(svc.hash_profile);
    return d;
}

// Validate before casting so pybind conversion errors follow the public API's
// ValueError convention, including integer overflow and nested field types.
template <typename T>
T RegisterValue(py::handle value, const std::string& field) {
    if constexpr (std::is_integral_v<T>) {
        if (!py::isinstance<py::int_>(value) ||
            py::isinstance<py::bool_>(value))
            throw py::value_error(field + " must be an integer");
    } else {
        if (!py::isinstance<py::str>(value))
            throw py::value_error(field + " must be a string");
    }
    try {
        return py::cast<T>(value);
    } catch (const py::cast_error&) {
        throw py::value_error("invalid type or value for register " + field);
    }
}

void ValidateKeys(const py::dict& config,
                  const std::set<std::string>& known_keys,
                  const std::string& field) {
    for (auto item : config) {
        if (!py::isinstance<py::str>(item.first))
            throw py::value_error(field + " keys must be strings");
        const auto key = py::cast<std::string>(item.first);
        if (!known_keys.count(key))
            throw py::value_error("unknown " + field + " key: " + key);
    }
}

}  // namespace

class PyConductorClient {
   public:
    int setup(const std::string& addr, int64_t connect_timeout_ms,
              int64_t request_timeout_ms) {
        auto result =
            client_.Setup({addr, connect_timeout_ms, request_timeout_ms});
        if (result) setup_called_ = true;
        return result ? 0 : static_cast<int>(result.error());
    }
    int close() {
        client_.Close();
        return 0;
    }
    int health_check() {
        // Same codes as store: 0 healthy / 1 uninitialized / 2 unreachable.
        if (!setup_called_) return 1;
        return client_.HealthCheck() ? 0 : 2;
    }
    py::dict query(const std::string& model_name, const std::string& lora_name,
                   int64_t block_size, const std::string& tenant_id,
                   const std::vector<int32_t>& token_ids,
                   const std::string& cache_salt,
                   const std::string& instance_filter) {
        py::dict out;
        mooncake::conductor::QueryRequest req;
        req.context.tenant_id = tenant_id;
        req.context.model_name = model_name;
        req.context.lora_name = lora_name;
        req.context.block_size = block_size;
        req.token_ids = token_ids;
        if (!cache_salt.empty()) req.cache_salt = cache_salt;
        if (!instance_filter.empty()) req.instance_filter = instance_filter;
        py::gil_scoped_release release;
        auto result = client_.Query(req);
        py::gil_scoped_acquire acquire;
        if (!result) {
            out["ret"] = static_cast<int>(result.error());
            out["hits"] = py::dict();
            return out;
        }
        py::dict hits;
        for (const auto& [id, hit] : result->instances) {
            hits[py::str(id)] = HitToDict(hit);
        }
        out["ret"] = 0;
        out["hits"] = std::move(hits);
        return out;
    }
    int register_service(py::object value) {
        if (!py::isinstance<py::dict>(value))
            throw py::value_error("register config must be a dict");
        const auto config = py::reinterpret_borrow<py::dict>(value);
        // Key names follow the Python API convention
        // (model_name/publisher_type, unlike the HTTP /register msgpack
        // fields modelname/type); unknown keys or wrong types raise
        // py::value_error (store's invalid-argument convention). Required:
        // instance_id, endpoint, model_name, block_size, hash_profile;
        // the server normalizes an omitted or empty tenant_id to "default".
        namespace mc = mooncake::conductor;
        mc::common::ServiceConfig svc;
        static const std::set<std::string> kKnownKeys = {
            "instance_id", "endpoint",    "replay_endpoint", "publisher_type",
            "model_name",  "lora_name",   "tenant_id",       "block_size",
            "dp_rank",     "cache_group", "hash_profile"};
        ValidateKeys(config, kKnownKeys, "register config");
        auto get_str = [&](const char* key, std::string* out) {
            if (!config.contains(key)) return;
            *out = RegisterValue<std::string>(config[key], key);
        };
        get_str("instance_id", &svc.instance_id);
        get_str("endpoint", &svc.endpoint);
        get_str("replay_endpoint", &svc.replay_endpoint);
        get_str("model_name", &svc.model_name);
        get_str("lora_name", &svc.lora_name);
        get_str("tenant_id", &svc.tenant_id);
        if (config.contains("block_size"))
            svc.block_size =
                RegisterValue<int64_t>(config["block_size"], "block_size");
        if (config.contains("dp_rank"))
            svc.dp_rank = RegisterValue<int>(config["dp_rank"], "dp_rank");
        if (config.contains("cache_group") && !config["cache_group"].is_none())
            svc.cache_group =
                RegisterValue<int64_t>(config["cache_group"], "cache_group");
        if (config.contains("publisher_type")) {
            auto kind =
                mc::common::ParsePublisherKind(RegisterValue<std::string>(
                    config["publisher_type"], "publisher_type"));
            if (!kind) throw py::value_error("invalid publisher_type");
            svc.publisher_kind = *kind;
        }
        if (config.contains("hash_profile")) {
            if (!py::isinstance<py::dict>(config["hash_profile"]))
                throw py::value_error("register hash_profile must be a dict");
            const auto hp =
                py::reinterpret_borrow<py::dict>(config["hash_profile"]);
            static const std::set<std::string> kHashKeys = {
                "strategy", "algorithm", "python_hash_seed",
                "index_projection"};
            ValidateKeys(hp, kHashKeys, "hash_profile");
            if (hp.contains("strategy"))
                svc.hash_profile.strategy = RegisterValue<std::string>(
                    hp["strategy"], "hash_profile.strategy");
            if (hp.contains("algorithm"))
                svc.hash_profile.algorithm = RegisterValue<std::string>(
                    hp["algorithm"], "hash_profile.algorithm");
            if (hp.contains("python_hash_seed"))
                svc.hash_profile.python_hash_seed = RegisterValue<std::string>(
                    hp["python_hash_seed"], "hash_profile.python_hash_seed");
            if (hp.contains("index_projection"))
                svc.hash_profile.index_projection = RegisterValue<std::string>(
                    hp["index_projection"], "hash_profile.index_projection");
        }
        // root_digest is derived server-side from the recipe, not supplied
        // here.
        py::gil_scoped_release release;
        auto result = client_.Register(svc);
        return result ? 0 : static_cast<int>(result.error());
    }
    int unregister(const std::string& instance_id, const std::string& tenant_id,
                   int dp_rank) {
        py::gil_scoped_release release;
        auto result = client_.Unregister(instance_id, tenant_id, dp_rank);
        return result ? 0 : static_cast<int>(result.error());
    }
    py::dict get_global_view() {
        py::gil_scoped_release release;
        auto result = client_.GetGlobalView();
        py::gil_scoped_acquire acquire;
        py::dict out;
        if (!result) {
            out["ret"] = static_cast<int>(result.error());
            out["context_count"] = 0;
            out["contexts"] = py::list();
            return out;
        }
        out["ret"] = 0;
        out["context_count"] = result->context_count;
        py::list contexts;
        for (const auto& view : result->contexts) {
            py::dict c;
            c["model_name"] = view.context.model_name;
            c["lora_name"] = view.context.lora_name;
            c["block_size"] = view.context.block_size;
            c["tenant_id"] = view.context.tenant_id;
            c["prefix_count"] = view.prefix_count;
            c["hash_profile"] = HashProfileToDict(view.profile);
            py::dict instances;
            for (const auto& [id, ranks] : view.instance_ranks) {
                instances[py::str(id)] = py::cast(ranks);
            }
            c["instances"] = std::move(instances);
            contexts.append(std::move(c));
        }
        out["contexts"] = std::move(contexts);
        return out;
    }
    py::dict list_services() {
        py::gil_scoped_release release;
        auto result = client_.ListServices();
        py::gil_scoped_acquire acquire;
        py::dict out;
        if (!result) {
            out["ret"] = static_cast<int>(result.error());
            out["count"] = 0;
            out["services"] = py::list();
            return out;
        }
        py::list services;
        for (const auto& svc : *result) services.append(ServiceToDict(svc));
        out["ret"] = 0;
        out["count"] = static_cast<int64_t>(result->size());
        out["services"] = std::move(services);
        return out;
    }

   private:
    mooncake::conductor::ConductorClient client_;
    bool setup_called_ = false;
};

PYBIND11_MODULE(_conductor, m) {
    py::class_<PyConductorClient>(m, "ConductorClient")
        .def(py::init<>())
        .def("setup", &PyConductorClient::setup, py::arg("conductor_addr"),
             py::arg("connect_timeout_ms") = 1000,
             py::arg("request_timeout_ms") = 3000)
        .def("close", &PyConductorClient::close)
        .def("health_check", &PyConductorClient::health_check)
        .def("query", &PyConductorClient::query, py::arg("model_name"),
             py::arg("lora_name") = "", py::arg("block_size"),
             py::arg("tenant_id") = "default", py::arg("token_ids"),
             py::arg("cache_salt") = "", py::arg("instance_filter") = "")
        .def("register", &PyConductorClient::register_service,
             py::arg("config"))
        .def("unregister", &PyConductorClient::unregister,
             py::arg("instance_id"), py::arg("tenant_id") = "default",
             py::arg("dp_rank") = 0)
        .def("get_global_view", &PyConductorClient::get_global_view)
        .def("list_services", &PyConductorClient::list_services);
}
