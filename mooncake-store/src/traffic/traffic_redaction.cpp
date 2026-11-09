#include "traffic/traffic_redaction.h"

#include <openssl/sha.h>

#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <ylt/struct_pack.hpp>
#include <ylt/util/utils.hpp>

#include "rpc_service.h"
#include "rpc_types.h"

namespace mooncake::traffic {
namespace {

// Replaces a string with a fixed-length token derived from its content. Equal
// values keep equal tokens and distinct values keep distinct tokens, so a
// session that is shared still carries the key cardinality and the hot-key
// shape of the recorded workload while the original text is gone.
std::string TokenFor(std::string_view value) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(value.data()), value.size(),
           digest);
    constexpr char kHex[] = "0123456789abcdef";
    std::string token = "<r:";
    token.reserve(20);
    for (std::size_t i = 0; i < 8; ++i) {
        token.push_back(kHex[digest[i] >> 4]);
        token.push_back(kHex[digest[i] & 0x0f]);
    }
    token.push_back('>');
    return token;
}

void Mask(std::string& value) { value = TokenFor(value); }

void Mask(std::vector<std::string>& values) {
    for (auto& value : values) {
        Mask(value);
    }
}

void Mask(std::vector<std::vector<std::string>>& values) {
    for (auto& value : values) {
        Mask(value);
    }
}

void Mask(std::optional<std::vector<std::string>>& values) {
    if (values) {
        Mask(*values);
    }
}

// A compatible field is an optional that carries the protocol version: an
// absent field means the client never sent it, so it stays absent.
template <typename T, std::uint64_t version>
void Mask(struct_pack::compatible<T, version>& values) {
    if (values) {
        Mask(*values);
    }
}

void Mask(ReplicateConfig& config) {
    Mask(config.preferred_segments);
    Mask(config.preferred_segment);
    Mask(config.preferred_nof_segments);
    Mask(config.host_id);
    Mask(config.group_ids);
    Mask(config.session_ids);
    Mask(config.preferred_hca_names);
    Mask(config.per_key_preferred_hca_names);
    Mask(config.residency_affinity_ids);
}

void Mask(ObjectMeta& meta) { Mask(meta.key); }

void Mask(std::vector<ObjectMeta>& metas) {
    for (auto& meta : metas) {
        Mask(meta);
    }
}

template <typename Tuple, typename Fn>
class TupleRedactor final : public Redactor {
   public:
    explicit TupleRedactor(Fn fn) : fn_(std::move(fn)) {}

    bool Redact(std::string_view input, std::string& output) const override {
        Tuple args{};
        if (struct_pack::deserialize_to(args, input) != struct_pack::errc::ok) {
            return false;
        }
        fn_(args);
        const auto bytes = struct_pack::serialize(args);
        output.assign(bytes.data(), bytes.size());
        return true;
    }

   private:
    Fn fn_;
};

template <typename Tuple, typename Fn>
void Add(RedactionRegistry& registry, std::uint32_t route_key, Fn fn) {
    registry.Add(route_key,
                 std::make_shared<TupleRedactor<Tuple, Fn>>(std::move(fn)));
}

template <auto Method>
constexpr std::uint32_t RouteKey() {
    return coro_rpc::func_id<Method>();
}

// Types that carry no text and therefore need no masking. A request field of
// any other type must have a Mask overload: without this check a new field
// type would be serialized back unchanged and the string inside it would be
// recorded in clear text.
template <typename T>
inline constexpr bool kMaskNotNeeded =
    std::is_arithmetic_v<T> || std::is_enum_v<T> || std::is_same_v<T, UUID> ||
    std::is_same_v<T, ReplicaType> || std::is_same_v<T, GrantLeasePolicy> ||
    std::is_same_v<T, std::vector<std::uint64_t>>;

// Masks every argument the redactors understand. A route whose signature gains
// a string field is then covered without anyone also having to remember to add
// it to a per-route list, which is how the first version leaked fields.
template <typename T>
void MaskIfSupported(T& value) {
    if constexpr (requires { Mask(value); }) {
        Mask(value);
    } else {
        static_assert(kMaskNotNeeded<T>,
                      "this request field type has no Mask overload; add one "
                      "so the field cannot be recorded in clear text");
    }
}

template <typename Tuple, std::size_t... I>
void MaskAll(Tuple& args, std::index_sequence<I...>) {
    (MaskIfSupported(std::get<I>(args)), ...);
}

template <typename Tuple>
void MaskAll(Tuple& args) {
    MaskAll(args, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Tuple, auto Method>
void AddMasked(RedactionRegistry& registry) {
    Add<Tuple>(registry, RouteKey<Method>(),
               [](Tuple& args) { MaskAll(args); });
}

}  // namespace

void RedactionRegistry::Add(std::uint32_t route_key,
                            std::shared_ptr<const Redactor> redactor) {
    redactors_[route_key] = std::move(redactor);
}

bool RedactionRegistry::Contains(std::uint32_t route_key) const {
    return redactors_.find(route_key) != redactors_.end();
}

bool RedactionRegistry::Redact(std::uint32_t route_key, std::string_view input,
                               std::string& output) const {
    const auto it = redactors_.find(route_key);
    if (it == redactors_.end()) {
        return false;
    }
    return it->second->Redact(input, output);
}

void RegisterMasterRedactors(RedactionRegistry& registry) {
    using Service = WrappedMasterService;

    AddMasked<std::tuple<std::string, std::string>, &Service::GetReplicaList>(
        registry);
    AddMasked<std::tuple<std::string, std::string>,
              &Service::GetReplicaListForAdmin>(registry);
    AddMasked<std::tuple<std::vector<std::string>, std::string>,
              &Service::BatchGetReplicaList>(registry);
    AddMasked<std::tuple<std::vector<std::string>, std::string>,
              &Service::BatchGetReplicaListForAdmin>(registry);
    AddMasked<std::tuple<std::string, std::string>, &Service::ExistKey>(
        registry);
    AddMasked<std::tuple<std::string, std::string>, &Service::ProbeKey>(
        registry);
    AddMasked<std::tuple<std::vector<std::string>, std::string>,
              &Service::BatchExistKey>(registry);
    AddMasked<
        std::tuple<GrantLeasePolicy, std::vector<std::string>, std::string>,
        &Service::BatchProbeKey>(registry);
    AddMasked<std::tuple<std::string, bool, std::string>, &Service::Remove>(
        registry);
    AddMasked<std::tuple<std::vector<std::string>, bool, std::string>,
              &Service::BatchRemove>(registry);
    AddMasked<std::tuple<UUID, std::string, std::uint64_t, ReplicateConfig,
                         std::string>,
              &Service::PutStart>(registry);
    AddMasked<
        std::tuple<UUID, std::vector<std::string>, std::vector<std::uint64_t>,
                   ReplicateConfig, std::string>,
        &Service::BatchPutStart>(registry);
    AddMasked<std::tuple<UUID, ObjectMeta, ReplicaType, std::string>,
              &Service::PutEnd>(registry);
    AddMasked<
        std::tuple<UUID, std::vector<ObjectMeta>, ReplicaType, std::string>,
        &Service::BatchPutEnd>(registry);
    AddMasked<std::tuple<UUID, std::string, ReplicaType, std::string>,
              &Service::PutRevoke>(registry);
    AddMasked<
        std::tuple<UUID, std::vector<std::string>, ReplicaType, std::string>,
        &Service::BatchPutRevoke>(registry);
    // The Upsert family takes the same fields as the Put family.
    AddMasked<std::tuple<UUID, std::string, std::uint64_t, ReplicateConfig,
                         std::string>,
              &Service::UpsertStart>(registry);
    AddMasked<
        std::tuple<UUID, std::vector<std::string>, std::vector<std::uint64_t>,
                   ReplicateConfig, std::string>,
        &Service::BatchUpsertStart>(registry);
    AddMasked<std::tuple<UUID, ObjectMeta, ReplicaType, std::string>,
              &Service::UpsertEnd>(registry);
    AddMasked<std::tuple<UUID, std::vector<ObjectMeta>, std::string>,
              &Service::BatchUpsertEnd>(registry);
    AddMasked<std::tuple<UUID, std::string, ReplicaType, std::string>,
              &Service::UpsertRevoke>(registry);
    AddMasked<std::tuple<UUID, std::vector<std::string>, std::string>,
              &Service::BatchUpsertRevoke>(registry);
}

}  // namespace mooncake::traffic
