#include <gtest/gtest.h>

#include <concepts>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>

#include "conductor/prefixindex/types.h"

namespace {

using conductor::prefixindex::ContextKey;
using conductor::prefixindex::EngineRegistration;
using conductor::prefixindex::GpuMutation;

template <typename T>
concept HasInstanceId = requires(T value) { value.instance_id; };

template <typename T>
concept HasCacheSalt = requires(T value) { value.cache_salt; };

template <typename T>
concept HasAdditionalSalt = requires(T value) { value.additional_salt; };

template <typename T>
concept HasCacheGroups = requires(T value) { value.cache_groups; };

static_assert(!HasInstanceId<ContextKey>);
static_assert(!HasCacheSalt<ContextKey>);
static_assert(!HasAdditionalSalt<ContextKey>);
static_assert(!HasCacheGroups<EngineRegistration>);
static_assert(!HasCacheGroups<GpuMutation>);
static_assert(std::same_as<decltype(EngineRegistration::cache_group),
                           std::optional<int64_t>>);
static_assert(
    std::same_as<decltype(GpuMutation::cache_group), std::optional<int64_t>>);

ContextKey BaseContext() {
    return {.tenant_id = "tenant-a",
            .model_name = "model-a",
            .lora_name = "lora-a",
            .block_size = 16};
}

TEST(ContextKey, IdenticalCopiesCompareAndHashEqual) {
    const ContextKey first = BaseContext();
    const ContextKey second = first;

    EXPECT_EQ(first, second);
    EXPECT_EQ(std::hash<ContextKey>{}(first), std::hash<ContextKey>{}(second));
}

struct FieldCase {
    const char* name;
    void (*change)(ContextKey*);
};

const FieldCase kFields[] = {
    {"tenant_id", [](ContextKey* context) { context->tenant_id = "tenant-b"; }},
    {"model_name",
     [](ContextKey* context) { context->model_name = "model-b"; }},
    {"lora_name", [](ContextKey* context) { context->lora_name = "lora-b"; }},
    {"block_size", [](ContextKey* context) { context->block_size = 32; }},
};

TEST(ContextKey, ExactlyFourFieldsParticipateInEqualityAndLookup) {
    std::unordered_map<ContextKey, int> contexts;
    contexts.emplace(BaseContext(), 1);

    for (const FieldCase& field : kFields) {
        SCOPED_TRACE(field.name);
        ContextKey changed = BaseContext();
        field.change(&changed);
        EXPECT_NE(changed, BaseContext());
        EXPECT_FALSE(contexts.contains(changed));
    }
}

TEST(ContextKey, OwnerAndRequestSaltLiveOutsideTheKey) {
    ContextKey shared = BaseContext();
    const std::string first_instance = "instance-a";
    const std::string second_instance = "instance-b";
    const std::string first_salt = "salt-a";
    const std::string second_salt = "salt-b";

    EXPECT_NE(first_instance, second_instance);
    EXPECT_NE(first_salt, second_salt);
    EXPECT_EQ(shared, BaseContext());
    EXPECT_EQ(std::hash<ContextKey>{}(shared),
              std::hash<ContextKey>{}(BaseContext()));
}

}  // namespace
