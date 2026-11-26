#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <unordered_map>

#include "conductor/prefixindex/types.h"

namespace {

using mooncake::conductor::prefixindex::ContextKey;

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

}  // namespace
