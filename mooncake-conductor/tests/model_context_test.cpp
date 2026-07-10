// ModelContext equality / hashing tests: all six fields must
// participate — a missed field would cause silent cross-context cache
// misses. Per-field perturbation must break equality.

#include <gtest/gtest.h>

#include <functional>
#include <unordered_map>

#include "conductor/prefixindex/prefix_indexer.h"

namespace {

using conductor::prefixindex::ModelContext;

ModelContext BaseContext() {
    ModelContext ctx;
    ctx.model_name = "model-a";
    ctx.lora_name = "lora-a";
    ctx.block_size = 16;
    ctx.additional_salt = "salt-a";
    ctx.tenant_id = "tenant-a";
    ctx.instance_id = "instance-a";
    return ctx;
}

TEST(ModelContext, EqualToIdenticalCopy) {
    EXPECT_EQ(BaseContext(), BaseContext());
    EXPECT_EQ(std::hash<ModelContext>{}(BaseContext()),
              std::hash<ModelContext>{}(BaseContext()));
}

// One test per field: perturbing it must break operator== AND change the
// behavior of the context as an unordered_map key.
struct FieldCase {
    const char* name;
    void (*perturb)(ModelContext*);
};

const FieldCase kFieldCases[] = {
    {"model_name", [](ModelContext* c) { c->model_name = "model-B"; }},
    {"lora_name", [](ModelContext* c) { c->lora_name = "lora-B"; }},
    {"block_size", [](ModelContext* c) { c->block_size = 32; }},
    {"additional_salt", [](ModelContext* c) { c->additional_salt = "salt-B"; }},
    {"tenant_id", [](ModelContext* c) { c->tenant_id = "tenant-B"; }},
    {"instance_id", [](ModelContext* c) { c->instance_id = "instance-B"; }},
};

TEST(ModelContext, EveryFieldParticipatesInEquality) {
    for (const auto& fc : kFieldCases) {
        ModelContext perturbed = BaseContext();
        fc.perturb(&perturbed);
        EXPECT_NE(BaseContext(), perturbed) << "field=" << fc.name;
    }
}

TEST(ModelContext, EveryFieldParticipatesAsMapKey) {
    std::unordered_map<ModelContext, int> map;
    map[BaseContext()] = 1;
    for (const auto& fc : kFieldCases) {
        ModelContext perturbed = BaseContext();
        fc.perturb(&perturbed);
        // The perturbed key must land in its own slot, not alias the base
        // key (this exercises hash + equality together).
        EXPECT_EQ(map.find(perturbed), map.end()) << "field=" << fc.name;
        map[perturbed] = 2;
        EXPECT_EQ(map.at(BaseContext()), 1) << "field=" << fc.name;
    }
    EXPECT_EQ(map.size(), 1 + std::size(kFieldCases));
}

// Field-swap check: two contexts with the same multiset of string values
// in different fields must not collide via operator==.
TEST(ModelContext, SwappedFieldValuesNotEqual) {
    ModelContext a = BaseContext();
    ModelContext b = BaseContext();
    b.tenant_id = a.instance_id;
    b.instance_id = a.tenant_id;
    EXPECT_NE(a, b);
}

}  // namespace
