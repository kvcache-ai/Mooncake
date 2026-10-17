"""Store-backed KV-cache reuse: logical keys, manifests, reshard planning.

This is the Store-side counterpart of the runtime-to-runtime reshard
modules in ``mooncake.reshard.kv_cache``. Where the R2R path plans between
two live runtimes, this path plans reads from Store-resident KV pages that
were written under a different topology, so it adds three things the R2R
path does not need:

- a model semantic domain and the logical page keys derived from it, so
  the same token prefix reuses the same Store key across topologies;
- an immutable, content-addressed source layout manifest describing what
  each Part covers, readable after the writer process has exited;
- target-driven byte-range planning that intersects the reader's
  requirements with a source manifest, plus a template cache for the
  resulting relative-offset plans.
"""

from .layout import HostLayoutGeometry, KVCacheHostLayout
from .model_domain import ModelSemanticDomain
from .planner import (
    PlannedByteRange,
    ReshardPlanningError,
    ReshardReadPlan,
    TargetRequirement,
    part_geometry,
    plan_reshard_read,
    target_geometry,
)
from .source_manifest import SourceLayoutManifest, SourcePartCoverage
from .template import ReadTemplate, ReadTemplateCache

__all__ = [
    "HostLayoutGeometry",
    "KVCacheHostLayout",
    "ModelSemanticDomain",
    "PlannedByteRange",
    "ReadTemplate",
    "ReadTemplateCache",
    "ReshardPlanningError",
    "ReshardReadPlan",
    "SourceLayoutManifest",
    "SourcePartCoverage",
    "TargetRequirement",
    "part_geometry",
    "plan_reshard_read",
    "target_geometry",
]
