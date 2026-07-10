"""State layer: page manager, radix tree, feature store, multimodal state.

This module realizes the RFC §4 unified state object with page-level
reference counting, copy-on-write, and content-addressed prefix caching.
It is built on top of raw PyTorch tensors so that every state operation
(fork, advance_step, release) reduces to O(1) pointer bookkeeping plus
page-granularity writes.
"""

from .page_manager import (
    BlockRef,
    PageId,
    PagedKVManager,
)
from .radix_tree import RadixTree
from .feature_store import FeatureBundle, FeatureBundleDescriptor, FeatureStore, TensorSpec
from .feature_handle import FeatureHandle, FeatureHandleError, FeatureHandleExpired, FeatureHandleRegistry
from .mooncake_feature_store import (
    MooncakeFeatureBundleStore,
    MooncakeFeatureBundleStoreConfig,
    MooncakeFeatureStoreError,
    build_mooncake_feature_uri,
    parse_mooncake_feature_uri,
)
from .mm_store import MMStore, MMStoreBackpressureError, MMStoreEvent, MMStoreHandle
from .direct_feature_buffer import (
    DirectFeatureBufferAllocation,
    DirectFeatureBufferRegistry,
    get_direct_feature_buffer_registry,
    iter_direct_feature_buffer_registries,
    register_direct_feature_buffer_registry,
    unregister_direct_feature_buffer_registry,
)
from .kv_state_store import (
    KVBlockDescriptor,
    KVStateDescriptor,
    MooncakeKVStateStore,
    MooncakeRemoteKVMaterializer,
)
from .vllm_feature_handle_provider import (
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    ResolvedFeatureHandles,
    clear_feature_handle_bundle_cache,
    close_default_feature_handle_provider,
    extract_feature_handle_payloads,
    get_default_feature_handle_provider,
    inject_feature_handles_into_vllm_mm_kwargs,
    maybe_inject_feature_handle_kwargs,
    publish_feature_bundle_to_dir,
    register_feature_handle_registry,
    resolve_feature_handles_for_vllm,
    unregister_feature_handle_registry,
    use_kv_transfer_params,
)
from .state import HandoffTransaction, MultimodalState, StateLayer, StateMeta
from .workflow_registry import WorkflowStateRecord, WorkflowStateRegistry
from .vllm_mm_hidden_cache import (
    VLLMMMHiddenStateCache,
    get_current_mm_hidden_cache_keys,
    get_global_mm_hidden_cache,
    get_or_compute_qwen3vl_image_embeds,
    record_vllm_native_encoder_cache_hit,
    record_vllm_precomputed_image_embeds_hit,
    trace_vllm_mm_hidden_event,
    use_mm_hidden_cache_keys,
)
from .omni_hidden_prefix_cache import (
    OmniHiddenPrefixCache,
    OmniHiddenPrefixCacheConfig,
    install_qwen2_5_omni_hidden_prefix_cache,
)
from .relay_recompute import RelayRecompute, Segment, split_segments
from .attention_similarity import AttentionSimilarityReuse, attention_similarity
from .anchor_pool import AnchorPool, Anchor

__all__ = [
    "DirectFeatureBufferAllocation",
    "DirectFeatureBufferRegistry",
    "get_direct_feature_buffer_registry",
    "iter_direct_feature_buffer_registries",
    "register_direct_feature_buffer_registry",
    "unregister_direct_feature_buffer_registry",
    "Anchor",
    "AnchorPool",
    "AttentionSimilarityReuse",
    "BlockRef",
    "FeatureBundle",
    "FeatureBundleDescriptor",
    "FeatureHandleRegistry",
    "FeatureHandleExpired",
    "FeatureHandleError",
    "FeatureHandle",
    "FeatureStore",
    "FeatureHandleProvider",
    "FeatureHandleProviderConfig",
    "TensorSpec",
    "HandoffTransaction",
    "MooncakeKVStateStore",
    "MooncakeRemoteKVMaterializer",
    "MooncakeFeatureBundleStore",
    "MooncakeFeatureBundleStoreConfig",
    "MooncakeFeatureStoreError",
    "KVStateDescriptor",
    "KVBlockDescriptor",
    "MMStore",
    "MMStoreBackpressureError",
    "MMStoreEvent",
    "MMStoreHandle",
    "MultimodalState",
    "PageId",
    "PagedKVManager",
    "OmniHiddenPrefixCache",
    "OmniHiddenPrefixCacheConfig",
    "RadixTree",
    "RelayRecompute",
    "ResolvedFeatureHandles",
    "Segment",
    "StateLayer",
    "StateMeta",
    "VLLMMMHiddenStateCache",
    "WorkflowStateRecord",
    "WorkflowStateRegistry",
    "attention_similarity",
    "get_current_mm_hidden_cache_keys",
    "get_global_mm_hidden_cache",
    "get_or_compute_qwen3vl_image_embeds",
    "install_qwen2_5_omni_hidden_prefix_cache",
    "extract_feature_handle_payloads",
    "inject_feature_handles_into_vllm_mm_kwargs",
    "maybe_inject_feature_handle_kwargs",
    "publish_feature_bundle_to_dir",
    "register_feature_handle_registry",
    "resolve_feature_handles_for_vllm",
    "record_vllm_native_encoder_cache_hit",
    "record_vllm_precomputed_image_embeds_hit",
    "trace_vllm_mm_hidden_event",
    "use_mm_hidden_cache_keys",
    "unregister_feature_handle_registry",
    "use_kv_transfer_params",
    "build_mooncake_feature_uri",
    "clear_feature_handle_bundle_cache",
    "close_default_feature_handle_provider",
    "get_default_feature_handle_provider",
    "parse_mooncake_feature_uri",
    "split_segments",
]
