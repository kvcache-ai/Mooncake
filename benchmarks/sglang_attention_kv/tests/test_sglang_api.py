# The SGLang API surface this benchmark calls, checked without a GPU: the
# allocators, the KV pool, the token table and the index kernel. A run on a
# machine whose SGLang moved one of these fails in `prepare()` on every case, so
# the surface is asserted here where it is cheap to see.

import inspect

import pytest

allocator_module = pytest.importorskip("sglang.srt.mem_cache.allocator")
memory_pool = pytest.importorskip("sglang.srt.mem_cache.memory_pool")
kv_indices = pytest.importorskip("sglang.kernels.ops.kvcache.kv_indices")

# What the replay calls on each class, in the order a run reaches them.
REQUIRED = {
    "PagedTokenToKVPoolAllocator": (
        "alloc",
        "alloc_extend",
        "alloc_decode",
        "free",
        # The random layout hands the pool's spare pages back through this, and it
        # is the one call a paged allocator has and a token allocator spells the
        # same way, so a version without it fails every random case.
        "free_page_ids",
        "clear",
    ),
    "TokenToKVPoolAllocator": ("alloc", "free", "free_page_ids", "clear"),
}


def test_the_allocators_have_the_methods_the_replay_calls():
    for name, methods in REQUIRED.items():
        cls = getattr(allocator_module, name, None)
        assert cls is not None, f"sglang.srt.mem_cache.allocator has no {name}"
        missing = [method for method in methods if not hasattr(cls, method)]
        assert not missing, f"{name} is missing {missing}"


def test_the_pool_has_the_constructor_and_writer_the_replay_calls():
    pool = getattr(memory_pool, "MHATokenToKVPool", None)
    assert pool is not None, "sglang.srt.mem_cache.memory_pool has no MHATokenToKVPool"
    for method in (
        "set_kv_buffer",
        "get_kv_buffer",
        "get_key_buffer",
        "get_value_buffer",
    ):
        assert hasattr(pool, method), f"MHATokenToKVPool is missing {method}"
    parameters = inspect.signature(pool.__init__).parameters
    for name in (
        "size",
        "page_size",
        "head_num",
        "head_dim",
        "layer_num",
        "enable_alt_stream",
    ):
        assert name in parameters, f"MHATokenToKVPool.__init__ has no {name}"


def test_the_token_table_has_the_columns_the_replay_writes():
    pool = getattr(memory_pool, "ReqToTokenPool", None)
    assert pool is not None, "sglang.srt.mem_cache.memory_pool has no ReqToTokenPool"
    for method in ("write", "free"):
        assert hasattr(pool, method), f"ReqToTokenPool is missing {method}"


def test_the_index_kernel_is_importable():
    kernel = getattr(kv_indices, "create_flashinfer_kv_indices_triton", None)
    assert kernel is not None, "the kv_indices module has no index kernel"
