"""Reshard exact bytes, mixed source layouts, reuse and invalid destinations."""

import ctypes
from dataclasses import replace

import pytest
from _store_test_utils import (
    FORMATS,
    MemoryNative,
    _manifest,
    memory_binding,
)
from mooncake.reshard.kv_cache import (
    KVCacheStoreError,
    KVCacheStoreFormat,
    KVCacheTransferLimits,
    MultipartKVCacheStore,
    MultipartReadContext,
    plan_kv_cache_store_upload,
)
from test_kv_cache_reshard import _placement


def setup_page():
    native = MemoryNative()
    source = _placement("source", ((0, 1), (2, 3)), 1)
    target = _placement("target", ((0, 1, 2, 3),), 2)
    fmt = KVCacheStoreFormat.PLHD
    plan = plan_kv_cache_store_upload(source, object_format=fmt)
    writer = MultipartKVCacheStore(native, _manifest(plan.layout))
    for part in source.parts:
        binding, keep, _ = memory_binding(source, part.participant_id, fmt, 1, "put")
        assert writer.upload(plan, ("page",), binding, operation_id="put") == (True,)
        assert keep
    store = MultipartKVCacheStore(
        native, _manifest(plan_kv_cache_store_upload(target, object_format=fmt).layout)
    )
    binding, buffers, expected = memory_binding(
        target, target.parts[0].participant_id, fmt, 1, "get", fill=False
    )
    context = MultipartReadContext(
        "get",
        writer.manifest.model_domain,
        ("page",),
        (writer.manifest.layout.layout_id,),
        (writer.manifest,),
    )
    return native, store, target, binding, buffers, expected, context


@pytest.mark.parametrize(
    "source_format,target_format,source_tp",
    [
        (KVCacheStoreFormat.PLHD, KVCacheStoreFormat.LPHD, 1),
        (KVCacheStoreFormat.LPHD, KVCacheStoreFormat.HPLD, 2),
        (KVCacheStoreFormat.HPLD, KVCacheStoreFormat.PLHD, 8),
        (KVCacheStoreFormat.PLHD, KVCacheStoreFormat.HPLD, 8),
        (KVCacheStoreFormat.HPLD, KVCacheStoreFormat.LPHD, 1),
        (KVCacheStoreFormat.LPHD, KVCacheStoreFormat.PLHD, 2),
    ],
)
def test_cross_layout_restores_exact_bytes(source_format, target_format, source_tp):
    native = MemoryNative()
    source = _placement("source", ((0,), (1, 2), (3,)), source_tp)
    target = _placement("target", ((0, 1), (2, 3)), 2)
    plan = plan_kv_cache_store_upload(source, object_format=source_format)
    writer = MultipartKVCacheStore(native, _manifest(plan.layout))
    for part in source.parts:
        if part.participant_id not in plan.part_writers:
            continue
        binding, keep, _ = memory_binding(
            source, part.participant_id, source_format, 1, "put"
        )
        assert writer.upload(plan, ("page",), binding, operation_id="put") == (True,)
        assert keep
    reader = MultipartKVCacheStore(
        native,
        _manifest(
            plan_kv_cache_store_upload(target, object_format=target_format).layout
        ),
    )
    reader.transfer_limits = KVCacheTransferLimits(
        max_batch_operations=7, max_batch_bytes=173
    )
    context = MultipartReadContext(
        "get",
        writer.manifest.model_domain,
        ("page",),
        (writer.manifest.layout.layout_id,),
        (writer.manifest,),
    )
    assert context.page_keys == ("page",)
    assert context.manifests[0].layout.object_format == source_format
    for part in target.parts:
        binding, buffers, expected = memory_binding(
            target, part.participant_id, target_format, 1, "get", fill=False
        )
        page_reader = reader.prepare_page_reader(target, binding)
        assert (
            page_reader.load(context, [{r.region_id: 0 for r in binding.regions}]) == 1
        )
        assert [bytes(b) for b in buffers] == expected


def test_mixed_layout_pages_bind_independent_manifest_references():
    native = MemoryNative()
    placement = _placement("source", ((0, 1), (2, 3)), 2)
    pages = tuple("page-" + fmt.value for fmt in FORMATS)
    manifests = []
    for fmt, page in zip(FORMATS, pages):
        plan = plan_kv_cache_store_upload(placement, object_format=fmt)
        writer = MultipartKVCacheStore(native, _manifest(plan.layout))
        manifests.append(writer.manifest)
        for part in placement.parts:
            binding, keep, _ = memory_binding(
                placement, part.participant_id, fmt, 1, "put", page_salts=(0,)
            )
            assert writer.upload(plan, (page,), binding, operation_id="put") == (True,)
            assert keep
    for fmt in FORMATS:
        reader = MultipartKVCacheStore(
            native,
            _manifest(plan_kv_cache_store_upload(placement, object_format=fmt).layout),
        )
        context = MultipartReadContext(
            "get",
            reader.manifest.model_domain,
            pages,
            tuple(m.layout.layout_id for m in manifests),
            tuple(manifests),
        )
        assert len(set(context.layout_ids)) == 3
        for part in placement.parts:
            binding, buffers, expected = memory_binding(
                placement,
                part.participant_id,
                fmt,
                1,
                "get",
                fill=False,
                page_salts=(0,),
            )
            page_reader = reader.prepare_page_reader(placement, binding)
            for page, layout_id in zip(context.page_keys, context.layout_ids):
                selected = replace(
                    context,
                    page_keys=(page,),
                    layout_ids=(layout_id,),
                    manifests=tuple(
                        m for m in context.manifests if m.layout.layout_id == layout_id
                    ),
                )
                assert (
                    page_reader.load(
                        selected, [{r.region_id: 0 for r in binding.regions}]
                    )
                    == 1
                )
                assert [bytes(b) for b in buffers] == expected
            assert len(page_reader._templates) == 3


def test_geometry_cache_is_local_and_immutable(monkeypatch):
    _native, store, target, binding, buffers, expected, context = setup_page()
    assert context.page_keys == ("page",)
    reader = store.prepare_page_reader(target, binding)
    offsets = [{key: 0 for key in reader.regions}]
    assert reader.load(context, offsets) == 1
    assert [bytes(b) for b in buffers] == expected

    def forbidden(*args):
        raise AssertionError("cached template should not be rebuilt")

    monkeypatch.setattr(reader, "_compile", forbidden)
    assert reader.load(replace(context, operation_id="again"), offsets) == 1


def test_reader_honors_range_limits_and_cancellation():
    native, store, target, binding, buffers, expected, context = setup_page()
    store.transfer_limits = KVCacheTransferLimits(
        max_batch_operations=2, max_batch_bytes=137
    )
    reader = store.prepare_page_reader(target, binding)
    offsets = [{key: 0 for key in reader.regions}]
    assert reader.load(context, offsets, cancelled=lambda: True) == 0
    assert native.range_reads == 0
    assert reader.load(context, offsets) == 1
    assert native.range_reads > 1
    assert [bytes(b) for b in buffers] == expected
    with pytest.raises(ValueError, match="regions"):
        reader.load(context, [{}])
    with pytest.raises(ValueError, match="bounds"):
        reader.load(context, [{key: 1 << 40 for key in reader.regions}])
    native.get_into_ranges_from_template = lambda *a, **kw: []
    with pytest.raises(KVCacheStoreError, match="shape"):
        reader.load(context, offsets)


def test_read_context_is_immutable_and_validates_supplied_sources():
    _, _, _, _, _, _, context = setup_page()
    keys, ids, manifests = (
        list(context.page_keys),
        list(context.layout_ids),
        list(context.manifests),
    )
    copied = MultipartReadContext("copy", context.model_domain, keys, ids, manifests)
    keys.clear()
    ids.clear()
    manifests.clear()
    assert copied.page_keys == context.page_keys
    assert copied.layout_ids == context.layout_ids
    assert copied.manifests == context.manifests
    for changes in (
        {"layout_ids": ()},
        {"manifests": ()},
        {"manifests": context.manifests * 2},
        {"page_keys": ("",)},
        {"model_domain": "0" * 64},
    ):
        with pytest.raises(ValueError):
            replace(context, **changes)


def test_unsupported_backend_fails_without_importing_native_store():
    with pytest.raises(KVCacheStoreError, match="batch_put_parts_from"):
        MultipartKVCacheStore(object(), _manifest())


@pytest.mark.parametrize("reply", [[], [True], ["0"]])
def test_upload_rejects_malformed_statuses(reply, monkeypatch):
    native = MemoryNative()
    source = _placement("source", ((0, 1), (2, 3)), 1)
    plan = plan_kv_cache_store_upload(source, object_format=KVCacheStoreFormat.PLHD)
    writer = MultipartKVCacheStore(native, _manifest(plan.layout))
    binding, keep, _ = memory_binding(
        source, source.parts[0].participant_id, KVCacheStoreFormat.PLHD, 1, "put"
    )
    monkeypatch.setattr(native, "batch_put_parts_from", lambda *args: reply)
    with pytest.raises(KVCacheStoreError, match="upload result shape"):
        writer.upload(plan, ("page",), binding, operation_id="put")
    assert keep


def test_cancelled_or_empty_upload_does_not_submit(monkeypatch):
    native = MemoryNative()
    source = _placement("source", ((0, 1), (2, 3)), 1)
    plan = plan_kv_cache_store_upload(source, object_format=KVCacheStoreFormat.PLHD)
    writer = MultipartKVCacheStore(native, _manifest(plan.layout))
    binding, keep, _ = memory_binding(
        source, source.parts[0].participant_id, KVCacheStoreFormat.PLHD, 1, "put"
    )

    def forbidden(*args):
        raise AssertionError("cancelled or empty upload must not call the backend")

    monkeypatch.setattr(native, "batch_put_parts_from", forbidden)
    assert writer.upload(plan, (), binding, operation_id="put") == ()
    assert writer.upload(
        plan, ("page",), binding, operation_id="put", cancelled=lambda: True
    ) == (False,)
    assert keep


def test_read_cache_never_reuses_a_previous_snapshot(monkeypatch):
    native, store, target, binding, buffers, expected, context = setup_page()
    reader = store.prepare_page_reader(target, binding)
    offsets = [{r.region_id: 0 for r in binding.regions}]
    assert reader.load(context, offsets) == 1
    assert [bytes(b) for b in buffers] == expected
    for b in buffers:
        ctypes.memset(ctypes.addressof(b), 0, len(b))

    def forbidden(*args):
        raise AssertionError("geometry must remain cached")

    monkeypatch.setattr(reader, "_compile", forbidden)
    _, parts = native.parts["page"]
    native.parts["page"] = "different-manifest", parts
    assert reader.load(context, offsets) == 0
    assert all(not any(bytes(b)) for b in buffers)


@pytest.mark.parametrize("missing", [0, 1, 2])
def test_read_reports_only_the_complete_page_prefix(missing):
    native, store, target, binding, _, expected, context = setup_page()
    pages = ("first", "second", "third")
    for key in pages:
        native.parts[key] = native.parts["page"]
    del native.parts[pages[missing]]
    buffers = [ctypes.create_string_buffer(3 * r.nbytes) for r in binding.regions]
    enlarged = replace(
        binding,
        regions=tuple(
            replace(r, address=ctypes.addressof(b), nbytes=3 * r.nbytes)
            for r, b in zip(binding.regions, buffers)
        ),
    )
    reader = store.prepare_page_reader(target, enlarged)
    context = replace(context, page_keys=pages, layout_ids=context.layout_ids * 3)
    offsets = [{r.region_id: i * r.nbytes for r in binding.regions} for i in range(3)]
    assert reader.load(context, offsets) == missing
    for r, buffer, data in zip(binding.regions, buffers, expected):
        for i in range(missing):
            assert bytes(buffer)[i * r.nbytes : (i + 1) * r.nbytes] == data


def test_cancellation_between_batches_stops_further_reads(monkeypatch):
    native, store, target, binding, buffers, _, context = setup_page()
    store.transfer_limits = KVCacheTransferLimits(
        max_batch_operations=1, max_batch_bytes=17
    )
    reader = store.prepare_page_reader(target, binding)
    original = native.get_into_ranges_from_template
    cancelled = False

    def read(*args, **kwargs):
        nonlocal cancelled
        result = original(*args, **kwargs)
        cancelled = True
        return result

    monkeypatch.setattr(native, "get_into_ranges_from_template", read)
    assert (
        reader.load(
            context,
            [{r.region_id: 0 for r in binding.regions}],
            cancelled=lambda: cancelled,
        )
        == 0
    )
    assert native.range_reads == 1
    assert buffers


def test_read_rejects_non_boolean_completion(monkeypatch):
    native, store, target, binding, _, _, context = setup_page()
    reader = store.prepare_page_reader(target, binding)
    monkeypatch.setattr(
        native,
        "get_into_ranges_from_template",
        lambda snapshot, templates, *args: [1] * len(templates),
    )
    with pytest.raises(KVCacheStoreError, match="read result shape"):
        reader.load(context, [{r.region_id: 0 for r in binding.regions}])
