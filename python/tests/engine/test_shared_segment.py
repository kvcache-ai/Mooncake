from __future__ import annotations

import builtins
import gc
import importlib.util
from pathlib import Path
import sys
import types
from typing import Any
import weakref

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = REPOSITORY_ROOT / "python" / "mooncake"
MODULE_PATH = PACKAGE_ROOT / "shared_segment.py"


class _FakeDType:
    def __init__(self, name: str = "torch.float16", element_size: int = 2) -> None:
        self.name = name
        self._element_size = element_size

    def __str__(self) -> str:
        return self.name


class _FakeEmptyTensor:
    def __init__(self, dtype: _FakeDType) -> None:
        self._dtype = dtype

    def element_size(self) -> int:
        return self._dtype._element_size


class _NativeSegment:
    def __init__(self, harness: types.SimpleNamespace, size: int) -> None:
        self._harness = harness
        self._size = size

    def __del__(self) -> None:
        self._harness.release_count += 1

    def complete(self, blobs: list[bytes]) -> None:
        self._harness.completed_blobs.append(blobs)
        if self._harness.complete_error:
            raise RuntimeError(self._harness.complete_error)

    def base_addr(self) -> int:
        return 0x1000

    def device_addr(self) -> int:
        return 0

    def size(self) -> int:
        return self._size


def _fake_torch_modules() -> tuple[types.ModuleType, types.ModuleType]:
    distributed = types.ModuleType("torch.distributed")
    distributed.all_gather = lambda *args, **kwargs: None
    distributed.all_gather_object = lambda *args, **kwargs: None

    torch = types.ModuleType("torch")
    torch.__path__ = []
    torch.Tensor = object
    torch.dtype = _FakeDType
    torch.uint8 = _FakeDType("torch.uint8", 1)
    torch.distributed = distributed
    torch.empty = lambda size, *, dtype, **kwargs: _FakeEmptyTensor(dtype)
    return torch, distributed


def _load_module(
    monkeypatch: pytest.MonkeyPatch, *, with_torch: bool = True
) -> tuple[types.ModuleType, types.SimpleNamespace]:
    harness = types.SimpleNamespace(
        complete_error="",
        completed_blobs=[],
        create_calls=[],
        created_ref=None,
        release_count=0,
        supported_calls=[],
        supported_result=True,
    )

    class NativeBinding:
        @staticmethod
        def supported(mmap: bool, host_register: bool) -> bool:
            harness.supported_calls.append((mmap, host_register))
            return harness.supported_result

        @staticmethod
        def create(*args: Any) -> tuple[_NativeSegment, bytes]:
            harness.create_calls.append(args)
            segment = _NativeSegment(harness, args[1])
            harness.created_ref = weakref.ref(segment)
            return segment, b"owner-blob"

    package = types.ModuleType("mooncake")
    package.__path__ = [str(PACKAGE_ROOT)]
    engine = types.ModuleType("mooncake.engine")
    engine.SharedSegment = NativeBinding
    monkeypatch.setitem(sys.modules, "mooncake", package)
    monkeypatch.setitem(sys.modules, "mooncake.engine", engine)
    monkeypatch.delitem(sys.modules, "mooncake.shared_segment", raising=False)

    torch_modules = [
        name for name in sys.modules if name == "torch" or name.startswith("torch.")
    ]
    for name in torch_modules:
        monkeypatch.delitem(sys.modules, name)

    if with_torch:
        torch, distributed = _fake_torch_modules()
        monkeypatch.setitem(sys.modules, "torch", torch)
        monkeypatch.setitem(sys.modules, "torch.distributed", distributed)

    spec = importlib.util.spec_from_file_location(
        "mooncake.shared_segment", MODULE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)

    if with_torch:
        spec.loader.exec_module(module)
    else:
        original_import = builtins.__import__

        def import_without_torch(
            name: str,
            globals: dict[str, Any] | None = None,
            locals: dict[str, Any] | None = None,
            fromlist: tuple[str, ...] = (),
            level: int = 0,
        ) -> Any:
            if name == "torch" or name.startswith("torch."):
                raise ModuleNotFoundError("No module named 'torch'", name="torch")
            return original_import(name, globals, locals, fromlist, level)

        with monkeypatch.context() as context:
            context.setattr(builtins, "__import__", import_without_torch)
            spec.loader.exec_module(module)

    return module, harness


def _blocks() -> dict[str, dict[str, Any]]:
    dtype = _FakeDType()
    return {
        "z": {"count": 3, "shape": (4,), "dtype": dtype},
        "a": {"count": 2, "shape": (2, 3), "dtype": dtype},
    }


def test_create_shared_segment_builds_sorted_layout_and_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, harness = _load_module(monkeypatch)

    segment = module.create_shared_segment(
        "cache",
        _blocks(),
        world_size=1,
        rank_id=0,
        device_id=7,
    )

    assert harness.create_calls == [
        (
            "cache|a:2:2x3:torch.float16:12;z:3:4:torch.float16:8",
            24_576,
            1,
            0,
            0,
            7,
            True,
            False,
        )
    ]
    assert harness.completed_blobs == [[b"owner-blob"]]
    assert segment.base_addr() == 0x1000
    assert segment.total_size() == 24_576


@pytest.mark.parametrize(
    ("mmap", "host_register", "message"),
    [
        (False, False, "no VMM backend"),
        (True, True, "cannot HostRegister"),
        (True, False, "no mmap shared-segment backend"),
    ],
)
def test_create_shared_segment_reports_missing_native_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    mmap: bool,
    host_register: bool,
    message: str,
) -> None:
    module, harness = _load_module(monkeypatch)
    harness.supported_result = False

    with pytest.raises(module.SharedSegmentError, match=message):
        module.create_shared_segment(
            "cache",
            _blocks(),
            world_size=1,
            rank_id=0,
            mmap=mmap,
            host_register=host_register,
        )

    assert harness.supported_calls == [(mmap, host_register)]
    assert not harness.create_calls


def test_host_registration_requires_mmap(monkeypatch: pytest.MonkeyPatch) -> None:
    module, harness = _load_module(monkeypatch)

    with pytest.raises(module.SharedSegmentError, match="requires mmap=True"):
        module.create_shared_segment(
            "cache",
            _blocks(),
            world_size=1,
            rank_id=0,
            mmap=False,
            host_register=True,
        )

    assert not harness.supported_calls


def test_failed_completion_releases_native_segment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, harness = _load_module(monkeypatch)
    harness.complete_error = "mapping failed"

    def create() -> None:
        with pytest.raises(module.SharedSegmentError, match="mapping failed"):
            module.create_shared_segment(
                "cache", _blocks(), world_size=1, rank_id=0, device_id=0
            )

    create()
    gc.collect()

    assert harness.created_ref() is None
    assert harness.release_count == 1


def test_capability_check_does_not_require_optional_torch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, harness = _load_module(monkeypatch, with_torch=False)

    assert module.shared_segment_supported(mmap=True, host_register=False)
    with pytest.raises(module.SharedSegmentError, match=r"\[hardware\]"):
        module.create_shared_segment("cache", {}, world_size=1, rank_id=0, device_id=0)

    assert harness.supported_calls == [(True, False)]
    assert not harness.create_calls
