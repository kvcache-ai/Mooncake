from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "mooncake"


class FakeRaw:
    """Stands in for ``mooncake._conductor.ConductorClient`` (the Task 9
    pybind class); records close() so the context-manager test can observe it.
    """

    def __init__(self) -> None:
        self.closed = False

    def setup(self, addr, connect_timeout_ms=1000, request_timeout_ms=3000):
        return 0

    def close(self):
        self.closed = True
        return 0

    def health_check(self):
        return 0

    def query(self, **kw):
        return {"ret": 0, "hits": {"i1": {"longest_matched": 4}}}

    def list_services(self):
        return {"ret": 0, "count": 0, "services": []}


@pytest.fixture
def conductor_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import ``mooncake.conductor`` from source against a fake ``_conductor``.

    Same pattern as test_cli_modules.py: a stub ``mooncake`` package backed by
    ``python/mooncake`` is injected into ``sys.modules`` so importing does not
    run the real ``__init__.py`` (which imports ``mooncake.buffer_pool``, absent
    in the source tree). The fake ``mooncake._conductor`` is injected before the
    import so ``from mooncake import _conductor`` inside conductor.py binds it.
    """
    pkg = ModuleType("mooncake")
    pkg.__path__ = [str(PACKAGE_ROOT)]
    pkg.__package__ = "mooncake"
    monkeypatch.setitem(sys.modules, "mooncake", pkg)

    fake = ModuleType("mooncake._conductor")
    fake.ConductorClient = FakeRaw
    monkeypatch.setitem(sys.modules, "mooncake._conductor", fake)

    # Re-import per test so the module under test binds this test's fake even
    # if a previous test (or an installed package) already cached it.
    monkeypatch.delitem(sys.modules, "mooncake.conductor", raising=False)
    import mooncake.conductor

    return mooncake.conductor


def test_query_returns_hits_on_success(conductor_module: ModuleType) -> None:
    ConductorClient = conductor_module.ConductorClient
    c = ConductorClient()
    assert c.setup("127.0.0.1:13334") == 0
    result = c.query(model_name="m", block_size=16, token_ids=[1, 2])
    assert result["ret"] == 0
    assert result["hits"]["i1"]["longest_matched"] == 4


def test_context_manager_closes(conductor_module: ModuleType) -> None:
    ConductorClient = conductor_module.ConductorClient
    with ConductorClient() as c:
        c.setup("127.0.0.1:13334")
    assert c._raw.closed
