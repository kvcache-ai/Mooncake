"""Opt-in redirect of the ``mooncake.*`` public API onto this backend.

Installed through a ``.pth`` file so the redirect is registered before user
code imports ``mooncake.store``.  That indirection is what lets the upstream
``mooncake`` wheel stay byte-for-byte unmodified: its C++ extension keeps
owning ``mooncake/store.so`` on disk, and this finder only shadows it in
``sys.meta_path``, and only when the operator opts in.

A plain environment variable is not enough on its own.  CPython registers
``ExtensionFileLoader`` ahead of ``SourceFileLoader``, so a co-installed
``mooncake/store.py`` would always lose to ``mooncake/store.so``.  A
``sys.meta_path`` entry is consulted before either.

Two constraints on anything added here:

* ``.pth`` files execute on every interpreter start, so this module must stay
  cheap and must never import the native extension.  ``mooncake_store_rs``
  itself is lazy for the same reason.
* A failure here would break unrelated interpreters, so installation is
  wrapped in a catch-all at import time.
"""

from __future__ import annotations

import importlib
import os
import sys
import types
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec, PathFinder

BACKEND_ENV = "MOONCAKE_STORE_BACKEND"
_ENABLED_VALUES = frozenset({"rs", "rust", "store-rs", "store_rs", "masterless"})

# Names this package implements or vendors, mapped onto their location here.
#
# The first group is where the two implementations genuinely diverge -- notably
# ``structured_object_store``, which is a reduced variant rather than a superset
# -- so they are redirected as a set: serving one half from upstream and the
# other from here does not work.
#
# The second group is upstream code that the wheel vendors verbatim (built from
# the same commit). Redirecting it is what lets this wheel stand alone: without
# it, ``from mooncake.engine import TransferEngine`` -- which SGLang does --
# would require the upstream wheel to be installed as well.
_REDIRECTS = {
    "mooncake.store": "mooncake_store_rs.store",
    "mooncake.buffer_pool": "mooncake_store_rs.buffer_pool",
    "mooncake.cli": "mooncake_store_rs.cli",
    "mooncake.cli_client": "mooncake_store_rs.cli_client",
    "mooncake.structured_object_store": "mooncake_store_rs.structured_object_store",
    "mooncake.engine": "mooncake_store_rs.engine",
    "mooncake.ep": "mooncake_store_rs.ep",
    "mooncake.pg": "mooncake_store_rs.pg",
    "mooncake.mooncake_config": "mooncake_store_rs.mooncake_config",
    "mooncake.mooncake_connector_v1": "mooncake_store_rs.mooncake_connector_v1",
    "mooncake.mooncake_ep_buffer": "mooncake_store_rs.mooncake_ep_buffer",
    "mooncake.mooncake_store_service": "mooncake_store_rs.mooncake_store_service",
    "mooncake.http_metadata_server": "mooncake_store_rs.http_metadata_server",
    "mooncake.transfer_engine_topology_dump": (
        "mooncake_store_rs.transfer_engine_topology_dump"
    ),
    "mooncake.vllm_v1_proxy_server": "mooncake_store_rs.vllm_v1_proxy_server",
}


def backend_selected() -> bool:
    """Whether ``MOONCAKE_STORE_BACKEND`` asks for this backend."""
    return os.environ.get(BACKEND_ENV, "").strip().lower() in _ENABLED_VALUES


class _AliasLoader(Loader):
    """Bind an already-importable module under a second name."""

    def __init__(self, target: str) -> None:
        self._target = target

    def create_module(self, spec: ModuleSpec) -> types.ModuleType | None:
        return None

    def exec_module(self, module: types.ModuleType) -> None:
        # Swap the placeholder for the real module so both names share one
        # instance -- and therefore one copy of module-level state.  Replacing
        # the entry here rather than returning the target from `create_module`
        # keeps the target's own `__spec__`/`__name__` untouched;
        # `_bootstrap._load` re-reads `sys.modules` after `exec_module`, so the
        # substitution is what the importer hands back.
        sys.modules[module.__name__] = importlib.import_module(self._target)


class StoreRsFinder(MetaPathFinder):
    """Resolve redirected ``mooncake.*`` names to this backend."""

    def find_spec(
        self,
        fullname: str,
        path: object = None,
        target: types.ModuleType | None = None,
    ) -> ModuleSpec | None:
        redirect = _REDIRECTS.get(fullname)
        if redirect is not None:
            return ModuleSpec(fullname, _AliasLoader(redirect), origin=redirect)
        if fullname == "mooncake":
            return _synthesise_parent()
        return None


def _synthesise_parent() -> ModuleSpec | None:
    """Provide a bare ``mooncake`` package when the upstream wheel is absent.

    Without this, ``from mooncake.store import ...`` fails while resolving the
    parent package for anyone who installed only this backend.  When the
    upstream wheel *is* present its package must win, so `PathFinder` is
    consulted directly -- going through `importlib.util.find_spec` would
    re-enter this finder.
    """
    if PathFinder.find_spec("mooncake") is not None:
        return None
    spec = ModuleSpec("mooncake", None, is_package=True)
    spec.submodule_search_locations = []
    return spec


def install() -> bool:
    """Register the finder if this backend is selected. Idempotent."""
    if not backend_selected():
        return False
    if any(isinstance(finder, StoreRsFinder) for finder in sys.meta_path):
        return True
    sys.meta_path.insert(0, StoreRsFinder())
    return True


def uninstall() -> None:
    """Drop the finder again. Intended for tests."""
    sys.meta_path[:] = [
        finder for finder in sys.meta_path if not isinstance(finder, StoreRsFinder)
    ]


def active_backend() -> str:
    """Report the backend a fresh ``import mooncake.store`` would resolve to."""
    if any(isinstance(finder, StoreRsFinder) for finder in sys.meta_path):
        return "store-rs"
    return "upstream"


try:
    install()
except Exception:
    # A .pth failure would surface on every interpreter start, including ones
    # that never touch Mooncake. Staying inert is the safer failure mode.
    pass
