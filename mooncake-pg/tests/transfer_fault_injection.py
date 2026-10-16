"""Test-only transfer fault injection for Mooncake PG workers."""

import ctypes
import os
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from pathlib import Path

from pg_test_utils import temporary_env

RankPair = tuple[int, int]


def _build_preload(output: Path) -> None:
    source = Path(__file__).with_name("transfer_fault_preload.cpp")
    repository_root = Path(__file__).resolve().parents[2]
    transfer_engine_include = (
        repository_root / "mooncake-transfer-engine" / "include"
    )
    process_group_include = repository_root / "mooncake-pg" / "include"
    command = [
        os.environ.get("CXX", "c++"),
        "-std=c++20",
        "-O2",
        "-shared",
        "-fPIC",
        f"-I{transfer_engine_include}",
        f"-I{process_group_include}",
        str(source),
        "-ldl",
        "-o",
        str(output),
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except OSError as error:
        raise unittest.SkipTest(
            f"transfer fault injection cannot invoke the compiler: {error}"
        ) from error
    except subprocess.CalledProcessError as error:
        reason = (error.stderr or "").strip()
        raise unittest.SkipTest(
            "transfer fault injection cannot build its preload shim: "
            f"{reason or error}"
        ) from error


def _verify_preload(library: Path, preload: str) -> None:
    """Verify LD_PRELOAD and symbol resolution in a fresh process."""
    probe = """
import ctypes
import sys

from mooncake import pg

library = ctypes.CDLL(sys.argv[1])
available = library.mooncakePgTestFaultAvailable
available.argtypes = []
available.restype = ctypes.c_int
if not available():
    print("required fault-injection symbols not found", file=sys.stderr)
    raise SystemExit(1)
clear_targets = library.mooncakePgTestClearFailedTargets
clear_targets.argtypes = []
clear_targets.restype = None
add_target = library.mooncakePgTestAddFailedTarget
add_target.argtypes = [ctypes.c_int]
add_target.restype = None
setter = library.mooncakePgTestSetFaultEnabled
setter.argtypes = [ctypes.c_int]
setter.restype = None
reset_counter = library.mooncakePgTestResetFailureCount
reset_counter.argtypes = []
reset_counter.restype = None
counter = library.mooncakePgTestGetFailureCount
counter.argtypes = []
counter.restype = ctypes.c_uint64
setter(0)
clear_targets()
add_target(1)
reset_counter()
setter(1)
setter(0)
clear_targets()
counter()
"""
    environment = os.environ.copy()
    environment["LD_PRELOAD"] = preload
    try:
        result = subprocess.run(
            [sys.executable, "-c", probe, str(library)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30.0,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise unittest.SkipTest(
            f"transfer fault injection is unavailable: {error}"
        ) from error

    if result.returncode != 0:
        reason = result.stderr.strip() or result.stdout.strip()
        if not reason:
            reason = f"exit code {result.returncode}"
        raise unittest.SkipTest(
            f"transfer fault injection is unavailable: {reason}"
        )


@contextmanager
def preload_transfer_fault() -> Generator[Path, None, None]:
    """Preload the transfer fault shim into spawned workers."""
    if not sys.platform.startswith("linux"):
        raise unittest.SkipTest("LD_PRELOAD fault injection requires Linux")

    with tempfile.TemporaryDirectory(prefix="mooncake-pg-fault-") as temp_dir:
        library = Path(temp_dir) / "libmooncake_pg_fault.so"
        _build_preload(library)

        preload_entries = [str(library)]
        if previous_preload := os.environ.get("LD_PRELOAD"):
            preload_entries.append(previous_preload)

        preload = os.pathsep.join(preload_entries)
        _verify_preload(library, preload)

        with temporary_env({"LD_PRELOAD": preload}):
            yield library


class TransferFault:
    """Coordinate directed link failures across worker processes."""

    def __init__(self, library: str | Path, *, local_rank: int) -> None:
        self._library = ctypes.CDLL(str(library))
        is_available = self._library.mooncakePgTestFaultAvailable
        is_available.argtypes = []
        is_available.restype = ctypes.c_int
        if not is_available():
            raise RuntimeError(
                "preloaded transfer fault shim cannot resolve its "
                "required symbols"
            )
        self._clear_failed_targets = (
            self._library.mooncakePgTestClearFailedTargets
        )
        self._clear_failed_targets.argtypes = []
        self._clear_failed_targets.restype = None
        self._add_failed_target = self._library.mooncakePgTestAddFailedTarget
        self._add_failed_target.argtypes = [ctypes.c_int]
        self._add_failed_target.restype = None
        self._set_enabled = self._library.mooncakePgTestSetFaultEnabled
        self._set_enabled.argtypes = [ctypes.c_int]
        self._set_enabled.restype = None
        self._reset_count = self._library.mooncakePgTestResetFailureCount
        self._reset_count.argtypes = []
        self._reset_count.restype = None
        self._get_count = self._library.mooncakePgTestGetFailureCount
        self._get_count.argtypes = []
        self._get_count.restype = ctypes.c_uint64
        if local_rank < 0:
            raise ValueError("local rank must be non-negative")
        self._local_rank = local_rank
        self._set_enabled(0)
        self._clear_failed_targets()
        self._reset_count()
        self._active = False

    @staticmethod
    def _normalize_links(
        links: Iterable[RankPair],
    ) -> tuple[RankPair, ...]:
        normalized = tuple(dict.fromkeys(links))
        if not normalized:
            raise ValueError("at least one failed link is required")
        for source_rank, target_rank in normalized:
            if (
                source_rank < 0
                or target_rank < 0
                or source_rank == target_rank
            ):
                raise ValueError(
                    "failed links require distinct, non-negative ranks"
                )
        return normalized

    @contextmanager
    def failing_links(
        self, links: Iterable[RankPair]
    ) -> Generator[None, None, None]:
        """Fail directed links whose source is this worker's rank."""
        if self._active:
            raise RuntimeError("fault scopes cannot be nested")
        normalized = self._normalize_links(links)
        local_targets = {
            target_rank
            for source_rank, target_rank in normalized
            if source_rank == self._local_rank
        }

        self._set_enabled(0)
        self._clear_failed_targets()
        for target_rank in local_targets:
            self._add_failed_target(target_rank)
        self._reset_count()
        self._set_enabled(bool(local_targets))
        self._active = True
        try:
            yield
        finally:
            self._set_enabled(0)
            self._clear_failed_targets()
            self._active = False

    @contextmanager
    def failing_link(
        self, source_rank: int, target_rank: int
    ) -> Generator[None, None, None]:
        """Fail one directed global-rank link within this scope."""
        with self.failing_links(((source_rank, target_rank),)):
            yield

    @property
    def injected_count(self) -> int:
        return int(self._get_count())
