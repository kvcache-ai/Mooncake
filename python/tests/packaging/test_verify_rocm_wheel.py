"""Exercise ROCm wheel provenance checks without loading native libraries."""

import importlib.util
from importlib.machinery import EXTENSION_SUFFIXES
from importlib.metadata import PackagePath
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "verify_rocm_wheel", ROOT / "scripts/tone_tests/python/verify_rocm_wheel.py"
)
verify = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify)


@pytest.fixture
def wheel_install(tmp_path, monkeypatch):
    def install(suffix):
        package_dir = tmp_path / "mooncake"
        package_dir.mkdir()
        modules = {
            "mooncake": SimpleNamespace(__file__=str(package_dir / "__init__.py"))
        }
        records = []
        for name in (f"engine{suffix}", f"store{suffix}", "mooncake_master"):
            path = package_dir / name
            path.write_bytes(b"native payload")
            record = PackagePath(f"mooncake/{name}")
            record.hash = SimpleNamespace(
                mode="sha256", value=verify.record_digest(path, "sha256")
            )
            records.append(record)
            if name != "mooncake_master":
                modules[f"mooncake.{name.split('.')[0]}"] = SimpleNamespace(
                    __file__=str(path)
                )
        distribution = SimpleNamespace(
            files=records, version="0.0.0", locate_file=lambda record: tmp_path / record
        )
        monkeypatch.setattr(verify.metadata, "distribution", lambda name: distribution)
        monkeypatch.setattr(verify.importlib, "import_module", modules.__getitem__)
        return records, modules

    return install


@pytest.mark.parametrize("suffix", list(dict.fromkeys([".so", *EXTENSION_SUFFIXES])))
def test_accepts_native_extension_names(wheel_install, suffix):
    wheel_install(suffix)
    verify.main()


@pytest.mark.parametrize(
    "failure", ["missing", "unhashed", "tampered", "wrong_module", "ambiguous"]
)
def test_rejects_invalid_wheel_provenance(wheel_install, failure):
    records, modules = wheel_install(EXTENSION_SUFFIXES[0])
    if failure == "missing":
        records.pop(0)
        message = "Expected exactly one wheel record"
    elif failure == "unhashed":
        records[0].hash = None
        message = "Missing hashed wheel record"
    elif failure == "tampered":
        Path(modules["mooncake.engine"].__file__).write_bytes(b"changed")
        message = "Installed file does not match wheel"
    elif failure == "wrong_module":
        modules["mooncake.engine"].__file__ += ".other"
        message = "loaded from an unexpected path"
    else:
        other_suffix = next(
            suffix for suffix in EXTENSION_SUFFIXES if suffix != EXTENSION_SUFFIXES[0]
        )
        records.append(PackagePath(f"mooncake/engine{other_suffix}"))
        message = "Expected exactly one wheel record"
    with pytest.raises(RuntimeError, match=message):
        verify.main()
