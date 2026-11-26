"""Unit tests for independent Ascend NPU release tags."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


REPO = Path(__file__).resolve().parents[3]
NPU_WF = REPO / ".github/workflows/release-npu.yaml"


class ReleaseNpuWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.wf = yaml.load(NPU_WF.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        cls.align_script = next(
            s["run"]
            for s in cls.wf["jobs"]["build"]["steps"]
            if s.get("name") == "Align wheel version with release tag"
        )

    def test_align_version_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wheel_dir = Path(tmp) / "mooncake-wheel"
            wheel_dir.mkdir()
            pyproject = wheel_dir / "pyproject.toml"
            pyproject.write_text('version = "0.3.14"\n', encoding="utf-8")

            res = subprocess.run(
                ["bash", "-c", self.align_script],
                cwd=tmp,
                env={**os.environ, "RELEASE_TAG": "npu-v0.3.14.post1"},
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, res.stderr)
            self.assertEqual(
                pyproject.read_text(encoding="utf-8"), 'version = "0.3.14.post1"\n'
            )

            for bad_tag in ("npu-v0.3.15", "v0.3.14", "npu-v0.3.14-rc1"):
                bad = subprocess.run(
                    ["bash", "-c", self.align_script],
                    cwd=tmp,
                    env={**os.environ, "RELEASE_TAG": bad_tag},
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(bad.returncode, 0, bad_tag)
