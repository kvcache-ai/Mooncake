from pathlib import Path
import unittest

import yaml


REPOSITORY = Path(__file__).resolve().parents[3]
WORKFLOWS = REPOSITORY / ".github" / "workflows"


def load_workflow(name):
    with (WORKFLOWS / name).open() as workflow_file:
        return yaml.safe_load(workflow_file)


def step_named(job, name):
    return next(step for step in job["steps"] if step.get("name") == name)


def step_index(job, name):
    return next(
        index for index, step in enumerate(job["steps"]) if step.get("name") == name
    )


class NightlyRocmPublishTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nightly = load_workflow("nightly.yml")
        cls.rocm = load_workflow("ci_rocm.yml")

    def test_rocm_builder_accepts_and_applies_version_override(self):
        triggers = self.rocm.get("on", self.rocm.get(True))
        version_input = triggers["workflow_call"]["inputs"]["version_override"]
        self.assertEqual(version_input["default"], "")
        self.assertEqual(version_input["type"], "string")

        build = self.rocm["jobs"]["build-wheel-rocm"]
        version_step = step_named(build, "Apply wheel version override")
        self.assertEqual(version_step["if"], "${{ inputs.version_override != '' }}")
        self.assertEqual(
            version_step["env"]["VERSION_OVERRIDE"],
            "${{ inputs.version_override }}",
        )
        self.assertIn("mooncake-wheel/pyproject.toml", version_step["run"])
        self.assertLess(
            step_index(build, "Apply wheel version override"),
            step_index(build, "Build Python wheel"),
        )

    def test_nightly_publishes_versioned_rocm_artifacts(self):
        jobs = self.nightly["jobs"]
        rocm_build = jobs["build-wheel-rocm"]
        self.assertEqual(rocm_build["needs"], "version-stamp")
        self.assertEqual(
            rocm_build["with"]["version_override"],
            "${{ needs.version-stamp.outputs.nightly_version }}",
        )

        publisher = jobs["publish-testpypi"]
        self.assertIn("build-wheel-rocm", publisher["needs"])
        patterns = {
            step.get("with", {}).get("pattern")
            for step in publisher["steps"]
            if step.get("uses") == "actions/download-artifact@v4"
        }
        self.assertIn("nightly-*", patterns)
        self.assertIn("mooncake-wheel-rocm-ubuntu-*", patterns)
        self.assertLess(
            step_index(publisher, "Download ROCm nightly wheel artifacts"),
            step_index(publisher, "Collect wheels for nightly publication"),
        )


if __name__ == "__main__":
    unittest.main()
