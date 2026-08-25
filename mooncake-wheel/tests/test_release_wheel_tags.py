"""Regression guards for shared wheel builds and the pre-release gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

SHARED_BUILD_WORKFLOW = "_build-wheel.yaml"
CORE_PACKAGES = {
    "mooncake-transfer-engine",
    "mooncake-transfer-engine-cuda13",
    "mooncake-transfer-engine-non-cuda",
}


def _find_workflows_dir() -> Path | None:
    """Locate .github/workflows, or None when the suite has been detached."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".github" / "workflows"
        if candidate.is_dir():
            return candidate
    return None


WORKFLOWS_DIR = _find_workflows_dir()

if WORKFLOWS_DIR is None:
    pytest.skip("workflow sources not available", allow_module_level=True)


def _load_workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS_DIR / name).read_text()) or {}


def _commands(job: dict) -> str:
    return "\n".join(str(step.get("run", "")) for step in job.get("steps", []))


def _command_step(job: dict, fragment: str) -> tuple[int, dict]:
    matches = [
        (index, step)
        for index, step in enumerate(job.get("steps", []))
        if fragment in str(step.get("run", ""))
    ]
    assert len(matches) == 1, f"expected one step containing {fragment!r}"
    return matches[0]


def _collect_build_jobs() -> list:
    jobs = []
    for path in sorted(WORKFLOWS_DIR.glob("*.y*ml")):
        workflow = yaml.safe_load(path.read_text()) or {}
        for job_name, job in (workflow.get("jobs") or {}).items():
            if SHARED_BUILD_WORKFLOW in str(job.get("uses", "")):
                jobs.append(f"{path.name}:{job_name}")
    return jobs


def test_shared_build_workflow_still_has_callers() -> None:
    callers = _collect_build_jobs()
    assert callers, (
        f"no job delegates to {SHARED_BUILD_WORKFLOW}; it was renamed or the "
        "wheel workflows stopped using it, so the release guards are disarmed"
    )


def test_shared_wheel_build_pins_glibc_floor_to_arch_specific_containers() -> None:
    """The reusable workflow, not each caller, owns the manylinux floor."""
    workflow = _load_workflow(SHARED_BUILD_WORKFLOW)
    trigger = workflow.get("on", workflow.get(True))
    build = workflow["jobs"]["build"]
    runner = str(build.get("runs-on", ""))
    container = str(build.get("container", ""))

    default_pythons = trigger["workflow_call"]["inputs"]["python-versions"]["default"]
    assert json.loads(default_pythons) == ["3.10", "3.11", "3.12", "3.13"]
    assert "ubuntu-22.04-arm" in runner
    assert "ubuntu-22.04" in runner
    for image in (
        "pytorch/manylinux2_28-builder:cuda12.8",
        "pytorch/manylinux2_28-builder:cuda13.0",
        "pytorch/manylinuxaarch64-builder:cuda12.8",
        "pytorch/manylinuxaarch64-builder:cuda13.0",
    ):
        assert image in container


def test_pre_release_build_uses_normalized_version_for_the_24_wheel_matrix() -> None:
    workflow = _load_workflow("pre-release.yaml")
    jobs = workflow["jobs"]
    trigger = workflow.get("on", workflow.get(True))
    version_job = jobs["version-stamp"]
    version_commands = _commands(version_job)
    build = jobs["build"]

    assert set(trigger["push"]["tags"]) == {
        "v*-rc*",
        "v*-alpha*",
        "v*-beta*",
        "v*-pre*",
    }
    assert "Version(tag[1:])" in version_commands
    assert "version.is_prerelease" in version_commands
    assert version_job["outputs"]["package_version"]
    assert build["needs"] == "version-stamp"
    assert build["with"]["version-override"] == (
        "${{ needs.version-stamp.outputs.package_version }}"
    )

    profiles = {
        (entry["variant"], entry["architecture"])
        for entry in build["strategy"]["matrix"]["include"]
    }
    assert profiles == {
        (variant, architecture)
        for variant in ("cuda", "cuda13", "non-cuda")
        for architecture in ("x86_64", "arm64")
    }
    assert "python-versions" not in build["with"]


def test_pre_release_publishes_only_validated_collision_free_testpypi_wheels() -> None:
    publish = _load_workflow("pre-release.yaml")["jobs"]["publish-testpypi"]
    commands = _commands(publish)
    validate_index, _ = _command_step(publish, "validate-local")
    collision_index, _ = _command_step(publish, "ensure-version-absent")
    credentials_index, credentials = _command_step(
        publish, "Missing required release gate credentials"
    )
    upload_index, upload = _command_step(publish, "twine upload")

    assert set(publish["needs"]) == {"version-stamp", "build"}
    assert publish["environment"] == "nightly"
    assert '"${wheel_count}" -ne 24' in commands
    assert "twine check" in commands
    assert "twine upload --repository testpypi" in commands
    assert "--skip-existing" not in commands
    assert validate_index < upload_index
    assert collision_index < upload_index
    assert credentials_index < upload_index
    assert credentials["env"]["TONE_USER_TOKEN"] == ("${{ secrets.TONE_USER_TOKEN }}")
    assert upload["env"]["TWINE_USERNAME"] == "__token__"
    assert upload["env"]["TWINE_PASSWORD"] == ("${{ secrets.TESTPYPI_API_TOKEN }}")


def test_pre_release_waits_for_the_complete_testpypi_index() -> None:
    verify = _load_workflow("pre-release.yaml")["jobs"]["verify-testpypi-index"]
    commands = _commands(verify)

    assert "publish-testpypi" in verify["needs"]
    assert "testpypi_wheel_gate.py wait-index" in commands
    assert "https://test.pypi.org/simple" in commands


def test_pre_release_consumes_each_indexed_package_without_local_artifacts() -> None:
    consume = _load_workflow("pre-release.yaml")["jobs"]["consume-testpypi"]
    commands = _commands(consume)

    assert "verify-testpypi-index" in consume["needs"]
    assert set(consume["strategy"]["matrix"]["package"]) == CORE_PACKAGES
    assert "pip download" in commands
    assert "https://test.pypi.org/simple" in commands
    assert "--no-deps" in commands
    assert '"${TARGET_PACKAGE}==${PACKAGE_VERSION}"' in commands
    assert "https://pypi.org/simple" in commands
    assert '"${wheels[0]}"' in commands
    assert "mooncake_http_metadata_server" in commands
    assert "import mooncake.engine, mooncake.store" in commands
    assert 'mooncake_master" --version' in commands
    assert "dist-release" not in commands


def test_pre_release_runs_tone_with_the_exact_indexed_cuda13_version() -> None:
    workflows = _load_workflow("pre-release.yaml")["jobs"]
    integration = workflows["tone-integration"]
    reusable = _load_workflow("integration-test.yml")
    trigger = reusable.get("on", reusable.get(True))
    tone = reusable["jobs"]["test-tone-integration"]
    commands = _commands(tone)
    _, tone_step = _command_step(tone, "tone_integration.py")

    assert set(integration["needs"]) == {"version-stamp", "consume-testpypi"}
    assert integration["uses"] == "./.github/workflows/integration-test.yml"
    assert integration["with"]["testpypi_version"] == (
        "${{ needs.version-stamp.outputs.package_version }}"
    )
    assert integration["secrets"] == "inherit"

    version_input = trigger["workflow_call"]["inputs"]["testpypi_version"]
    assert version_input["required"] is False
    assert version_input["default"] == ""
    assert "mooncake-transfer-engine-cuda13==${TESTPYPI_VERSION}" in commands
    assert "https://test.pypi.org/simple" in commands
    assert "--no-deps" in commands
    assert "--only-binary=:all:" in commands
    assert tone_step["env"]["TESTPYPI_ARTIFACT_ID"] == (
        "${{ steps.testpypi-wheel.outputs.artifact-id }}"
    )
    assert tone_step["env"]["TESTPYPI_VERSION"] == ("${{ inputs.testpypi_version }}")
    assert tone_step["env"]["TONE_USER_TOKEN"] == ("${{ secrets.TONE_USER_TOKEN }}")
    checkouts = [
        step for step in tone["steps"] if step.get("uses") == "actions/checkout@v4"
    ]
    assert len(checkouts) == 1
    assert checkouts[0]["with"]["persist-credentials"] is False
    assert checkouts[0]["with"]["sparse-checkout"] == ("scripts/ci/tone_integration.py")
    assert "TONE_USER_NAME and TONE_USER_TOKEN are required" in commands
    assert "tone_user_token" not in tone["env"]


def test_pre_release_has_no_production_or_github_release_publisher() -> None:
    workflow_text = (WORKFLOWS_DIR / "pre-release.yaml").read_text()

    assert "_publish-wheel.yaml" not in workflow_text
    assert "secrets.PYPI_API_TOKEN" not in workflow_text
    assert "gh release" not in workflow_text
    assert "softprops/action-gh-release" not in workflow_text


def test_pre_release_summary_reports_publication_and_consumer_results() -> None:
    summary = _load_workflow("pre-release.yaml")["jobs"]["summary"]
    commands = _commands(summary)

    assert set(summary["needs"]) == {
        "version-stamp",
        "publish-testpypi",
        "verify-testpypi-index",
        "consume-testpypi",
        "tone-integration",
    }
    assert "always()" in summary["if"]
    assert "TestPyPI publication" in commands
    assert "TestPyPI index validation" in commands
    assert "TestPyPI consumer validation" in commands
    assert "T-one integration (TestPyPI CUDA 13 wheel)" in commands
    for package in CORE_PACKAGES:
        assert f"{package}==${{PACKAGE_VERSION:-unavailable}}" in commands
