"""Guard the glibc floor of released wheels.

Release wheels must take their manylinux platform tag from a pinned container
image, never from the GitHub runner. ``scripts/build_wheel.sh`` derives
``PLATFORM_TAG`` from the *build host's* glibc (``detect_glibc_version``), so a
job running on a bare runner silently re-tags itself whenever GitHub bumps the
runner image. That is not hypothetical: the published aarch64 floor moved from
``manylinux_2_35`` (0.3.9) to ``manylinux_2_39`` (0.3.10) with no code change,
which stops ARM Ubuntu 22.04 from resolving any wheel and contradicts the
"OS: Ubuntu 22.04 LTS+" contract in docs/source/getting_started/build.md.

Running inside a manylinux container makes the detected glibc a constant of the
image instead of a property of the runner. See #2858.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

# Jobs delegating to this reusable workflow are the ones that publish wheels.
# release-{efa,efa-non-cuda,musa,npu}.yaml do not call it and are out of scope.
SHARED_BUILD_WORKFLOW = "_build-wheel.yaml"

# The floor must come from a named manylinux image, matched to the runner's
# architecture: an aarch64 image on an x86 runner (or vice versa) is a
# misconfiguration, not a pinned floor. Deliberately not a digest assertion;
# pinning digests is worth doing for both arches at once, not here.
ARCH_CONTAINER = {
    "aarch64": re.compile(r"^pytorch/manylinuxaarch64-builder:cuda\d+\.\d+$"),
    "x86_64": re.compile(r"^pytorch/manylinux2_28-builder:cuda\d+\.\d+$"),
}


def _runner_arch(runner: str) -> str:
    return "aarch64" if runner.endswith("-arm") else "x86_64"


def _find_workflows_dir() -> Path | None:
    """Locate .github/workflows, or None when the suite has been detached.

    scripts/test_installation.sh copies this directory into a scratch tree, so
    the workflow sources are not always reachable from __file__.
    """
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".github" / "workflows"
        if candidate.is_dir():
            return candidate
    return None


WORKFLOWS_DIR = _find_workflows_dir()

if WORKFLOWS_DIR is None:
    pytest.skip("workflow sources not available", allow_module_level=True)


def _collect_build_jobs() -> list:
    jobs = []
    # Both extensions: the repo uses .yml for most workflows and .yaml for the
    # release set, so globbing one silently ignores callers named in the other.
    for path in sorted(WORKFLOWS_DIR.glob("*.y*ml")):
        workflow = yaml.safe_load(path.read_text()) or {}
        for job_name, job in (workflow.get("jobs") or {}).items():
            if SHARED_BUILD_WORKFLOW in str(job.get("uses", "")):
                jobs.append(
                    pytest.param(job.get("with") or {}, id=f"{path.name}:{job_name}")
                )
    return jobs


BUILD_JOBS = _collect_build_jobs()


def test_shared_build_workflow_still_has_callers() -> None:
    """Fail loudly rather than skip if the marker stops matching.

    Without this, renaming _build-wheel.yaml would empty BUILD_JOBS and leave
    every assertion below silently unenforced.
    """
    assert BUILD_JOBS, (
        f"no job delegates to {SHARED_BUILD_WORKFLOW}; it was renamed or the "
        "release workflows stopped using it, so this guard is disarmed"
    )


@pytest.mark.parametrize("with_block", BUILD_JOBS)
def test_release_build_pins_glibc_floor_to_a_container(with_block: dict) -> None:
    runner = str(with_block.get("runner", ""))
    container = with_block.get("container", "")
    arch = _runner_arch(runner)
    expected = ARCH_CONTAINER[arch]

    assert container, (
        "job builds release wheels on a bare runner, so its manylinux tag "
        "follows the runner's glibc and drifts when GitHub bumps the image; "
        f"set container to a {expected.pattern} image"
    )
    assert expected.match(container), (
        f"container {container!r} is not the pinned manylinux builder image "
        f"for {arch} (runner {runner!r}); expected one matching "
        f"{expected.pattern}"
    )
    # sbsa-* makes _build-wheel.yaml install the CUDA toolkit with apt, which
    # the AlmaLinux-based manylinux image does not have. Any other value
    # ('container', 'none') keeps the build inside the image.
    cuda = str(with_block.get("cuda", ""))
    assert not cuda.startswith("sbsa-"), (
        f"cuda is {cuda!r}; a job pinned to a manylinux container cannot "
        "install CUDA via the host package manager"
    )
