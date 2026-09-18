"""Guard the glibc floor of CI and release wheels.

Distributable wheels must take their manylinux platform tag from a pinned
container image, never from the GitHub runner. ``scripts/repair_wheel.sh`` derives
``PLATFORM_TAG`` from the *build host's* glibc (``getconf GNU_LIBC_VERSION``), so a
job running on a bare runner silently re-tags itself whenever GitHub bumps the
runner image. That is not hypothetical: the published aarch64 floor moved from
``manylinux_2_35`` (0.3.9) to ``manylinux_2_39`` (0.3.10) with no code change,
which stops ARM Ubuntu 22.04 from resolving any wheel and contradicts the
"OS: Ubuntu 22.04 LTS+" contract in docs/source/getting_started/build.md.

Running CI and release builds through the same manylinux workflow makes the
detected glibc a constant of the image instead of a property of the runner. See
#2858.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

# Jobs delegating to this reusable workflow produce the standard CUDA and
# non-CUDA wheel artifacts. Hardware-specific release-{efa,efa-non-cuda,musa,
# npu}.yaml workflows do not call it and are out of scope.
SHARED_BUILD_WORKFLOW = "_build-wheel.yaml"

# The floor must come from a named manylinux image, matched to the runner's
# architecture: an aarch64 image on an x86 runner (or vice versa) is a
# misconfiguration, not a pinned floor. Deliberately not a digest assertion;
# pinning digests is worth doing for both arches at once, not here.
ARCH_CONTAINER = {
    "aarch64": re.compile(r"^pytorch/manylinuxaarch64-builder:cuda\d+\.\d+$"),
    "x86_64": re.compile(r"^pytorch/manylinux2_28-builder:cuda\d+\.\d+$"),
}


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
        "wheel workflows stopped using it, so this guard is disarmed"
    )


@pytest.mark.parametrize("with_block", BUILD_JOBS)
def test_callers_do_not_override_the_build_environment(with_block: dict) -> None:
    # Architecture/variant are public inputs now; runner/container/toolkit
    # selection belongs to the shared workflow rather than its callers.
    assert not {"runner", "container", "cuda"}.intersection(with_block)


def test_shared_workflow_pins_glibc_floor_to_architecture() -> None:
    workflow = yaml.safe_load((WORKFLOWS_DIR / SHARED_BUILD_WORKFLOW).read_text())
    job = workflow["jobs"]["build"]
    assert job["runs-on"] == (
        "${{ inputs.architecture == 'arm64' && 'ubuntu-22.04-arm' || 'ubuntu-22.04' }}"
    )
    container = " ".join(job.get("container", "").split())
    # Check both the image floors and their routing. Merely finding four image
    # names would miss a swapped architecture or a branch that uses the host.
    images = re.findall(r"'(pytorch/[^']+)'", container)
    assert len(images) == 4, "all architecture/CUDA branches need a manylinux image"
    for arch, image in zip(("aarch64", "aarch64", "x86_64", "x86_64"), images):
        assert ARCH_CONTAINER[arch].fullmatch(image), (arch, image)
    arm13, arm12, x8613, x8612 = images
    assert container == (
        "${{ inputs.architecture == 'arm64' && "
        f"(inputs.variant == 'cuda13' && '{arm13}' || '{arm12}') || "
        f"(inputs.variant == 'cuda13' && '{x8613}' || '{x8612}') }}}}"
    )
