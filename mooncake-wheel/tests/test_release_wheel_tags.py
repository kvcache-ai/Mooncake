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

# The floor must come from a named manylinux image. Deliberately not a digest
# assertion: pinning digests is worth doing for both arches at once, not here.
ALLOWED_CONTAINER = re.compile(
    r"^pytorch/manylinux(2_28|aarch64)-builder:cuda\d+\.\d+$"
)


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


def _collect_build_jobs() -> list:
    workflows_dir = _find_workflows_dir()
    if workflows_dir is None:
        return []

    jobs = []
    for path in sorted(workflows_dir.glob("*.yaml")):
        workflow = yaml.safe_load(path.read_text()) or {}
        for job_name, job in (workflow.get("jobs") or {}).items():
            if SHARED_BUILD_WORKFLOW in str(job.get("uses", "")):
                jobs.append(
                    pytest.param(job.get("with") or {}, id=f"{path.name}:{job_name}")
                )
    return jobs


BUILD_JOBS = _collect_build_jobs()

if not BUILD_JOBS:
    pytest.skip("workflow sources not available", allow_module_level=True)


@pytest.mark.parametrize("with_block", BUILD_JOBS)
def test_release_build_pins_glibc_floor_to_a_container(with_block: dict) -> None:
    container = with_block.get("container", "")
    assert container, (
        "job builds release wheels on a bare runner, so its manylinux tag "
        "follows the runner's glibc and drifts when GitHub bumps the image; "
        f"set container to a {ALLOWED_CONTAINER.pattern} image"
    )
    assert ALLOWED_CONTAINER.match(container), (
        f"container {container!r} is not a pinned manylinux builder image; "
        f"expected one matching {ALLOWED_CONTAINER.pattern}"
    )
    # CUDA must come from that image; otherwise the job installs a toolkit with
    # the host package manager, which the manylinux (AlmaLinux) image lacks.
    assert with_block.get("cuda") == "container", (
        f"cuda is {with_block.get('cuda')!r}; a job pinned to a manylinux "
        "container must take CUDA from the image"
    )
