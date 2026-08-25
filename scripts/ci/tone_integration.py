#!/usr/bin/env python3
"""Resolve a Mooncake wheel artifact and run the T-one integration gate."""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

GITHUB_API_URL = "https://api.github.com"
TONE_API_URL = "https://tone.openanolis.cn/api/job"
TONE_RESULT_URL = "https://tone.openanolis.cn/ws/gclfnh19/test_result"
GITHUB_API_VERSION = "2022-11-28"

JsonObject = dict[str, Any]
JsonFetcher = Callable[[str], JsonObject]
JsonPoster = Callable[[str, Mapping[str, Any]], JsonObject]


class ToneIntegrationError(RuntimeError):
    """The T-one integration gate could not complete successfully."""


def resolve_source_context(
    event_name: str,
    github_sha: str,
    explicit_sha: str = "",
    event_pr_sha: str = "",
    explicit_pr: str = "",
    event_pr: str = "",
) -> tuple[str, str]:
    """Return the source SHA and optional PR number for the T-one job."""
    if event_name == "push":
        return github_sha, ""
    return explicit_sha or event_pr_sha or github_sha, explicit_pr or event_pr


def select_workflow_run(payload: Mapping[str, Any]) -> str:
    """Select the Build & Test workflow run from a GitHub API response."""
    for run in payload.get("workflow_runs", []):
        if run.get("path") == ".github/workflows/ci.yml" or run.get("name") == (
            "Build & Test (Linux)"
        ):
            run_id = run.get("id")
            if run_id is not None:
                return str(run_id)
    return ""


def select_cuda13_artifact(payload: Mapping[str, Any]) -> str:
    """Select the Python 3.12 CUDA 13 Mooncake artifact."""
    for artifact in payload.get("artifacts", []):
        name = str(artifact.get("name", ""))
        if all(fragment in name for fragment in ("py312", "mooncake", "cu130")):
            artifact_id = artifact.get("id")
            if artifact_id is not None:
                return str(artifact_id)
    return ""


def build_env_info(
    artifact_id: str,
    repository: str,
    *,
    branch: str = "",
    pr_number: str = "",
) -> str:
    """Build the space-delimited environment understood by T-one."""
    values = [f"ARTIFACT_ID={artifact_id}", f"GIT_REPO={repository}"]
    if branch:
        values.append(f"BRANCH={branch}")
    if pr_number:
        values.append(f"PR_ID={pr_number}")
    return " ".join(values)


def make_signature(username: str, token: str, timestamp: float) -> str:
    """Create the signature format required by the T-one API."""
    value = f"{username}|{token}|{timestamp}".encode()
    return base64.b64encode(value).decode()


class JsonClient:
    """Small JSON HTTP client for GitHub and T-one."""

    @staticmethod
    def _request(request: Request) -> JsonObject:
        try:
            with urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
        except HTTPError as error:
            raise ToneIntegrationError(
                f"HTTP {error.code} from {request.full_url}"
            ) from error
        except (TimeoutError, URLError) as error:
            raise ToneIntegrationError(
                f"request failed for {request.full_url}: {error}"
            ) from error

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as error:
            raise ToneIntegrationError(
                f"invalid JSON from {request.full_url}: {error}"
            ) from error
        if not isinstance(payload, dict):
            raise ToneIntegrationError(
                f"expected a JSON object from {request.full_url}"
            )
        return payload

    def github_get(self, url: str) -> JsonObject:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
            "User-Agent": "Mooncake-Tone-integration/1",
        }
        return self._request(Request(url, headers=headers))

    def post(self, url: str, payload: Mapping[str, Any]) -> JsonObject:
        request = Request(
            url,
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mooncake-Tone-integration/1",
            },
            method="POST",
        )
        return self._request(request)


def find_ci_artifact(
    repository: str,
    sha: str,
    fetch_json: JsonFetcher,
    *,
    attempts: int = 120,
    sleeper: Callable[[float], None] = time.sleep,
) -> str:
    """Wait for the matching ordinary CI CUDA 13 artifact."""
    query = urlencode({"head_sha": sha, "per_page": 100})
    runs_url = f"{GITHUB_API_URL}/repos/{repository}/actions/runs?{query}"
    last_error = "matching workflow run was not found"

    for attempt in range(1, attempts + 1):
        print(f"Attempt {attempt}: fetching artifact for SHA {sha}", flush=True)
        try:
            runs = fetch_json(runs_url)
            run_id = select_workflow_run(runs)
            if not run_id:
                available = [
                    f"{run.get('id')} {run.get('name')} {run.get('path')} "
                    f"{run.get('status')} {run.get('conclusion')}"
                    for run in runs.get("workflow_runs", [])
                ]
                last_error = (
                    f"matching workflow run was not found; available={available}"
                )
            else:
                artifacts_url = (
                    f"{GITHUB_API_URL}/repos/{repository}/actions/runs/"
                    f"{run_id}/artifacts?per_page=100"
                )
                artifacts = fetch_json(artifacts_url)
                artifact_id = select_cuda13_artifact(artifacts)
                if artifact_id:
                    print(f"Using CI artifact id {artifact_id}")
                    return artifact_id
                names = [item.get("name") for item in artifacts.get("artifacts", [])]
                last_error = (
                    f"Python 3.12 CUDA 13 artifact was not found in run {run_id}; "
                    f"available={names}"
                )
        except ToneIntegrationError as error:
            last_error = str(error)

        if attempt < attempts:
            delay = min(attempt * 60, 600)
            print(f"Artifact is not ready: {last_error}; retrying in {delay}s")
            sleeper(delay)

    raise ToneIntegrationError(
        f"failed to find the CI artifact after {attempts} attempts: {last_error}"
    )


def create_tone_job(
    post_json: JsonPoster,
    username: str,
    token: str,
    sha: str,
    env_info: str,
    *,
    clock: Callable[[], float] = time.time,
) -> str:
    """Create a T-one integration job and return its ID."""
    response = post_json(
        f"{TONE_API_URL}/create/",
        {
            "workspace": "mooncake_test",
            "project": "mooncake-ci",
            "template": "mooncake-ci-test",
            "name": f"mooncake-ci-{sha}",
            "username": username,
            "env_ifs": " ",
            "env_info": env_info,
            "signature": make_signature(username, token, clock()),
        },
    )
    if response.get("code") != 200:
        raise ToneIntegrationError(
            f"T-one job creation failed with code {response.get('code')!r}"
        )
    data = response.get("data")
    if not isinstance(data, dict) or data.get("id") is None:
        raise ToneIntegrationError("T-one job response has no job id")
    return str(data["id"])


def wait_for_tone_job(
    post_json: JsonPoster,
    username: str,
    token: str,
    job_id: str,
    *,
    max_total_attempts: int = 2880,
    max_running_attempts: int = 240,
    interval: float = 30,
    clock: Callable[[], float] = time.time,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    """Poll T-one until the job passes, fails, or reaches a timeout."""
    running_attempts = 0
    for total_attempt in range(1, max_total_attempts + 1):
        response = post_json(
            f"{TONE_API_URL}/query/",
            {
                "username": username,
                "signature": make_signature(username, token, clock()),
                "job_id": job_id,
            },
        )
        if response.get("code") != 200:
            raise ToneIntegrationError(
                f"T-one job query failed with code {response.get('code')!r}"
            )
        data = response.get("data")
        if not isinstance(data, dict):
            raise ToneIntegrationError("T-one job query has no data")

        job_state = str(data.get("job_state", "")).lower()
        job_status = str(data.get("job_second_state", "")).lower()
        if "pass" in job_status:
            print("T-one integration passed")
            return
        if "fail" in job_status:
            raise ToneIntegrationError(
                f"T-one integration failed or stopped: {job_status}"
            )

        if "running" in job_state:
            running_attempts += 1
            if running_attempts > max_running_attempts:
                raise ToneIntegrationError(
                    "T-one running timeout reached after "
                    f"{max_running_attempts} polling intervals"
                )
        if total_attempt < max_total_attempts:
            sleeper(interval)

    raise ToneIntegrationError(
        f"T-one total timeout reached after {max_total_attempts} polling intervals"
    )


def _required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ToneIntegrationError(f"{name} is required")
    return value


def main() -> int:
    try:
        username = _required_env("TONE_USER_NAME")
        token = _required_env("TONE_USER_TOKEN")
        repository = _required_env("GITHUB_REPOSITORY")
        github_sha = _required_env("GITHUB_SHA")
        source_sha, pr_number = resolve_source_context(
            os.environ.get("GITHUB_EVENT_NAME", ""),
            github_sha,
            os.environ.get("INPUT_PR_SHA", ""),
            os.environ.get("EVENT_PR_SHA", ""),
            os.environ.get("INPUT_PR_NUMBER", ""),
            os.environ.get("EVENT_PR_NUMBER", ""),
        )

        client = JsonClient()
        artifact_id = os.environ.get("TESTPYPI_ARTIFACT_ID", "")
        if artifact_id:
            print(f"Using TestPyPI wheel artifact id {artifact_id}")
        else:
            artifact_id = find_ci_artifact(repository, source_sha, client.github_get)

        branch = ""
        if os.environ.get("TESTPYPI_VERSION", ""):
            branch = _required_env("GITHUB_REF_NAME")
        env_info = build_env_info(
            artifact_id,
            repository,
            branch=branch,
            pr_number=pr_number,
        )
        job_id = create_tone_job(client.post, username, token, source_sha, env_info)
        print(f"T-one job: {TONE_RESULT_URL}/{job_id}?tab=4", flush=True)
        wait_for_tone_job(client.post, username, token, job_id)
    except ToneIntegrationError as error:
        print(f"T-one integration failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
