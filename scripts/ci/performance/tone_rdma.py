#!/usr/bin/env python3
"""Submit a pinned T-One RDMA job and independently enforce its measurements."""

import argparse
import base64
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from perf_gate import compare, load_policy, policy_digest, report, require, write_json
from rdma_collect import benchmark_digest


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Tone:
    def __init__(self):
        self.username = os.environ["TONE_USER_NAME"]
        self.token = os.environ["TONE_USER_TOKEN"]
        require(self.username and self.token, "T-One credentials are required")
        self.opener = urllib.request.build_opener(NoRedirect)

    def read(self, request, limit=4 * 1024 * 1024):
        try:
            with self.opener.open(request, timeout=30) as response:
                body = response.read(limit + 1)
                require(len(body) <= limit, "T-One response exceeds size limit")
                return json.loads(body)
        except urllib.error.URLError:
            # A stop request's URL contains the signature; never print the exception URL.
            raise RuntimeError("T-One HTTP request failed") from None

    def api(self, method, **fields):
        signature = base64.b64encode(
            f"{self.username}|{self.token}|{time.time()}".encode()
        ).decode()
        body = {"username": self.username, "signature": signature, **fields}
        url = "https://tone.openanolis.cn/api/job/" + method + "/"
        if method == "stop":
            request = urllib.request.Request(url + "?" + urllib.parse.urlencode(body))
        else:
            request = urllib.request.Request(
                url,
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
            )
        response = self.read(request)
        require(response.get("code") == 200, f"T-One {method} request was rejected")
        return response.get("data")

    def result(self, job_id, origin):
        records = self.api("log/query", job_id=job_id, page_size=100, page_num=1)
        urls = {
            url
            for record in records
            if record["test_suite"] == "mooncake-rdma-perf"
            for url in record.get("logs", [])
            if urllib.parse.urlsplit(url).path.endswith("/rdma-results.log")
        }
        require(len(urls) == 1, "expected exactly one rdma-results.log from T-One")
        url = urllib.parse.urljoin(origin + "/", urls.pop())
        parsed = urllib.parse.urlsplit(url)
        allowed = urllib.parse.urlsplit(origin)
        require(
            (parsed.scheme, parsed.netloc) == (allowed.scheme, allowed.netloc)
            and not parsed.username
            and not parsed.password,
            "untrusted result download origin",
        )
        return self.read(urllib.request.Request(url))


def import_results(bundle, args, policy):
    require(bundle["run_id"] == args.run_id, "stale T-One result")
    build_configs = set()
    for side in ("baseline", "candidate"):
        result = bundle[side]
        require(
            isinstance(result["errors"], list)
            and all(isinstance(error, str) for error in result["errors"]),
            "invalid collection errors",
        )
        env = result["environment"]
        require(
            env["run_id"] == args.run_id and env["harness_sha"] == args.harness_sha,
            "result harness or run identity mismatch",
        )
        require(
            env["benchmark_sha256"] == benchmark_digest(args.benchmark),
            "benchmark source mismatch",
        )
        if not result["errors"]:
            require(
                len(env["hosts"]) == 2
                and len({host["boot_id"] for host in env["hosts"]}) == 2,
                "missing independent host identities",
            )
            require(len(result["builds"]) == 2, "missing build manifests")
            for build in result["builds"]:
                require(
                    build["revision"] == getattr(args, side + "_sha")
                    and re.fullmatch(r"[0-9a-f]{64}", build["build_config_sha256"])
                    and re.fullmatch(r"[0-9a-f]{64}", build["binary_sha256"])
                    and build["libraries"]
                    and all(
                        re.fullmatch(r"[0-9a-f]{64}", value)
                        for value in build["libraries"].values()
                    ),
                    "invalid build provenance",
                )
                build_configs.add(build["build_config_sha256"])
        write_json(Path(args.output) / (side + ".json"), result)
    require(
        len(build_configs) <= 1, "baseline and candidate build configurations differ"
    )
    return compare(
        policy,
        bundle["baseline"],
        bundle["candidate"],
        args.baseline_sha,
        args.candidate_sha,
    )


def run(args, client=None):
    args.artifact_name = "rdma-performance-results"
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    args.baseline, args.candidate = (
        str(output / "baseline.json"),
        str(output / "candidate.json"),
    )
    job_id, finished, error, policy = None, False, None, None
    try:
        for sha in (args.baseline_sha, args.candidate_sha, args.harness_sha):
            require(
                re.fullmatch(r"[0-9a-f]{40}", sha),
                "full baseline, candidate and harness SHAs are required",
            )
        require(re.fullmatch(r"[a-zA-Z0-9_-]+", args.run_id), "invalid run id")
        origin = urllib.parse.urlsplit(args.result_origin)
        require(
            origin.scheme == "https"
            and origin.netloc
            and origin.path in ("", "/")
            and not origin.query
            and not origin.fragment
            and not origin.username,
            "configure an HTTPS TONE_RESULT_ORIGIN",
        )
        policy = load_policy(args.policy)
        write_json(output / "policy.json", policy)
        client = client or Tone()
        env = {
            "BASELINE_SHA": args.baseline_sha,
            "CANDIDATE_SHA": args.candidate_sha,
            "HARNESS_SHA": args.harness_sha,
            "RDMA_RUN_ID": args.run_id,
        }
        job = client.api(
            "create",
            workspace=args.workspace,
            project=args.project,
            template=args.template,
            name="mooncake-rdma-" + args.run_id,
            env_ifs=" ",
            env_info=" ".join(f"{key}={value}" for key, value in env.items()),
        )
        job_id = job["id"]
        require(str(job_id).isdigit(), "invalid T-One job id")
        write_json(output / "job.json", {"id": job_id, **env})
        print(f"T-One RDMA job: {job_id}", flush=True)
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            status = client.api("query", job_id=job_id)
            write_json(output / "job-status.json", status)
            state = status["job_state"]
            require(
                state in ("pending", "pending_q", "running", "success", "fail", "stop"),
                "unknown T-One job state",
            )
            if state in ("success", "fail", "stop"):
                finished = True
                break
            time.sleep(15)
        require(finished, "T-One RDMA job timed out")
        bundle = client.result(job_id, args.result_origin.rstrip("/"))
        write_json(output / "rdma-results.log", bundle)
        rows = import_results(bundle, args, policy)
        api_failed = state != "success" or status.get("job_second_state") in (
            "fail",
            "failed",
            "stop",
        )
        require(
            not (api_failed and all(row["status"] == "PASS" for row in rows)),
            "T-One execution failed despite passing measurements",
        )
    except Exception as exc:
        error = str(exc)
    finally:
        if job_id is not None and not finished:
            try:
                client.api("stop", job_id=job_id)
            except Exception:
                error = (
                    error or ""
                ) + "; could not cancel T-One job (remote worker has a watchdog)"
        if error:
            # Preserve partial normalized measurements, but never let them pass.
            for side, sha in (
                ("baseline", args.baseline_sha),
                ("candidate", args.candidate_sha),
            ):
                path = output / (side + ".json")
                result = (
                    json.loads(path.read_text())
                    if path.exists()
                    else {
                        "schema_version": 1,
                        "revision": sha,
                        "policy_sha256": policy_digest(policy) if policy else "",
                        "environment": {},
                        "cases": {},
                        "errors": [],
                    }
                )
                result["errors"].append(error)
                write_json(path, result)
            (output / "error.log").write_text(error + "\n")
    code = report(args)
    if job_id is not None:
        note = f"\nT-One job ID: `{job_id}` (job-status.json contains the job link and execution status).\n"
        for path in (output / "summary.md", args.summary):
            if path:
                with Path(path).open("a") as stream:
                    stream.write(note)
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline-sha", "candidate-sha", "harness-sha", "run-id", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument(
        "--policy", default=str(Path(__file__).with_name("rdma-policy.json"))
    )
    parser.add_argument(
        "--benchmark",
        default=str(
            Path(__file__).resolve().parents[3] / "mooncake-transfer-engine/benchmark"
        ),
    )
    parser.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY"))
    parser.add_argument(
        "--workspace", default=os.environ.get("TONE_WORKSPACE") or "mooncake_test"
    )
    parser.add_argument(
        "--project", default=os.environ.get("TONE_PROJECT") or "mooncake-ci"
    )
    parser.add_argument(
        "--template",
        default=os.environ.get("TONE_RDMA_TEMPLATE") or "mooncake-rdma-perf",
    )
    parser.add_argument(
        "--result-origin", default=os.environ.get("TONE_RESULT_ORIGIN", "")
    )
    parser.add_argument("--timeout", type=int, default=2700)

    def interrupted(signum, frame):
        raise RuntimeError(f"interrupted by signal {signum}")

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(run(parser.parse_args()))
