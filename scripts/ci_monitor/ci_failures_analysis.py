#!/usr/bin/env python3
"""
Mooncake CI Failure Analysis

Fetches recent CI workflow runs, identifies failing jobs, and produces
a summary report. Runs every 12 hours via GitHub Actions.

Usage:
    python ci_failures_analysis.py \
        --token $GITHUB_TOKEN \
        --limit 50 \
        --output ci_failure_report.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import requests

REPO = "kvcache-ai/Mooncake"
GITHUB_API = "https://api.github.com"
WORKFLOWS_TO_MONITOR = ["Build & Test (Linux)", "E2E CI"]


def get_recent_runs(token: str, limit: int) -> list:
    """Fetch recent workflow runs."""
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    runs = []
    for workflow_name in WORKFLOWS_TO_MONITOR:
        url = f"{GITHUB_API}/repos/{REPO}/actions/runs"
        params = {"per_page": limit, "status": "completed"}
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        for run in resp.json().get("workflow_runs", []):
            if run["name"] == workflow_name:
                runs.append(run)
    return runs[:limit]


def analyze_failures(runs: list, token: str) -> dict:
    """Analyze failed runs and extract failure patterns."""
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    failures = {}

    for run in runs:
        if run["conclusion"] != "failure":
            continue

        jobs_url = run["jobs_url"]
        resp = requests.get(jobs_url, headers=headers)
        resp.raise_for_status()

        for job in resp.json().get("jobs", []):
            if job["conclusion"] != "failure":
                continue

            job_name = job["name"]
            if job_name not in failures:
                failures[job_name] = {
                    "count": 0,
                    "runs": [],
                    "first_seen": run["created_at"],
                    "last_seen": run["created_at"],
                }
            failures[job_name]["count"] += 1
            failures[job_name]["runs"].append({
                "run_id": run["id"],
                "sha": run["head_sha"][:8],
                "branch": run["head_branch"],
                "url": run["html_url"],
                "date": run["created_at"],
            })
            failures[job_name]["last_seen"] = run["created_at"]

    return failures


def main():
    parser = argparse.ArgumentParser(description="Mooncake CI Failure Analysis")
    parser.add_argument("--token", required=True, help="GitHub token")
    parser.add_argument("--limit", type=int, default=50, help="Number of runs to analyze")
    parser.add_argument("--output", default="ci_failure_report.json", help="Output file")
    args = parser.parse_args()

    print(f"Fetching last {args.limit} CI runs...")
    runs = get_recent_runs(args.token, args.limit)
    print(f"Found {len(runs)} runs")

    print("Analyzing failures...")
    failures = analyze_failures(runs, args.token)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs_analyzed": len(runs),
        "failure_summary": {
            name: {
                "count": data["count"],
                "first_seen": data["first_seen"],
                "last_seen": data["last_seen"],
                "recent_runs": data["runs"][:5],
            }
            for name, data in sorted(failures.items(), key=lambda x: x[1]["count"], reverse=True)
        },
    }

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nCI Failure Report ({len(failures)} failing jobs):")
    for name, data in sorted(failures.items(), key=lambda x: x[1]["count"], reverse=True):
        print(f"  {name}: {data['count']} failures (last: {data['last_seen'][:10]})")

    if not failures:
        print("  All green! No failures found.")

    print(f"\nReport saved to {args.output}")


if __name__ == "__main__":
    main()
