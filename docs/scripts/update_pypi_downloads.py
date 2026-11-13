"""Refresh the static PyPIStats snapshot, keeping usable data on upstream failure."""

import argparse
import copy
import datetime as dt
import json
import re
import time
import urllib.request
from pathlib import Path

PREFIX = "mooncake-transfer-engine"
ENDPOINTS = ("recent", "overall", "python_major", "python_minor", "system")
DEFAULT_PATH = (
    Path(__file__).resolve().parents[1] / "source/_static/pypi-downloads.json"
)


def now():
    return dt.datetime.now(dt.timezone.utc)


def fresh(result):
    try:
        age = now() - dt.datetime.fromisoformat(result["fetchedAt"])
        return result["status"] == "ok" and dt.timedelta(0) <= age < dt.timedelta(
            days=1
        )
    except (KeyError, ValueError, TypeError):
        return False


def open_url(url):
    request = urllib.request.Request(
        url, headers={"User-Agent": "Mooncake-Docs-PyPIStats/1.0"}
    )
    return urllib.request.urlopen(request, timeout=30)


def discover():
    with open_url("https://pypi.org/simple/") as response:
        names = set()
        for line in response:
            names.update(
                re.findall(rb">(mooncake-transfer-engine(?:-[a-z0-9-]+)?)<", line)
            )
    if PREFIX.encode() not in names:
        raise ValueError("Incomplete PyPI package index")
    return sorted(name.decode() for name in names)


def validate(payload, name, endpoint):
    if payload.get("package") != name:
        raise ValueError("Package name mismatch")
    data = payload["data"]
    if endpoint == "recent":
        values = [data[key] for key in ("last_day", "last_week", "last_month")]
    else:
        values = [row["downloads"] for row in data]
        for row in data:
            dt.date.fromisoformat(row["date"])
            if row["category"] is not None and not isinstance(row["category"], str):
                raise ValueError("Invalid category")
        keys = [(row["date"], row["category"]) for row in data]
        if len(set(keys)) != len(keys):
            raise ValueError("Duplicate date/category")
    if not all(type(value) is int and value >= 0 for value in values):
        raise ValueError("Invalid download count")
    return payload


def update(previous):
    result = copy.deepcopy(previous)
    catalog = result["catalog"]
    if not fresh(catalog):
        try:
            result["catalog"] = dict(
                names=discover(),
                source="https://pypi.org/simple/",
                fetchedAt=now().isoformat(),
                status="ok",
            )
        except Exception as error:
            catalog.update(status="stale", error=str(error))
    by_name = {p["name"]: p for p in result["packages"]}
    result["packages"] = []
    rate_limited = False
    for name in result["catalog"]["names"]:
        package = by_name.get(name, {"name": name, "endpoints": {}})
        for endpoint in ENDPOINTS:
            old = package["endpoints"].get(endpoint, {})
            if fresh(old) and "payload" in old:
                continue
            url = f"https://pypistats.org/api/packages/{name}/{endpoint}"
            try:
                if rate_limited:
                    raise RuntimeError("Skipped after upstream rate limit")
                try:
                    with open_url(url) as response:
                        payload = validate(json.load(response), name, endpoint)
                finally:
                    time.sleep(1.1)
                package["endpoints"][endpoint] = dict(
                    payload=payload,
                    source=url,
                    fetchedAt=now().isoformat(),
                    status="ok",
                )
            except Exception as error:
                rate_limited |= getattr(error, "code", None) == 429
                package["endpoints"][endpoint] = {
                    **old,
                    "source": url,
                    "error": str(error),
                    "status": "stale" if "payload" in old else "error",
                }
                print(f"Warning: {name}/{endpoint}: {error}")
        result["packages"].append(package)
    result["collectedAt"] = now().isoformat()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_PATH)
    args = parser.parse_args()
    result = update(json.loads(args.output.read_text()))
    temporary = args.output.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, separators=(",", ":")) + "\n")
    temporary.replace(args.output)
    print(f"Saved {len(result['packages'])} packages to {args.output}")


if __name__ == "__main__":
    main()
