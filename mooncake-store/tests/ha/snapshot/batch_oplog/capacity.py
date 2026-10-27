#!/usr/bin/env python3
"""Evidence collection and assertions for the disposable N13 etcd harness."""

import argparse
import base64
import json
from pathlib import Path
import subprocess
import time
import urllib.request


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class Etcd:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def run(self, *args, value=None):
        return subprocess.run(
            ["etcdctl", "--endpoints=" + self.endpoint, "--command-timeout=15s", *args],
            input=value,
            text=True,
            capture_output=True,
            timeout=20,
        )

    def json(self, *args):
        result = self.run(*args, "--write-out=json")
        require(result.returncode == 0, result.stderr)
        return json.loads(result.stdout)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def status(etcd, directory):
    directory.mkdir(parents=True, exist_ok=False)
    endpoints = etcd.json("endpoint", "status")
    save(directory / "status.json", endpoints)
    alarm = etcd.json("alarm", "list")
    save(directory / "alarm.json", alarm)
    # Retain the actual quota gauge, not only a configured/default value.
    with urllib.request.urlopen("http://" + etcd.endpoint + "/metrics", timeout=5) as r:
        metrics = r.read().decode()
    (directory / "etcd.prom").write_text(metrics)
    health = etcd.run("endpoint", "health")
    (directory / "health.log").write_text(health.stdout + health.stderr)
    require(
        health.returncode == 0 or alarm.get("alarms"), "unhealthy etcd without alarm"
    )
    return endpoints, alarm


def sample(etcd, directory, cluster, ports):
    endpoints, alarm = status(etcd, directory)
    prefix = "/oplog/" + cluster + "/"
    keys = etcd.json("get", prefix + "batches/", "--prefix", "--keys-only")
    save(directory / "batch-keys.json", keys)
    revision = keys["header"]["revision"]
    # Pin control reads to the same revision as the live key set.
    control = etcd.json(
        "get", prefix + "snapshot/", "--prefix", "--rev=" + str(revision)
    )
    durable = etcd.json("get", prefix + "durable_prefix", "--rev=" + str(revision))
    save(directory / "control.json", control)
    save(directory / "durable.json", durable)
    values = {
        base64.b64decode(kv["key"]).decode().rsplit("/", 1)[-1]: json.loads(
            base64.b64decode(kv["value"])
        )
        for kv in control.get("kvs", [])
    }
    batches = [
        int(base64.b64decode(kv["key"]).decode().rsplit("/", 1)[-1])
        for kv in keys.get("kvs", [])
    ]
    head = json.loads(base64.b64decode(durable["kvs"][0]["value"]))
    for index, port in enumerate(ports.split(",")):
        with urllib.request.urlopen(
            "http://127.0.0.1:" + port + "/metrics", timeout=5
        ) as r:
            metrics = r.read().decode()
            (directory / f"master-{index}.prom").write_text(metrics)
            require(
                "ha_snapshot_enabled " in metrics, "M02 snapshot metrics unavailable"
            )
    state = {
        "time": time.time(),
        "revision": revision,
        "live_batches": len(batches),
        "min_batch": min(batches, default=0),
        "max_batch": max(batches, default=0),
        "durable": head,
        "retained_durable_batches": sum(
            values.get("compaction_floor", 0) < b <= head["batch_id"] for b in batches
        ),
        "latest": values.get("latest"),
        "fallback": values.get("fallback"),
        "floor": values.get("compaction_floor", 0),
        "status": endpoints,
        "alarms": alarm.get("alarms", []),
    }
    save(directory / "sample.json", state)
    return state


def pruned(state):
    latest, fallback = state["latest"], state["fallback"]
    return bool(
        latest
        and fallback
        and state["floor"] > 0
        and latest["snapshot_id"] != fallback["snapshot_id"]
        and latest["last_included_batch_id"] > fallback["last_included_batch_id"]
        and state["floor"] <= fallback["last_included_batch_id"]
        and (state["min_batch"] == 0 or state["min_batch"] > state["floor"])
    )


def validate(samples, max_batches):
    require(len(samples) >= 3, "need at least three capacity samples")
    floors = [s["floor"] for s in samples]
    require(floors == sorted(floors), "floor moved backwards")
    require(len(set(floors) - {0}) >= 3, "need at least three distinct positive floors")
    require(all(not s["alarms"] for s in samples), "unexpected etcd alarm during soak")
    require(
        max(s["live_batches"] for s in samples) <= max_batches,
        "live batch bound exceeded",
    )
    require(pruned(samples[-1]), "final batch deletion did not reach the floor")
    for state in samples:
        require(
            state["retained_durable_batches"]
            == state["durable"]["batch_id"] - state["floor"],
            "missing batch above floor",
        )
    final = samples[-1]
    require(
        final["max_batch"] <= final["durable"]["batch_id"] + 1,
        "batch beyond writer head",
    )
    return {
        "samples": len(samples),
        "floor_advances": sum(a < b for a, b in zip(floors, floors[1:])),
        "peak_live_batches": max(s["live_batches"] for s in samples),
        "final_floor": final["floor"],
        "elapsed_seconds": samples[-1]["time"] - samples[0]["time"],
    }


def maintain(etcd, root, label):
    before, _ = status(etcd, root / (label + "-before"))
    require(len(before) == 1, "test maintenance requires its single local member")
    revision = before[0]["Status"]["header"]["revision"]
    for name, args in [
        ("compact", ("compact", str(revision), "--physical")),
        ("defrag", ("defrag",)),
    ]:
        result = etcd.run(*args)
        (root / (label + "-" + name + ".log")).write_text(result.stdout + result.stderr)
        require(result.returncode == 0, name + " failed: " + result.stderr)
        status(etcd, root / (label + "-after-" + name))
    after = json.loads((root / (label + "-after-defrag/status.json")).read_text())
    old, new = before[0]["Status"], after[0]["Status"]
    require(
        old["leader"] == new["leader"] == new["header"]["member_id"],
        "local etcd leader changed during maintenance",
    )
    metrics = (root / (label + "-after-defrag/etcd.prom")).read_text()
    quota = next(
        float(line.split()[1])
        for line in metrics.splitlines()
        if line.startswith("etcd_server_quota_backend_bytes ")
    )
    require(new["dbSize"] < quota, "backend still above quota; do not disarm")
    require(new["dbSize"] < old["dbSize"], "defrag did not reduce backend size")
    require(
        new["dbSize"] <= new["dbSizeInUse"] + 1024 * 1024,
        "backend remains fragmented after defrag",
    )


def nospace(etcd, root):
    # Revisions of one expendable key fill MVCC without touching Mooncake data.
    # Limit attempts so a wrong quota cannot consume the operator's disk.
    for _ in range(256):
        result = etcd.run("put", "/n13-capacity-padding", value="x" * (256 * 1024))
        if result.returncode:
            (root / "nospace-put.log").write_text(result.stdout + result.stderr)
            require(
                "database space exceeded" in result.stderr, "unexpected fill failure"
            )
            break
    else:
        raise RuntimeError("quota was not reached within 64 MiB of test writes")
    _, alarm = status(etcd, root / "nospace-alarm")
    require(
        any(a["alarm"] == 1 for a in alarm.get("alarms", [])), "NOSPACE not observed"
    )
    deleted = etcd.run("del", "/n13-capacity-padding")
    require(deleted.returncode == 0, deleted.stderr)
    maintain(etcd, root, "nospace")
    result = etcd.run("alarm", "disarm")
    (root / "disarm.log").write_text(result.stdout + result.stderr)
    require(result.returncode == 0, result.stderr)
    _, alarm = status(etcd, root / "nospace-disarmed")
    require(not alarm.get("alarms"), "alarm persists after disarm")
    result = etcd.run("put", "/n13-capacity-probe", "recovered")
    require(result.returncode == 0, result.stderr)
    require(etcd.json("get", "/n13-capacity-probe")["count"] == 1, "probe not readable")
    health = etcd.run("endpoint", "health")
    (root / "recovered-health.log").write_text(health.stdout + health.stderr)
    require(health.returncode == 0, health.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["sample", "ready", "validate", "maintain", "nospace"]
    )
    parser.add_argument("directory", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--cluster")
    parser.add_argument("--ports")
    parser.add_argument("--max-batches", type=int, default=2048)
    args = parser.parse_args()
    if args.command == "ready":
        return 0 if pruned(json.loads(args.directory.read_text())) else 1
    if args.command == "validate":
        samples = [
            json.loads(p.read_text())
            for p in sorted(args.directory.glob("sample-*/sample.json"))
        ]
        save(args.directory / "soak-result.json", validate(samples, args.max_batches))
    elif args.command == "sample":
        sample(Etcd(args.endpoint), args.directory, args.cluster, args.ports)
    elif args.command == "maintain":
        maintain(Etcd(args.endpoint), args.directory, "soak")
    else:
        nospace(Etcd(args.endpoint), args.directory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
