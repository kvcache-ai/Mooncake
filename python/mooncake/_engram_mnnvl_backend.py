"""Immutable EGM tables over a fixed membership; the control plane runs only
during initialization and shutdown.
"""

from __future__ import annotations

import math
import operator
import os
from pathlib import Path
import socket
import time
from urllib.error import URLError
import uuid

import numpy as np

from . import engram_session as session

LIMIT = (1 << 63) - 1


def numa_available(numa, root="/sys/devices/system/node"):
    """MemFree plus reclaimable file pages of one host NUMA node, or None."""
    try:
        text = Path(root, f"node{numa}", "meminfo").read_text()
    except OSError:
        return None
    free = file_pages = 0
    for line in text.splitlines():
        name, _, tail = line.partition(":")
        if not tail:
            continue
        key = name.split()[-1]
        if key in ("MemFree", "FilePages"):
            value = int(tail.split()[0]) * 1024
            if key == "MemFree":
                free = value
            else:
                file_pages = value
    return free + file_pages


def integer(value, name, minimum=0):
    if isinstance(value, bool):
        raise ValueError(f"{name} does not accept bool")
    value = operator.index(value)
    if value < minimum or value > LIMIT:
        raise ValueError(f"{name} is out of range")
    return value


def timeout(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("timeout must be a finite positive number")
    return value


def align(value, granularity):
    granularity = integer(granularity, "granularity", 1)
    if granularity & (granularity - 1):
        raise ValueError("granularity must be a power of two")
    result = (value + granularity - 1) // granularity * granularity
    if result > LIMIT:
        raise ValueError("arena size overflow")
    return result


def plan_layout(specs, owners, members, num_shards, num_callers):
    """Each table has exactly one writer; native compute shards may be
    published by different callers on the same NUMA node.
    """
    writers, hosts, gpu_ids, budgets = {}, {}, set(), {}
    for member in members:
        shard, caller = member["shard"], member["caller"]
        if not (0 <= shard < num_shards and 0 <= caller < num_callers):
            raise ValueError("Invalid member identity")
        host = member["host"]
        if shard in hosts and hosts[shard] != host:
            raise ValueError("host_id mismatch for the same storage shard")
        hosts[shard] = host
        if member["gpu_uuid"] in gpu_ids:
            raise ValueError("Multiple callers use the same physical GPU")
        gpu_ids.add(member["gpu_uuid"])
        key = (shard, integer(member["numa"], "NUMA"))
        budget = integer(member["budget"], "NUMA budget", 1)
        budgets[key] = min(budgets.get(key, budget), budget)
        for name in member["tables"]:
            if name not in specs or owners[name] != shard or name in writers:
                raise ValueError(
                    "Duplicate table writer, wrong ownership, or unknown "
                    "table name"
                )
            writers[name] = (shard, caller)
    if len(hosts) != num_shards or len(set(hosts.values())) != num_shards:
        raise ValueError("Storage shards must map to distinct host_ids")
    if set(writers) != set(specs):
        raise ValueError("A table was not submitted by its owner")
    layouts, used = {}, {}
    for member in members:
        key = (member["shard"], integer(member["numa"], "NUMA"))
        used[key] = used.get(key, 0) + integer(
            member.get("used_bytes", 0), "used_bytes"
        )
    for member in members:
        identity = (member["shard"], member["caller"])
        end, tables = 0, {}
        for name in sorted(member["tables"]):
            spec = specs[name]
            start = align(end, 64)
            size = spec["num_rows"] * spec["row_bytes"]
            if size > LIMIT - start:
                raise ValueError("table byte offset overflow")
            tables[name] = {"offset": start, "length": size}
            end = start + size
        size = align(end, member["granularity"])
        numa_key = (member["shard"], member["numa"])
        used[numa_key] = used.get(numa_key, 0) + size
        if used[numa_key] > budgets[numa_key]:
            raise ValueError("Total arena size of all writers exceeds the NUMA budget")
        layouts[identity] = {"length": size, "tables": tables}
    return writers, layouts


class Backend:
    def __init__(
        self,
        specs,
        namespace,
        *,
        shard_id,
        num_shards,
        table_owners,
        master_addr,
        session_id,
        caller_id,
        num_callers,
        max_lookup_rows,
        max_egm_bytes_per_numa,
        device=None,
        host_id=None,
        numa_node=None,
        rpc_hostname="",
        init_timeout_s=300,
        close_timeout_s=60,
    ):
        self.specs = specs
        self.namespace = namespace
        self.shard = integer(shard_id, "shard_id")
        self.shards = integer(num_shards, "num_shards", 1)
        self.caller = integer(caller_id, "caller_id")
        self.callers = integer(num_callers, "num_callers", 1)
        if self.shard >= self.shards or self.caller >= self.callers:
            raise ValueError("caller/shard ID is outside the membership range")
        self.capacity = integer(max_lookup_rows, "max_lookup_rows", 1)
        self.budget = integer(max_egm_bytes_per_numa, "max_egm_bytes_per_numa", 1)
        self.owners = {
            name: integer(owner, "table owner") for name, owner in table_owners.items()
        }
        if set(self.owners) != set(specs) or any(
            v >= self.shards for v in self.owners.values()
        ):
            raise ValueError("table_owners must fully cover specs with valid owners")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("Each deployment requires a new nonempty session_id")
        self.session_id = session_id
        self.metadata = session.Metadata(master_addr)
        configured_host = os.environ.get("MOONCAKE_HOST_ID", "").strip(" \t\r\n\f\v")
        self.host = (
            host_id
            if host_id is not None
            else (configured_host or socket.gethostname())
        )
        if not isinstance(self.host, str) or not self.host.strip(" \t\r\n\f\v"):
            raise ValueError("host_id must not be empty")
        self.host = self.host.strip(" \t\r\n\f\v")
        self.device = None if device is None else integer(device, "device")
        self.numa = None if numa_node is None else integer(numa_node, "numa_node")
        self.rpc_hostname = rpc_hostname
        self.init_timeout = timeout(init_timeout_s)
        self.close_timeout = timeout(close_timeout_s)
        self.prefix = (
            "engram/v1/" + session.digest(session_id) + "/" + session.digest(namespace)
        )
        self.layout_digest = session.digest(
            {
                "schema": 1,
                "backend": "mnnvl",
                "specs": specs,
                "owners": self.owners,
                "shards": self.shards,
                "callers": self.callers,
                "capacity": self.capacity,
            }
        )
        self.state = "NEW"
        self.runtime = None
        self.tables = {}
        self.mappings = []
        self.allocation = None
        self.published = False
        self.claimed = False

    def key(self, stage, shard=None, caller=None):
        return f"{self.prefix}/{stage}/{self.shard if shard is None else shard}/{self.caller if caller is None else caller}"

    def put(self, stage, value, deadline):
        self.metadata.create(self.key(stage), value, deadline)

    def wait_all(self, stage, deadline, ignore_failure=False):
        expected = [(s, c) for s in range(self.shards) for c in range(self.callers)]
        found = {}
        while len(found) != len(expected):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Engram timed out waiting for {stage}; missing "
                    f"{set(expected) - found.keys()}"
                )
            for identity in expected:
                try:
                    if not ignore_failure:
                        failure = self.metadata.get(
                            self.key("failed", *identity), deadline
                        )
                        if failure is not None:
                            raise RuntimeError(
                                f"Engram peer {identity} initialization failed: "
                                f"{failure['type']}"
                            )
                    if identity not in found:
                        value = self.metadata.get(self.key(stage, *identity), deadline)
                        if value is not None:
                            found[identity] = value
                except (URLError, OSError):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"Engram timed out waiting for {stage}"
                        ) from None
            if len(found) != len(expected):
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        return [found[identity] for identity in expected]

    def initialize(self, rows):
        if self.state != "NEW":
            raise RuntimeError("Engram backend cannot be initialized twice")
        deadline = time.monotonic() + self.init_timeout
        try:
            for name, array in rows.items():
                spec = self.specs.get(name)
                if (
                    spec is None
                    or self.owners[name] != self.shard
                    or array.dtype != np.uint8
                    or not array.flags.c_contiguous
                    or array.shape != (spec["num_rows"], spec["row_bytes"])
                ):
                    raise ValueError(
                        "local_rows must be complete contiguous uint8 tables "
                        "for the owner"
                    )
            from mooncake import _engram_mnnvl as native

            if self.device is None:
                self.device = native.current_device()
            self.runtime = session.acquire_runtime(
                self.metadata.endpoint,
                self.session_id,
                self.shard,
                self.caller,
                self.device,
                self.rpc_hostname,
                self.namespace,
            )
            with self.runtime.lock:
                info = self.runtime.native.info()
            nodes = {int(node): size for node, size in info["numa"].items()}
            self.numa = min(nodes) if self.numa is None else self.numa
            if self.numa not in nodes:
                raise ValueError(
                    "numa_node must be an EGM host attachment of the current GPU"
                )
            member = {
                "shard": self.shard,
                "caller": self.caller,
                "host": self.host,
                "gpu_uuid": info["gpu_uuid"],
                "numa": self.numa,
                "granularity": nodes[self.numa],
                "budget": self.budget,
                "used_bytes": info.get("used_bytes", {}).get(self.numa, 0),
                "tables": sorted(rows),
                "segment": info["segment"],
                "digest": self.layout_digest,
                "nonce": uuid.uuid4().hex,
            }
            self.put("members", member, deadline)
            self.claimed = True
            members = self.wait_all("members", deadline)
            for index, peer in enumerate(members):
                if peer["digest"] != self.layout_digest or (
                    peer["shard"],
                    peer["caller"],
                ) != divmod(index, self.callers):
                    raise ValueError(
                        "Engram layout or membership configuration mismatch"
                    )
            writers, layouts = plan_layout(
                self.specs, self.owners, members, self.shards, self.callers
            )
            self.state = "DISCOVERED"
            layout = layouts[(self.shard, self.caller)]
            samples = {}
            descriptor = None
            # All callers of a shard consume this node's NUMA; cuMemCreate OOM
            # has no readable diagnostics, so check reclaimable memory up front
            # and raise an actionable error.
            demand = sum(
                layouts[(self.shard, member["caller"])]["length"]
                for member in members
                if member["shard"] == self.shard and member["numa"] == self.numa
            )
            available = numa_available(self.numa)
            if available is not None and demand > available:
                raise RuntimeError(
                    f"EGM needs {demand} bytes on NUMA {self.numa}, but only "
                    f"{available} bytes of reclaimable memory are available; "
                    "interleave the host tables and this process across NUMA "
                    "nodes (e.g. numactl --interleave=all) and retry"
                )
            with self.runtime.lock:
                if layout["length"]:
                    self.allocation = self.runtime.native.allocate(
                        layout["length"], self.numa, self.budget
                    )
                    self.state = "ALLOCATED"
                    for name, array in rows.items():
                        region = layout["tables"][name]
                        self.runtime.native.write(
                            self.allocation, region["offset"], array
                        )
                        if array.nbytes:
                            raw = memoryview(array).cast("B")
                            n = min(64, array.nbytes)
                            samples[name] = [
                                bytes(raw[:n]).hex(),
                                bytes(raw[-n:]).hex(),
                            ]
                    self.state = "FILLED"
                    # Even if the HTTP publish response is lost, treat it as
                    # possibly already exported.
                    self.published = True
                    descriptor = self.runtime.native.publish(self.allocation, self.numa)
            self.put("arenas", {"descriptor": descriptor, "samples": samples}, deadline)
            self.state = "EXPORTED"
            arenas = self.wait_all("arenas", deadline)
            mapped = {}
            with self.runtime.lock:
                for peer, data in zip(members, arenas):
                    identity = (peer["shard"], peer["caller"])
                    expected = layouts[identity]
                    descriptor = data["descriptor"]
                    if not expected["length"]:
                        if descriptor is not None:
                            raise ValueError(
                                "An empty provider must not publish an arena"
                            )
                        continue
                    if (
                        not isinstance(descriptor, dict)
                        or descriptor["length"] != expected["length"]
                        or descriptor["segment"] != peer["segment"]
                        or integer(descriptor["address"], "arena address", 1)
                        > LIMIT - expected["length"]
                    ):
                        raise ValueError(
                            "EGM arena descriptor does not match the layout"
                        )
                    handle = self.runtime.native.acquire(
                        descriptor["segment"],
                        descriptor["address"],
                        descriptor["length"],
                    )
                    self.mappings.append(handle)
                    mapped[identity] = handle
                    for name, region in expected["tables"].items():
                        if not region["length"]:
                            continue
                        first, last = data["samples"][name]
                        n = min(64, region["length"])
                        got_first = self.runtime.native.sample(
                            handle, region["offset"], n
                        ).hex()
                        got_last = self.runtime.native.sample(
                            handle, region["offset"] + region["length"] - n, n
                        ).hex()
                        if got_first != first or got_last != last:
                            raise RuntimeError(
                                "EGM GPU readback bytes mismatch"
                                f": table={name} writer={identity}"
                                f" offset={region['offset']} length={region['length']}"
                                f" first_exp={first} first_got={got_first}"
                                f" last_exp={last} last_got={got_last}"
                            )
                for name, spec in self.specs.items():
                    writer = writers[name]
                    self.tables[name] = self.runtime.native.table(
                        mapped.get(writer, 0),
                        layouts[writer]["tables"][name]["offset"],
                        spec["num_rows"],
                        spec["row_bytes"],
                    )
            self.state = "MAPPED"
            self.put("ready", {"digest": self.layout_digest}, deadline)
            if any(
                item != {"digest": self.layout_digest}
                for item in self.wait_all("ready", deadline)
            ):
                raise ValueError("READY acknowledgement mismatch")
            self.state = "READY"
        except BaseException as exc:
            self.state = "FAILED"
            if self.claimed:
                try:
                    self.put(
                        "failed", {"type": type(exc).__name__}, time.monotonic() + 1
                    )
                except Exception:
                    pass
            raise
        finally:
            if self.runtime is not None:
                with session._lock:
                    self.runtime.initializing = None

    def validate_lookup(self, ids, stream):
        if self.state != "READY":
            raise RuntimeError(f"Engram backend is not READY yet: {self.state}")
        if stream is None:
            raise NotImplementedError("MNNVL Engram supports only CUDA lookup")
        if (
            not getattr(ids, "is_cuda", False)
            or ids.device.index != self.device
            or len(ids.shape) != 2
        ):
            raise ValueError(
                "IDs must be a 2-D CUDA tensor on the initialization device"
            )
        if ids.shape[0] * ids.shape[1] > self.capacity:
            raise ValueError(
                "lookup exceeds max_lookup_rows; growth during capture is "
                "forbidden"
            )

    def close(self):
        if self.state == "CLOSED":
            return
        self.state = "CLOSING"
        deadline = time.monotonic() + self.close_timeout
        if self.runtime is not None:
            with self.runtime.lock:
                self.runtime.native.synchronize()
                self.tables.clear()
                while self.mappings:
                    self.runtime.native.release_mapping(self.mappings[-1])
                    self.mappings.pop()
            if self.claimed:
                self.put("released", {"digest": self.layout_digest}, deadline)
            if self.published:
                acknowledgements = self.wait_all(
                    "released", deadline, ignore_failure=True
                )
                if any(
                    value != {"digest": self.layout_digest}
                    for value in acknowledgements
                ):
                    raise ValueError("RELEASED acknowledgement mismatch")
            with self.runtime.lock:
                if self.allocation is not None:
                    self.runtime.native.free(self.allocation)
                    self.allocation = None
                session.release_runtime(self.runtime, self.namespace)
            self.runtime = None
        self.state = "CLOSED"

    def abandon(self):
        if self.state != "CLOSED" and self.runtime is not None:
            session.abandon(self)
