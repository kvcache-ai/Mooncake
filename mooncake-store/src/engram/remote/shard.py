"""Static node-shard layout and Master rendezvous (no tensor dependencies)."""

import hashlib
import json
import operator
import os
import time

import requests


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class ShardLayout:
    """Table names are opaque; the model supplies the head-to-node assignment."""

    def __init__(self, tables, owners, num_shards):
        self.num_shards = operator.index(num_shards)
        if self.num_shards < 1 or set(tables) != set(owners):
            raise ValueError("Every table needs exactly one storage owner")
        widths = {s["row_bytes"] for s in tables.values()}
        if len(widths) != 1:
            raise ValueError("A remote store requires equal byte-row widths")
        self.width = widths.pop()
        self.stride = (self.width + 7) // 8 * 8
        self.tables = tables
        self.owners = {k: operator.index(v) for k, v in owners.items()}
        self.starts = {}
        counts = [0] * self.num_shards
        for name in sorted(tables):
            owner = self.owners[name]
            if not 0 <= owner < self.num_shards:
                raise ValueError(f"Invalid owner for {name}")
            self.starts[name] = counts[owner]
            counts[owner] += tables[name]["num_rows"]
        # DeepEP addresses equal logical slots; unused tail rows are never read.
        self.slot_rows = max(1, *counts)
        if self.slot_rows * self.num_shards > 2**31 - 1:
            raise ValueError("DeepEP Engram indices exceed signed int32")
        self.identity = fingerprint(
            {"tables": tables, "owners": self.owners, "num_shards": self.num_shards}
        )

    def local_names(self, shard_id):
        return [name for name in sorted(self.tables) if self.owners[name] == shard_id]


class MasterRendezvous:
    """Use the Master's HTTP metadata service; immutable records reject reuse.

    rpc_meta keys have create-only/idempotent semantics in both Mooncake Master
    and the standalone metadata service. No weights travel through this API.
    session_id must be new for each deployment, and identical on all nodes.
    """

    def __init__(self, address, namespace, session_id, caller_id, timeout):
        if not address.startswith(("http://", "https://")):
            raise ValueError("master_addr must be its HTTP /metadata endpoint")
        if not session_id or not isinstance(session_id, str):
            raise ValueError("A deployment-specific session_id is required")
        if timeout <= 0:
            raise ValueError("initialize timeout must be positive")
        self.address = address.rstrip("/")
        self.prefix = "rpc_meta/engram/" + fingerprint(
            [namespace, session_id, str(caller_id)]
        )
        self.deadline = time.monotonic() + timeout
        self.http = requests.Session()
        self.http.trust_env = False
        if os.environ.get("MC_METADATA_HTTP_TOKEN"):
            self.http.headers["Authorization"] = (
                "Bearer " + os.environ["MC_METADATA_HTTP_TOKEN"]
            )
        self.published = []

    def _timeout(self):
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("Timed out waiting for Engram shards at Master")
        return min(5.0, left)

    def publish(self, key, value):
        name = f"{self.prefix}/{key}"
        response = self.http.put(
            self.address,
            params={"key": name},
            data=json.dumps(value, sort_keys=True),
            timeout=self._timeout(),
        )
        response.raise_for_status()
        self.published.append(name)

    def wait(self, key):
        while True:
            response = self.http.get(
                self.address,
                params={"key": f"{self.prefix}/{key}"},
                timeout=self._timeout(),
            )
            if response.status_code != 404:
                response.raise_for_status()
                return response.json()
            time.sleep(min(0.05, self._timeout()))

    def close(self):
        # Keep immutable tombstones until Master is restarted: reusing a session
        # after a partial failure must fail instead of accepting stale addresses.
        self.http.close()
