"""Backend-independent fingerprints and legacy Master HTTP rendezvous."""

import hashlib
import json
import os
import time

import requests


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


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
