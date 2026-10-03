"""Pythonic wrapper over the native _conductor module.

Keeps the pybind layer's return conventions verbatim (int rc for lifecycle and
mutating calls, {"ret", ...} dicts for reads) so behavior matches
mooncake.store's client exactly; this module only adds ergonomics.
"""

from typing import Any

from mooncake import _conductor

# Re-export error-code constants for callers that don't want magic numbers.
OK = 0
INTERNAL_ERROR = -1
INVALID_PARAMS = -600
RPC_FAIL = -900
RPC_TIMEOUT = -901
CONDUCTOR_UNAVAILABLE = -2000
SERVICE_NOT_FOUND = -2001

HEALTH_OK = 0
HEALTH_NOT_INITIALIZED = 1
HEALTH_UNREACHABLE = 2

__all__ = [
    "ConductorClient",
    "OK",
    "INTERNAL_ERROR",
    "INVALID_PARAMS",
    "RPC_FAIL",
    "RPC_TIMEOUT",
    "CONDUCTOR_UNAVAILABLE",
    "SERVICE_NOT_FOUND",
    "HEALTH_OK",
    "HEALTH_NOT_INITIALIZED",
    "HEALTH_UNREACHABLE",
]


class ConductorClient:
    """Thin wrapper adding context-manager support and docstrings."""

    def __init__(self) -> None:
        self._raw = _conductor.ConductorClient()

    def __enter__(self) -> "ConductorClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # The methods below forward to the same-named methods on self._raw,
    # keeping signatures and docstrings aligned with
    # _conductor.ConductorClient (setup/close/health_check/query/register/
    # unregister/get_global_view/list_services). The forwarding is written
    # out explicitly per method instead of __getattr__ dispatch, so IDE
    # completion and type annotations keep working.

    def setup(
        self,
        conductor_addr: str,
        connect_timeout_ms: int = 1000,
        request_timeout_ms: int = 3000,
    ) -> int:
        """Connect to the conductor at ``host:port``.

        Returns OK (0) on success, otherwise a negative error code
        (CONDUCTOR_UNAVAILABLE / RPC_FAIL / RPC_TIMEOUT / INTERNAL_ERROR).
        """
        return self._raw.setup(conductor_addr, connect_timeout_ms, request_timeout_ms)

    def close(self) -> int:
        """Release the underlying RPC resources. Always returns OK (0)."""
        return self._raw.close()

    def health_check(self) -> int:
        """Probe conductor liveness.

        Returns HEALTH_OK (0), HEALTH_NOT_INITIALIZED (1) if setup() never
        succeeded, or HEALTH_UNREACHABLE (2) if the conductor does not answer.
        """
        return self._raw.health_check()

    def query(
        self,
        model_name: str,
        *,
        lora_name: str = "",
        block_size: int,
        tenant_id: str = "default",
        token_ids: list[int],
        cache_salt: str = "",
        instance_filter: str = "",
    ) -> dict[str, Any]:
        """Query prefix-cache hits for ``token_ids``.

        Returns {"ret": int, "hits": dict}; "hits" maps instance_id to
        {"longest_matched": int, "dp": {rank: tokens}, "rank_matches": {...},
        "npu": int, "cpu_local": int, "cpu_share": int, "disk": int}. On error
        "ret" is a negative error code and "hits" is empty.

        All parameters after ``model_name`` are keyword-only, mirroring the
        pybind signature where the required ``block_size`` and ``token_ids``
        follow defaulted parameters.
        """
        return self._raw.query(
            model_name=model_name,
            lora_name=lora_name,
            block_size=block_size,
            tenant_id=tenant_id,
            token_ids=token_ids,
            cache_salt=cache_salt,
            instance_filter=instance_filter,
        )

    def register(self, config: dict[str, Any]) -> int:
        """Register one serving instance from a config dict.

        Keys follow the Python API convention (model_name / publisher_type,
        not the HTTP /register wire names modelname / type): instance_id,
        endpoint, replay_endpoint, publisher_type, model_name, lora_name,
        tenant_id, block_size, dp_rank, cache_group, hash_profile. Required:
        instance_id, endpoint, model_name, block_size, hash_profile.
        tenant_id defaults to "default" when omitted or empty.
        hash_profile accepts four fields: strategy, algorithm,
        python_hash_seed, index_projection. Unknown keys or wrong types raise
        ValueError.

        Returns OK (0) or a negative error code.
        """
        return self._raw.register(config)

    def unregister(
        self,
        instance_id: str,
        tenant_id: str = "default",
        dp_rank: int = 0,
    ) -> int:
        """Unregister one DP rank of ``instance_id``. Returns OK or error code."""
        return self._raw.unregister(instance_id, tenant_id, dp_rank)

    def get_global_view(self) -> dict[str, Any]:
        """Return {"ret": int, "context_count": int, "contexts": list}.

        Each context carries model_name/lora_name/block_size/tenant_id,
        prefix_count, a "hash_profile" dict (strategy, algorithm,
        python_hash_seed, root_digest, index_projection), and an
        instance_id -> dp_ranks mapping under "instances". On error "ret"
        is a negative error code and "contexts" is empty.
        """
        return self._raw.get_global_view()

    def list_services(self) -> dict[str, Any]:
        """Return {"ret": int, "count": int, "services": list}.

        Service entries use the /services wire key style (Endpoint, Type,
        ModelName, LoraName, TenantID, InstanceID, BlockSize, DPRank,
        CacheGroup, HashProfile). On error "ret" is a negative error code and
        "services" is empty.
        """
        return self._raw.list_services()
