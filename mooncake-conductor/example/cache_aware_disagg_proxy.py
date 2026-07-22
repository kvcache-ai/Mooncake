# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import logging
import math
import os
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)

SUPPORTED_HASH_PROFILE = {
    "strategy": "vllm_v1",
    "algorithm": "sha256_cbor",
    "index_projection": "low64_be",
}
MAX_DP_RANK = (1 << 31) - 1
DEFAULT_PREFILLER_HOSTS = ("localhost",)
DEFAULT_PREFILLER_PORTS = (8100,)
DEFAULT_DECODER_HOSTS = ("localhost",)
DEFAULT_DECODER_PORTS = (8200,)
DEFAULT_QUERY_TIMEOUT_SECONDS = 1.0
DEFAULT_REGISTRATION_TIMEOUT_SECONDS = 5.0

CLI_CONFIG_OPTIONS = (
    ("prefiller_hosts", "--prefiller-hosts"),
    ("prefiller_ports", "--prefiller-ports"),
    ("decoder_hosts", "--decoder-hosts"),
    ("decoder_ports", "--decoder-ports"),
    ("prefiller_instance_ids", "--prefiller-instance-ids"),
    ("prefiller_registrations", "--prefiller-registration"),
    ("conductor_address", "--conductor-address"),
    ("modelname", "--modelname"),
    ("block_size", "--block-size"),
    ("tenant_id", "--tenant-id"),
    ("lora_name", "--lora-name"),
    ("python_hash_seed", "--python-hash-seed"),
    ("query_timeout_seconds", "--query-timeout-seconds"),
    ("registration_timeout_seconds", "--registration-timeout-seconds"),
)


class ConfigError(ValueError):
    """Raised when the example configuration is invalid."""


class ConductorProtocolError(RuntimeError):
    """Raised when Conductor returns an unusable success response."""


@dataclass(frozen=True)
class HashProfileConfig:
    strategy: str
    algorithm: str
    python_hash_seed: str
    index_projection: str

    def as_dict(self) -> dict[str, str]:
        return {
            "strategy": self.strategy,
            "algorithm": self.algorithm,
            "python_hash_seed": self.python_hash_seed,
            "index_projection": self.index_projection,
        }


@dataclass(frozen=True)
class RegistrationConfig:
    endpoint: str
    dp_rank: int


@dataclass(frozen=True)
class PrefillInstanceConfig:
    instance_id: str
    http_endpoint: str
    registrations: tuple[RegistrationConfig, ...]


@dataclass(frozen=True)
class DecodeInstanceConfig:
    http_endpoint: str


@dataclass(frozen=True)
class ConductorConfig:
    address: str
    query_timeout_seconds: float
    registration_timeout_seconds: float


@dataclass(frozen=True)
class PrefillSharedConfig:
    model_name: str
    block_size: int
    tenant_id: str
    lora_name: str
    hash_profile: HashProfileConfig


@dataclass(frozen=True)
class PrefillPoolConfig:
    config: PrefillSharedConfig
    instances: tuple[PrefillInstanceConfig, ...]


@dataclass(frozen=True)
class DecodePoolConfig:
    instances: tuple[DecodeInstanceConfig, ...]


@dataclass(frozen=True)
class ProxyConfig:
    conductor: ConductorConfig
    prefill: PrefillPoolConfig
    decode: DecodePoolConfig


@dataclass
class PrefillClient:
    config: PrefillInstanceConfig
    client: httpx.AsyncClient


@dataclass
class DecodeClient:
    config: DecodeInstanceConfig
    client: httpx.AsyncClient


ClientFactory = Callable[[str, float | None], httpx.AsyncClient]


def _require_object(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must be a JSON object")
    return value


def _reject_unknown_fields(
    value: Mapping[str, Any], allowed: set[str], path: str
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ConfigError(f"{path} contains unsupported field: {unknown[0]}")


def _require_nonempty_string(value: Mapping[str, Any], field: str, path: str) -> str:
    raw = value.get(field)
    if not isinstance(raw, str) or not raw:
        raise ConfigError(f"{path}.{field} must be a non-empty string")
    return raw


def _optional_string(
    value: Mapping[str, Any], field: str, default: str, path: str
) -> str:
    raw = value.get(field, default)
    if not isinstance(raw, str):
        raise ConfigError(f"{path}.{field} must be a string")
    return raw


def _positive_number(
    value: Mapping[str, Any], field: str, default: float, path: str
) -> float:
    raw = value.get(field, default)
    if (
        isinstance(raw, bool)
        or not isinstance(raw, (int, float))
        or not math.isfinite(float(raw))
        or raw <= 0
    ):
        raise ConfigError(f"{path}.{field} must be a positive number")
    return float(raw)


def _http_base_url(raw: str, path: str) -> str:
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError(f"{path} must be an absolute HTTP URL")
    if parsed.username or parsed.password:
        raise ConfigError(f"{path} must not contain user information")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ConfigError(f"{path} must not contain a path, query, or fragment")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _parse_hash_profile(raw: Any, path: str) -> HashProfileConfig:
    value = _require_object(raw, path)
    allowed = {
        "strategy",
        "algorithm",
        "python_hash_seed",
        "index_projection",
    }
    _reject_unknown_fields(value, allowed, path)
    for field, expected in SUPPORTED_HASH_PROFILE.items():
        actual = _require_nonempty_string(value, field, path)
        if actual != expected:
            raise ConfigError(f"{path}.{field} must be {expected!r}")

    seed = _require_nonempty_string(value, "python_hash_seed", path)
    if seed != "random":
        if not seed.isascii() or any(char < "0" or char > "9" for char in seed):
            raise ConfigError(
                f"{path}.python_hash_seed must be 'random' or ASCII decimal text"
            )
        if int(seed) > 0xFFFFFFFF:
            raise ConfigError(f"{path}.python_hash_seed must be in the uint32 range")

    return HashProfileConfig(
        strategy=value["strategy"],
        algorithm=value["algorithm"],
        python_hash_seed=seed,
        index_projection=value["index_projection"],
    )


def _parse_conductor(raw: Any) -> ConductorConfig:
    path = "conductor"
    value = _require_object(raw, path)
    _reject_unknown_fields(
        value,
        {
            "address",
            "query_timeout_seconds",
            "registration_timeout_seconds",
        },
        path,
    )
    address = _http_base_url(
        _require_nonempty_string(value, "address", path), f"{path}.address"
    )
    return ConductorConfig(
        address=address,
        query_timeout_seconds=_positive_number(
            value, "query_timeout_seconds", 1.0, path
        ),
        registration_timeout_seconds=_positive_number(
            value, "registration_timeout_seconds", 5.0, path
        ),
    )


def _parse_prefill_shared_config(raw: Any) -> PrefillSharedConfig:
    path = "prefill.config"
    value = _require_object(raw, path)
    _reject_unknown_fields(
        value,
        {
            "modelname",
            "block_size",
            "tenant_id",
            "lora_name",
            "hash_profile",
        },
        path,
    )
    model_name = _require_nonempty_string(value, "modelname", path)
    block_size = value.get("block_size")
    if (
        isinstance(block_size, bool)
        or not isinstance(block_size, int)
        or block_size <= 0
    ):
        raise ConfigError(f"{path}.block_size must be a positive integer")
    tenant_id = _optional_string(value, "tenant_id", "default", path) or "default"
    lora_name = _optional_string(value, "lora_name", "", path)
    if "hash_profile" not in value:
        raise ConfigError(f"{path}.hash_profile is required")

    return PrefillSharedConfig(
        model_name=model_name,
        block_size=block_size,
        tenant_id=tenant_id,
        lora_name=lora_name,
        hash_profile=_parse_hash_profile(value["hash_profile"], f"{path}.hash_profile"),
    )


def _parse_registration(raw: Any, path: str) -> RegistrationConfig:
    value = _require_object(raw, path)
    _reject_unknown_fields(value, {"endpoint", "dp_rank"}, path)
    endpoint = _require_nonempty_string(value, "endpoint", path)
    dp_rank = value.get("dp_rank")
    if (
        isinstance(dp_rank, bool)
        or not isinstance(dp_rank, int)
        or dp_rank < 0
        or dp_rank > MAX_DP_RANK
    ):
        raise ConfigError(f"{path}.dp_rank must be a non-negative int")
    return RegistrationConfig(endpoint=endpoint, dp_rank=dp_rank)


def _parse_prefill(raw: Any, index: int) -> PrefillInstanceConfig:
    path = f"prefill.instances[{index}]"
    value = _require_object(raw, path)
    _reject_unknown_fields(
        value, {"instance_id", "http_endpoint", "registrations"}, path
    )
    instance_id = _require_nonempty_string(value, "instance_id", path)
    http_endpoint = _http_base_url(
        _require_nonempty_string(value, "http_endpoint", path),
        f"{path}.http_endpoint",
    )
    raw_registrations = value.get("registrations")
    if not isinstance(raw_registrations, list) or not raw_registrations:
        raise ConfigError(f"{path}.registrations must be a non-empty array")
    registrations = tuple(
        _parse_registration(registration, f"{path}.registrations[{rank_index}]")
        for rank_index, registration in enumerate(raw_registrations)
    )
    ranks = [registration.dp_rank for registration in registrations]
    if len(ranks) != len(set(ranks)):
        raise ConfigError(f"{path}.registrations contains a duplicate dp_rank")
    return PrefillInstanceConfig(
        instance_id=instance_id,
        http_endpoint=http_endpoint,
        registrations=registrations,
    )


def _parse_decode(raw: Any, index: int) -> DecodeInstanceConfig:
    path = f"decode.instances[{index}]"
    value = _require_object(raw, path)
    _reject_unknown_fields(value, {"http_endpoint"}, path)
    return DecodeInstanceConfig(
        http_endpoint=_http_base_url(
            _require_nonempty_string(value, "http_endpoint", path),
            f"{path}.http_endpoint",
        )
    )


def _parse_prefill_pool(raw: Any) -> PrefillPoolConfig:
    path = "prefill"
    value = _require_object(raw, path)
    _reject_unknown_fields(value, {"config", "instances"}, path)
    if "config" not in value:
        raise ConfigError("prefill.config is required")
    raw_instances = value.get("instances")
    if not isinstance(raw_instances, list) or not raw_instances:
        raise ConfigError("prefill.instances must be a non-empty array")
    return PrefillPoolConfig(
        config=_parse_prefill_shared_config(value["config"]),
        instances=tuple(
            _parse_prefill(instance, index)
            for index, instance in enumerate(raw_instances)
        ),
    )


def _parse_decode_pool(raw: Any) -> DecodePoolConfig:
    path = "decode"
    value = _require_object(raw, path)
    _reject_unknown_fields(value, {"instances"}, path)
    raw_instances = value.get("instances")
    if not isinstance(raw_instances, list) or not raw_instances:
        raise ConfigError("decode.instances must be a non-empty array")
    return DecodePoolConfig(
        instances=tuple(
            _parse_decode(instance, index)
            for index, instance in enumerate(raw_instances)
        )
    )


def parse_config(raw: Any) -> ProxyConfig:
    value = _require_object(raw, "config")
    _reject_unknown_fields(value, {"conductor", "prefill", "decode"}, "config")
    for field in ("conductor", "prefill", "decode"):
        if field not in value:
            raise ConfigError(f"config.{field} is required")

    conductor = _parse_conductor(value["conductor"])
    prefill = _parse_prefill_pool(value["prefill"])
    decode = _parse_decode_pool(value["decode"])

    instance_ids = [instance.instance_id for instance in prefill.instances]
    if len(instance_ids) != len(set(instance_ids)):
        raise ConfigError("prefill.instances contains duplicate instance_id")

    event_endpoints: set[str] = set()
    service_keys: set[tuple[str, int]] = set()
    for instance in prefill.instances:
        for registration in instance.registrations:
            service_key = (instance.instance_id, registration.dp_rank)
            if service_key in service_keys:
                raise ConfigError(
                    "config contains a duplicate registration service key"
                )
            service_keys.add(service_key)
            if registration.endpoint in event_endpoints:
                raise ConfigError("config contains a duplicate event endpoint")
            event_endpoints.add(registration.endpoint)

    return ProxyConfig(
        conductor=conductor,
        prefill=prefill,
        decode=decode,
    )


def load_config(path: str | Path) -> ProxyConfig:
    config_path = Path(path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"failed to load {config_path}: {exc}") from exc
    return parse_config(raw)


def _required_cli_value(args: argparse.Namespace, field: str, option: str) -> Any:
    value = getattr(args, field)
    if value is None:
        raise ConfigError(f"CLI configuration requires {option}")
    return value


def _cli_config_dict(args: argparse.Namespace) -> dict[str, Any]:
    prefiller_hosts = args.prefiller_hosts or list(DEFAULT_PREFILLER_HOSTS)
    prefiller_ports = args.prefiller_ports or list(DEFAULT_PREFILLER_PORTS)
    decoder_hosts = args.decoder_hosts or list(DEFAULT_DECODER_HOSTS)
    decoder_ports = args.decoder_ports or list(DEFAULT_DECODER_PORTS)
    instance_ids = _required_cli_value(
        args, "prefiller_instance_ids", "--prefiller-instance-ids"
    )
    raw_registrations = _required_cli_value(
        args, "prefiller_registrations", "--prefiller-registration"
    )

    if len(prefiller_hosts) != len(prefiller_ports):
        raise ConfigError(
            "number of prefiller hosts must match number of prefiller ports"
        )
    if len(decoder_hosts) != len(decoder_ports):
        raise ConfigError("number of decoder hosts must match number of decoder ports")
    if len(instance_ids) != len(prefiller_hosts):
        raise ConfigError(
            "number of prefiller instance IDs must match number of prefiller hosts"
        )

    registrations_by_instance: dict[str, list[dict[str, Any]]] = {
        instance_id: [] for instance_id in instance_ids
    }
    for instance_id, dp_rank_text, endpoint in raw_registrations:
        if instance_id not in registrations_by_instance:
            raise ConfigError(
                "--prefiller-registration references unknown instance ID "
                f"{instance_id!r}"
            )
        try:
            dp_rank = int(dp_rank_text, 10)
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                "--prefiller-registration DP_RANK must be an integer"
            ) from exc
        registrations_by_instance[instance_id].append(
            {"endpoint": endpoint, "dp_rank": dp_rank}
        )

    conductor_address = _required_cli_value(
        args, "conductor_address", "--conductor-address"
    )
    modelname = _required_cli_value(args, "modelname", "--modelname")
    block_size = _required_cli_value(args, "block_size", "--block-size")
    python_hash_seed = _required_cli_value(
        args, "python_hash_seed", "--python-hash-seed"
    )

    return {
        "conductor": {
            "address": conductor_address,
            "query_timeout_seconds": (
                args.query_timeout_seconds
                if args.query_timeout_seconds is not None
                else DEFAULT_QUERY_TIMEOUT_SECONDS
            ),
            "registration_timeout_seconds": (
                args.registration_timeout_seconds
                if args.registration_timeout_seconds is not None
                else DEFAULT_REGISTRATION_TIMEOUT_SECONDS
            ),
        },
        "prefill": {
            "config": {
                "modelname": modelname,
                "block_size": block_size,
                "tenant_id": (
                    args.tenant_id if args.tenant_id is not None else "default"
                ),
                "lora_name": args.lora_name if args.lora_name is not None else "",
                "hash_profile": {
                    "strategy": SUPPORTED_HASH_PROFILE["strategy"],
                    "algorithm": SUPPORTED_HASH_PROFILE["algorithm"],
                    "python_hash_seed": python_hash_seed,
                    "index_projection": SUPPORTED_HASH_PROFILE["index_projection"],
                },
            },
            "instances": [
                {
                    "instance_id": instance_id,
                    "http_endpoint": f"http://{host}:{port}",
                    "registrations": registrations_by_instance[instance_id],
                }
                for instance_id, host, port in zip(
                    instance_ids, prefiller_hosts, prefiller_ports, strict=True
                )
            ],
        },
        "decode": {
            "instances": [
                {"http_endpoint": f"http://{host}:{port}"}
                for host, port in zip(decoder_hosts, decoder_ports, strict=True)
            ]
        },
    }


def resolve_config(args: argparse.Namespace) -> ProxyConfig:
    cli_options = [
        option
        for field, option in CLI_CONFIG_OPTIONS
        if getattr(args, field) is not None
    ]
    if args.config is not None:
        if cli_options:
            raise ConfigError(
                "--config cannot be combined with CLI configuration option "
                f"{cli_options[0]}"
            )
        return load_config(args.config)
    return parse_config(_cli_config_dict(args))


def default_client_factory(
    base_url: str, timeout_seconds: float | None
) -> httpx.AsyncClient:
    timeout: httpx.Timeout | None
    if timeout_seconds is None:
        timeout = None
    else:
        timeout = httpx.Timeout(timeout_seconds)
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout,
        limits=httpx.Limits(
            max_connections=None,
            max_keepalive_connections=None,
        ),
    )


def _authorization_headers(request_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id,
    }


class ConductorClient:
    def __init__(
        self,
        conductor_config: ConductorConfig,
        prefill_config: PrefillSharedConfig,
        client: httpx.AsyncClient,
    ) -> None:
        self.conductor_config = conductor_config
        self.prefill_config = prefill_config
        self.client = client

    def registration_payloads(
        self, prefills: tuple[PrefillInstanceConfig, ...]
    ) -> tuple[dict[str, Any], ...]:
        payloads: list[dict[str, Any]] = []
        for prefill in prefills:
            for registration in prefill.registrations:
                payloads.append(
                    {
                        "endpoint": registration.endpoint,
                        "type": "vLLM",
                        "modelname": self.prefill_config.model_name,
                        "instance_id": prefill.instance_id,
                        "block_size": self.prefill_config.block_size,
                        "dp_rank": registration.dp_rank,
                        "tenant_id": self.prefill_config.tenant_id,
                        "lora_name": self.prefill_config.lora_name,
                        "hash_profile": self.prefill_config.hash_profile.as_dict(),
                    }
                )
        return tuple(payloads)

    async def register_all(self, prefills: tuple[PrefillInstanceConfig, ...]) -> None:
        for payload in self.registration_payloads(prefills):
            response = await self.client.post(
                "/register",
                json=payload,
                timeout=self.conductor_config.registration_timeout_seconds,
            )
            try:
                response.raise_for_status()
                body = _response_object(response, "Conductor register")
                if (
                    body.get("status") != "registered successfully"
                    or body.get("instance_id") != payload["instance_id"]
                ):
                    raise ConductorProtocolError(
                        "Conductor register returned an unexpected success body"
                    )
            finally:
                await response.aclose()
            logger.info(
                "registered instance_id=%s dp_rank=%s endpoint=%s",
                payload["instance_id"],
                payload["dp_rank"],
                payload["endpoint"],
            )

    async def query(
        self,
        request_data: Mapping[str, Any],
        token_ids: list[int],
        request_id: str,
    ) -> dict[str, Any]:
        model = request_data.get("model")
        if not isinstance(model, str) or not model:
            raise ConductorProtocolError("request model is missing or invalid")
        payload: dict[str, Any] = {
            "model": model,
            "block_size": self.prefill_config.block_size,
            "token_ids": token_ids,
            "tenant_id": self.prefill_config.tenant_id,
            "lora_name": self.prefill_config.lora_name,
        }
        if "cache_salt" in request_data:
            payload["cache_salt"] = request_data["cache_salt"]

        response = await self.client.post(
            "/query",
            json=payload,
            headers={"X-Request-Id": request_id},
            timeout=self.conductor_config.query_timeout_seconds,
        )
        try:
            response.raise_for_status()
            return _response_object(response, "Conductor query")
        finally:
            await response.aclose()


def _response_object(response: httpx.Response, operation: str) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        raise ConductorProtocolError(f"{operation} returned invalid JSON") from exc
    if not isinstance(body, dict):
        raise ConductorProtocolError(f"{operation} response must be a JSON object")
    return body


class ProxyRuntime:
    def __init__(
        self,
        config: ProxyConfig,
        client_factory: ClientFactory = default_client_factory,
    ) -> None:
        self.config = config
        self.client_factory = client_factory
        self.prefill_clients: list[PrefillClient] = []
        self.decode_clients: list[DecodeClient] = []
        self.prefill_by_id: Mapping[str, PrefillClient] = MappingProxyType({})
        self.conductor: ConductorClient | None = None
        self._conductor_http_client: httpx.AsyncClient | None = None
        self._tokenizer_iterator: Any = None
        self._prefill_fallback_iterator: Any = None
        self._decode_iterator: Any = None
        self._tie_offsets: dict[tuple[str, ...], int] = {}
        self.started = False

    async def start(self) -> None:
        if self.started:
            return
        try:
            self.prefill_clients = []
            for prefill in self.config.prefill.instances:
                self.prefill_clients.append(
                    PrefillClient(
                        config=prefill,
                        client=self.client_factory(prefill.http_endpoint, None),
                    )
                )
            self.decode_clients = []
            for decode in self.config.decode.instances:
                self.decode_clients.append(
                    DecodeClient(
                        config=decode,
                        client=self.client_factory(decode.http_endpoint, None),
                    )
                )
            self.prefill_by_id = MappingProxyType(
                {
                    prefill.config.instance_id: prefill
                    for prefill in self.prefill_clients
                }
            )
            self._tokenizer_iterator = itertools.cycle(self.prefill_clients)
            self._prefill_fallback_iterator = itertools.cycle(self.prefill_clients)
            self._decode_iterator = itertools.cycle(self.decode_clients)
            self._tie_offsets = {}

            conductor_config = self.config.conductor
            self._conductor_http_client = self.client_factory(
                conductor_config.address,
                conductor_config.query_timeout_seconds,
            )
            self.conductor = ConductorClient(
                conductor_config,
                self.config.prefill.config,
                self._conductor_http_client,
            )
            await self.conductor.register_all(self.config.prefill.instances)
            self.started = True
            logger.info(
                "initialized %d prefill instances and %d decode instances",
                len(self.prefill_clients),
                len(self.decode_clients),
            )
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        clients = [prefill.client for prefill in self.prefill_clients]
        clients.extend(decode.client for decode in self.decode_clients)
        if self._conductor_http_client is not None:
            clients.append(self._conductor_http_client)
        if clients:
            await asyncio.gather(
                *(client.aclose() for client in clients), return_exceptions=True
            )
        self.started = False

    def _ensure_started(self) -> None:
        if not self.started or self.conductor is None:
            raise RuntimeError("proxy runtime is not started")

    def next_decode(self) -> DecodeClient:
        self._ensure_started()
        return next(self._decode_iterator)

    def _fallback_prefill(self, request_id: str, reason: str) -> PrefillClient:
        selected = next(self._prefill_fallback_iterator)
        logger.warning(
            "request_id=%s cache_aware_fallback reason=%s selected_instance=%s",
            request_id,
            reason,
            selected.config.instance_id,
        )
        return selected

    async def select_prefill(
        self, request_data: Mapping[str, Any], request_id: str
    ) -> PrefillClient:
        self._ensure_started()
        tokenizer_client = next(self._tokenizer_iterator)
        try:
            token_ids = await self._tokenize(tokenizer_client, request_data, request_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("request_id=%s tokenization_error=%r", request_id, exc)
            return self._fallback_prefill(request_id, "tokenization_failed")

        assert self.conductor is not None
        try:
            query_result = await self.conductor.query(
                request_data, token_ids, request_id
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("request_id=%s conductor_query_error=%r", request_id, exc)
            return self._fallback_prefill(request_id, "conductor_query_failed")

        instances = query_result.get("instances")
        if not isinstance(instances, dict):
            return self._fallback_prefill(request_id, "invalid_instances_response")

        candidates: dict[str, int] = {}
        for instance_id, result in instances.items():
            if instance_id not in self.prefill_by_id or not isinstance(result, dict):
                continue
            longest_matched = result.get("longest_matched")
            if type(longest_matched) is not int or longest_matched < 0:
                continue
            candidates[instance_id] = longest_matched

        if not candidates:
            return self._fallback_prefill(request_id, "no_routable_cache_result")

        max_hit = max(candidates.values())
        if max_hit == 0:
            return self._fallback_prefill(request_id, "all_cache_hits_zero")

        leaders = tuple(
            sorted(
                instance_id for instance_id, hit in candidates.items() if hit == max_hit
            )
        )
        offset = self._tie_offsets.get(leaders, 0)
        selected_id = leaders[offset % len(leaders)]
        self._tie_offsets[leaders] = offset + 1
        logger.info(
            "request_id=%s cache_candidates=%s selected_instance=%s longest_matched=%d",
            request_id,
            candidates,
            selected_id,
            max_hit,
        )
        return self.prefill_by_id[selected_id]

    async def _tokenize(
        self,
        client_info: PrefillClient,
        request_data: Mapping[str, Any],
        request_id: str,
    ) -> list[int]:
        payload = dict(request_data)
        payload["stream"] = False
        payload["max_tokens"] = 1
        if "max_completion_tokens" in payload:
            payload["max_completion_tokens"] = 1
        payload.pop("stream_options", None)
        response = await client_info.client.post(
            "/tokenize",
            json=payload,
            headers=_authorization_headers(request_id),
            timeout=self.config.conductor.query_timeout_seconds,
        )
        try:
            response.raise_for_status()
            body = response.json()
        finally:
            await response.aclose()
        if not isinstance(body, dict):
            raise ValueError("tokenize response must be a JSON object")
        tokens = body.get("tokens")
        if not isinstance(tokens, list) or any(
            type(token) is not int for token in tokens
        ):
            raise ValueError("tokenize response tokens must be an integer array")
        return tokens

    async def send_prefill(
        self,
        client_info: PrefillClient,
        api: str,
        request_data: Mapping[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        payload = dict(request_data)
        payload["kv_transfer_params"] = {
            "do_remote_decode": True,
            "do_remote_prefill": False,
            "remote_engine_id": None,
            "remote_block_ids": None,
            "remote_host": None,
            "remote_port": None,
        }
        payload["stream"] = False
        payload["max_tokens"] = 1
        if "max_completion_tokens" in payload:
            payload["max_completion_tokens"] = 1
        payload.pop("stream_options", None)
        response = await client_info.client.post(
            api, json=payload, headers=_authorization_headers(request_id)
        )
        try:
            response.raise_for_status()
            await response.aread()
            body = response.json()
        finally:
            await response.aclose()
        if not isinstance(body, dict):
            raise ValueError("prefill response must be a JSON object")
        return body

    async def stream_decode(
        self,
        client_info: DecodeClient,
        api: str,
        request_data: Mapping[str, Any],
        request_id: str,
    ) -> AsyncIterator[bytes]:
        async with client_info.client.stream(
            "POST",
            api,
            json=dict(request_data),
            headers=_authorization_headers(request_id),
        ) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                yield chunk


def create_app(
    config: ProxyConfig,
    client_factory: ClientFactory = default_client_factory,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime = ProxyRuntime(config, client_factory)
        app.state.proxy_runtime = runtime
        await runtime.start()
        try:
            yield
        finally:
            await runtime.close()

    app = FastAPI(lifespan=lifespan)

    async def handle_completions(api: str, request: Request) -> StreamingResponse:
        runtime: ProxyRuntime = request.app.state.proxy_runtime
        request_data = await request.json()
        if not isinstance(request_data, dict):
            raise ValueError("request body must be a JSON object")
        request_id = str(uuid.uuid4())

        prefill = await runtime.select_prefill(request_data, request_id)
        prefill_response = await runtime.send_prefill(
            prefill, api, request_data, request_id
        )
        kv_transfer_params = prefill_response.get("kv_transfer_params", {})
        if kv_transfer_params:
            request_data["kv_transfer_params"] = kv_transfer_params

        decode = runtime.next_decode()
        logger.debug(
            "request_id=%s using prefill=%s decode=%s",
            request_id,
            prefill.config.instance_id,
            decode.config.http_endpoint,
        )

        async def generate_stream() -> AsyncIterator[bytes]:
            async for chunk in runtime.stream_decode(
                decode, api, request_data, request_id
            ):
                yield chunk

        return StreamingResponse(generate_stream(), media_type="application/json")

    @app.post("/v1/completions")
    async def handle_completion_request(request: Request) -> StreamingResponse:
        return await handle_completions("/v1/completions", request)

    @app.post("/v1/chat/completions")
    async def handle_chat_completion_request(request: Request) -> StreamingResponse:
        return await handle_completions("/v1/chat/completions", request)

    @app.get("/healthcheck")
    async def healthcheck(request: Request) -> dict[str, Any]:
        runtime: ProxyRuntime = request.app.state.proxy_runtime
        return {
            "status": "ok",
            "prefill_instances": len(runtime.prefill_clients),
            "decode_instances": len(runtime.decode_clients),
        }

    return app


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser("Conductor cache-aware vLLM P/D proxy example")
    parser.add_argument(
        "--config",
        help="Path to proxy JSON config; cannot be combined with CLI config flags",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="INFO")

    parser.add_argument(
        "--prefiller-hosts",
        "--prefiller-host",
        type=str,
        nargs="+",
        help="Prefill HTTP hosts (CLI mode; default: localhost)",
    )
    parser.add_argument(
        "--prefiller-ports",
        "--prefiller-port",
        type=int,
        nargs="+",
        help="Prefill HTTP ports (CLI mode; default: 8100)",
    )
    parser.add_argument(
        "--decoder-hosts",
        "--decoder-host",
        type=str,
        nargs="+",
        help="Decode HTTP hosts (CLI mode; default: localhost)",
    )
    parser.add_argument(
        "--decoder-ports",
        "--decoder-port",
        type=int,
        nargs="+",
        help="Decode HTTP ports (CLI mode; default: 8200)",
    )
    parser.add_argument(
        "--prefiller-instance-ids",
        "--prefiller-instance-id",
        nargs="+",
        help="Instance IDs paired positionally with prefiller hosts and ports",
    )
    parser.add_argument(
        "--prefiller-registration",
        "--prefill-registration",
        dest="prefiller_registrations",
        action="append",
        nargs=3,
        metavar=("INSTANCE_ID", "DP_RANK", "EVENT_ENDPOINT"),
        help="Conductor registration triple; repeat once per prefill DP rank",
    )
    parser.add_argument("--conductor-address", help="Conductor HTTP origin")
    parser.add_argument(
        "--model", "--modelname", dest="modelname", help="Registered model name"
    )
    parser.add_argument("--block-size", type=int)
    parser.add_argument("--tenant-id")
    parser.add_argument("--lora-name")
    parser.add_argument("--python-hash-seed")
    parser.add_argument("--query-timeout-seconds", type=float)
    parser.add_argument("--registration-timeout-seconds", type=float)

    args = parser.parse_args(argv)
    try:
        args.proxy_config = resolve_config(args)
    except ConfigError as exc:
        parser.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    import uvicorn

    uvicorn.run(create_app(args.proxy_config), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
