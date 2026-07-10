"""Mooncake-backed FeatureBundle object store.

This module is the production E→P boundary for multimodal hidden-state
FeatureHandles.  Encoder workers serialize a validated FeatureBundle into the
real Mooncake Store (HTTP service or Python SDK); Prefill workers resolve the
``mooncake://`` URI and validate the descriptor before injecting hidden states
into vLLM.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import struct
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse

import requests
import torch

from .feature_handle import FeatureHandle
from .feature_store import FeatureBundle, FeatureBundleDescriptor, TensorSpec


class MooncakeFeatureStoreError(RuntimeError):
    """Raised when a FeatureBundle cannot be published/resolved via Mooncake."""


@dataclass(frozen=True)
class MooncakeFeatureBundleStoreConfig:
    store_id: str = "mooncake-mm-store"
    store_url: Optional[str] = None
    config_path: Optional[str] = None
    timeout_s: float = 30.0
    key_prefix: str = "epd-mm-feature"
    cleanup_on_close: bool = False
    serialization_format: str = "raw_v2"
    http_binary_payload: bool = False

    @classmethod
    def from_env(cls) -> "MooncakeFeatureBundleStoreConfig":
        timeout = os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_TIMEOUT_S", "30")
        try:
            timeout_s = max(0.1, float(timeout))
        except Exception:
            timeout_s = 30.0
        return cls(
            store_id=(
                os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_ID")
                or os.getenv("MOONCAKE_EPD_MOONCAKE_STORE_ID")
                or "mooncake-mm-store"
            ),
            store_url=(
                _empty_to_none(os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_URL"))
                or _empty_to_none(os.getenv("MOONCAKE_STORE_URL"))
            ),
            config_path=(
                _empty_to_none(os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_CONFIG"))
                or _empty_to_none(os.getenv("MOONCAKE_CONFIG_PATH"))
            ),
            timeout_s=timeout_s,
            key_prefix=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_KEY_PREFIX", "epd-mm-feature"),
            serialization_format=os.getenv(
                "MOONCAKE_EPD_FEATURE_HANDLE_SERIALIZATION",
                "raw_v2",
            ),
            http_binary_payload=_env_bool("MOONCAKE_EPD_FEATURE_HANDLE_HTTP_BINARY", False),
        )


def _empty_to_none(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _safe_key_component(value: Any) -> str:
    raw = str(value or "unknown")
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)[:220]


def parse_mooncake_feature_uri(uri: str) -> Tuple[str, str]:
    parsed = urlparse(str(uri))
    if parsed.scheme != "mooncake":
        raise ValueError(f"not a mooncake feature uri: {uri!r}")
    store_id = parsed.netloc
    key = parsed.path.lstrip("/")
    if not store_id or not key:
        raise ValueError(f"invalid mooncake feature uri: {uri!r}")
    return unquote(store_id), unquote(key)


def build_mooncake_feature_uri(store_id: str, key: str) -> str:
    return f"mooncake://{quote(str(store_id), safe='')}/{quote(str(key), safe='')}"


class MooncakeFeatureBundleStore:
    """FeatureBundle object store backed by real Mooncake Store APIs.

    Backend selection order:
    1. HTTP Mooncake store service (`MOONCAKE_STORE_URL`, `/api/put` + `/api/get`).
    2. Python `MooncakeDistributedStore` SDK configured by `MOONCAKE_CONFIG_PATH`
       or the standard Mooncake env vars.

    No filesystem fallback is implemented here; callers that want local dev
    transport should use `publish_feature_bundle_to_dir` explicitly.
    """

    def __init__(self, config: Optional[MooncakeFeatureBundleStoreConfig] = None):
        self.config = config or MooncakeFeatureBundleStoreConfig.from_env()
        self._session: Optional[requests.Session] = None
        self._store = None
        self._store_initialized = False

    @property
    def store_id(self) -> str:
        return self.config.store_id

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
        if self._store is not None:
            close = getattr(self._store, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            self._store = None
            self._store_initialized = False

    def publish_bundle(
        self,
        bundle: FeatureBundle,
        *,
        checksum: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
        key: Optional[str] = None,
    ) -> FeatureHandle:
        descriptor = bundle.descriptor(checksum=checksum)
        object_key = key or self.make_key(bundle.image_hash, descriptor=descriptor)
        payload = self._serialize_bundle(bundle, descriptor=descriptor)
        self._put_bytes(object_key, payload)
        md = dict(metadata or {})
        md.update(
            {
                "backend": "mooncake_store",
                "store_id": self.store_id,
                "object_key": object_key,
                "published_at_unix": time.time(),
                "payload_bytes": len(payload),
            }
        )
        return FeatureHandle(
            handle_id=f"mooncake-{_safe_key_component(bundle.image_hash)}-{int(time.time() * 1_000_000)}",
            feature_id=str(bundle.image_hash),
            store_id=self.store_id,
            uri=build_mooncake_feature_uri(self.store_id, object_key),
            descriptor=descriptor,
            metadata=md,
        )

    def load_bundle(self, uri_or_key: str) -> FeatureBundle:
        key = str(uri_or_key)
        if key.startswith("mooncake://"):
            store_id, parsed_key = parse_mooncake_feature_uri(key)
            if store_id != self.store_id:
                raise MooncakeFeatureStoreError(
                    f"Mooncake feature store mismatch: uri store={store_id}, configured={self.store_id}"
                )
            key = parsed_key
        payload = self._get_bytes(key)
        return self._deserialize_bundle(payload)

    def make_key(self, feature_id: str, *, descriptor=None) -> str:
        prefix = _safe_key_component(self.config.key_prefix)
        if descriptor is None:
            return f"{prefix}-{_safe_key_component(feature_id)}.ptb64"
        # Object keys must not be based on source image hash alone.  The same
        # image can be encoded by different model/processor versions, with or
        # without Qwen3-VL DeepStack tensors, or with different dtypes.  Several
        # real Mooncake Store deployments also keep the first value for a key
        # long enough that a same-key publish can resolve stale bytes.  Include
        # the descriptor digest in the object key so the control-plane handle is
        # content/schema-addressed while still preserving feature_id as the
        # cross-request reuse key.
        digest_payload = json.dumps(
            descriptor.to_dict(),
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hashlib.sha256(digest_payload).hexdigest()[:16]
        return f"{prefix}-{_safe_key_component(feature_id)}-v2-{digest}.ptb64"

    RAW_V2_MAGIC = b"MOONCAKE_EPD_FEATURE_BUNDLE_RAW_V2\n"

    def _serialize_bundle(self, bundle: FeatureBundle, *, checksum: bool = False, descriptor=None) -> bytes:
        descriptor = descriptor or bundle.descriptor(checksum=checksum)
        fmt = str(self.config.serialization_format or "raw_v2").strip().lower()
        if fmt in {"raw_v2", "raw", "v2"}:
            return self._serialize_bundle_raw_v2(bundle, descriptor=descriptor)
        if fmt not in {"torch_pickle", "pickle", "pt", "legacy"}:
            raise MooncakeFeatureStoreError(
                f"unsupported FeatureBundle serialization format: {self.config.serialization_format!r}"
            )
        return self._serialize_bundle_torch_pickle(bundle, descriptor=descriptor)

    @staticmethod
    def _serialize_bundle_torch_pickle(bundle: FeatureBundle, *, descriptor) -> bytes:
        buf = io.BytesIO()
        torch.save(
            {
                "version": 1,
                "kind": "mooncake_epd_feature_bundle",
                "bundle": bundle,
                "descriptor": descriptor.to_dict(),
                "written_at": time.time(),
            },
            buf,
        )
        return buf.getvalue()

    @staticmethod
    def _serialize_bundle_raw_v2(bundle: FeatureBundle, *, descriptor: FeatureBundleDescriptor) -> bytes:
        """Serialize tensors as a compact raw-byte container.

        The previous Mooncake Store path wrapped the entire FeatureBundle with
        ``torch.save`` and then base64 encoded it for the HTTP store API.  For
        large Qwen-VL hidden states, first resolve time was dominated by Python
        pickle/zip deserialization rather than network or validation.  Raw v2
        keeps the Store wire contract (ASCII-safe bytes) but replaces pickle
        with a small JSON header and contiguous tensor byte ranges.  The bytes
        can be stored directly by Mooncake SDK / binary HTTP, or base64-wrapped
        only when using the legacy JSON HTTP endpoint.
        """

        tensors: List[Tuple[str, Optional[int], torch.Tensor, TensorSpec]] = [
            ("last_hidden", None, bundle.last_hidden, descriptor.last_hidden),
        ]
        if bundle.grid_thw is not None and descriptor.grid_thw is not None:
            tensors.append(("grid_thw", None, bundle.grid_thw, descriptor.grid_thw))
        elif bundle.grid_thw is not descriptor.grid_thw:
            raise MooncakeFeatureStoreError("grid_thw descriptor mismatch")
        if len(bundle.intermediates) != len(descriptor.intermediates):
            raise MooncakeFeatureStoreError(
                f"intermediate descriptor count mismatch: bundle={len(bundle.intermediates)} "
                f"descriptor={len(descriptor.intermediates)}"
            )
        for ordinal, ((layer, tensor), (desc_layer, spec)) in enumerate(
            zip(bundle.intermediates, descriptor.intermediates)
        ):
            if int(layer) != int(desc_layer):
                raise MooncakeFeatureStoreError(
                    f"intermediate descriptor mismatch: bundle layer={layer} descriptor layer={desc_layer}"
                )
            tensors.append((f"intermediate:{int(layer)}:{ordinal}", int(layer), tensor, spec))

        offset = 0
        tensor_headers: List[Dict[str, Any]] = []
        raw_parts: List[bytes] = []
        for name, layer, tensor, spec in tensors:
            padding = (-offset) % 64
            if padding:
                raw_parts.append(b"\0" * padding)
                offset += padding
            cpu_tensor = tensor.detach().contiguous().cpu()
            raw = cpu_tensor.view(torch.uint8).numpy().tobytes()
            nbytes = len(raw)
            expected = int(spec.nbytes)
            if nbytes != expected:
                raise MooncakeFeatureStoreError(
                    f"{name} raw byte size mismatch: descriptor={expected} actual={nbytes}"
                )
            tensor_headers.append(
                {
                    "name": name,
                    "layer": layer,
                    "shape": list(spec.shape),
                    "dtype": spec.dtype,
                    "nbytes": nbytes,
                    "offset": offset,
                    "checksum": spec.checksum,
                }
            )
            raw_parts.append(raw)
            offset += nbytes

        header = {
            "version": 2,
            "kind": "mooncake_epd_feature_bundle_raw",
            "descriptor": descriptor.to_dict(),
            "image_hash": str(bundle.image_hash),
            "metadata": _json_sanitize(bundle.metadata),
            "written_at": time.time(),
            "total_tensor_bytes": offset,
            "tensors": tensor_headers,
        }
        header_bytes = json.dumps(
            header,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        blob = (
            MooncakeFeatureBundleStore.RAW_V2_MAGIC
            + struct.pack("<Q", len(header_bytes))
            + header_bytes
            + b"".join(raw_parts)
        )
        return blob

    @staticmethod
    def _deserialize_bundle(payload: bytes) -> FeatureBundle:
        raw = payload
        try:
            raw = base64.b64decode(payload.strip(), validate=True)
        except Exception:
            # Python SDK and the binary HTTP endpoint store raw bytes directly.
            pass
        if raw.startswith(MooncakeFeatureBundleStore.RAW_V2_MAGIC):
            return MooncakeFeatureBundleStore._deserialize_bundle_raw_v2(raw)
        loaded = torch.load(io.BytesIO(raw), map_location="cpu", weights_only=False)
        if isinstance(loaded, FeatureBundle):
            return loaded
        if isinstance(loaded, dict) and isinstance(loaded.get("bundle"), FeatureBundle):
            return loaded["bundle"]
        raise MooncakeFeatureStoreError("Mooncake object does not contain a FeatureBundle")

    @staticmethod
    def _deserialize_bundle_raw_v2(raw: bytes) -> FeatureBundle:
        prefix_len = len(MooncakeFeatureBundleStore.RAW_V2_MAGIC)
        if len(raw) < prefix_len + 8:
            raise MooncakeFeatureStoreError("truncated raw_v2 FeatureBundle header")
        header_len = struct.unpack("<Q", raw[prefix_len : prefix_len + 8])[0]
        header_start = prefix_len + 8
        header_end = header_start + int(header_len)
        if header_end > len(raw):
            raise MooncakeFeatureStoreError("raw_v2 FeatureBundle header exceeds payload size")
        try:
            header = json.loads(raw[header_start:header_end].decode("utf-8"))
        except Exception as exc:
            raise MooncakeFeatureStoreError("invalid raw_v2 FeatureBundle JSON header") from exc
        if int(header.get("version", 0) or 0) != 2:
            raise MooncakeFeatureStoreError(f"unsupported raw_v2 FeatureBundle version: {header.get('version')!r}")

        # Keep this as a zero-copy memoryview over the base64-decoded payload.
        # Converting to bytearray avoids PyTorch's non-writable-buffer warning
        # but costs hundreds of milliseconds for large Qwen-VL hidden states,
        # which is exactly the first-resolve bottleneck this format removes.
        data = memoryview(raw)[header_end:]
        descriptor = FeatureBundleDescriptor.from_dict(dict(header.get("descriptor") or {}))
        tensors = list(header.get("tensors") or [])
        if not tensors:
            raise MooncakeFeatureStoreError("raw_v2 FeatureBundle contains no tensors")

        tensor_map: Dict[str, torch.Tensor] = {}
        intermediate_layers: Dict[str, int] = {}
        for item in tensors:
            if not isinstance(item, dict):
                raise MooncakeFeatureStoreError("raw_v2 tensor entry is not an object")
            name = str(item.get("name") or "")
            if not name:
                raise MooncakeFeatureStoreError("raw_v2 tensor entry missing name")
            offset = int(item.get("offset", -1))
            nbytes = int(item.get("nbytes", -1))
            if offset < 0 or nbytes < 0 or offset + nbytes > len(data):
                raise MooncakeFeatureStoreError(
                    f"raw_v2 tensor {name} byte range invalid: offset={offset} nbytes={nbytes} payload={len(data)}"
                )
            shape = tuple(int(dim) for dim in (item.get("shape") or ()))
            dtype = _torch_dtype(str(item.get("dtype") or ""))
            expected_nbytes = _shape_numel(shape) * torch.empty((), dtype=dtype).element_size()
            if expected_nbytes != nbytes:
                raise MooncakeFeatureStoreError(
                    f"raw_v2 tensor {name} shape/dtype bytes mismatch: expected={expected_nbytes} nbytes={nbytes}"
                )
            count = _shape_numel(shape)
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="The given buffer is not writable.*",
                    category=UserWarning,
                )
                tensor = torch.frombuffer(data, dtype=dtype, count=count, offset=offset).reshape(shape)
            tensor_map[name] = tensor
            if name.startswith("intermediate:"):
                layer = item.get("layer")
                if layer is None:
                    try:
                        layer = int(name.split(":", 2)[1])
                    except Exception as exc:
                        raise MooncakeFeatureStoreError(f"raw_v2 tensor {name} missing layer") from exc
                intermediate_layers[name] = int(layer)

        last_hidden = tensor_map.get("last_hidden")
        if last_hidden is None:
            raise MooncakeFeatureStoreError("raw_v2 FeatureBundle missing last_hidden tensor")
        grid_thw = tensor_map.get("grid_thw")
        intermediates: List[Tuple[int, torch.Tensor]] = []
        for item in tensors:
            name = str(item.get("name") or "")
            if name.startswith("intermediate:"):
                intermediates.append((intermediate_layers[name], tensor_map[name]))
        bundle = FeatureBundle(
            image_hash=str(header.get("image_hash") or descriptor.feature_id),
            last_hidden=last_hidden,
            intermediates=intermediates,
            grid_thw=grid_thw,
            metadata=dict(header.get("metadata") or descriptor.metadata or {}),
        )
        return bundle

    def _put_bytes(self, key: str, payload: bytes) -> None:
        if self.config.store_url:
            session = self._http_session()
            if self.config.http_binary_payload:
                resp = session.put(
                    f"{self.config.store_url.rstrip('/')}/api/put_bytes/{quote(key, safe='')}",
                    data=payload,
                    headers={"content-type": "application/octet-stream"},
                    timeout=self.config.timeout_s,
                )
                resp.raise_for_status()
                return
            resp = session.put(
                f"{self.config.store_url.rstrip('/')}/api/put",
                json={"key": key, "value": base64.b64encode(payload).decode("ascii")},
                timeout=self.config.timeout_s,
            )
            resp.raise_for_status()
            return
        store = self._python_store()
        rc = store.put(key, payload)
        if rc != 0:
            raise MooncakeFeatureStoreError(f"MooncakeDistributedStore.put failed: rc={rc}, key={key}")

    def _get_bytes(self, key: str) -> bytes:
        if self.config.store_url:
            session = self._http_session()
            resp = session.get(
                f"{self.config.store_url.rstrip('/')}/api/get/{quote(key, safe='')}",
                timeout=self.config.timeout_s,
            )
            resp.raise_for_status()
            if not resp.content:
                raise MooncakeFeatureStoreError(f"Mooncake HTTP store returned empty object: {key}")
            return resp.content
        store = self._python_store()
        payload = store.get(key)
        if not payload:
            raise MooncakeFeatureStoreError(f"MooncakeDistributedStore missing object: {key}")
        return bytes(payload)

    def _http_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.trust_env = False
        return self._session

    def _python_store(self):
        if self._store_initialized and self._store is not None:
            return self._store
        try:
            from mooncake.store import MooncakeDistributedStore
        except Exception as exc:
            raise MooncakeFeatureStoreError(
                "Mooncake Python SDK is unavailable and no MOONCAKE_STORE_URL was configured"
            ) from exc
        store = MooncakeDistributedStore()
        config = self._load_store_config()
        if config is None:
            raise MooncakeFeatureStoreError(
                "Mooncake store config is missing; set MOONCAKE_STORE_URL or MOONCAKE_CONFIG_PATH/standard Mooncake env vars"
            )
        setup = getattr(store, "setup")
        try:
            rc = setup(config)
        except TypeError:
            rc = setup(
                config["local_hostname"],
                config["metadata_server"],
                int(config["global_segment_size"]),
                int(config["local_buffer_size"]),
                config["protocol"],
                config.get("device_name", ""),
                config["master_server_address"],
            )
        if rc != 0:
            raise MooncakeFeatureStoreError(f"MooncakeDistributedStore.setup failed: rc={rc}")
        self._store = store
        self._store_initialized = True
        return store

    def _load_store_config(self) -> Optional[Dict[str, Any]]:
        path = self.config.config_path
        if path and Path(path).expanduser().exists():
            return json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
        metadata_server = os.getenv("MOONCAKE_TE_META_DATA_SERVER")
        master_server = os.getenv("MOONCAKE_MASTER")
        if not metadata_server or not master_server:
            return None
        return {
            "local_hostname": os.getenv("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1"),
            "metadata_server": metadata_server,
            "global_segment_size": int(os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", str(16 * 1024 * 1024))),
            "local_buffer_size": int(os.getenv("MOONCAKE_LOCAL_BUFFER_SIZE", str(16 * 1024 * 1024))),
            "protocol": os.getenv("MOONCAKE_PROTOCOL", "tcp"),
            "device_name": os.getenv("MOONCAKE_DEVICE_NAME", ""),
            "master_server_address": master_server,
        }


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() not in {"", "0", "false", "no", "off"}


def _torch_dtype(value: str) -> torch.dtype:
    dtype = str(value).replace("torch.", "")
    aliases = {
        "float": torch.float32,
        "float32": torch.float32,
        "float16": torch.float16,
        "half": torch.float16,
        "bfloat16": torch.bfloat16,
        "float64": torch.float64,
        "double": torch.float64,
        "int64": torch.int64,
        "long": torch.int64,
        "int32": torch.int32,
        "int": torch.int32,
        "int16": torch.int16,
        "short": torch.int16,
        "int8": torch.int8,
        "uint8": torch.uint8,
        "bool": torch.bool,
    }
    if dtype not in aliases:
        raise MooncakeFeatureStoreError(f"unsupported raw_v2 tensor dtype: {value!r}")
    return aliases[dtype]


def _shape_numel(shape: Tuple[int, ...]) -> int:
    n = 1
    for dim in shape:
        if int(dim) < 0:
            raise MooncakeFeatureStoreError(f"invalid negative raw_v2 tensor dimension: {shape}")
        n *= int(dim)
    return int(n)


def _json_sanitize(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_sanitize(item) for item in value]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(key, str):
                out[key] = _json_sanitize(item)
        return out
    return str(value)
