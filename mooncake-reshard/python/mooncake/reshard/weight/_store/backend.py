"""Fail-closed adapter for the native Mooncake Store object boundary."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from importlib import import_module
from typing import Literal, Optional, Protocol, cast

from ..._typing import TypeAlias

from .errors import WeightStoreError


StoreRecordType: TypeAlias = Literal["payload", "metadata"]
StoreConfigFactory: TypeAlias = Callable[[Sequence[str], StoreRecordType], object]
RangeResults: TypeAlias = tuple[tuple[tuple[int, ...], ...], ...]


class _NativeReplicateConfig(Protocol):
    group_ids: list[str]
    with_hard_pin: bool
    data_type: object


def default_config_factory(
    group_ids: Sequence[str], record_type: StoreRecordType
) -> object:
    """Build a native replication config without importing it in canonical code."""

    try:
        module = import_module("mooncake.store")
        replicate_config_type = getattr(module, "ReplicateConfig")
        object_data_type = getattr(module, "ObjectDataType")
    except (ImportError, AttributeError) as error:
        raise WeightStoreError(
            "native Mooncake Store configuration is unavailable"
        ) from error
    if not callable(replicate_config_type):
        raise WeightStoreError("native ReplicateConfig constructor is invalid")
    try:
        config = cast(_NativeReplicateConfig, replicate_config_type())
        config.group_ids = list(group_ids)
    except AttributeError as error:
        raise WeightStoreError(
            "native Mooncake Store must expose ReplicateConfig.group_ids"
        ) from error
    try:
        config.with_hard_pin = True
        if record_type == "payload":
            config.data_type = object_data_type.WEIGHT
        else:
            config.data_type = object_data_type.METADATA
    except (AttributeError, TypeError, ValueError) as error:
        raise WeightStoreError(
            "native Mooncake Store configuration is invalid"
        ) from error
    return config


class StoreBackend:
    """Normalize the raw Store API to checked operations used by weight flows."""

    def __init__(self, raw: object) -> None:
        self._raw = raw

    def get(self, key: str) -> bytes:
        result = self._call("get", key)
        if isinstance(result, bytearray):
            return bytes(result)
        if isinstance(result, bytes):
            return result
        raise WeightStoreError(f"get returned invalid payload for {key}")

    def put(self, key: str, value: bytes, config: object) -> int:
        return self._status("put", key, value, config)

    def remove(self, key: str, *, force: bool) -> int:
        return self._status("remove", key, force=force)

    def is_exist(self, key: str) -> int:
        result = self._call("is_exist", key)
        if type(result) is not int:
            raise WeightStoreError(f"is_exist returned invalid status for {key}")
        return result

    def batch_is_exist(self, keys: Sequence[str]) -> Optional[tuple[int, ...]]:
        candidate = self._optional_method("batch_is_exist")
        if candidate is None:
            return None
        result = self._invoke(candidate, list(keys), operation="batch_is_exist")
        if type(result) is int:
            raise WeightStoreError(f"existence check failed: {result}")
        return self._int_sequence(result, "batch_is_exist")

    def batch_put_from(
        self,
        keys: Sequence[str],
        addresses: Sequence[int],
        sizes: Sequence[int],
        config: object,
    ) -> tuple[int, ...]:
        result = self._call(
            "batch_put_from",
            list(keys),
            list(addresses),
            list(sizes),
            config,
        )
        return self._int_sequence(result, "batch_put_from")

    def register_buffer(self, address: int, nbytes: int) -> int:
        return self._status("register_buffer", address, nbytes)

    def unregister_buffer(self, address: int) -> int:
        return self._status("unregister_buffer", address)

    def get_into_ranges(
        self,
        addresses: Sequence[int],
        all_keys: Sequence[Sequence[str]],
        all_target_offsets: Sequence[Sequence[Sequence[int]]],
        all_source_offsets: Sequence[Sequence[Sequence[int]]],
        all_sizes: Sequence[Sequence[Sequence[int]]],
    ) -> RangeResults:
        result = self._call(
            "get_into_ranges",
            list(addresses),
            [list(keys) for keys in all_keys],
            [[list(offsets) for offsets in groups] for groups in all_target_offsets],
            [[list(offsets) for offsets in groups] for groups in all_source_offsets],
            [[list(sizes) for sizes in groups] for groups in all_sizes],
        )
        return self._range_results(result)

    def _status(self, method_name: str, *args: object, **kwargs: object) -> int:
        result = self._call(method_name, *args, **kwargs)
        if type(result) is not int:
            raise WeightStoreError(f"{method_name} returned an invalid status")
        return result

    def _call(self, method_name: str, *args: object, **kwargs: object) -> object:
        method = self._required_method(method_name)
        return self._invoke(method, *args, operation=method_name, **kwargs)

    def _required_method(self, method_name: str) -> Callable[..., object]:
        method = self._optional_method(method_name)
        if method is None:
            raise WeightStoreError(f"Store backend does not implement {method_name}")
        return method

    def _optional_method(
        self,
        method_name: str,
    ) -> Optional[Callable[..., object]]:
        try:
            candidate = getattr(self._raw, method_name, None)
        except Exception as error:
            raise WeightStoreError(
                f"failed to access Store method {method_name}"
            ) from error
        if candidate is None:
            return None
        if not callable(candidate):
            raise WeightStoreError(f"Store attribute {method_name} is not callable")
        return candidate

    @staticmethod
    def _invoke(
        method: Callable[..., object],
        *args: object,
        operation: str,
        **kwargs: object,
    ) -> object:
        try:
            return method(*args, **kwargs)
        except Exception as error:
            raise WeightStoreError(f"{operation} failed: {error}") from error

    @staticmethod
    def _int_sequence(value: object, operation: str) -> tuple[int, ...]:
        if type(value) is int:
            raise WeightStoreError(f"{operation} failed: {value}")
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(
            value, Sequence
        ):
            raise WeightStoreError(f"{operation} returned invalid result sequence")
        values = tuple(cast(Sequence[object], value))
        if any(type(item) is not int for item in values):
            raise WeightStoreError(f"{operation} returned non-integer status")
        return cast(tuple[int, ...], values)

    @classmethod
    def _range_results(cls, value: object) -> RangeResults:
        if type(value) is int:
            raise WeightStoreError(f"get_into_ranges failed: {value}")
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(
            value, Sequence
        ):
            raise WeightStoreError("get_into_ranges returned invalid buffer result")
        buffers: list[tuple[tuple[int, ...], ...]] = []
        for buffer_result in cast(Sequence[object], value):
            buffers.append(cls._range_buffer_result(buffer_result))
        return tuple(buffers)

    @staticmethod
    def _range_buffer_result(value: object) -> tuple[tuple[int, ...], ...]:
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(
            value, Sequence
        ):
            raise WeightStoreError("get_into_ranges returned invalid object result")
        groups: list[tuple[int, ...]] = []
        for group in cast(Sequence[object], value):
            groups.append(StoreBackend._int_sequence(group, "get_into_ranges"))
        return tuple(groups)


__all__ = [
    "RangeResults",
    "StoreBackend",
    "StoreConfigFactory",
    "StoreRecordType",
    "default_config_factory",
]
