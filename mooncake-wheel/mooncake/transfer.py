"""Declarative Python helpers for Mooncake TransferEngine transfers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TransferEndpoint:
    """A local or remote memory range used by ``transfer``."""

    address: int
    length: Optional[int] = None
    hostname: Optional[str] = None

    @classmethod
    def local(cls, address: int, length: Optional[int] = None) -> "TransferEndpoint":
        return cls(address=address, length=length)

    @classmethod
    def remote(
        cls, hostname: str, address: int, length: Optional[int] = None
    ) -> "TransferEndpoint":
        return cls(address=address, length=length, hostname=hostname)

    def __post_init__(self) -> None:
        if not _is_non_bool_int(self.address) or self.address < 0:
            raise ValueError("endpoint address must be a non-negative integer")

        if self.length is not None:
            if not _is_non_bool_int(self.length) or self.length < 0:
                raise ValueError("endpoint length must be a non-negative integer")

        if self.hostname is not None:
            if not isinstance(self.hostname, str) or not self.hostname:
                raise ValueError("remote endpoint hostname must be a non-empty string")

    @property
    def is_remote(self) -> bool:
        return self.hostname is not None

    @property
    def is_local(self) -> bool:
        return self.hostname is None


@dataclass(frozen=True)
class TransferRequest:
    """A declarative synchronous transfer request."""

    src: TransferEndpoint
    dst: TransferEndpoint
    length: Optional[int] = None
    transport_hint: str = ""

    def execute(self, engine):
        return transfer(
            self.src,
            self.dst,
            engine=engine,
            length=self.length,
            transport_hint=self.transport_hint,
        )


def local_buffer(address: int, length: Optional[int] = None) -> TransferEndpoint:
    """Create a local endpoint from a registered local memory address."""

    return TransferEndpoint.local(address, length)


def remote_buffer(
    hostname: str, address: int, length: Optional[int] = None
) -> TransferEndpoint:
    """Create a remote endpoint from a peer hostname and memory address."""

    return TransferEndpoint.remote(hostname, address, length)


def transfer(
    src: TransferEndpoint,
    dst: TransferEndpoint,
    *,
    engine,
    length: Optional[int] = None,
    transport_hint: str = "",
):
    """Synchronously transfer one memory range between local and remote endpoints.

    ``src`` and ``dst`` must contain exactly one local endpoint and one remote
    endpoint. Local-to-remote transfers call ``transfer_sync_write``; remote-to-
    local transfers call ``transfer_sync_read``.
    """

    if not isinstance(src, TransferEndpoint) or not isinstance(dst, TransferEndpoint):
        raise TypeError("src and dst must be TransferEndpoint instances")

    resolved_length = _resolve_length(src, dst, length)

    if src.is_local and dst.is_remote:
        return engine.transfer_sync_write(
            dst.hostname,
            src.address,
            dst.address,
            resolved_length,
            transport_hint,
        )

    if src.is_remote and dst.is_local:
        return engine.transfer_sync_read(
            src.hostname,
            dst.address,
            src.address,
            resolved_length,
            transport_hint,
        )

    raise ValueError("transfer requires exactly one remote endpoint")


def _resolve_length(
    src: TransferEndpoint,
    dst: TransferEndpoint,
    explicit_length: Optional[int],
) -> int:
    lengths = [
        value for value in (explicit_length, src.length, dst.length) if value is not None
    ]

    if not lengths:
        raise ValueError("transfer length must be provided")

    for value in lengths:
        if not _is_non_bool_int(value) or value <= 0:
            raise ValueError("transfer length must be a positive integer")

    if len(set(lengths)) != 1:
        raise ValueError("transfer has conflicting endpoint lengths")

    return lengths[0]


def _is_non_bool_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


__all__ = [
    "TransferEndpoint",
    "TransferRequest",
    "local_buffer",
    "remote_buffer",
    "transfer",
]
