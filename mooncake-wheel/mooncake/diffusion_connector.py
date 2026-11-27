"""Connector for moving tensors between disaggregated diffusion pipeline stages.

Provides producer/consumer primitives for passing stage outputs (text/image
embeddings, denoising latents, checkpoint tensors) between the encode,
denoise, and decode stages of a disaggregated diffusion pipeline, mirroring
the producer/consumer split of the vLLM ``MooncakeConnector``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Literal, Mapping, Optional

VALID_ROLES = ("encode", "denoise", "decode")
VALID_TRANSFER_MODES = ("p2p", "store", "auto")
VALID_PAYLOAD_KINDS = ("embedding", "latent", "checkpoint")

ConnectorRole = Literal[VALID_ROLES]
TransferMode = Literal[VALID_TRANSFER_MODES]
PayloadKind = Literal[VALID_PAYLOAD_KINDS]


@dataclass(frozen=True)
class StagePayload:
    """One stage output a producer wants to make available to a consumer.

    Attributes:
        request_id: End-to-end request identifier; the same id must be used
            by every stage that publishes or consumes tensors for a request.
        kind: What the tensor holds (``embedding``, ``latent``, or
            ``checkpoint``); consumers use it to route the payload to the
            right stage input.
        tensor: The producer's real inference output tensor. It is published
            in place (registered with the transfer engine, no staging copy),
            so it must stay alive and unmodified until the lease expires or
            the payload is consumed.
        chunk_dim: Dimension along which the tensor may be split for chunked
            transfer, or ``None`` for whole-tensor transfer. Chunking itself
            happens at the transfer engine level; the connector just passes
            these parameters through.
        num_chunks: Number of chunks along ``chunk_dim``; must be 1 when
            ``chunk_dim`` is ``None``.
        lease: Lease duration in seconds after which an unconsumed payload
            may be reclaimed. Consumed payloads are deleted on consume
            (delete-on-consume lease semantics).
    """

    request_id: str
    kind: PayloadKind
    tensor: Any
    chunk_dim: Optional[int] = None
    num_chunks: int = 1
    lease: Optional[float] = None

    def __post_init__(self) -> None:
        if self.kind not in VALID_PAYLOAD_KINDS:
            raise ValueError(
                f"invalid payload kind {self.kind!r}, "
                f"expected one of {VALID_PAYLOAD_KINDS}"
            )
        if self.chunk_dim is None and self.num_chunks != 1:
            raise ValueError(
                "num_chunks must be 1 for whole-tensor payloads (chunk_dim=None)"
            )
        if self.num_chunks < 1:
            raise ValueError(f"num_chunks must be >= 1, got {self.num_chunks}")
        if self.lease is not None and self.lease <= 0:
            raise ValueError(f"lease must be > 0, got {self.lease}")


@dataclass(frozen=True)
class P2PSegmentRef:
    """Location of a published tensor inside a producer's exposed segment."""

    segment: str
    addr: int
    nbytes: int


@dataclass(frozen=True)
class StageHandle:
    """Transport handle returned by publish() and redeemed by consume().

    A handle carries everything a consumer on another node needs to pull the
    payload: either the Store key (store mode) or the producer's P2P segment
    reference (p2p mode), plus the tensor metadata needed to validate the
    destination buffer.

    Attributes:
        request_id: Request the payload belongs to.
        kind: Payload kind, mirrored from the published ``StagePayload``.
        mode: Transport mode the payload was published with; ``auto``
            resolves to a concrete mode at publish time.
        store_key: Mooncake Store object key; set iff ``mode == "store"``.
        segment: Producer segment reference; set iff ``mode == "p2p"``.
        tensor_metadata: Packed ``TensorMetadata`` struct as defined in
            ``mooncake-integration/integration_utils.h`` (dtype, shape, and
            layout; no device field).
        chunk_dim: Chunking dimension mirrored from the payload, so the
            consumer can iterate chunk views.
        num_chunks: Number of chunks mirrored from the payload.
        lease: Lease duration in seconds mirrored from the payload.
    """

    request_id: str
    kind: PayloadKind
    mode: Literal["p2p", "store"]
    store_key: Optional[str] = None
    segment: Optional[P2PSegmentRef] = None
    tensor_metadata: Optional[bytes] = None
    chunk_dim: Optional[int] = None
    num_chunks: int = 1
    lease: Optional[float] = None


class DiffusionConnector:
    """Producer/consumer connector for one diffusion pipeline stage.

    Each stage process creates one connector for its role. Producers call
    :meth:`publish` on their stage outputs; consumers redeem the returned
    :class:`StageHandle` with :meth:`consume`.
    """

    def __init__(
        self,
        role: ConnectorRole,
        transfer_mode: TransferMode = "auto",
        store_config: Optional[Mapping[str, Any]] = None,
        te_config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Initialize a connector for one pipeline stage.

        Args:
            role: Pipeline stage this process runs (``encode``, ``denoise``,
                or ``decode``).
            transfer_mode: ``p2p`` for direct producer-to-consumer transfer,
                ``store`` to relay through Mooncake Store, or ``auto`` to
                select per payload (see :meth:`_select_transfer_mode`).
            store_config: Mooncake Store connection settings; required for
                ``store`` mode and for ``auto`` mode (which may fall back to
                Store-relay).
            te_config: Transfer Engine settings (hostname, metadata server,
                protocol); required for ``p2p`` and ``auto`` modes.
        """
        if role not in VALID_ROLES:
            raise ValueError(f"invalid role {role!r}, expected one of {VALID_ROLES}")
        if transfer_mode not in VALID_TRANSFER_MODES:
            raise ValueError(
                f"invalid transfer_mode {transfer_mode!r}, "
                f"expected one of {VALID_TRANSFER_MODES}"
            )
        self.role = role
        self.transfer_mode = transfer_mode
        self.store_config = store_config
        self.te_config = te_config

    def publish(self, payload: StagePayload) -> StageHandle:
        """Make a stage output available to consumers and return its handle.

        Producer-side contract: the payload's tensor is the producer's real
        inference output buffer. It is registered with the transfer engine
        (or handed to the Store zero-copy path) in place — no staging copy is
        made — so the producer must keep it alive and unmodified until the
        payload is consumed or its lease expires.

        Args:
            payload: The stage output to publish.

        Returns:
            A :class:`StageHandle` that can be serialized and sent to a
            consumer on another node; it carries the Store key or P2P segment
            reference plus the tensor metadata.
        """
        raise NotImplementedError

    def consume(self, handle: StageHandle, dst: Any) -> Iterator[Any]:
        """Pull a published payload into a caller-provided destination tensor.

        Consumer-side contract: ``dst`` must be a pre-allocated,
        pre-registered device tensor matching the handle's tensor metadata.
        Data lands directly in ``dst``; the returned iterator yields views
        into ``dst`` (one per chunk, a single view for whole-tensor
        transfers), never copies. Consuming releases the payload on the
        producer side (delete-on-consume lease semantics).

        Args:
            handle: Handle produced by :meth:`publish`, possibly on another
                node.
            dst: Pre-allocated, pre-registered destination tensor.

        Yields:
            Views into ``dst``, one per transferred chunk.
        """
        raise NotImplementedError

    def _select_transfer_mode(
        self,
        payload: StagePayload,
        consumer_known: bool,
        consumer_has_capacity: bool,
    ) -> Literal["p2p", "store"]:
        """Resolve ``auto`` mode to a concrete transport for one payload.

        P2P is chosen when the consumer is known at schedule time and has
        capacity to receive. Store-relay is chosen whenever producer and
        consumer lifetimes must decouple: queue dispatch, unknown consumer,
        elastic worker pools, or retry-on-failure.
        """
        if self.transfer_mode != "auto":
            return self.transfer_mode
        if consumer_known and consumer_has_capacity:
            return "p2p"
        return "store"
