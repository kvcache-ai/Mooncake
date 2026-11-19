"""Compatibility exports for the shared structured-object implementation."""

from mooncake.structured_object_store import (
    AUTO_PARALLEL_MIN_BYTES,
    AUTO_PARALLEL_MIN_CHUNKS,
    DEFAULT_BUNDLE_CHUNK_BYTES,
    MISSING_OBJECT_ERROR,
    STRUCTURED_FIELD_SPECS_KEY,
    BundleStore,
    BundleTransferPolicy,
    MooncakeBundleTransfer,
    RemoteBundleRef,
    StructuredMemberSlice,
    StructuredObjectPayload,
    StructuredObjectReadSpec,
    StructuredObjectResult,
)

__all__ = [
    "AUTO_PARALLEL_MIN_BYTES",
    "AUTO_PARALLEL_MIN_CHUNKS",
    "DEFAULT_BUNDLE_CHUNK_BYTES",
    "MISSING_OBJECT_ERROR",
    "STRUCTURED_FIELD_SPECS_KEY",
    "BundleStore",
    "BundleTransferPolicy",
    "MooncakeBundleTransfer",
    "RemoteBundleRef",
    "StructuredMemberSlice",
    "StructuredObjectPayload",
    "StructuredObjectReadSpec",
    "StructuredObjectResult",
]
