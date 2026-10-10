# Store-RS Cold-Tier Validation

The cold-tier architecture and current runtime behavior are described in the
[design page](../../design/store/store-rs/cold-tier-design.md). Admin device
endpoint details are in the
[HTTP API reference](../../api-reference/http/store-rs-admin.md).

## SSD end-to-end validation record

The current e2e binary now validates the directory-backed SSD flow end to end:

- explicit SSD cold tier target wiring through `ColdTierTargetConfig::directory(...)`
- hot write -> pending cold backing -> materialized cold backing transition
- backend object materialization on disk
- overwrite path with new cold backing identity
- tenant-scoped delete and backend cleanup
- benchmark phases followed by benchmark object cleanup so later shrink validation is not polluted by leftover benchmark routes
- true client shrink validation with convergence-aware waiting in the full e2e environment

This design record reports a full local validation with a directory-backed SSD
target. For current Store-RS validation entry points and hardware-specific
scenarios, see the [test and validation guide](testing.md).
