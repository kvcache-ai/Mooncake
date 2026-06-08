# Module-Specific Guidance

When modifying files in specific modules, you MUST read the relevant design
documents and skill files BEFORE making changes.

## Transfer Engine (`mooncake-transfer-engine/`)
- Read: `docs/source/design/architecture.md`
- Read: `.claude/skills/mooncake-troubleshoot/SKILL.md` (for common issues)
- The transport layer is pluggable (RDMA, TCP, EFA, Ascend, HIP). Changes to
  one transport must not break others.

## Store (`mooncake-store/`)
- Read: `docs/source/design/mooncake-store.md`
- Read: `docs/source/design/hicache-design.md`
- The store has HA (high availability) components — test failover scenarios.

## EP (`mooncake-ep/`)
- Expert parallelism is performance-critical. Profile before and after changes.

## PG (`mooncake-pg/`)
- PyTorch process group backend. Must be compatible with PyTorch distributed APIs.
- Test with both NCCL and Gloo backends.

## Wheel (`mooncake-wheel/`)
- Read: `.claude/skills/mooncake-api/SKILL.md`
- Python API surface — changes here affect all downstream users.
- Run `python scripts/test_tensor_api.py` after any API changes.

## Integration (`mooncake-integration/`)
- Integration with vLLM and SGLang. Changes must be tested against the
  specific inference engine version documented in the integration guide.

## Common (`mooncake-common/`)
- Shared by all modules. Changes here have wide blast radius.
- Run full test suite: `bash scripts/run_ci_test.sh`
