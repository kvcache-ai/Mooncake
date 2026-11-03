# RFC: Unified V2 BlockIndex

Status: the workspace owner approved implementation and explicitly accepted
this local RFC in place of a GitHub issue because the connected integration
returns HTTP 403 when creating issues.

## Problem

V2 tilers maintain authoritative block visibility and eviction ordering in
separate components. Mutations update both components, and reconciliation is
needed to repair disagreements.

## Design

Make BlockIndex an abstract interface for insert, lookup, identity-checked
removal, access recording, victim selection, snapshots, draining, and statistics.
CreateBlockIndex chooses the backend at initialization; TilerManager accepts an
injected unique_ptr<BlockIndex>.

OrderedBlockIndex stores each registration in one sharded node container.
Each node owns an immutable BlockEntry snapshot, heat metadata, and intrusive
ordering links. Preserve lru, multi_lru, and tinylfu_lru, with tinylfu_lru as the
default. Lookups take only their shard's shared lock. Mutations take the ordering
lock followed by one shard lock; victim selection takes the ordering lock.
Publish visibility and ordering together and roll back allocation failures.
Release physical resources outside index locks. Weak BlockToken candidates
continue to require BlockId validation before deletion.

Remove the separate EvictionIndex and reconciliation protocol. Merge settings
into v2.block_index with field-level tiers[].block_index overrides. Reject
removed eviction_index and placement sketch/band settings with migration
messages. Changes affect V2 only; runtime backend swapping is out of scope.

## Validation

Exercise all three ordering strategies through the same index contract tests.
Cover stale tokens, delayed resource release, bounded selection, allocation
failure rollback, concurrent mutation, configuration defaults and overrides,
and configuration migration errors. Run V2 component tests and DataManager
contract and differential tests in a fresh build directory.
