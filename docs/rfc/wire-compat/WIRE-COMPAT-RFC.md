# RFC: A Frozen Wire Contract and a Mechanical Wire-Compat CI Gate for Mooncake Store

Status: Draft (for discussion)
Scope: Mooncake Store master <-> client coro_rpc surface, version handshake, on-disk formats
Relationship to prior work: **Complement to #1920 ([RFC]: Rolling Upgrade Support for Mooncake Store).** This RFC does not restate or replace #1920. #1920 defines the *policy and feature set* for rolling upgrades (forward-compatible serialization, graceful step-down, version-aware drain, graceful shutdown, reconnection robustness). This RFC fills the *enforcement and definition* gaps that #1920 explicitly leaves to "code review": a machine-checkable definition of the wire contract, a CI gate that detects wire breaks before merge, a version-gate redesign, and a reusable cross-version interop test methodology.

---

## 1. Motivation

#1920 makes the case for rolling upgrades and proposes the right *direction* (additive-only `struct_pack::compatible<T>`, a relaxed version check, a `MOONCAKE_PROTOCOL_VERSION`). But its compatibility guarantee currently rests on human discipline. From #1920's own Risks table:

> | `compatible<T>` only supports additive changes | Enforce via code review; breaking changes require major version bump |

"Enforce via code review" is not enforcement. The Mooncake master speaks a binary protocol whose RPC identity is derived implicitly by the compiler:

- **coro_rpc dispatch** keys on `function_id = MD5Hash32(qualified-function-name)` (`ylt/util/utils.hpp`, `consteval func_id()`; name from `coro_rpc::get_func_name<&func>()` = the fully-qualified `mooncake::WrappedMasterService::<Method>`).
- **struct_pack argument framing** keys on `arg_type_code = MD5Hash32(type-layout-literal) & 0xFFFFFFFE` (`ylt/struct_pack/type_calculate.hpp:515`, `get_types_code_impl`).

Both are computed from source *layout*, not from any declared version. A developer can break the wire with an edit that looks completely innocent and compiles cleanly:

- adding a field to a struct that appears in an argument tuple (shifts `arg_type_code`),
- adding/removing/reordering a handler argument (shifts `arg_type_code`),
- renaming a registered RPC method (shifts `function_id`).

This already happened in this codebase. #2288 ("Propagate tenant identity through object RPCs") appended a bare trailing `tenant_id` to ~30 handlers and shifted their argument type-codes, e.g. `PutStart 0xfad0c534 -> 0x22f8edba`. A v0.3.11 peer and a #2288 master then hard-fail each other's requests with `errc::invalid_rpc_arguments`. Nothing in CI caught it; it was found by hand.

We have built the missing pieces while validating a byte-compatible reimplementation of the master, and we want to contribute them upstream as a complement to #1920.

---

## 2. Gaps in #1920 this RFC fills

Mapping to a complete wire-compat story. "Covered" = #1920 already handles it; the rest are this RFC's contribution.

| Item | Need | #1920 status | This RFC |
|---|---|---|---|
| (a) Version-gate granularity | A gate that allows compatible upgrades AND catches same-version wire drift | PARTIAL: proposes major-version relax + `MOONCAKE_PROTOCOL_VERSION`, but a hand-edited constant has the same blind spot as the 2.0.0 string | **Fills**: derive the contract from the code, not a hand-edited constant (Section 4 + 5) |
| (b) Frozen wire-contract definition | An explicit list of function-ids + arg type-codes + struct layouts + on-disk formats | NOT COVERED | **Fills**: Section 3 + the checked-in golden file |
| (c) CI gate detecting wire breaks pre-merge | Mechanical pre-merge check | NOT COVERED ("enforce via code review") | **Fills**: Section 5 + `check_wire_contract.sh` |
| (d) Evolution rule | "additive-only", and "never rename a registered RPC" | PARTIAL: additive policy stated, no enforcement; rename-is-a-break not called out | **Fills**: Section 6, enforced by the gate |
| (f) Cross-version interop test methodology | A repeatable way to prove old<->new bytes round-trip | NOT COVERED | **Fills**: Section 7 (the #2551 methodology, generalized) |
| (e) Metadata schema versioning, (g) version-skew window | OpLog/snapshot schema evolution; how far apart peers may be | PARTIAL / NOT | **Scoped** in Section 8; not the core of this RFC |

#1920 keeps ownership of the rolling-upgrade *features*. This RFC owns the *contract and its enforcement*.

---

## 3. The frozen wire-contract surface

A gate must pin every surface whose change is observable by a peer of a different build. For Mooncake Store master these are:

### 3.1 coro_rpc handlers (function-id + arg type-code)

The 59 handlers registered in `mooncake-store/src/rpc_service.cpp` `RegisterRpcService()` (v0.3.11.post1, commit e9c6107). Each row of the golden file (`gate/wire_contract_golden.txt`) pins `(name, function_id, arg_type_code)`. The function-ids are real coro_rpc dispatch keys; the arg type-codes are real `struct_pack::get_type_code`. Representative rows (full list in the golden):

```
ExistKey                   0x89385f09 0x9dcffa76
PutStart                   0x65ea9ac1 0x3d15a970
PutEnd                     0x8e8b2099 0x61232de0
BatchPutStart              0x56be1822 0x955f4c96
MountSegment               0x3e7ec4bb OMITTED   (arg uses Segment struct; pinned by in-build gate)
Ping                       0xd6c2dae5 0x3c6d83e0
ServiceReady               0x7b81bb56 OMITTED   (no-arg handshake; see 3.3)
EvictDiskReplica           0xd00ac42b 0x61232de0
```

`OMITTED` marks handlers whose argument types use heavier production structs (`Segment`, `*Request`, `*Response`); these are pinned by the in-build production gate (`gate/wire_contract_test.cpp`) that includes the real headers. The standalone generator pins the 47 self-contained ones plus all 59 function-ids.

Note the live `PutStart` code is `0x3d15a970`, not the `0xfad0c534` recorded during the #2288 investigation. That difference is real: later field additions to `ReplicateConfig` (NoF metadata #2143, then `prefer_alloc_in_same_node` / `data_type` / `group_ids`) silently shifted `PutStart`'s argument type-code. This is precisely the drift the gate makes visible.

### 3.2 On-wire structs serialized by struct_pack

Structs that appear inside argument tuples or responses and are framed by struct_pack. Their layout is part of the contract because it feeds the arg type-code:

- `Replica::Descriptor` (response payloads of `PutStart`/`GetReplicaList`/...),
- `ReplicateConfig` (`mooncake-store/include/replica.h:81`) -- the #2143 field additions changed it,
- `Segment` / `NoFSegment` (mount/remount tuples),
- `ReplicaCopyPayload` / `ReplicaMovePayload` (the #2288 struct-side break),
- request/response structs: `GetReplicaListResponse`, `PingResponse`, `GetStorageConfigResponse`, `PromotionAllocStartResponse`, `CopyStartResponse`, `MoveStartResponse`, `CreateDrainJobRequest`, `QueryJobResponse`, `TaskCompleteRequest`, `TaskAssignment`, `StorageObjectMetadata`, `SegmentStatus`.

The in-build gate pins these by computing `get_type_code` over each as used in its handler tuple.

### 3.3 Version handshake

`MasterClient::Connect` calls the no-argument `ServiceReady` RPC (`mooncake-store/src/master_client.cpp:425`) and compares strings (`:433`):

```cpp
std::string server_version = result.value();
std::string client_version = GetMooncakeStoreVersion();
if (server_version != client_version) {
    ... return ErrorCode::INVALID_VERSION;
}
```

`ServiceReady` itself returns `GetMooncakeStoreVersion()` (`rpc_service.cpp:1726`), and `MOONCAKE_STORE_VERSION` is `2.0.0`, hardcoded at `mooncake-store/CMakeLists.txt:1` (`project(MooncakeStore VERSION 2.0.0)`). `ServiceReady` is a no-arg RPC (function-id `0x7b81bb56`), so the handshake itself never changes shape -- the gate and the version-gate redesign (Section 4) can both ride on it safely.

### 3.4 On-disk formats (completeness scope)

For a full upgrade story these are part of "the contract" between versions of the same node across a restart, even though they are not on the RPC wire:

- **OpLog**: jsoncpp (JSON), naturally forward-compatible (unknown keys ignored). Schema evolution is in scope for #1920 item (e); see Section 8.
- **Snapshot**: master metadata bootstrap.
- **Offload KVEntry**: `struct_pb` protobuf (`mooncake-store/src/storage_backend.cpp:18,1050` `struct_pb::to_pb(kv, ...)`; field 1 key tag `0x0A`, field 2 value tag `0x12`). protobuf is tag-based and additive-tolerant.

These are tracked in the contract document but are not the focus of the binary gate, which targets the struct_pack/coro_rpc surface where breaks are silent.

---

## 4. Version-gate redesign (#1920 item (a))

#1920 proposes relaxing the exact-string check to a major-version check and adding `MOONCAKE_PROTOCOL_VERSION`. That is the right move for *allowing* compatible upgrades. But a hand-edited `MOONCAKE_PROTOCOL_VERSION` integer has the same blind spot the `2.0.0` string has today: it tells you nothing about whether the wire actually changed. A developer who breaks an arg tuple but forgets to bump the constant ships a silent break; a developer who bumps the constant for a non-wire change blocks a compatible upgrade for no reason.

Proposal, layered on #1920's:

1. Keep #1920's structured `ServiceReady` response and major-version admission check for the *connection policy* (do these two peers agree to talk).
2. Additionally expose a **wire-contract digest** in the same structured `ServiceReady` response: a stable hash of the frozen contract (the sorted `(function_id, arg_type_code)` set plus pinned struct codes). This digest is *computed from the code*, the same way the gate computes it -- not hand-maintained.
3. The client logs (at minimum) when the peer's contract digest differs from its own within the same major version. This converts "same major version but silently drifted" -- the exact 2.0.0/2.0.0 #2288 situation -- from invisible into observable, without rejecting the connection (compatible additive changes via `compatible<T>` keep the same digest by construction).

The digest is the runtime mirror of the CI gate: the gate prevents drift pre-merge; the digest surfaces drift between already-deployed peers.

---

## 5. The CI gate (#1920 item (c), (b))

`gate/wire_contract_gen.cpp` is a small C++ program that, using the **real** yalantinglibs codec, emits the wire-contract golden table: for every handler it prints the real `function_id` and (where the arg types are self-contained) the real `arg_type_code`. `gate/check_wire_contract.sh` rebuilds it from the current tree, runs a self-check, regenerates the table, and diffs it against the checked-in `gate/wire_contract_golden.txt`. Any drift fails the build with a readable diff.

### 5.1 Faithfulness self-check (NEVER fabricate codes)

Before trusting itself, the gate verifies the codec engine against codes recorded independently during the #2288 investigation. Real output:

```
[selfcheck] ExistKey bare<string>                              got=0x9dcffa76 want=0x9dcffa76 OK
[selfcheck] PutStart #2288-era 4-arg (3-field RC)              got=0xfad0c534 want=0xfad0c534 OK
[selfcheck] PutStart + bare trailing string (#2288 break)      got=0x22f8edba want=0x22f8edba OK
[selfcheck] PutStart + compatible<string> (the fix)            got=0xfad0c534 want=0xfad0c534 OK
```

All four match the values recorded for #2288 (including that the fix -- a trailing `struct_pack::compatible<std::string>` -- restores the original code while a bare string does not). The gate computes these with the same `struct_pack::get_type_code` used on the wire.

### 5.2 Green on a clean tree

```
[gate] PASS: master wire contract unchanged (59 handlers)
```

### 5.3 Red on injected drift (demonstration)

To prove the gate is not cosmetic, we flipped one handler signature -- appending a bare trailing `const std::string&` to `PutEnd` (the literal shape of the #2288 break) -- and re-ran the gate. Real output:

```
============================================================
 WIRE CONTRACT DRIFT DETECTED -- this change alters the
 on-wire RPC identity and will break existing-version peers.
============================================================
--- gate/wire_contract_golden.txt
+++ gate/.wire_contract_live.txt
@@ -10,7 +10,7 @@
 PutStart                   0x65ea9ac1 0x3d15a970
-PutEnd                     0x8e8b2099 0x61232de0
+PutEnd                     0x8e8b2099 0x1b7a228a
 PutRevoke                  0xa0767c44 0x61232de0
```
(exit code 1)

The `function_id` is unchanged (the method name did not change); the `arg_type_code` shifted `0x61232de0 -> 0x1b7a228a`. That is exactly the silent, compiles-cleanly break class the gate exists to catch. Reverting the change returns the gate to green.

### 5.4 Two faces, honestly split

- The **standalone generator** (`wire_contract_gen.cpp`) builds with only the yalantinglibs headers, so the gate runs even in a minimal CI job. It pins all 59 function-ids and the 47 self-contained arg type-codes. It freezes argument struct layouts as local mirrors -- correct, because the *wire contract is the frozen layout*, and the self-check proves the mirrors reproduce the real recorded codes.
- The **in-build production gate** (`wire_contract_test.cpp`, a ctest) includes the real `types.h`/`replica.h`/`rpc_service.h` and computes the same codes over the actual production structs, so it tracks real layout drift end-to-end (it would, for instance, have flagged the #2143 `ReplicateConfig` change to `PutStart`). Both share the same golden file.

---

## 6. Evolution rules the gate enforces (#1920 item (d))

#1920 states the additive-only rule. The gate turns it into enforced invariants:

1. **Never rename a registered RPC.** A rename changes `function_id = MD5Hash32(qualified-name)` and silently routes to "function not found" on the peer. The gate's golden keys on name -> a rename shows as one row removed + one added.
2. **Add fields only as `struct_pack::compatible<T>`.** A bare added field shifts `arg_type_code`; a `compatible<T>` field is excluded from the type hash (proven by the self-check: bare string breaks, `compatible<string>` does not). The gate fails on the former, passes the latter.
3. **Never reorder or retype existing fields/arguments.** Both shift the type-code; the gate fails.
4. **Single-argument handlers cannot grow a second argument.** Adding any second argument flips coro_rpc framing from a bare value to a tuple and changes the type-code regardless of `compatible<>`; carry side-data in the request attachment instead (this is what #2288's fix did for `ExistKey`/`BatchExistKey`/`GetReplicaList*`/`RemoveAll`).
5. **Intentional breaks require a major-version bump** and a reviewed `--update` of the golden, making every wire break an explicit, visible commit.

---

## 7. Cross-version interop test methodology (#1920 item (f))

The gate proves *identity stability*. It does not prove that old bytes actually round-trip through new code. For that we contribute the methodology already validated in PR #2551's `tenant_id_wire_compat_test.cpp`, generalized:

For each evolved surface, assert with real `struct_pack`:
1. **Type-code parity**: the evolved tuple's `get_type_code` equals the frozen one (additive `compatible<>` field excluded from the hash).
2. **Old client -> new server**: serialize the legacy tuple, `deserialize_to` the evolved tuple, assert OK and that the new field decodes as `nullopt`/default.
3. **New client -> old server**: serialize the evolved tuple, `deserialize_to` the legacy tuple, assert OK and that the legacy fields are intact (trailing new bytes ignored).

This is the "round-trip both directions" proof that a digest or a type-code alone cannot give. `wire_contract_test.cpp` includes a template (`AssertInteropBothWays<Legacy..., Evolved...>`) so each new `compatible<>` field lands with three assertions.

---

## 8. Out of / adjacent scope

- **Metadata schema versioning (#1920 e)** and **version-skew window (#1920 g)**: OpLog is jsoncpp and snapshot is a separate bootstrap path; both deserve their own additive-only discipline, but they are JSON/tag-based and forward-tolerant, so they are a lower-risk follow-up. This RFC notes them in the contract document (Section 3.4) but does not gate them in the binary check.
- **Transfer Engine wire protocol**: out of scope, consistent with #1920.
- **Client library / Python surface**: this RFC targets the master coro_rpc + struct_pack surface, where breaks are silent. JSON/HTTP surfaces are forward-tolerant and out of scope here.

---

## 9. What we are proposing to land

1. `gate/wire_contract_gen.cpp` + `gate/wire_contract_golden.txt` + `gate/check_wire_contract.sh`: the standalone golden gate (this RFC's mechanism, demonstrated green/red above).
2. `gate/wire_contract_test.cpp`: the in-build ctest that pins the same contract over the real production structs and carries the #2551 interop template.
3. `ci/wire-compat-gate.yml`: a GitHub Actions job that runs the gate on every PR touching the master RPC surface.
4. The `ServiceReady` wire-contract digest (Section 4), layered on #1920's structured handshake.

Items 1-3 are additive (new files, no behavior change) and can land independently of #1920. Item 4 should be coordinated with #1920's `ServiceReady` restructuring so the digest field ships in the same one-time handshake change #1920 already plans.

---

## Appendix A: Reproduction

```
# headers: a yalantinglibs install (the gate needs only struct_pack + util/function_name.h)
cd gate
./check_wire_contract.sh --ylt /path/to/ylt-install/include      # PASS, 59 handlers
./wire_contract_gen --selfcheck                                  # 4/4 reference codes OK
# demonstrate drift: append a bare arg to any handler stub + its type-code row, re-run -> exit 1
```

## Appendix B: Key citations (v0.3.11.post1, commit e9c6107)

- Handler registration: `mooncake-store/src/rpc_service.cpp:1850-1986` (`RegisterRpcService`, 59 `register_handler` calls).
- function-id: `ylt/util/utils.hpp` `consteval func_id()` = `MD5Hash32(get_func_name<func>())`; `ylt/coro_rpc/impl/router.hpp:123` `auto_gen_register_key`.
- arg type-code: `ylt/struct_pack/type_calculate.hpp:515` `MD5Hash32(...) & 0xFFFFFFFE`.
- Version gate: `mooncake-store/src/master_client.cpp:425` (`ServiceReady` call), `:433` (exact-string compare).
- ServiceReady handler: `mooncake-store/src/rpc_service.cpp:1726` (`return GetMooncakeStoreVersion()`).
- Version constant: `mooncake-store/CMakeLists.txt:1` (`project(MooncakeStore VERSION 2.0.0)`).
- #2288 type-code shift: `PutStart 0xfad0c534 -> 0x22f8edba`, `ExistKey` baseline `0x9dcffa76` (reproduced by the gate self-check).
- Offload on-disk format: `mooncake-store/src/storage_backend.cpp:18,1050` (`struct_pb::to_pb(kv, ...)`).
- Parent RFC: #1920. Prior interop test: PR #2551 `tenant_id_wire_compat_test.cpp`.
