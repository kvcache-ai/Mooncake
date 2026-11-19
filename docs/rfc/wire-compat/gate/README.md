# Wire-contract gate

Mechanical pre-merge gate for the Mooncake Store master coro_rpc surface. See
`../WIRE-COMPAT-RFC.md` for the full proposal (a complement to issue #1920).

## Files

- `wire_contract_gen.cpp` -- standalone generator. Builds with only the
  yalantinglibs headers. Emits, per handler, the real coro_rpc `function_id` and
  the real struct_pack argument `arg_type_code`. Has a `--selfcheck` that proves
  the codec engine against the recorded #2288 reference codes.
- `wire_contract_golden.txt` -- the checked-in golden table (59 handlers).
- `check_wire_contract.sh` -- the CI driver: rebuild, self-check, regenerate,
  diff against the golden. Exit 0 = intact, 1 = drift, 2 = build error.
- `wire_contract_test.cpp` -- in-build ctest face. Includes the real Mooncake
  headers, pins codes over the actual production structs, and carries the #2551
  cross-version interop round-trips.

## Run

```
# needs a yalantinglibs install (struct_pack + util/function_name.h only)
./check_wire_contract.sh --ylt /path/to/ylt-install/include
./wire_contract_gen --selfcheck       # 4/4 reference codes OK
```

Self-check expected output:

```
[selfcheck] ExistKey bare<string>                              got=0x9dcffa76 want=0x9dcffa76 OK
[selfcheck] PutStart #2288-era 4-arg (3-field RC)              got=0xfad0c534 want=0xfad0c534 OK
[selfcheck] PutStart + bare trailing string (#2288 break)      got=0x22f8edba want=0x22f8edba OK
[selfcheck] PutStart + compatible<string> (the fix)            got=0xfad0c534 want=0xfad0c534 OK
```

Clean tree: `[gate] PASS: master wire contract unchanged (59 handlers)`.

## Demonstrating drift (for reviewers)

Append a bare trailing argument to any handler stub plus its type-code row, e.g.
add `const std::string&` to `PutEnd`, rebuild and re-run: the gate prints a diff
and exits 1. The `function_id` is unchanged (name unchanged); the `arg_type_code`
shifts -- exactly the silent, compiles-cleanly break class #2288 was. Revert to
return to green.

## Updating the golden (intentional wire bump only)

```
./check_wire_contract.sh --update    # regenerate; bump the major protocol version too
```

This makes every wire break an explicit, reviewable commit.

## Notes

- The compiled `wire_contract_gen` binary is a build artifact; do not commit it.
- Generated/temp files (`.wire_contract_live.txt`, `.build.log`,
  `.wire_contract.diff`) are not committed.
- Toolchain used for the demonstration: g++ 11.4 (C++20), yalantinglibs as
  vendored by Mooncake v0.3.11.post1 (commit e9c6107).
