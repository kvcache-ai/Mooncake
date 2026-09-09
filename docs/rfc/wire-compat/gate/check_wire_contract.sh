#!/usr/bin/env bash
# check_wire_contract.sh -- the CI gate driver.
#
# Rebuilds the wire-contract generator from the CURRENT source tree, emits the
# live wire-contract table, and diffs it against the checked-in golden. Any drift
# in a function-id or an arg-type-code (i.e. any change that would break an
# existing-version peer) fails the build with a readable diff.
#
# Usage:
#   check_wire_contract.sh [--ylt <ylt-install-include-dir>] [--update]
#
#   --update   regenerate the golden instead of checking (use only for an
#              intentional, reviewed major-version wire bump).
#
# Exit codes: 0 = contract intact; 1 = drift detected; 2 = build/setup error.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ylt="${YLT_INCLUDE:-/tmp/compat-official/ylt-install/include}"
update=0
while [ $# -gt 0 ]; do
  case "$1" in
    --ylt)
      if [ $# -lt 2 ] || [ -z "$2" ]; then
        echo "ERROR: --ylt requires a value (ylt-install include dir)" >&2
        exit 2
      fi
      ylt="$2"; shift 2;;
    --update) update=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

gen="$here/wire_contract_gen"
src="$here/wire_contract_gen.cpp"
golden="$here/wire_contract_golden.txt"

if [ ! -f "$ylt/ylt/struct_pack.hpp" ]; then
  echo "ERROR: yalantinglibs headers not found at $ylt (set YLT_INCLUDE or --ylt)" >&2
  exit 2
fi

echo "[gate] building generator from $src"
if ! g++ -std=c++20 -I"$ylt" "$src" -o "$gen" 2> "$here/.build.log"; then
  echo "ERROR: generator failed to build:" >&2
  cat "$here/.build.log" >&2
  exit 2
fi

echo "[gate] self-check (codec faithfulness vs recorded #2288 reference codes)"
if ! "$gen" --selfcheck; then
  echo "ERROR: codec self-check failed -- the gate cannot trust its own type codes" >&2
  exit 2
fi

live="$here/.wire_contract_live.txt"
if ! "$gen" > "$live"; then
  echo "ERROR: generator failed to emit the live wire contract" >&2
  rm -f "$live"
  exit 2
fi

if [ "$update" = "1" ]; then
  cp "$live" "$golden"
  echo "[gate] golden UPDATED -> $golden (intentional wire bump; ensure major version was bumped)"
  exit 0
fi

if [ ! -f "$golden" ]; then
  echo "ERROR: golden $golden missing; run with --update to create it" >&2
  exit 2
fi

if diff -u "$golden" "$live" > "$here/.wire_contract.diff"; then
  echo "[gate] PASS: master wire contract unchanged ($(grep -vc '^#' "$golden") handlers)"
  rm -f "$live"
  exit 0
else
  echo "" >&2
  echo "============================================================" >&2
  echo " WIRE CONTRACT DRIFT DETECTED -- this change alters the" >&2
  echo " on-wire RPC identity and will break existing-version peers." >&2
  echo "============================================================" >&2
  cat "$here/.wire_contract.diff" >&2
  echo "" >&2
  echo "If this is an intentional, reviewed MAJOR-version wire bump," >&2
  echo "bump MOONCAKE_PROTOCOL_VERSION and re-run with --update." >&2
  echo "Otherwise: revert the signature/struct change, or carry the new" >&2
  echo "field as struct_pack::compatible<T> (additive, type-code stable)." >&2
  rm -f "$live"
  exit 1
fi
