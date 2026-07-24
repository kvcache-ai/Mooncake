"""Tests for release-wheel ELF program-header validation."""

from __future__ import annotations

import importlib.util
import struct
import unittest
from pathlib import Path


def _find_validator() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "scripts" / "validate_wheel_elf.py"
        if candidate.is_file():
            return candidate
    return None


VALIDATOR_PATH = _find_validator()
if VALIDATOR_PATH is None:
    raise unittest.SkipTest("wheel ELF validator is not available")

SPEC = importlib.util.spec_from_file_location("validate_wheel_elf", VALIDATOR_PATH)
assert SPEC and SPEC.loader
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)


def _elf64(program_headers: list[tuple[int, ...]]) -> bytes:
    ident = b"\x7fELF" + bytes([2, 1, 1, 0, 0]) + bytes(7)
    header = struct.pack(
        "<HHIQQQIHHHHHH",
        3,
        62,
        1,
        0,
        64,
        0,
        0,
        64,
        56,
        len(program_headers),
        0,
        0,
        0,
    )
    headers = b"".join(struct.pack("<IIQQQQQQ", *values) for values in program_headers)
    image = ident + header + headers
    return image.ljust(0x2000, b"\0")


class WheelElfValidationTest(unittest.TestCase):
    def test_accepts_dynamic_segment_inside_writable_load(self) -> None:
        image = _elf64(
            [
                (1, 6, 0, 0x1000, 0, 0x2000, 0x2000, 0x1000),
                (2, 6, 0x800, 0x1800, 0, 0x100, 0x100, 8),
            ]
        )

        self.assertEqual(VALIDATOR.validate_elf(image, "valid"), [])

    def test_rejects_dynamic_segment_crossing_out_of_writable_load(self) -> None:
        image = _elf64(
            [
                (1, 6, 0, 0x1000, 0, 0x1000, 0x1000, 0x1000),
                (2, 6, 0xF00, 0x1F00, 0, 0x200, 0x200, 8),
                (1, 4, 0x1000, 0x2000, 0, 0x1000, 0x1000, 0x1000),
            ]
        )

        self.assertEqual(
            VALIDATOR.validate_elf(image, "broken"),
            [
                "broken: PT_DYNAMIC [0x1f00, 0x2100) is not fully covered by "
                "a PT_LOAD segment"
            ],
        )


if __name__ == "__main__":
    unittest.main()
