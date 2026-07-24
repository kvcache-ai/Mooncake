#!/usr/bin/env python3
"""Validate loader-critical ELF invariants in repaired wheel artifacts."""

from __future__ import annotations

import argparse
import struct
import sys
import zipfile
from pathlib import Path

ELF_MAGIC = b"\x7fELF"
ELFCLASS32 = 1
ELFCLASS64 = 2
ELFDATA2LSB = 1
ELFDATA2MSB = 2
PT_LOAD = 1
PT_DYNAMIC = 2
PF_W = 2
PN_XNUM = 0xFFFF


def _unpack_from(
    byte_order: str, layout: str, data: bytes, offset: int, name: str
) -> tuple:
    size = struct.calcsize(layout)
    if offset < 0 or offset + size > len(data):
        raise ValueError(f"{name}: truncated ELF structure at file offset 0x{offset:x}")
    return struct.unpack_from(byte_order + layout, data, offset)


def validate_elf(data: bytes, name: str) -> list[str]:
    """Return violations of ELF program-header invariants for one object."""
    if not data.startswith(ELF_MAGIC):
        raise ValueError(f"{name}: not an ELF object")
    if len(data) < 16:
        raise ValueError(f"{name}: truncated ELF identification")

    elf_class = data[4]
    encoding = data[5]
    if encoding == ELFDATA2LSB:
        byte_order = "<"
    elif encoding == ELFDATA2MSB:
        byte_order = ">"
    else:
        raise ValueError(f"{name}: unsupported ELF data encoding {encoding}")

    if elf_class == ELFCLASS64:
        header_layout = "HHIQQQIHHHHHH"
        program_layout = "IIQQQQQQ"
        header_size = 64
    elif elf_class == ELFCLASS32:
        header_layout = "HHIIIIIHHHHHH"
        program_layout = "IIIIIIII"
        header_size = 52
    else:
        raise ValueError(f"{name}: unsupported ELF class {elf_class}")

    header = _unpack_from(byte_order, header_layout, data, 16, name)
    program_offset = header[4]
    elf_header_size = header[7]
    program_entry_size = header[8]
    program_count = header[9]
    expected_entry_size = struct.calcsize(program_layout)

    if elf_header_size < header_size:
        raise ValueError(
            f"{name}: ELF header is {elf_header_size} bytes, expected at least "
            f"{header_size}"
        )
    if program_count == PN_XNUM:
        raise ValueError(f"{name}: extended program-header counts are unsupported")
    if program_count and program_entry_size < expected_entry_size:
        raise ValueError(
            f"{name}: program header is {program_entry_size} bytes, expected at "
            f"least {expected_entry_size}"
        )

    loads: list[tuple[int, int, int]] = []
    dynamics: list[tuple[int, int]] = []
    violations: list[str] = []

    for index in range(program_count):
        offset = program_offset + index * program_entry_size
        fields = _unpack_from(byte_order, program_layout, data, offset, name)
        if elf_class == ELFCLASS64:
            program_type, flags = fields[:2]
            file_offset, virtual_address = fields[2:4]
            file_size, memory_size, alignment = fields[5:8]
        else:
            program_type = fields[0]
            file_offset, virtual_address = fields[1:3]
            file_size, memory_size, flags, alignment = fields[4:8]

        if program_type == PT_LOAD:
            if alignment > 1 and (
                file_offset % alignment != virtual_address % alignment
            ):
                violations.append(
                    f"{name}: PT_LOAD #{index} has incongruent file and virtual "
                    f"addresses for alignment 0x{alignment:x}"
                )
            if file_offset + file_size > len(data):
                violations.append(
                    f"{name}: PT_LOAD #{index} extends past the end of the file"
                )
            loads.append((virtual_address, virtual_address + memory_size, flags))
        elif program_type == PT_DYNAMIC:
            dynamics.append((virtual_address, virtual_address + memory_size))

    for start, end in dynamics:
        covering_loads = [
            flags
            for load_start, load_end, flags in loads
            if load_start <= start and end <= load_end
        ]
        if not covering_loads:
            violations.append(
                f"{name}: PT_DYNAMIC [0x{start:x}, 0x{end:x}) is not fully "
                "covered by a PT_LOAD segment"
            )
        elif not any(flags & PF_W for flags in covering_loads):
            violations.append(
                f"{name}: PT_DYNAMIC [0x{start:x}, 0x{end:x}) is not covered "
                "by a writable PT_LOAD segment"
            )

    return violations


def validate_wheel(path: Path) -> tuple[int, list[str]]:
    """Validate each ELF member of a wheel and return count and violations."""
    elf_count = 0
    violations: list[str] = []
    with zipfile.ZipFile(path) as wheel:
        for member in wheel.infolist():
            if member.is_dir():
                continue
            with wheel.open(member) as source:
                data = source.read()
            if not data.startswith(ELF_MAGIC):
                continue
            elf_count += 1
            name = f"{path}:{member.filename}"
            try:
                violations.extend(validate_elf(data, name))
            except ValueError as error:
                violations.append(str(error))
    return elf_count, violations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", nargs="+", type=Path)
    args = parser.parse_args()

    failed = False
    for wheel in args.wheels:
        try:
            elf_count, violations = validate_wheel(wheel)
        except (OSError, zipfile.BadZipFile) as error:
            print(f"{wheel}: cannot read wheel: {error}", file=sys.stderr)
            failed = True
            continue
        if not elf_count:
            print(f"{wheel}: no ELF objects found", file=sys.stderr)
            failed = True
            continue
        if violations:
            for violation in violations:
                print(violation, file=sys.stderr)
            failed = True
        else:
            print(f"{wheel}: validated {elf_count} ELF objects")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
