#!/usr/bin/env python3
"""
Snapshot Serialization Consistency Checker.

Checks whether a PR changed struct/class fields but did not update the
corresponding snapshot serialization code.

Primary mode:
  - Compare base/head revisions.
  - Parse headers with libclang AST.
  - Detect field additions/removals/type changes.
  - Verify matched serialization anchors were modified.

Fallback mode:
  - If AST parsing is unavailable, fall back to regex field extraction.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class FileDiff:
    path: str
    added_lines: set[int] = field(default_factory=set)
    removed_lines: set[int] = field(default_factory=set)


@dataclass
class FieldInfo:
    name: str
    line: int
    type_spelling: str
    canonical_type: str


@dataclass
class StructInfo:
    name: str
    qualified_name: str
    file: str
    start_line: int
    end_line: int
    fields: list[FieldInfo] = field(default_factory=list)


@dataclass
class SerializationTarget:
    file: str
    anchors: list[str] = field(default_factory=list)
    anchor_policy: str = "all"  # "all" or "any"


@dataclass
class SerializationMapping:
    struct_name: str
    header_file: str
    targets: list[SerializationTarget]
    match_policy: str = "any"  # "all" or "any"
    qualified_name: Optional[str] = None
    ignored_fields: set[str] = field(default_factory=set)


@dataclass
class FieldChange:
    change_type: str  # "added" | "removed" | "type_changed"
    name: str
    line: int
    old_type: str = ""
    new_type: str = ""


@dataclass
class TargetStatus:
    target: SerializationTarget
    updated: bool
    missing_anchors: list[str] = field(default_factory=list)
    unresolved_anchors: list[str] = field(default_factory=list)
    reason: str = ""


@dataclass
class CheckResult:
    struct_name: str
    header_file: str
    changed_fields: list[FieldChange]
    missing_targets: list[str]


@dataclass
class CheckOutput:
    results: list[CheckResult]
    warnings: list[str]
    fatal_errors: list[str]


# ---------------------------------------------------------------------------
# Git utilities
# ---------------------------------------------------------------------------


def run_git(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        check=check,
    )


def build_compare_spec(base_ref: str, head_ref: str) -> str:
    return f"{base_ref}...{head_ref}"


def get_changed_files(compare_spec: str) -> list[str]:
    result = run_git(["diff", "--name-only", compare_spec])
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def get_file_diff(compare_spec: str, file_path: str) -> FileDiff:
    result = run_git(["diff", "--unified=0", compare_spec, "--", file_path])
    diff = FileDiff(path=file_path)

    hunk_pattern = re.compile(
        r"@@ -(?P<old_start>\d+)(?:,(?P<old_count>\d+))? "
        r"\+(?P<new_start>\d+)(?:,(?P<new_count>\d+))? @@"
    )

    for line in result.stdout.splitlines():
        if not line.startswith("@@"):
            continue

        match = hunk_pattern.search(line)
        if not match:
            continue

        old_start = int(match.group("old_start"))
        old_count = int(match.group("old_count") or "1")
        new_start = int(match.group("new_start"))
        new_count = int(match.group("new_count") or "1")

        if old_count > 0:
            diff.removed_lines.update(range(old_start, old_start + old_count))
        if new_count > 0:
            diff.added_lines.update(range(new_start, new_start + new_count))

    return diff


def get_file_content_at_revision(revision: str, file_path: str) -> Optional[str]:
    result = run_git(["show", f"{revision}:{file_path}"], check=False)
    if result.returncode != 0:
        return None
    return result.stdout


def get_project_root() -> str:
    return run_git(["rev-parse", "--show-toplevel"]).stdout.strip()


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _normalize_policy(value: Optional[str], default: str) -> str:
    if not value:
        return default
    lowered = value.lower()
    if lowered in ("all", "any"):
        return lowered
    raise ValueError(f"Invalid policy '{value}', must be 'all' or 'any'")


def load_config(config_path: str) -> list[SerializationMapping]:
    with open(config_path) as f:
        data = json.load(f)

    mappings: list[SerializationMapping] = []
    for entry in data.get("mappings", []):
        targets: list[SerializationTarget] = []

        if "targets" in entry:
            for target in entry["targets"]:
                targets.append(
                    SerializationTarget(
                        file=target["file"],
                        anchors=target.get("anchors", []),
                        anchor_policy=_normalize_policy(
                            target.get("anchor_policy"), "all"
                        ),
                    )
                )
        else:
            # Backward compatibility with old schema
            for file_path in entry.get("serialization_files", []):
                targets.append(SerializationTarget(file=file_path))

        if not targets:
            raise ValueError(
                f"Mapping '{entry.get('struct_name', '<unknown>')}' has no targets"
            )

        mappings.append(
            SerializationMapping(
                struct_name=entry["struct_name"],
                header_file=entry["header_file"],
                targets=targets,
                match_policy=_normalize_policy(entry.get("match_policy"), "any"),
                qualified_name=entry.get("qualified_name"),
                ignored_fields=set(entry.get("ignored_fields", [])),
            )
        )

    return mappings


# ---------------------------------------------------------------------------
# compile_commands.json loading
# ---------------------------------------------------------------------------


def _tokenize_compile_command(command: str, arguments: list[str]) -> list[str]:
    if arguments:
        return arguments
    try:
        return shlex.split(command)
    except ValueError:
        return command.split()


def _extract_relevant_compile_args(tokens: list[str]) -> list[str]:
    value_flags = {"-I", "-isystem", "-D", "-U", "-include", "-iquote"}
    args: list[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in value_flags and i + 1 < len(tokens):
            args.append(tok)
            args.append(tokens[i + 1])
            i += 2
            continue
        if tok.startswith(("-I", "-D", "-U", "-std=", "-isystem", "-iquote")):
            args.append(tok)
        i += 1
    return args


def load_compile_commands(build_dir: str) -> dict[str, list[str]]:
    cc_path = os.path.join(build_dir, "compile_commands.json")
    if not os.path.exists(cc_path):
        return {}

    with open(cc_path) as f:
        commands = json.load(f)

    result: dict[str, list[str]] = {}
    for entry in commands:
        directory = entry.get("directory", "")
        file_path = entry.get("file", "")
        command = entry.get("command", "")
        arguments = entry.get("arguments", [])

        if not file_path:
            continue

        if os.path.isabs(file_path):
            absolute_file = os.path.abspath(file_path)
        else:
            absolute_file = os.path.abspath(os.path.join(directory, file_path))

        tokens = _tokenize_compile_command(command, arguments)
        result[absolute_file] = _extract_relevant_compile_args(tokens)

    return result


def select_compile_args(
    header_abs_path: str, compile_commands: dict[str, list[str]]
) -> list[str]:
    if header_abs_path in compile_commands:
        return compile_commands[header_abs_path]

    preferred_suffixes = [
        "mooncake-store/src/serialize/serializer.cpp",
        "mooncake-store/src/master_service.cpp",
        "mooncake-store/src/segment.cpp",
        "mooncake-store/src/task_manager.cpp",
    ]
    for suffix in preferred_suffixes:
        for file_path, args in compile_commands.items():
            if file_path.endswith(suffix):
                return args

    for file_path, args in compile_commands.items():
        if "/mooncake-store/src/" in file_path:
            return args

    for _, args in compile_commands.items():
        return args

    return []


# ---------------------------------------------------------------------------
# AST extraction (libclang)
# ---------------------------------------------------------------------------


def find_libclang() -> Optional[str]:
    import platform

    candidates: list[str] = []
    if platform.system() == "Linux":
        for ver in ("20", "19", "18", "17", "16", "15", "14"):
            candidates.append(f"/usr/lib/llvm-{ver}/lib/libclang.so")
            candidates.append(f"/usr/lib/llvm-{ver}/lib/libclang.so.1")
            candidates.append(f"/usr/lib/x86_64-linux-gnu/libclang-{ver}.so")
            candidates.append(f"/usr/lib/x86_64-linux-gnu/libclang-{ver}.so.1")
        candidates.append("/usr/lib/x86_64-linux-gnu/libclang.so")
        candidates.append("/usr/lib/x86_64-linux-gnu/libclang.so.1")
    elif platform.system() == "Darwin":
        candidates.append("/Library/Developer/CommandLineTools/usr/lib/libclang.dylib")
        for ver in range(20, 13, -1):
            candidates.append(f"/opt/homebrew/opt/llvm@{ver}/lib/libclang.dylib")

    for path in candidates:
        if os.path.exists(path):
            return path
    return None


_LIBCLANG_CONFIGURED = False


def _configure_libclang() -> None:
    global _LIBCLANG_CONFIGURED
    if _LIBCLANG_CONFIGURED:
        return

    from clang.cindex import Config as ClangConfig

    libclang_path = find_libclang()
    if libclang_path:
        try:
            ClangConfig.set_library_file(libclang_path)
        except Exception:
            pass
    _LIBCLANG_CONFIGURED = True


def _normalize_type(type_spelling: str) -> str:
    return " ".join(type_spelling.strip().split())


def analyze_header_ast(
    file_path: str,
    source_text: str,
    compile_args: list[str],
    nosnapshot_marker: str = "NOSNAPSHOT",
) -> list[StructInfo]:
    from clang.cindex import CursorKind, Index, TranslationUnit

    _configure_libclang()
    index = Index.create()

    parse_options = (
        TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD
        | TranslationUnit.PARSE_SKIP_FUNCTION_BODIES
    )
    args = ["-x", "c++", "-std=c++20", *compile_args]
    tu = index.parse(
        file_path,
        args=args,
        unsaved_files=[(file_path, source_text)],
        options=parse_options,
    )

    if not tu:
        return []

    abs_file_path = os.path.abspath(file_path)
    source_lines = source_text.splitlines()

    def has_nosnapshot_comment(line_num: int) -> bool:
        if 1 <= line_num <= len(source_lines):
            return nosnapshot_marker in source_lines[line_num - 1]
        return False

    def get_qualified_name(cursor) -> str:
        parts = []
        current = cursor
        while current and current.kind != CursorKind.TRANSLATION_UNIT:
            if current.spelling:
                parts.append(current.spelling)
            current = current.semantic_parent
        return "::".join(reversed(parts))

    structs: list[StructInfo] = []

    def visit(cursor) -> None:
        if cursor.location.file:
            cursor_file = os.path.abspath(cursor.location.file.name)
            if cursor_file != abs_file_path:
                return

        if cursor.kind in (CursorKind.STRUCT_DECL, CursorKind.CLASS_DECL):
            if cursor.spelling and cursor.extent.end.line >= cursor.extent.start.line:
                info = StructInfo(
                    name=cursor.spelling,
                    qualified_name=get_qualified_name(cursor),
                    file=file_path,
                    start_line=cursor.extent.start.line,
                    end_line=cursor.extent.end.line,
                )
                for child in cursor.get_children():
                    if child.kind != CursorKind.FIELD_DECL:
                        continue
                    line = child.location.line
                    if has_nosnapshot_comment(line):
                        continue
                    type_spelling = child.type.spelling if child.type else ""
                    canonical_type = (
                        child.type.get_canonical().spelling if child.type else type_spelling
                    )
                    info.fields.append(
                        FieldInfo(
                            name=child.spelling,
                            line=line,
                            type_spelling=type_spelling,
                            canonical_type=canonical_type,
                        )
                    )
                structs.append(info)

        for child in cursor.get_children():
            visit(child)

    visit(tu.cursor)
    return structs


# ---------------------------------------------------------------------------
# Regex fallback field extraction
# ---------------------------------------------------------------------------


STRUCT_START_TEMPLATE = r"\b(?:struct|class)\s+{name}\b"

# Very lightweight fallback matcher; AST remains the authoritative mode.
FALLBACK_FIELD_PATTERN = re.compile(
    r"""
    ^\s*
    (?P<type>[^;(){}]+?)
    \s+
    (?P<name>[A-Za-z_]\w*)
    \s*
    (?:=\s*[^;]+|\{[^;]*\})?
    \s*;
    \s*$
    """,
    re.VERBOSE,
)


def extract_struct_fields_regex(
    source_text: str,
    struct_name: str,
    nosnapshot_marker: str = "NOSNAPSHOT",
) -> tuple[dict[str, FieldInfo], bool]:
    lines = source_text.splitlines()
    struct_start = re.compile(STRUCT_START_TEMPLATE.format(name=re.escape(struct_name)))

    fields: dict[str, FieldInfo] = {}
    in_struct = False
    waiting_for_brace = False
    brace_depth = 0
    found_struct = False

    for idx, raw in enumerate(lines, start=1):
        line = raw.rstrip("\n")
        stripped = line.strip()

        if not in_struct and not waiting_for_brace:
            if struct_start.search(line):
                found_struct = True
                waiting_for_brace = True
                brace_depth = line.count("{") - line.count("}")
                if "{" in line:
                    in_struct = True
                    waiting_for_brace = False
            continue

        if waiting_for_brace:
            brace_depth += line.count("{") - line.count("}")
            if "{" in line:
                in_struct = True
                waiting_for_brace = False
            continue

        if not in_struct:
            continue

        if nosnapshot_marker in line:
            brace_depth += line.count("{") - line.count("}")
            if brace_depth <= 0:
                break
            continue

        if stripped.startswith(("//", "/*", "*", "#")):
            brace_depth += line.count("{") - line.count("}")
            if brace_depth <= 0:
                break
            continue

        if stripped.startswith(("friend ", "using ", "typedef ", "template ")):
            brace_depth += line.count("{") - line.count("}")
            if brace_depth <= 0:
                break
            continue

        if stripped.startswith("YLT_REFL("):
            brace_depth += line.count("{") - line.count("}")
            if brace_depth <= 0:
                break
            continue

        if "(" not in line and ")" not in line and stripped.endswith(";"):
            match = FALLBACK_FIELD_PATTERN.match(line)
            if match:
                type_spelling = _normalize_type(match.group("type"))
                name = match.group("name")
                fields[name] = FieldInfo(
                    name=name,
                    line=idx,
                    type_spelling=type_spelling,
                    canonical_type=type_spelling,
                )

        brace_depth += line.count("{") - line.count("}")
        if brace_depth <= 0:
            break

    return fields, found_struct


# ---------------------------------------------------------------------------
# Field change detection
# ---------------------------------------------------------------------------


def _field_type_for_compare(field_info: FieldInfo) -> str:
    return _normalize_type(field_info.canonical_type or field_info.type_spelling)


def detect_field_changes(
    base_fields: dict[str, FieldInfo],
    head_fields: dict[str, FieldInfo],
) -> list[FieldChange]:
    changes: list[FieldChange] = []
    names = set(base_fields) | set(head_fields)

    for name in sorted(names):
        in_base = name in base_fields
        in_head = name in head_fields

        if in_base and in_head:
            base_type = _field_type_for_compare(base_fields[name])
            head_type = _field_type_for_compare(head_fields[name])
            if base_type != head_type:
                changes.append(
                    FieldChange(
                        change_type="type_changed",
                        name=name,
                        line=head_fields[name].line,
                        old_type=base_type,
                        new_type=head_type,
                    )
                )
        elif in_head:
            changes.append(
                FieldChange(
                    change_type="added",
                    name=name,
                    line=head_fields[name].line,
                    new_type=_field_type_for_compare(head_fields[name]),
                )
            )
        else:
            changes.append(
                FieldChange(
                    change_type="removed",
                    name=name,
                    line=base_fields[name].line,
                    old_type=_field_type_for_compare(base_fields[name]),
                )
            )

    return sorted(changes, key=lambda c: (c.line or 10**9, c.name, c.change_type))


def pick_struct(
    structs: list[StructInfo],
    mapping: SerializationMapping,
    warnings: list[str],
    revision_label: str,
) -> Optional[StructInfo]:
    if mapping.qualified_name:
        for struct in structs:
            if struct.qualified_name == mapping.qualified_name:
                return struct
        return None

    matched = [struct for struct in structs if struct.name == mapping.struct_name]
    if not matched:
        return None
    if len(matched) > 1:
        warnings.append(
            f"Multiple structs named '{mapping.struct_name}' in {mapping.header_file} "
            f"({revision_label}); using first match '{matched[0].qualified_name}'."
        )
    return matched[0]


def build_field_dict(
    struct_info: Optional[StructInfo], ignored_fields: set[str]
) -> dict[str, FieldInfo]:
    if not struct_info:
        return {}
    return {
        field_info.name: field_info
        for field_info in struct_info.fields
        if field_info.name not in ignored_fields
    }


# ---------------------------------------------------------------------------
# Anchor matching for serialization updates
# ---------------------------------------------------------------------------


def _find_scope_for_anchor(lines: list[str], start_idx: int) -> Optional[tuple[int, int]]:
    max_scan = min(len(lines), start_idx + 40)
    open_line = -1
    open_col = -1

    for idx in range(start_idx, max_scan):
        line = lines[idx]
        brace_pos = line.find("{")
        semicolon_pos = line.find(";")
        if brace_pos != -1 and (semicolon_pos == -1 or brace_pos < semicolon_pos):
            open_line = idx
            open_col = brace_pos
            break
        if semicolon_pos != -1 and brace_pos == -1:
            # Looks like a call/declaration, not a definition scope.
            return None

    if open_line == -1:
        return None

    depth = 0
    for idx in range(open_line, len(lines)):
        segment = lines[idx]
        if idx == open_line:
            segment = segment[open_col:]
        for ch in segment:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return (start_idx + 1, idx + 1)

    return (start_idx + 1, len(lines))


def find_anchor_ranges(source_text: str, anchor: str) -> list[tuple[int, int]]:
    lines = source_text.splitlines()
    ranges: list[tuple[int, int]] = []

    for idx, line in enumerate(lines):
        if anchor not in line:
            continue
        scope = _find_scope_for_anchor(lines, idx)
        if scope:
            ranges.append(scope)

    deduped = sorted(set(ranges))
    return deduped


def _line_hits_ranges(lines: set[int], ranges: list[tuple[int, int]]) -> bool:
    if not lines or not ranges:
        return False
    for line in lines:
        for start, end in ranges:
            if start <= line <= end:
                return True
    return False


def evaluate_target_update(
    target: SerializationTarget,
    file_diff: FileDiff,
    base_source: Optional[str],
    head_source: Optional[str],
    anchor_cache: dict[tuple[str, str], list[tuple[int, int]]],
) -> TargetStatus:
    if not target.anchors:
        return TargetStatus(target=target, updated=True, reason="file changed")

    missing_anchors: list[str] = []
    unresolved_anchors: list[str] = []
    hit_values: list[bool] = []

    for anchor in target.anchors:
        base_key = (f"base::{target.file}", anchor)
        head_key = (f"head::{target.file}", anchor)

        if base_key not in anchor_cache:
            anchor_cache[base_key] = (
                find_anchor_ranges(base_source, anchor) if base_source else []
            )
        if head_key not in anchor_cache:
            anchor_cache[head_key] = (
                find_anchor_ranges(head_source, anchor) if head_source else []
            )

        base_ranges = anchor_cache[base_key]
        head_ranges = anchor_cache[head_key]

        if not base_ranges and not head_ranges:
            unresolved_anchors.append(anchor)
            missing_anchors.append(anchor)
            hit_values.append(False)
            continue

        hit = _line_hits_ranges(file_diff.removed_lines, base_ranges) or _line_hits_ranges(
            file_diff.added_lines, head_ranges
        )
        if not hit:
            missing_anchors.append(anchor)
        hit_values.append(hit)

    if target.anchor_policy == "any":
        updated = any(hit_values) if hit_values else True
    else:
        updated = all(hit_values) if hit_values else True

    reason = ""
    if not updated:
        if target.anchor_policy == "any":
            reason = f"none of anchors changed: {', '.join(missing_anchors)}"
        else:
            reason = f"missing anchor updates: {', '.join(missing_anchors)}"
    if unresolved_anchors:
        suffix = f"unresolved anchors: {', '.join(unresolved_anchors)}"
        reason = f"{reason}; {suffix}" if reason else suffix

    return TargetStatus(
        target=target,
        updated=updated,
        missing_anchors=missing_anchors,
        unresolved_anchors=unresolved_anchors,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# Main check
# ---------------------------------------------------------------------------


def run_check(
    base_ref: str,
    head_ref: str,
    config_path: str,
    build_dir: str,
    use_ast: bool = True,
    strict_ci: bool = False,
) -> CheckOutput:
    warnings: list[str] = []
    fatal_errors: list[str] = []

    try:
        mappings = load_config(config_path)
    except Exception as exc:
        return CheckOutput(
            results=[],
            warnings=[],
            fatal_errors=[f"Failed to load config '{config_path}': {exc}"],
        )

    compare_spec = build_compare_spec(base_ref, head_ref)
    try:
        changed_files = set(get_changed_files(compare_spec))
    except subprocess.CalledProcessError as exc:
        return CheckOutput(
            results=[],
            warnings=[],
            fatal_errors=[f"Failed to compute git diff ({compare_spec}): {exc.stderr.strip()}"],
        )

    if not changed_files:
        return CheckOutput(results=[], warnings=warnings, fatal_errors=fatal_errors)

    # Keep checks cheap when no mapped header changed.
    mapped_headers = {mapping.header_file for mapping in mappings}
    if not changed_files.intersection(mapped_headers):
        return CheckOutput(results=[], warnings=warnings, fatal_errors=fatal_errors)

    project_root = get_project_root()
    compile_commands: dict[str, list[str]] = {}

    if use_ast:
        try:
            import clang.cindex  # noqa: F401
        except Exception as exc:
            msg = f"libclang unavailable, falling back to regex: {exc}"
            if strict_ci:
                fatal_errors.append(msg)
            else:
                warnings.append(msg)
            use_ast = False

    if use_ast:
        compile_commands = load_compile_commands(build_dir)
        if not compile_commands:
            msg = f"compile_commands.json not found in '{build_dir}', falling back to regex."
            if strict_ci:
                fatal_errors.append(msg)
            else:
                warnings.append(msg)
            use_ast = False

    if fatal_errors:
        return CheckOutput(results=[], warnings=warnings, fatal_errors=fatal_errors)

    diff_cache: dict[str, FileDiff] = {}
    source_cache: dict[tuple[str, str], Optional[str]] = {}
    ast_cache: dict[tuple[str, str], list[StructInfo]] = {}
    regex_cache: dict[tuple[str, str, str], tuple[dict[str, FieldInfo], bool]] = {}
    anchor_cache: dict[tuple[str, str], list[tuple[int, int]]] = {}

    def get_diff_cached(file_path: str) -> FileDiff:
        if file_path not in diff_cache:
            diff_cache[file_path] = get_file_diff(compare_spec, file_path)
        return diff_cache[file_path]

    def get_source_cached(revision: str, file_path: str) -> Optional[str]:
        key = (revision, file_path)
        if key not in source_cache:
            source_cache[key] = get_file_content_at_revision(revision, file_path)
        return source_cache[key]

    def get_struct_fields_ast(
        revision: str, mapping: SerializationMapping
    ) -> tuple[dict[str, FieldInfo], bool]:
        source_text = get_source_cached(revision, mapping.header_file)
        if source_text is None:
            return {}, False

        cache_key = (revision, mapping.header_file)
        if cache_key not in ast_cache:
            header_abs = os.path.abspath(os.path.join(project_root, mapping.header_file))
            args = select_compile_args(header_abs, compile_commands)
            ast_cache[cache_key] = analyze_header_ast(header_abs, source_text, args)

        struct_info = pick_struct(
            ast_cache[cache_key], mapping, warnings, revision_label=revision
        )
        if not struct_info:
            return {}, False
        return build_field_dict(struct_info, mapping.ignored_fields), True

    def get_struct_fields_regex(
        revision: str, mapping: SerializationMapping
    ) -> tuple[dict[str, FieldInfo], bool]:
        source_text = get_source_cached(revision, mapping.header_file)
        if source_text is None:
            return {}, False
        cache_key = (revision, mapping.header_file, mapping.struct_name)
        if cache_key not in regex_cache:
            regex_cache[cache_key] = extract_struct_fields_regex(
                source_text, mapping.struct_name
            )
        fields, found_struct = regex_cache[cache_key]
        if not found_struct:
            return {}, False
        filtered = {
            name: info
            for name, info in fields.items()
            if name not in mapping.ignored_fields
        }
        return filtered, True

    results: list[CheckResult] = []

    for mapping in mappings:
        if mapping.header_file not in changed_files:
            continue

        base_fields: dict[str, FieldInfo] = {}
        head_fields: dict[str, FieldInfo] = {}
        base_found = False
        head_found = False
        used_regex_fallback = False

        if use_ast:
            try:
                base_fields, base_found = get_struct_fields_ast(base_ref, mapping)
                head_fields, head_found = get_struct_fields_ast(head_ref, mapping)
            except Exception as exc:
                msg = (
                    f"AST parse failed for {mapping.header_file}::{mapping.struct_name}: {exc}"
                )
                if strict_ci:
                    fatal_errors.append(msg)
                    continue
                warnings.append(msg)
                used_regex_fallback = True

        if not use_ast or used_regex_fallback:
            base_fields, base_found = get_struct_fields_regex(base_ref, mapping)
            head_fields, head_found = get_struct_fields_regex(head_ref, mapping)

        if not base_found and not head_found:
            msg = (
                f"Struct '{mapping.struct_name}' not found in both base/head for "
                f"{mapping.header_file}. Check mapping config."
            )
            if strict_ci:
                fatal_errors.append(msg)
            else:
                warnings.append(msg)
            continue

        field_changes = detect_field_changes(base_fields, head_fields)
        if not field_changes:
            continue

        target_statuses: list[TargetStatus] = []
        for target in mapping.targets:
            if target.file not in changed_files:
                target_statuses.append(
                    TargetStatus(
                        target=target,
                        updated=False,
                        reason="file not changed",
                    )
                )
                continue

            file_diff = get_diff_cached(target.file)
            base_source = get_source_cached(base_ref, target.file)
            head_source = get_source_cached(head_ref, target.file)
            status = evaluate_target_update(
                target=target,
                file_diff=file_diff,
                base_source=base_source,
                head_source=head_source,
                anchor_cache=anchor_cache,
            )
            target_statuses.append(status)

            if status.unresolved_anchors:
                msg = (
                    f"Unresolved anchor(s) for {mapping.struct_name} in {target.file}: "
                    f"{', '.join(status.unresolved_anchors)}"
                )
                if strict_ci:
                    fatal_errors.append(msg)
                else:
                    warnings.append(msg)

        if mapping.match_policy == "all":
            mapping_updated = all(status.updated for status in target_statuses)
        else:
            mapping_updated = any(status.updated for status in target_statuses)

        if mapping_updated:
            continue

        missing_targets: list[str] = []
        for status in target_statuses:
            if status.updated:
                continue
            detail = status.reason if status.reason else "missing expected update"
            missing_targets.append(f"{status.target.file} ({detail})")

        results.append(
            CheckResult(
                struct_name=mapping.struct_name,
                header_file=mapping.header_file,
                changed_fields=field_changes,
                missing_targets=missing_targets,
            )
        )

    return CheckOutput(results=results, warnings=warnings, fatal_errors=fatal_errors)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def format_field_change(change: FieldChange) -> str:
    if change.change_type == "added":
        return f"+ {change.new_type} {change.name}"
    if change.change_type == "removed":
        return f"- {change.old_type} {change.name}"
    return f"~ {change.name}: {change.old_type} -> {change.new_type}"


def first_change_line(changes: list[FieldChange]) -> Optional[int]:
    lines = [change.line for change in changes if change.line > 0]
    if not lines:
        return None
    return min(lines)


def escape_github_annotation(text: str) -> str:
    # GitHub Actions command escaping
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def emit_github_annotations(output: CheckOutput) -> None:
    for warning in output.warnings:
        print(f"::warning::{escape_github_annotation(warning)}")

    for fatal in output.fatal_errors:
        print(f"::error::{escape_github_annotation(fatal)}")

    for result in output.results:
        line = first_change_line(result.changed_fields)
        message = (
            f"{result.struct_name} changed fields require serialization updates in: "
            f"{'; '.join(result.missing_targets)}"
        )
        if line:
            print(
                f"::error file={result.header_file},line={line}::"
                f"{escape_github_annotation(message)}"
            )
        else:
            print(
                f"::error file={result.header_file}::"
                f"{escape_github_annotation(message)}"
            )


def format_output(output: CheckOutput) -> str:
    lines: list[str] = []

    if output.fatal_errors:
        lines.append("❌ Snapshot serialization check FAILED (fatal errors).")
        lines.append("")
        for err in output.fatal_errors:
            lines.append(f"- {err}")
        lines.append("")

    if output.results:
        lines.append("❌ Snapshot serialization consistency check FAILED.")
        lines.append("")
        lines.append(
            "Struct fields changed, but required serialization updates were not found."
        )
        lines.append("")
        for result in output.results:
            lines.append(f"Struct: {result.struct_name} ({result.header_file})")
            lines.append("Changed fields:")
            for field_change in result.changed_fields:
                lines.append(f"  {format_field_change(field_change)}")
            lines.append("Missing serialization updates:")
            for target in result.missing_targets:
                lines.append(f"  - {target}")
            lines.append("")

    if output.warnings:
        lines.append("Warnings:")
        for warning in output.warnings:
            lines.append(f"- {warning}")
        lines.append("")

    if not output.fatal_errors and not output.results:
        lines.append("✅ Snapshot serialization consistency check passed.")
        if output.warnings:
            lines.append("")
            lines.append("(passed with warnings)")

    return "\n".join(lines)


def write_step_summary(text: str) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    try:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("## Snapshot Serialization Check\n\n")
            f.write("```\n")
            f.write(text)
            f.write("\n```\n")
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check snapshot serialization consistency in PRs."
    )
    parser.add_argument(
        "--base",
        default="origin/main",
        help="Base ref to compare from (default: origin/main).",
    )
    parser.add_argument(
        "--head",
        default="HEAD",
        help="Head ref to compare to (default: HEAD).",
    )
    parser.add_argument(
        "--base-sha",
        default=None,
        help="Explicit base commit SHA (overrides --base).",
    )
    parser.add_argument(
        "--head-sha",
        default=None,
        help="Explicit head commit SHA (overrides --head).",
    )
    parser.add_argument(
        "--config",
        default=None,
        help=(
            "Path to mapping config "
            "(default: scripts/snapshot_serialization_map.json)."
        ),
    )
    parser.add_argument(
        "--build-dir",
        default="build",
        help="Build directory containing compile_commands.json (default: build).",
    )
    parser.add_argument(
        "--no-ast",
        action="store_true",
        help="Disable AST mode and force regex fallback.",
    )
    parser.add_argument(
        "--strict-ci",
        action="store_true",
        help="Fail when AST/config/anchor prerequisites are missing.",
    )
    parser.add_argument(
        "--github-actions",
        action="store_true",
        help="Emit GitHub Actions annotations.",
    )
    args = parser.parse_args()

    github_actions = args.github_actions or os.environ.get("GITHUB_ACTIONS") == "true"

    project_root = get_project_root()
    config_path = args.config or os.path.join(
        project_root, "scripts", "snapshot_serialization_map.json"
    )
    if not os.path.exists(config_path):
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    build_dir = args.build_dir
    if not os.path.isabs(build_dir):
        build_dir = os.path.join(project_root, build_dir)

    base_ref = args.base_sha or args.base
    head_ref = args.head_sha or args.head

    output = run_check(
        base_ref=base_ref,
        head_ref=head_ref,
        config_path=config_path,
        build_dir=build_dir,
        use_ast=not args.no_ast,
        strict_ci=args.strict_ci,
    )

    rendered = format_output(output)
    print(rendered)

    if github_actions:
        emit_github_annotations(output)
        write_step_summary(rendered)

    if output.fatal_errors or output.results:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
