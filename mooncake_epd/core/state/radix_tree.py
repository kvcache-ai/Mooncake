"""Radix tree for content-addressed KV prefix caching with workflow scope."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from .page_manager import BlockRef, PagedKVManager


GLOBAL_SCOPE = "__global__"


def _find_subsequence(haystack: Tuple[int, ...], needle: Tuple[int, ...]) -> int:
    n = len(needle)
    if n == 0:
        return 0
    for i in range(len(haystack) - n + 1):
        if haystack[i : i + n] == needle:
            return i
    return -1


@dataclass
class _RadixNode:
    tokens: Tuple[int, ...]
    refs: List[BlockRef] = field(default_factory=list)
    children: Dict[int, "_RadixNode"] = field(default_factory=dict)
    last_access: float = field(default_factory=time.monotonic)

    @property
    def is_leaf(self) -> bool:
        return not self.children

    @property
    def num_tokens(self) -> int:
        return len(self.tokens)


class RadixTree:
    """Token-prefix radix tree with per-workflow isolation."""

    def __init__(
        self,
        page_manager: PagedKVManager,
        max_entries: int = 4096,
    ):
        self._pm = page_manager
        self._max_entries = max_entries
        self._entry_count = 0
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._roots: Dict[str, _RadixNode] = {GLOBAL_SCOPE: _RadixNode(tokens=())}
        # Backward compatibility for internal helpers/tests that access `_root`.
        self._root = self._roots[GLOBAL_SCOPE]

    def _scope_key(self, scope: Optional[str]) -> str:
        return GLOBAL_SCOPE if scope in (None, "") else str(scope)

    def _scope_root(self, scope: Optional[str]) -> _RadixNode:
        key = self._scope_key(scope)
        root = self._roots.get(key)
        if root is None:
            root = _RadixNode(tokens=())
            self._roots[key] = root
        return root

    def insert(self, token_ids: Sequence[int], refs: Sequence[BlockRef], scope: Optional[str] = None) -> None:
        if not token_ids or not refs:
            return
        tokens = tuple(int(t) for t in token_ids)
        with self._lock:
            self._maybe_evict()
            parent = self._scope_root(scope)
            i = 0
            while i < len(tokens):
                first = tokens[i]
                if first not in parent.children:
                    child = _RadixNode(tokens=tokens[i:], refs=list(refs))
                    parent.children[first] = child
                    for r in refs:
                        self._pm.incref(r.physical_id)
                    self._entry_count += 1
                    return
                node = parent.children[first]
                common = self._common_prefix(tokens[i:], node.tokens)
                if common < len(node.tokens):
                    old_tokens = node.tokens
                    old_refs = list(node.refs)
                    old_children = node.children

                    parent_refs = self._slice_refs(old_refs, 0, common)
                    old_remainder = old_tokens[common:]
                    old_remainder_refs = self._slice_refs(
                        old_refs, common, len(old_tokens) - common
                    )
                    node.tokens = old_remainder
                    node.refs = old_remainder_refs
                    node.children = old_children

                    new_parent = _RadixNode(tokens=old_tokens[:common], refs=parent_refs)
                    parent.children[first] = new_parent
                    new_parent.children[old_remainder[0]] = node

                    new_remainder = tokens[i + common :]
                    if new_remainder:
                        new_child_refs = self._slice_refs(
                            list(refs), common, len(tokens) - i - common
                        )
                        new_parent.children[new_remainder[0]] = _RadixNode(
                            tokens=new_remainder,
                            refs=new_child_refs,
                        )
                        self._entry_count += 2
                    else:
                        self._entry_count += 1

                    for old_ref in old_refs:
                        self._pm.decref(old_ref.global_block_id or old_ref.physical_id)
                    return
                i += common
                if i >= len(tokens):
                    node.last_access = time.monotonic()
                    return
                parent = node

    def match_longest_prefix(
        self, token_ids: Sequence[int], scope: Optional[str] = None
    ) -> Tuple[List[BlockRef], List[int]]:
        tokens = tuple(int(t) for t in token_ids)
        matched_refs: List[BlockRef] = []
        matched_tokens = 0
        with self._lock:
            parent = self._scope_root(scope)
            i = 0
            while i < len(tokens):
                first = tokens[i]
                child = parent.children.get(first)
                if child is None:
                    break
                common = self._common_prefix(tokens[i:], child.tokens)
                if common == 0:
                    break
                if common == len(child.tokens):
                    matched_refs.extend(child.refs)
                    matched_tokens += common
                    child.last_access = time.monotonic()
                    i += common
                    parent = child
                else:
                    break
            if matched_refs:
                self._hits += 1
            else:
                self._misses += 1
        return matched_refs, list(tokens[matched_tokens:])

    def match_substring(
        self, token_ids: Sequence[int], scope: Optional[str] = None
    ) -> Tuple[List[BlockRef], int]:
        tokens = tuple(int(t) for t in token_ids)
        if not tokens:
            return [], 0
        with self._lock:
            paths: List[Tuple[Tuple[int, ...], List[BlockRef]]] = []

            def walk(node: "_RadixNode", path_tokens: Tuple[int, ...], path_refs: List[BlockRef]):
                current_tokens = path_tokens + node.tokens
                seen: set = set()
                current_refs: List[BlockRef] = []
                for r in path_refs + node.refs:
                    ref_key = (
                        r.physical_id,
                        int(r.virtual_offset),
                        int(r.filled),
                    )
                    if ref_key not in seen:
                        seen.add(ref_key)
                        current_refs.append(r)
                if node.tokens:
                    paths.append((current_tokens, current_refs))
                for child in node.children.values():
                    walk(child, current_tokens, current_refs)

            walk(self._scope_root(scope), (), [])

            best_path: Optional[Tuple[Tuple[int, ...], List[BlockRef]]] = None
            best_offset = -1
            for path_tokens, path_refs in paths:
                idx = _find_subsequence(path_tokens, tokens)
                if idx >= 0 and (best_path is None or len(path_tokens) > len(best_path[0])):
                    best_path = (path_tokens, path_refs)
                    best_offset = idx
            if best_path is None:
                return [], 0

        _, path_refs = best_path
        return list(path_refs), best_offset

    def evict_lru(self, count: int = 1) -> int:
        evicted = 0
        with self._lock:
            for _ in range(count):
                if not self._evict_one():
                    break
                evicted += 1
        return evicted

    def _collect_leaves(self) -> List[Tuple[float, _RadixNode, _RadixNode, int]]:
        leaves: List[Tuple[float, _RadixNode, _RadixNode, int]] = []

        def walk(parent: _RadixNode):
            for key, child in parent.children.items():
                if child.is_leaf and child.refs:
                    leaves.append((child.last_access, parent, child, key))
                else:
                    walk(child)

        for root in self._roots.values():
            walk(root)
        return leaves

    def _evict_one(self) -> bool:
        leaves = self._collect_leaves()
        if not leaves:
            return False
        _, parent, child, edge_key = min(leaves, key=lambda x: x[0])
        for ref in child.refs:
            self._pm.decref(ref.physical_id)
        del parent.children[edge_key]
        self._entry_count = max(0, self._entry_count - 1)
        return True

    def _maybe_evict(self) -> None:
        while self._entry_count >= self._max_entries:
            if not self._evict_one():
                break

    def _slice_refs(
        self,
        refs: Sequence[BlockRef],
        token_start: int,
        token_count: int,
    ) -> List[BlockRef]:
        token_count = max(0, int(token_count))
        if token_count == 0 or not refs:
            return []
        sliced = self._pm.reference_token_span(
            refs,
            token_start=max(0, int(token_start)),
            token_count=token_count,
        )
        return sliced

    @staticmethod
    def _common_prefix(a: Sequence[int], b: Sequence[int]) -> int:
        n = min(len(a), len(b))
        i = 0
        while i < n and a[i] == b[i]:
            i += 1
        return i

    def stats(self) -> Dict[str, float]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "entries": self._entry_count,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total else 0.0,
                "scopes": len(self._roots),
            }

    def clear(self) -> None:
        with self._lock:
            for root in self._roots.values():
                def walk(node: _RadixNode):
                    for ref in node.refs:
                        self._pm.decref(ref.physical_id)
                    for child in list(node.children.values()):
                        walk(child)
                    node.children.clear()
                    node.refs.clear()
                walk(root)
            self._roots = {GLOBAL_SCOPE: _RadixNode(tokens=())}
            self._root = self._roots[GLOBAL_SCOPE]
            self._entry_count = 0
            self._hits = 0
            self._misses = 0
