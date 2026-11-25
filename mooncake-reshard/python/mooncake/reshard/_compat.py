"""Compatibility helpers for supported Python runtimes."""

from __future__ import annotations

import sys
from itertools import zip_longest
from typing import Iterable, Iterator, TypeVar, cast, overload


_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")


@overload
def _strict_zip(first: Iterable[_T1]) -> Iterator[tuple[_T1]]: ...


@overload
def _strict_zip(
    first: Iterable[_T1], second: Iterable[_T2]
) -> Iterator[tuple[_T1, _T2]]: ...


@overload
def _strict_zip(
    first: Iterable[_T1],
    second: Iterable[_T2],
    third: Iterable[_T3],
) -> Iterator[tuple[_T1, _T2, _T3]]: ...


@overload
def _strict_zip(
    first: Iterable[_T1],
    second: Iterable[_T2],
    third: Iterable[_T3],
    fourth: Iterable[_T4],
) -> Iterator[tuple[_T1, _T2, _T3, _T4]]: ...


@overload
def _strict_zip(*iterables: Iterable[object]) -> Iterator[tuple[object, ...]]: ...


def _strict_zip(*iterables: Iterable[object]) -> Iterator[tuple[object, ...]]:
    """Use native strict zip when available, with a Python 3.9 fallback."""

    if sys.version_info >= (3, 10):
        return zip(*iterables, strict=True)
    return _legacy_strict_zip(*iterables)


def _legacy_strict_zip(*iterables: Iterable[object]) -> Iterator[tuple[object, ...]]:
    sentinel = object()
    for values in zip_longest(*iterables, fillvalue=sentinel):
        if any(value is sentinel for value in values):
            raise ValueError("zip() arguments have different lengths")
        yield cast(tuple[object, ...], values)
