from __future__ import annotations

import sys

import pytest

from mooncake.reshard._compat import _legacy_strict_zip, _strict_zip


def test_strict_zip_preserves_equal_length_values() -> None:
    assert tuple(_strict_zip((1, 2), (3, 4))) == ((1, 3), (2, 4))


def test_strict_zip_rejects_different_lengths() -> None:
    with pytest.raises(ValueError, match=r"zip\(\)"):
        tuple(_strict_zip((1,), (2, 3)))


@pytest.mark.skipif(sys.version_info < (3, 10), reason="requires native strict zip")
def test_modern_strict_zip_uses_native_iterator() -> None:
    assert type(_strict_zip((1,), (2,))) is zip


def test_strict_zip_accepts_zero_and_single_iterables() -> None:
    assert tuple(_strict_zip()) == ()
    assert tuple(_strict_zip(iter((1, 2)))) == ((1,), (2,))


@pytest.mark.parametrize("lengths", [(1, 2), (2, 1), (0, 1), (1, 0)])
def test_strict_zip_rejects_generator_length_mismatches(lengths) -> None:
    first, second = lengths
    with pytest.raises(ValueError):
        tuple(_strict_zip((n for n in range(first)), (n for n in range(second))))


def test_strict_zip_consumes_generators_lazily_and_propagates_errors() -> None:
    consumed = []

    def values():
        consumed.append(1)
        yield 1
        consumed.append(2)
        raise RuntimeError("input failed")

    result = _strict_zip(values(), iter((3, 4)))
    assert consumed == []
    assert next(result) == (1, 3)
    assert consumed == [1]
    with pytest.raises(RuntimeError, match="input failed"):
        next(result)
    assert consumed == [1, 2]


def test_legacy_strict_zip_keeps_python39_fallback_behavior() -> None:
    assert tuple(_legacy_strict_zip()) == ()
    assert tuple(_legacy_strict_zip(iter((1, 2)))) == ((1,), (2,))
    assert tuple(_legacy_strict_zip((n for n in (1, 2)), (3, 4))) == (
        (1, 3),
        (2, 4),
    )
    for first, second in (((1,), (2, 3)), ((1, 2), (3,))):
        with pytest.raises(ValueError, match="different lengths"):
            tuple(_legacy_strict_zip(first, second))


@pytest.mark.skipif(sys.version_info < (3, 10), reason="requires native strict zip")
def test_native_strict_zip_initializes_iterators_at_call_time() -> None:
    initialized = []

    class Values:
        def __iter__(self):
            initialized.append("iter")
            return iter((1,))

    result = _strict_zip(Values())
    assert initialized == ["iter"]
    assert next(result) == (1,)

    class BrokenValues:
        def __iter__(self):
            raise RuntimeError("iterator initialization failed")

    with pytest.raises(RuntimeError, match="iterator initialization failed"):
        _strict_zip(BrokenValues())


def test_legacy_strict_zip_defers_iterator_initialization() -> None:
    class BrokenValues:
        def __iter__(self):
            raise RuntimeError("iterator initialization failed")

    result = _legacy_strict_zip(BrokenValues())
    with pytest.raises(RuntimeError, match="iterator initialization failed"):
        next(result)
