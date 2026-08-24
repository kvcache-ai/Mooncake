from __future__ import annotations

import pytest

from mooncake.reshard._compat import _strict_zip


def test_strict_zip_preserves_equal_length_values() -> None:
    assert tuple(_strict_zip((1, 2), (3, 4))) == ((1, 3), (2, 4))


def test_strict_zip_rejects_different_lengths() -> None:
    with pytest.raises(ValueError, match="different lengths"):
        tuple(_strict_zip((1,), (2, 3)))
