from __future__ import annotations

import ctypes
import unittest

import numpy as np

try:
    from mooncake._fast_copy import concat_arrays_into
except Exception as error:  # pragma: no cover - depends on built extension
    concat_arrays_into = None
    _FAST_COPY_IMPORT_ERROR = error
else:
    _FAST_COPY_IMPORT_ERROR = None


@unittest.skipIf(
    concat_arrays_into is None,
    f"native fast-copy extension is unavailable: {_FAST_COPY_IMPORT_ERROR}",
)
class FastCopyTests(unittest.TestCase):
    def test_concat_arrays_into_copies_selected_range(self) -> None:
        arrays = [
            np.arange(3, dtype=np.uint8),
            np.arange(4, dtype=np.uint8) + 10,
            np.arange(2, dtype=np.uint8) + 20,
        ]
        expected = np.concatenate(arrays[1:])
        destination = ctypes.create_string_buffer(expected.nbytes)

        copied = concat_arrays_into(
            arrays, ctypes.addressof(destination), len(destination), 1, 2
        )

        self.assertEqual(copied, expected.nbytes)
        self.assertEqual(destination.raw[:copied], expected.tobytes())

    def test_concat_arrays_into_rejects_small_destination(self) -> None:
        arrays = [np.arange(4, dtype=np.uint8), np.arange(4, dtype=np.uint8)]
        destination = ctypes.create_string_buffer(7)

        with self.assertRaisesRegex(ValueError, "destination buffer too small"):
            concat_arrays_into(arrays, ctypes.addressof(destination), len(destination))

    def test_concat_arrays_into_rejects_non_contiguous_source(self) -> None:
        arrays = [np.arange(8, dtype=np.uint8)[::2]]
        destination = ctypes.create_string_buffer(arrays[0].nbytes)

        with self.assertRaisesRegex(ValueError, "C-contiguous"):
            concat_arrays_into(arrays, ctypes.addressof(destination), len(destination))

    def test_concat_arrays_into_empty_range_returns_zero(self) -> None:
        arrays = [np.arange(4, dtype=np.uint8)]
        destination = ctypes.create_string_buffer(1)

        copied = concat_arrays_into(arrays, ctypes.addressof(destination), 0, 0, 0)

        self.assertEqual(copied, 0)


if __name__ == "__main__":
    unittest.main()
