"""Native range-argument validation; run with the built _store on PYTHONPATH."""

import unittest

import _store


class OwnedRangeArgumentsTest(unittest.TestCase):
    def setUp(self):
        self.store = _store.MooncakeDistributedStore()

    def test_invalid_ranges_fail_before_client_initialization(self):
        cases = [
            ([(-1, 1)], ValueError),
            ([(0, -1)], ValueError),
            ([(0, 2**64)], OverflowError),
            ([(True, 1)], TypeError),
            ([(1.5, 1)], TypeError),
            ([(0,)], ValueError),
            ("invalid", TypeError),
        ]
        for operation in (self.store.get, self.store.get_buffer):
            for ranges, exception in cases:
                with self.subTest(operation=operation, ranges=ranges):
                    with self.assertRaises(exception):
                        operation("key", ranges=ranges)

    def test_batch_range_shape_and_item_validation(self):
        for operation in (self.store.get_batch, self.store.batch_get_buffer):
            for ranges, exception in [
                ([], ValueError),
                ([None, []], ValueError),
                ([[(0, -1)]], ValueError),
                ([[(0, 2**64)]], OverflowError),
            ]:
                with self.subTest(operation=operation, ranges=ranges):
                    with self.assertRaises(exception):
                        operation(["key"], ranges_per_key=ranges)

    def test_full_unsigned_range_and_large_negative_values(self):
        for number in (2**63 - 1, 2**63, 2**64 - 1):
            with self.subTest(number=number):
                self.assertEqual(
                    self.store.batch_get_buffer(
                        ["key"], ranges_per_key=[[(number, number)]]
                    ),
                    [None],
                )
        for number in (-(2**63), -(2**63) - 1, -(2**100)):
            with self.subTest(number=number):
                with self.assertRaises(ValueError):
                    self.store.get_buffer("key", ranges=[(number, 1)])

    def test_generic_sequence_pairs_remain_supported(self):
        class Pair:
            def __len__(self):
                return 2

            def __getitem__(self, index):
                return (4, 8)[index]

        self.assertEqual(
            self.store.batch_get_buffer(["key"], ranges_per_key=[[Pair()]]),
            [None],
        )

    def test_immutable_batch_ranges_and_mutable_pair_rechecks(self):
        windows = ((0, 1), (2, 3))
        self.assertEqual(
            self.store.batch_get_buffer(["a", "b"], ranges_per_key=[windows] * 2),
            [None, None],
        )

        class ChangingPair:
            visits = 0

            def __len__(self):
                return 2

            def __getitem__(self, index):
                if index == 0:
                    self.visits += 1
                    return 0 if self.visits == 1 else -1
                if index == 1:
                    return 1
                raise IndexError(index)

        windows = (ChangingPair(),)
        with self.assertRaises(ValueError):
            self.store.batch_get_buffer(["a", "b"], ranges_per_key=[windows] * 2)


if __name__ == "__main__":
    unittest.main()
