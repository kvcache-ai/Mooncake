#!/usr/bin/env python3
import unittest

class TestImportStructure(unittest.TestCase):

    def test_new_import_structure(self):
        """Test that the new import structure works."""
        import mooncake.engine

        # Verify the module exists
        self.assertIsNotNone(mooncake.engine)

        # Verify direct access to TransferEngine
        self.assertIsNotNone(mooncake.engine.TransferEngine)

        # Verify direct access to TransferOpcode
        self.assertIsNotNone(mooncake.engine.TransferOpcode)

        from mooncake.store import MooncakeDistributedStore

        # Just verify we can create instances
        store = MooncakeDistributedStore()

        self.assertIsNotNone(store)

    def test_direct_import(self):
        """Test direct import of specific components."""
        from mooncake.engine import TransferEngine, TransferOpcode

        # Verify direct imports work
        self.assertIsNotNone(TransferEngine)
        self.assertIsNotNone(TransferOpcode)

if __name__ == '__main__':
    unittest.main()
