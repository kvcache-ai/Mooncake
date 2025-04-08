#!/usr/bin/env python3
import unittest

class TestImportStructure(unittest.TestCase):
    def test_backward_compatibility(self):
        """Test that the old import style still works."""
        from mooncake import MooncakeDistributedStore
        from mooncake import mooncake_vllm_adaptor

        # Just verify we can create instances
        store = MooncakeDistributedStore()
        adaptor = mooncake_vllm_adaptor()

        self.assertIsNotNone(store)
        self.assertIsNotNone(adaptor)

    def test_new_import_structure(self):
        """Test that the new import structure works."""
        import mooncake.transfer

        # Verify the module exists
        self.assertIsNotNone(mooncake.transfer)

        # Verify direct access to TransferEngine
        self.assertIsNotNone(mooncake.transfer.TransferEngine)

        # Verify direct access to TransferOpcode
        self.assertIsNotNone(mooncake.transfer.TransferOpcode)

    def test_direct_import(self):
        """Test direct import of specific components."""
        from mooncake.transfer import TransferEngine, TransferOpcode

        # Verify direct imports work
        self.assertIsNotNone(TransferEngine)
        self.assertIsNotNone(TransferOpcode)

if __name__ == '__main__':
    unittest.main()
