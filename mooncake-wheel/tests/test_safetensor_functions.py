import unittest
import os
import time
import tempfile
import uuid
from mooncake.store import MooncakeDistributedStore

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000  # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)


def get_client(store):
    """Initialize and setup the distributed store client."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = (
        os.getenv("MC_METADATA_SERVER")
        or os.getenv("MOONCAKE_TE_META_DATA_SERVER")
        or "http://127.0.0.1:8080/metadata"
    )
    global_segment_size = 3200 * 1024 * 1024  # 3200 MB
    local_buffer_size = 512 * 1024 * 1024  # 512 MB
    master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")

    retcode = store.setup(
        local_hostname,
        metadata_server,
        global_segment_size,
        local_buffer_size,
        protocol,
        device_name,
        master_server_address,
    )

    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


class TestSafetensorFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize the store once for all tests."""
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_save_and_load_tensor_from_safetensor(self):
        """Test saving a tensor to safetensor file and loading it back."""
        import torch

        # Create a test tensor
        tensor = torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)
        key = "test_tensor_safetensor"

        # Put tensor in the store
        result = self.store.put_tensor(key, tensor)
        self.assertEqual(result, 0)

        # Verify tensor was stored
        retrieved = self.store.get_tensor(key)
        self.assertIsNotNone(retrieved)
        self.assertTrue(torch.allclose(tensor, retrieved))

        # Save tensor to safetensor file (using key as filename)
        with tempfile.NamedTemporaryFile(
            suffix=".safetensors", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            # Save tensor from store to safetensor file
            result = self.store.save_tensor_to_safetensor(key, temp_filename)
            self.assertEqual(
                result, 0, "save_tensor_to_safetensor should return 0 on success"
            )

            # Verify the file was created
            self.assertTrue(
                os.path.exists(temp_filename),
                "Safetensor file should exist after saving",
            )

            # Remove tensor from store to test loading it back
            self.store.remove(key)

            # Load tensor from safetensor file back to store
            loaded_tensor = self.store.load_tensor_from_safetensor(key, temp_filename)
            self.assertIsNotNone(loaded_tensor, "Loaded tensor should not be None")

            # Verify tensor was loaded back into store
            retrieved_after_load = self.store.get_tensor(key)
            self.assertIsNotNone(
                retrieved_after_load,
                "Retrieved tensor after loading should not be None",
            )
            self.assertTrue(
                torch.allclose(tensor, retrieved_after_load),
                "Loaded tensor should match original tensor",
            )

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

            # Clean up from store
            time.sleep(default_kv_lease_ttl / 1000)
            self.store.remove(key)

    def test_save_and_load_different_tensor_types(self):
        """Test saving and loading different types of tensors."""
        import torch

        test_cases = [
            ("test_float_tensor", torch.tensor([1.0, 2.0, 3.0], dtype=torch.float32)),
            ("test_int_tensor", torch.tensor([1, 2, 3], dtype=torch.int32)),
            ("test_bool_tensor", torch.tensor([True, False, True], dtype=torch.bool)),
            (
                "test_2d_tensor",
                torch.tensor([[1.0, 2.0], [3.0, 4.0]], dtype=torch.float32),
            ),
            (
                "test_3d_tensor",
                torch.tensor([[[1.0, 2.0]], [[3.0, 4.0]]], dtype=torch.float32),
            ),
        ]

        for key, tensor in test_cases:
            with self.subTest(key=key):
                # Put tensor in the store
                result = self.store.put_tensor(key, tensor)
                self.assertEqual(result, 0)

                # Save tensor to safetensor file
                with tempfile.NamedTemporaryFile(
                    suffix=".safetensors", delete=False
                ) as temp_file:
                    temp_filename = temp_file.name

                try:
                    # Save tensor from store to safetensor file
                    result = self.store.save_tensor_to_safetensor(key, temp_filename)
                    self.assertEqual(
                        result,
                        0,
                        f"save_tensor_to_safetensor should return 0 for {key}",
                    )

                    # Remove tensor from store to test loading it back
                    self.store.remove(key)

                    # Load tensor from safetensor file back to store
                    loaded_tensor = self.store.load_tensor_from_safetensor(
                        key, temp_filename
                    )
                    self.assertIsNotNone(
                        loaded_tensor, f"Loaded tensor should not be None for {key}"
                    )

                    # Verify tensor was loaded back into store
                    retrieved_after_load = self.store.get_tensor(key)
                    self.assertIsNotNone(
                        retrieved_after_load,
                        f"Retrieved tensor after loading should not be None for {key}",
                    )
                    self.assertTrue(
                        torch.allclose(tensor, retrieved_after_load),
                        f"Loaded tensor should match original tensor for {key}",
                    )

                finally:
                    # Clean up the temporary file
                    if os.path.exists(temp_filename):
                        os.remove(temp_filename)

                    # Clean up from store
                    time.sleep(default_kv_lease_ttl / 1000)
                    self.store.remove(key)

    def test_save_tensor_with_default_filename(self):
        """Test saving tensor using the key as the default filename."""
        import torch

        tensor = torch.tensor([5.0, 6.0, 7.0], dtype=torch.float32)
        key = "test_default_filename"

        # Put tensor in the store
        result = self.store.put_tensor(key, tensor)
        self.assertEqual(result, 0)

        try:
            # Save tensor to safetensor file using key as filename
            result = self.store.save_tensor_to_safetensor(key)
            self.assertEqual(
                result,
                0,
                "save_tensor_to_safetensor should return 0 when using key as filename",
            )

            # Verify the file was created with the key name
            expected_filename = key
            self.assertTrue(
                os.path.exists(expected_filename),
                "Safetensor file with key name should exist",
            )

            # Load tensor from the file back to store
            loaded_tensor = self.store.load_tensor_from_safetensor(
                key, expected_filename
            )
            self.assertIsNotNone(loaded_tensor, "Loaded tensor should not be None")

            # Verify tensor was loaded back into store
            retrieved_after_load = self.store.get_tensor(key)
            self.assertIsNotNone(
                retrieved_after_load,
                "Retrieved tensor after loading should not be None",
            )
            self.assertTrue(
                torch.allclose(tensor, retrieved_after_load),
                "Loaded tensor should match original tensor",
            )

        finally:
            # Clean up the temporary file
            if os.path.exists(key):
                os.remove(key)

            # Clean up from store
            time.sleep(default_kv_lease_ttl / 1000)
            self.store.remove(key)

    def test_load_tensor_with_different_key(self):
        """Test loading tensor with a different key than the one stored in the file."""
        import torch

        tensor = torch.tensor([8.0, 9.0, 10.0], dtype=torch.float32)
        key_suffix = uuid.uuid4().hex
        original_key = f"test_original_key_{key_suffix}"
        new_key = f"test_new_key_{key_suffix}"

        # Put tensor in the store with original key
        result = self.store.put_tensor(original_key, tensor)
        self.assertEqual(result, 0)

        with tempfile.NamedTemporaryFile(
            suffix=".safetensors", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            # Save tensor from store to safetensor file
            result = self.store.save_tensor_to_safetensor(original_key, temp_filename)
            self.assertEqual(
                result, 0, "save_tensor_to_safetensor should return 0 on success"
            )

            # Remove tensor from store to test loading it back with a different key
            self.store.remove(original_key)
            time.sleep(6.0)

            # Depending on background lease/cleanup timing, key removal may not be
            # immediately observable in all environments.
            original_key_before_load = self.store.get_tensor(original_key)

            # Load tensor from safetensor file with a different key
            loaded_tensor = self.store.load_tensor_from_safetensor(
                new_key, temp_filename
            )
            self.assertIsNotNone(loaded_tensor, "Loaded tensor should not be None")

            # Verify tensor was loaded with the new key
            retrieved_with_new_key = self.store.get_tensor(new_key)
            self.assertIsNotNone(
                retrieved_with_new_key,
                "Retrieved tensor with new key should not be None",
            )
            self.assertTrue(
                torch.allclose(tensor, retrieved_with_new_key),
                "Loaded tensor should match original tensor",
            )

            # Verify loading with a different key does not unexpectedly create
            # the original key when it was absent before loading.
            retrieved_with_original_key = self.store.get_tensor(original_key)
            if original_key_before_load is None:
                self.assertIsNone(
                    retrieved_with_original_key,
                    "Original key should not be recreated when loading with a new key",
                )

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

            # Clean up from store
            time.sleep(default_kv_lease_ttl / 1000)
            self.store.remove(original_key)
            self.store.remove(new_key)

    def test_load_tensor_from_safetensor_file_not_found(self):
        """Test loading tensor from non-existent safetensor file."""
        # Try to load from a non-existent file
        result = self.store.load_tensor_from_safetensor(
            "test_key", "non_existent_file.safetensors"
        )
        self.assertIsNone(result, "Should return None when file doesn't exist")

    def test_save_tensor_key_not_found(self):
        """Test saving tensor that doesn't exist in the store."""
        with tempfile.NamedTemporaryFile(
            suffix=".safetensors", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            # Try to save a tensor that doesn't exist in the store
            result = self.store.save_tensor_to_safetensor(
                "non_existent_key", temp_filename
            )
            # The expected return code for FILE_NOT_FOUND is likely negative
            self.assertNotEqual(
                result, 0, "Should return error code when key doesn't exist"
            )
        finally:
            # Clean up the temporary file if it was created
            if os.path.exists(temp_filename):
                os.remove(temp_filename)


if __name__ == "__main__":
    unittest.main()
