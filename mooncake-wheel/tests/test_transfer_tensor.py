import ctypes
import struct
import unittest
import os
import time
import threading
import random
import torch
from mooncake import engine


def get_transfer_engine():
    """Initialize and setup the transfer engine."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE")

    te = engine.TransferEngine()
    retcode = te.initialize(local_hostname, metadata_server, protocol, device_name)

    if retcode:
        raise RuntimeError(f"Failed to initialize transfer engine. Return code: {retcode}")
    
    return te


class TestTransferEngineTensor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize the transfer engines for both sender and receiver."""
        cls.sender_engine = get_transfer_engine()
        cls.receiver_engine = get_transfer_engine() 
        
        sender_port = cls.sender_engine.get_rpc_port()
        receiver_port = cls.receiver_engine.get_rpc_port()
        
        cls.sender_hostname = f"localhost:{sender_port}"
        cls.receiver_hostname = f"localhost:{receiver_port}"
        
        print(f"Sender engine on: {cls.sender_hostname}")
        print(f"Receiver engine on: {cls.receiver_hostname}")
        
        time.sleep(1)

    def test_transfer_tensor_float32(self):
        """Test transferring float32 tensor."""
        # Create a float32 tensor on sender side
        tensor = torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)
        
        # Calculate total size (metadata + tensor data)
        metadata_size = 24  # sizeof(TensorMetadata) = 4 * 4 bytes
        tensor_size = tensor.numel() * tensor.element_size()
        total_size = metadata_size + tensor_size
        
        # Allocate buffer on receiver side
        receiver_buffer = self.receiver_engine.allocate_managed_buffer(total_size)
        self.assertNotEqual(receiver_buffer, 0)
        
        try:
            # Transfer tensor from sender to receiver
            result = self.sender_engine.transfer_tensor_sync_write(
                self.receiver_hostname, tensor, receiver_buffer)
            self.assertEqual(result, 0)
            
            # Read tensor back on sender side (simulating cross-node retrieval)
            retrieved_tensor = self.sender_engine.transfer_tensor_sync_read(
                self.receiver_hostname, receiver_buffer, total_size)
            
            # Verify the retrieved tensor
            self.assertIsNotNone(retrieved_tensor)
            self.assertEqual(retrieved_tensor.dtype, tensor.dtype)
            self.assertEqual(tuple(retrieved_tensor.shape), tuple(tensor.shape))
            self.assertTrue(torch.allclose(tensor, retrieved_tensor))
            
        finally:
            # Clean up
            self.receiver_engine.free_managed_buffer(receiver_buffer, total_size)

    def test_transfer_tensor_int32(self):
        """Test transferring int32 tensor."""
        # Create an int32 tensor
        tensor = torch.tensor([1, 2, 3, 4], dtype=torch.int32)
        
        # Calculate total size
        metadata_size = 24
        tensor_size = tensor.numel() * tensor.element_size()
        total_size = metadata_size + tensor_size
        
        # Allocate buffer on receiver side
        receiver_buffer = self.receiver_engine.allocate_managed_buffer(total_size)
        self.assertNotEqual(receiver_buffer, 0)
        
        try:
            # Transfer tensor
            result = self.sender_engine.transfer_tensor_sync_write(
                self.receiver_hostname, tensor, receiver_buffer)
            self.assertEqual(result, 0)
            
            # Read tensor back
            retrieved_tensor = self.sender_engine.transfer_tensor_sync_read(
                self.receiver_hostname, receiver_buffer, total_size)
            
            # Verify
            self.assertIsNotNone(retrieved_tensor)
            self.assertEqual(retrieved_tensor.dtype, tensor.dtype)
            self.assertEqual(tuple(retrieved_tensor.shape), tuple(tensor.shape))
            self.assertTrue(torch.equal(tensor, retrieved_tensor))
            
        finally:
            self.receiver_engine.free_managed_buffer(receiver_buffer, total_size)

    def test_transfer_tensor_bool(self):
        """Test transferring bool tensor."""
        # Create a bool tensor
        tensor = torch.tensor([True, False, False, True], dtype=torch.bool)
        
        # Calculate total size
        metadata_size = 24
        tensor_size = tensor.numel() * tensor.element_size()
        total_size = metadata_size + tensor_size
        
        # Allocate buffer on receiver side
        receiver_buffer = self.receiver_engine.allocate_managed_buffer(total_size)
        self.assertNotEqual(receiver_buffer, 0)
        
        try:
            # Transfer tensor
            result = self.sender_engine.transfer_tensor_sync_write(
                self.receiver_hostname, tensor, receiver_buffer)
            self.assertEqual(result, 0)
            
            # Read tensor back
            retrieved_tensor = self.sender_engine.transfer_tensor_sync_read(
                self.receiver_hostname, receiver_buffer, total_size)
            
            # Verify
            self.assertIsNotNone(retrieved_tensor)
            self.assertEqual(retrieved_tensor.dtype, tensor.dtype)
            self.assertEqual(tuple(retrieved_tensor.shape), tuple(tensor.shape))
            self.assertTrue(torch.equal(tensor, retrieved_tensor))
            
        finally:
            self.receiver_engine.free_managed_buffer(receiver_buffer, total_size)

    def test_transfer_tensor_large(self):
        """Test transferring large tensor."""
        # Create a larger tensor
        tensor = torch.randn(1000, dtype=torch.float32)
        
        # Calculate total size
        metadata_size = 24
        tensor_size = tensor.numel() * tensor.element_size()
        total_size = metadata_size + tensor_size
        
        # Allocate buffer on receiver side
        receiver_buffer = self.receiver_engine.allocate_managed_buffer(total_size)
        self.assertNotEqual(receiver_buffer, 0)
        
        try:
            # Transfer tensor
            result = self.sender_engine.transfer_tensor_sync_write(
                self.receiver_hostname, tensor, receiver_buffer)
            self.assertEqual(result, 0)
            
            # Read tensor back
            retrieved_tensor = self.sender_engine.transfer_tensor_sync_read(
                self.receiver_hostname, receiver_buffer, total_size)
            
            # Verify
            self.assertIsNotNone(retrieved_tensor)
            self.assertEqual(retrieved_tensor.dtype, tensor.dtype)
            self.assertEqual(tuple(retrieved_tensor.shape), tuple(tensor.shape))
            self.assertTrue(torch.allclose(tensor, retrieved_tensor))
            
        finally:
            self.receiver_engine.free_managed_buffer(receiver_buffer, total_size)

    def test_transfer_tensor_multidimensional(self):
        """Test transferring multi-dimensional tensor."""
        # Create a 2D tensor
        tensor = torch.randn(3, 4, dtype=torch.float32)
        
        # Calculate total size
        metadata_size = 24
        tensor_size = tensor.numel() * tensor.element_size()
        total_size = metadata_size + tensor_size
        
        # Allocate buffer on receiver side
        receiver_buffer = self.receiver_engine.allocate_managed_buffer(total_size)
        self.assertNotEqual(receiver_buffer, 0)
        
        try:
            # Transfer tensor
            result = self.sender_engine.transfer_tensor_sync_write(
                self.receiver_hostname, tensor, receiver_buffer)
            self.assertEqual(result, 0)
            
            # Read tensor back
            retrieved_tensor = self.sender_engine.transfer_tensor_sync_read(
                self.receiver_hostname, receiver_buffer, total_size)
            
            # Verify
            self.assertIsNotNone(retrieved_tensor)
            self.assertEqual(retrieved_tensor.dtype, tensor.dtype)
            self.assertEqual(tuple(retrieved_tensor.shape), tuple(tensor.shape))
            self.assertTrue(torch.allclose(tensor, retrieved_tensor))
            
        finally:
            self.receiver_engine.free_managed_buffer(receiver_buffer, total_size)

    @classmethod
    def tearDownClass(cls):
        """Clean up transfer engines."""
        pass


if __name__ == '__main__':
    unittest.main()