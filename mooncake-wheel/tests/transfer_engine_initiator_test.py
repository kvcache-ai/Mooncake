import unittest
import ctypes
import os
import random
import string
from mooncake.engine import TransferEngine


def generate_random_string(length):
    chars = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choices(chars, k=length))


class TestVLLMAdaptorTransfer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.target_server_name = os.getenv("TARGET_SERVER_NAME", "127.0.0.1:12345")
        cls.initiator_server_name = os.getenv("INITIATOR_SERVER_NAME", "127.0.0.1:12347")
        cls.metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
        cls.protocol = os.getenv("PROTOCOL", "tcp")        # "rdma" or "tcp"
        cls.circle = int(os.getenv("CIRCLE", 1000))

        cls.adaptor = TransferEngine()
        ret = cls.adaptor.initialize(
            cls.initiator_server_name,
            cls.metadata_server,
            cls.protocol,
            ""
        )
        if ret != 0:
            raise RuntimeError(f"Initialization failed with code {ret}")

    def test_random_write_circle_times(self):
        """Test circle times of random string write/read via buffer transfer."""

        adaptor = self.adaptor
        circles = self.circle

        src_addr = adaptor.get_first_buffer_address(self.initiator_server_name)
        dst_addr = adaptor.get_first_buffer_address(self.target_server_name)

        for i in range(circles):
            str_len = random.randint(16, 256)
            src_data = generate_random_string(str_len).encode('utf-8')
            data_len = len(src_data)

            #Write to local buffer
            result = adaptor.write_bytes_to_buffer(src_addr, src_data, data_len)
            self.assertEqual(result, 0, f"[{i}] writeBytesToBuffer failed")

            #Write to the remote end
            result = adaptor.transfer_sync_write(
                self.target_server_name, src_addr, dst_addr, data_len
            )
            self.assertEqual(result, 0, f"[{i}] WRITE transferSyncExt failed")

            #Clear the local buffer
            clear_data = bytes([0] * data_len)
            result = adaptor.write_bytes_to_buffer(src_addr, clear_data, data_len)
            self.assertEqual(result, 0, f"[{i}] Clear buffer failed")

            #Read it back from the remote end
            result = adaptor.transfer_sync_read(
                self.target_server_name, src_addr, dst_addr, data_len
            )
            self.assertEqual(result, 0, f"[{i}] READ transferSyncExt failed")

            #Verify data consistency
            read_back = adaptor.read_bytes_from_buffer(src_addr, data_len)
            self.assertEqual(read_back, src_data, f"[{i}] Data mismatch")

        print(f"[✓] {circles} iterations of random write-read passed successfully.")

    def test_batch_write_read(self):
        """Test batch_transfer_sync_write and batch_transfer_sync_read for batch write/read consistency."""

        adaptor = self.adaptor
        batch_size = 100  # Adjust batch size if needed
        circles = max(2, self.circle // 100)  # Number of batch test rounds

        base_src_addr = adaptor.get_first_buffer_address(self.initiator_server_name)
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        
        src_addr_list = []
        dst_addr_list = []
        offset_size = 1024  # 1KB offset between each buffer
        
        for i in range(batch_size):
            src_addr_list.append(base_src_addr + i * offset_size)
            dst_addr_list.append(base_dst_addr + i * offset_size)

        for i in range(circles):
            # Generate multiple groups of random data
            data_list = []
            data_len_list = []
            for _ in range(batch_size):
                str_len = random.randint(32, min(128, offset_size))
                src_data = generate_random_string(str_len).encode('utf-8')
                data_list.append(src_data)
                data_len_list.append(len(src_data))

            # Write to local buffers in batch
            for j in range(batch_size):
                result = adaptor.write_bytes_to_buffer(src_addr_list[j], data_list[j], data_len_list[j])
                self.assertEqual(result, 0, f"[{i}-{j}] writeBytesToBuffer failed")

            # Batch write to remote
            result = adaptor.batch_transfer_sync_write(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertEqual(result, 0, f"[{i}] batch_transfer_sync_write failed")

            # Clear local buffers
            for j in range(batch_size):
                clear_data = bytes([0] * data_len_list[j])
                result = adaptor.write_bytes_to_buffer(src_addr_list[j], clear_data, data_len_list[j])
                self.assertEqual(result, 0, f"[{i}-{j}] Clear buffer failed")

            # Batch read back from remote
            result = adaptor.batch_transfer_sync_read(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertEqual(result, 0, f"[{i}] batch_transfer_sync_read failed")

            # Verify data consistency
            for j in range(batch_size):
                read_back = adaptor.read_bytes_from_buffer(src_addr_list[j], data_len_list[j])
                self.assertEqual(read_back, data_list[j], f"[{i}-{j}] Data mismatch in batch read")

        print(f"[✓] {circles} rounds of batch_write_read passed, batch size {batch_size}.")

    def test_async_batch_write_read(self):
        """Test batch_transfer_async_write and batch_transfer_async_read for batch write/read consistency."""

        adaptor = self.adaptor
        batch_size = 100  # Adjust batch size if needed
        circles = max(2, self.circle // 100)  # Number of batch test rounds

        base_src_addr = adaptor.get_first_buffer_address(self.initiator_server_name)
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        
        src_addr_list = []
        dst_addr_list = []
        offset_size = 1024  # 1KB offset between each buffer
        
        for i in range(batch_size):
            src_addr_list.append(base_src_addr + i * offset_size)
            dst_addr_list.append(base_dst_addr + i * offset_size)

        for i in range(circles):
            # Generate multiple groups of random data
            data_list = []
            data_len_list = []
            for _ in range(batch_size):
                str_len = random.randint(32, min(128, offset_size))
                src_data = generate_random_string(str_len).encode('utf-8')
                data_list.append(src_data)
                data_len_list.append(len(src_data))

            # Write to local buffers in batch
            for j in range(batch_size):
                result = adaptor.write_bytes_to_buffer(src_addr_list[j], data_list[j], data_len_list[j])
                self.assertEqual(result, 0, f"[{i}-{j}] writeBytesToBuffer failed")

            # Batch write to remote
            batch_id = adaptor.batch_transfer_async_write(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertNotEqual(batch_id, 0, f"[{i}] batch_transfer_async_write {batch_id} failed in submitting task(s)")

            result = adaptor.get_batch_transfer_status([batch_id])
            self.assertEqual(result, 0, f"[{i}] batch {batch_id} failed during transferring")

            # Clear local buffers
            for j in range(batch_size):
                clear_data = bytes([0] * data_len_list[j])
                result = adaptor.write_bytes_to_buffer(src_addr_list[j], clear_data, data_len_list[j])
                self.assertEqual(result, 0, f"[{i}-{j}] Clear buffer failed")

            # Batch read back from remote
            batch_id = adaptor.batch_transfer_async_read(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertNotEqual(batch_id, 0, f"[{i}] batch_transfer_async_read {batch_id} failed in submitting task(s)")

            result = adaptor.get_batch_transfer_status([batch_id])
            self.assertEqual(result, 0, f"[{i}] batch {batch_id} failed during transferring")

            # Verify data consistency
            for j in range(batch_size):
                read_back = adaptor.read_bytes_from_buffer(src_addr_list[j], data_len_list[j])
                self.assertEqual(read_back, data_list[j], f"[{i}-{j}] Data mismatch in batch read")

        print(f"[✓] {circles} rounds of batch_write_async_read passed, batch size {batch_size}.")

    def run_test_register_memory(self, dst_addr, with_location):
        adaptor = self.adaptor
        circles = self.circle
        buffer_size = 10 * 1024
        buffer = ctypes.create_string_buffer(buffer_size)
        buffer_addr = ctypes.addressof(buffer)

        if with_location:
            adaptor.register_memory(buffer_addr, buffer_size, "cpu")
        else:
            adaptor.register_memory(buffer_addr, buffer_size)

        try:
            for i in range(circles):
                str_len = random.randint(16, 256)
                src_data = generate_random_string(str_len).encode('utf-8')
                data_len = len(src_data)
                offset = random.randint(0, 1024)
                assert offset + data_len <= buffer_size
                buffer[offset:offset + data_len] = src_data

                #Write to the remote end
                result = adaptor.transfer_sync_write(
                    self.target_server_name, buffer_addr + offset, dst_addr, data_len
                )
                self.assertEqual(result, 0, f"[{i}] WRITE transferSyncExt failed")

                #Clear the local buffer
                clear_data = bytes([0] * data_len)
                buffer[offset:offset + data_len] = clear_data

                #Read it back from the remote end
                dst_offset = random.randint(0, 1024)
                assert dst_offset + data_len <= buffer_size

                result = adaptor.transfer_sync_read(
                    self.target_server_name, buffer_addr + dst_offset, dst_addr, data_len
                )
                self.assertEqual(result, 0, f"[{i}] READ transferSyncExt failed")

                #Verify data consistency
                read_back = bytes(buffer[dst_offset:dst_offset + data_len])
                self.assertEqual(read_back, src_data, f"[{i}] Data mismatch")

                #Clear the local buffer
                buffer[dst_offset:dst_offset + data_len] = clear_data
            print(f"[✓] {circles} iterations of random write-read with custom buffer passed successfully ({with_location=}).")
        finally:
            adaptor.unregister_memory(buffer_addr)

    def test_register_memory(self):
        adaptor = self.adaptor
        dst_addr = adaptor.get_first_buffer_address(self.target_server_name)

        for with_location in [False, True]:
            self.run_test_register_memory(dst_addr, with_location)


if __name__ == '__main__':
    unittest.main()
