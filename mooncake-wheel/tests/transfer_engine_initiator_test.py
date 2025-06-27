import unittest
import os
from mooncake.engine import TransferEngine


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
        import random, string

        def generate_random_string(length):
            chars = string.ascii_letters + string.digits + string.punctuation
            return ''.join(random.choices(chars, k=length))

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
        import random, string

        def generate_random_string(length):
            chars = string.ascii_letters + string.digits + string.punctuation
            return ''.join(random.choices(chars, k=length))

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

if __name__ == '__main__':
    unittest.main()
