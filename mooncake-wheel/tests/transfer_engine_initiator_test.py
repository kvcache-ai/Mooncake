import unittest
import os
from mooncake import mooncake_vllm_adaptor


class TestVLLMAdaptorTransfer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.target_server_name = os.getenv("TARGET_SERVER_NAME", "127.0.0.1:12345")
        cls.initiator_server_name = os.getenv("INITIATOR_SERVER_NAME", "127.0.0.1:12347")
        cls.metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
        cls.protocol = os.getenv("PROTOCOL", "tcp")        # "rdma" or "tcp"
        cls.circle = int(os.getenv("CIRCLE", 1000))

        cls.adaptor = mooncake_vllm_adaptor()
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

        src_addr = adaptor.getFirstBufferAddress(self.initiator_server_name)
        dst_addr = adaptor.getFirstBufferAddress(self.target_server_name)

        for i in range(circles):
            str_len = random.randint(16, 256)
            src_data = generate_random_string(str_len).encode('utf-8')
            data_len = len(src_data)

            #Write to local buffer
            result = adaptor.writeBytesToBuffer(src_addr, src_data, data_len)
            self.assertEqual(result, 0, f"[{i}] writeBytesToBuffer failed")

            #Write to the remote end
            result = adaptor.transferSyncExt(
                self.target_server_name, src_addr, dst_addr, data_len, adaptor.TransferOpcode.WRITE
            )
            self.assertEqual(result, 0, f"[{i}] WRITE transferSyncExt failed")

            #Clear the local buffer
            clear_data = bytes([0] * data_len)
            result = adaptor.writeBytesToBuffer(src_addr, clear_data, data_len)
            self.assertEqual(result, 0, f"[{i}] Clear buffer failed")

            #Read it back from the remote end
            result = adaptor.transferSyncExt(
                self.target_server_name, src_addr, dst_addr, data_len, adaptor.TransferOpcode.READ
            )
            self.assertEqual(result, 0, f"[{i}] READ transferSyncExt failed")

            #Verify data consistency
            read_back = adaptor.readBytesFromBuffer(src_addr, data_len)
            self.assertEqual(read_back, src_data, f"[{i}] Data mismatch")

        print(f"[✓] {circles} iterations of random write-read passed successfully.")

if __name__ == '__main__':
    unittest.main()
