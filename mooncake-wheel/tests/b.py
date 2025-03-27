import unittest
import os
from mooncake import mooncake_vllm_adaptor


class TestVLLMAdaptorTransfer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.FLAGS = {
            "target_server_name": "127.0.0.1:12345",
            "initiator_server_name": "127.0.0.2:12347",
            "metadata_server": "127.0.0.1:2379",
            "protocol": "tcp",   # Protocol type: "rdma" or "tcp"
            "circle": 1000,
        }

        cls.adaptor = mooncake_vllm_adaptor()
        ret = cls.adaptor.initialize(
            cls.FLAGS["initiator_server_name"],
            cls.FLAGS["metadata_server"],
            cls.FLAGS["protocol"],
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

        FLAGS = self.FLAGS
        adaptor = self.adaptor
        circles = FLAGS["circle"]

        src_addr = adaptor.getFirstBufferAddress(FLAGS["initiator_server_name"])
        dst_addr = adaptor.getFirstBufferAddress(FLAGS["target_server_name"])

        for i in range(circles):
            str_len = random.randint(16, 256)
            src_data = generate_random_string(str_len).encode('utf-8')
            data_len = len(src_data)

            #Step 1: Write to local buffer
            result = adaptor.writeBytesToBuffer(src_addr, src_data, data_len)
            self.assertEqual(result, 0, f"[{i}] writeBytesToBuffer failed")

            #Step 2: Write to the remote end
            result = adaptor.transferSyncExt(
                FLAGS["target_server_name"], src_addr, dst_addr, data_len, adaptor.TransferOpcode.WRITE
            )
            self.assertEqual(result, 0, f"[{i}] WRITE transferSyncExt failed")

            #Step 3: Clear the local buffer
            clear_data = bytes([0] * data_len)
            result = adaptor.writeBytesToBuffer(src_addr, clear_data, data_len)
            self.assertEqual(result, 0, f"[{i}] Clear buffer failed")

            #Step 4: Read it back from the remote end
            result = adaptor.transferSyncExt(
                FLAGS["target_server_name"], src_addr, dst_addr, data_len, adaptor.TransferOpcode.READ
            )
            self.assertEqual(result, 0, f"[{i}] READ transferSyncExt failed")

            #Step 5: Verify data consistency
            read_back = adaptor.readBytesFromBuffer(src_addr, data_len)
            self.assertEqual(read_back, src_data, f"[{i}] Data mismatch")

        print(f"[✓] {circles} iterations of random write-read passed successfully.")

if __name__ == '__main__':
    unittest.main()
