import unittest
import requests
import subprocess
import time
import uuid

class TestMetadataService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.process = subprocess.Popen(['go', 'run', 'your_server_file.go'])  # 替换为你的文件名
        cls.wait_until_server_up()

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()

    @classmethod
    def wait_until_server_up(cls, timeout=10):
        start = time.time()
        while True:
            try:
                response = requests.get('http://localhost:8080/metadata?key=healthcheck')
                if response.status_code == 404:
                    break
            except requests.exceptions.ConnectionError:
                pass
            if time.time() - start > timeout:
                raise TimeoutError("Server did not start in time")
            time.sleep(0.1)

    def setUp(self):
        self.key = str(uuid.uuid4())

    def tearDown(self):
        requests.delete(f'http://localhost:8080/metadata?key={self.key}')

    def test_put_and_get_metadata(self):
        test_data = b'{"name": "test"}'
        put_response = requests.put(
            f'http://localhost:8080/metadata?key={self.key}',
            data=test_data
        )
        self.assertEqual(put_response.status_code, 200)
        self.assertEqual(put_response.text, 'metadata updated')

        get_response = requests.get(f'http://localhost:8080/metadata?key={self.key}')
        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(get_response.content, test_data)

    def test_get_non_existent_key(self):
        response = requests.get(f'http://localhost:8080/metadata?key={self.key}')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.text, 'metadata not found')

    def test_delete_metadata(self):
        test_data = b'{"temp": "data"}'
        requests.put(f'http://localhost:8080/metadata?key={self.key}', data=test_data)
        
        del_response = requests.delete(f'http://localhost:8080/metadata?key={self.key}')
        self.assertEqual(del_response.status_code, 200)
        self.assertEqual(del_response.text, 'metadata deleted')

        get_response = requests.get(f'http://localhost:8080/metadata?key={self.key}')
        self.assertEqual(get_response.status_code, 404)

    def test_delete_non_existent_key(self):
        del_response = requests.delete(f'http://localhost:8080/metadata?key=nonexistent_key')
        self.assertEqual(del_response.status_code, 404)
        self.assertEqual(del_response.text, 'metadata not found')

    def test_put_overwrite_existing_key(self):
        first_data = b'first'
        second_data = b'second'

        requests.put(f'http://localhost:8080/metadata?key={self.key}', data=first_data)
        put_response = requests.put(f'http://localhost:8080/metadata?key={self.key}', data=second_data)
        self.assertEqual(put_response.status_code, 200)

        get_response = requests.get(f'http://localhost:8080/metadata?key={self.key}')
        self.assertEqual(get_response.content, second_data)

    def test_put_with_empty_key(self):
        # Test Blank
        self.key = ""  # NULL
        test_data = b'empty_key_data'

        put_response = requests.put('http://localhost:8080/metadata', data=test_data)
        self.assertEqual(put_response.status_code, 200)

        get_response = requests.get('http://localhost:8080/metadata?key=')
        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(get_response.content, test_data)

if __name__ == '__main__':
    unittest.main()
