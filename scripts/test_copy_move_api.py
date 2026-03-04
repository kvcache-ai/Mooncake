import os
import time
import unittest
import multiprocessing as mp
from multiprocessing.queues import Queue as MpQueue
from multiprocessing.synchronize import Event as MpEvent
import queue
import random
import concurrent.futures as cf
from mooncake.store import (
    MooncakeDistributedStore,
    ReplicateConfig,
    QueryTaskResponse,
    TaskStatus,
)
from mooncake.mooncake_config import MooncakeConfig


def create_store_instance(config: MooncakeConfig) -> MooncakeDistributedStore:
    """
    Create and setup a MooncakeDistributedStore instance
    1. Setup MooncakeDistributedStore
    3. Return the store instance
    """
    print(
        f"[{os.getpid()}] Connecting to Mooncake Master at {config.master_server_address} "
        f"as {config.local_hostname} using {config.protocol}..."
    )
    store = MooncakeDistributedStore()
    rc = store.setup(
        config.local_hostname,
        config.metadata_server,
        config.global_segment_size,
        config.local_buffer_size,
        config.protocol,
        config.device_name,
        config.master_server_address,
    )
    if rc != 0:
        raise RuntimeError(f"Failed to setup mooncake store, return code: {rc}")
    return store


def client_process_main(config_dict: dict, ready_q: MpQueue, stop_ev: MpEvent):
    """
    create a client process with given config
    """
    # Reconstruct config from dict
    config = MooncakeConfig(**config_dict)

    # Setup store
    store = create_store_instance(config)

    # Notify main process this client is ready
    ready_q.put({"local_hostname": config.local_hostname})

    # Keep process alive to maintain heartbeats/leases
    while not stop_ev.is_set():
        time.sleep(1)

    _ = store


def query_task_until_complete(
    store: MooncakeDistributedStore, task_id: tuple[int, int]
) -> QueryTaskResponse:
    """
    query the task until the actual copy and move have been completed
    """
    while True:
        resp, err = store.query_task(task_id)
        if err != 0:
            raise RuntimeError(f"Query task failed with err={err}")
        if resp.status in (TaskStatus.SUCCESS, TaskStatus.FAILED):
            return resp
        time.sleep(0.5)


def get_descriptor_transport_endpoints(
    store: MooncakeDistributedStore, key: str
) -> list[str]:
    """
    Get the transport endpoints from the replica descriptors of a given key
    """
    resp = store.get_replica_desc(key)
    transport_endpoints = []
    for replica_descriptor in resp:
        if replica_descriptor.is_memory_replica:
            mem_descriptor = replica_descriptor.get_memory_descriptor()
            transport_endpoints.append(
                mem_descriptor.buffer_descriptor.transport_endpoint
            )
    return transport_endpoints


class MooncakeCopyMoveAPITest(unittest.TestCase):
    """
    Test the Copy and Move API of MooncakeDistributedStore
    1. Setup three client processes simulating three storage servers
    2. Run copy and move tasks between the clients
    3. Verify the results
    4. Cleanup the client processes
    """

    MAX_NUM_WORKERS = 8

    @classmethod
    def setUpClass(cls):
        config = MooncakeConfig.load_from_env()
        base_config = config.__dict__

        segment_unit = 16 * 1024 * 1024  # 16 MB
        clients = [
            ("localhost:10001", segment_unit),
            ("localhost:10002", segment_unit),
            ("localhost:10003", 2 * segment_unit),
        ]

        cls.ctx = mp.get_context("spawn")
        cls.ready_q: MpQueue = cls.ctx.Queue()
        cls.stop_ev: MpEvent = cls.ctx.Event()
        cls.process = []

        print("[main] Spawning client processes...")
        for hostname, segment_size in clients:
            # clone config and update config dict for each client
            client_config = base_config.copy()
            client_config["local_hostname"] = hostname
            client_config["global_segment_size"] = segment_size
            client_config["local_buffer_size"] = (
                segment_size * 2
            )  # Double the segment size

            p = cls.ctx.Process(
                target=client_process_main,
                args=(client_config, cls.ready_q, cls.stop_ev),
                daemon=True,
            )
            p.start()
            cls.process.append(p)

        # Wait for all clients to be ready
        ready_clients = []
        for _ in clients:
            try:
                info = cls.ready_q.get(timeout=30)
                ready_clients.append(info["local_hostname"])
                print(f"[main] Client ready: {info['local_hostname']}")
            except queue.Empty:
                print("[main] Timeout waiting for clients to start")
                break

        if len(ready_clients) != len(clients):
            print("[main] Not all clients started. Exiting.")
            cls.stop_ev.set()
            for p in cls.process:
                p.join()
            raise RuntimeError("Failed to start all client processes")

        print("[main] All clients are ready. Running tests...")

        # Create a coordinator client to run tests
        coordinator_config = base_config.copy()
        coordinator_config["local_hostname"] = "localhost:12001"
        coordinator_config["local_buffer_size"] = 2 * segment_unit
        coordinator_config["global_segment_size"] = 0

        cls.coord_store = create_store_instance(MooncakeConfig(**coordinator_config))
        cls.client_segments = list(ready_clients)

    @classmethod
    def tearDownClass(cls):
        # Cleanup
        print("[main] Stopping client processes...")
        cls.stop_ev.set()
        for p in cls.process:
            p.join()
        print("[main] All client processes stopped.")

        # Remove all keys created during tests
        cls.coord_store.remove_all()

    def test_basic_copy_operation(self):
        """
        Given:
            1. Three storage server with segment names: localhost:10001, localhost:10002
            and localhost:10003
            2. Put the key in the client localhost:10001
        When:
            Create a copy task which copy the key to localhost:10002 and localhost:10003
        Then:
            1. The task should execute with success status
            2. The key's replica should reside in clients localhost:10001, localhost:10002
            and localhost:10003
        """
        # 1. Put Data to the client1: localhost:10001
        key = "test_key" + str(random.randint(1, 10000))
        print(f"[main] Putting key: {key}")
        payload = os.urandom(2 * 1024 * 1024)  # 2 MB random data
        replicate_config = ReplicateConfig()
        replicate_config.replica_num = 1
        # Prefer client1: localhost:10001
        replicate_config.preferred_segment = self.client_segments[0]
        rc = self.coord_store.put(key, payload, replicate_config)
        if rc != 0:
            raise RuntimeError(f"Put failed with rc={rc}")

        # 2. Copy Data to clients: localhost:10002, localhost:10003
        print(f"[main] Copying key: {key} to clients: {self.client_segments[1:]}")
        task_id, err = self.coord_store.create_copy_task(key, self.client_segments[1:])
        if err != 0:
            raise RuntimeError(f"Create copy task failed with err={err}")
        print(f"[main] Created copy task with ID: {task_id}")

        # 3. Query Task
        print(f"[main] Querying task {task_id} status...")
        resp = query_task_until_complete(self.coord_store, task_id)
        print(f"[main] Task {task_id} status: {resp}")

        # 4. Verify the key is reside in clients localhost:10001,
        # localhost:10002 and localhost:10003
        transport_endpoints = get_descriptor_transport_endpoints(self.coord_store, key)
        print(f"[main] key: {key} is in transport_endpoints: {transport_endpoints}")
        self.assertListEqual(sorted(transport_endpoints), sorted(self.client_segments))

    def test_basic_move_operation(self):
        """
        Given:
            1. Three storage server with segment names: localhost:10001, localhost:10002
            and localhost:10003
            2. Put the key in the client localhost:10001
        When:
            Create a move task which move the key from localhost:10001 to localhost:10002
        Then:
            1. The task should execute with success status
            2. The key's replica should reside in clients localhost:10002 only
        """
        # 1. Put Data to the client1: localhost:10001
        key = "move_test_key" + str(random.randint(1, 10000))
        print(f"[main] Putting key: {key}")
        payload = os.urandom(2 * 1024 * 1024)  # 2 MB random data
        replicate_config = ReplicateConfig()
        replicate_config.replica_num = 1
        # Prefer client1: localhost:10001
        replicate_config.preferred_segment = self.client_segments[0]
        rc = self.coord_store.put(key, payload, replicate_config)
        if rc != 0:
            raise RuntimeError(f"Put failed with rc={rc}")

        # 2. Move Data from client localhost:10001 to localhost:10002
        print(
            f"""[main] Moving key: {key} from {self.client_segments[0]} to {
                self.client_segments[1]}"""
        )
        task_id, err = self.coord_store.create_move_task(
            key, self.client_segments[0], self.client_segments[1]
        )
        if err != 0:
            raise RuntimeError(f"Create move task failed with err={err}")
        print(f"[main] Created move task with ID: {task_id}")

        # 3. Query Task
        print(f"[main] Querying move task {task_id} status...")
        resp = query_task_until_complete(self.coord_store, task_id)
        print(f"[main] Move Task {task_id} status: {resp}")

        # 4. Verify the key is reside in client localhost:10002 only
        transport_endpoints = get_descriptor_transport_endpoints(self.coord_store, key)
        print(f"[main] key: {key} is in transport_endpoints: {transport_endpoints}")
        self.assertEqual(len(transport_endpoints), 1)
        self.assertEqual(transport_endpoints[0], self.client_segments[1])

    def test_copy_nonexistent_key(self):
        """
        Given:
            A nonexistent key
        When:
            Create a copy task for the nonexistent key
        Then:
            The task should execute with failed status -704 (OBJECT_NOT_FOUND)
        """
        key = "nonexistent_key_" + str(random.randint(1, 10000))
        print(f"[main] Creating copy task for nonexistent key: {key}")
        _, err = self.coord_store.create_copy_task(key, self.client_segments[1:])
        self.assertEqual(
            err, -704, "Creating copy task for nonexistent key should fail"
        )

    def test_move_nonexistent_key(self):
        """
        Given:
            A nonexistent key
        When:
            Create a move task for the nonexistent key
        Then:
            The task should execute with failed status -704 (OBJECT_NOT_FOUND)
        """
        key = "nonexistent_key_" + str(random.randint(1, 10000))
        print(f"[main] Creating move task for nonexistent key: {key}")
        _, err = self.coord_store.create_move_task(
            key, self.client_segments[0], self.client_segments[1]
        )
        self.assertEqual(
            err, -704, "Creating move task for nonexistent key should fail"
        )

    def test_copy_with_unknown_segments(self):
        """
        Given:
            A valid key
        When:
            Create a copy task with unknown segment names
        Then:
            The task should execute with failed status -600 (INVALID_PARAMS)
        """
        # 1. Put Data to the client1: localhost:10001
        key = "test_key_unknown_segment_" + str(random.randint(1, 10000))
        print(f"[main] Putting key: {key}")
        payload = os.urandom(2 * 1024)  # 2 kB random data
        replicate_config = ReplicateConfig()
        replicate_config.replica_num = 1
        # Prefer client1: localhost:10001
        replicate_config.preferred_segment = self.client_segments[0]
        rc = self.coord_store.put(key, payload, replicate_config)
        if rc != 0:
            raise RuntimeError(f"Put failed with rc={rc}")

        # 2. Copy Data to unknown segments
        unknown_segments = ["unknown_segment_1", "unknown_segment_2"]
        print(f"[main] Copying key: {key} to unknown segments: {unknown_segments}")
        _, err = self.coord_store.create_copy_task(key, unknown_segments)
        self.assertEqual(
            err, -600, "Creating copy task with unknown segments should fail"
        )

    def test_move_with_unknown_segments(self):
        """
        Given:
            A valid key
        When:
            Create a move task with unknown segment names
        Then:
            The task should execute with failed status -600 (INVALID_PARAMS)
        """
        # 1. Put Data to the client1: localhost:10001
        key = "test_key_unknown_segment_move_" + str(random.randint(1, 10000))
        print(f"[main] Putting key: {key}")
        payload = os.urandom(2 * 1024)  # 2 kB random data
        replicate_config = ReplicateConfig()
        replicate_config.replica_num = 1
        # Prefer client1: localhost:10001
        replicate_config.preferred_segment = self.client_segments[0]
        rc = self.coord_store.put(key, payload, replicate_config)
        if rc != 0:
            raise RuntimeError(f"Put failed with rc={rc}")

        # 2. Move Data from unknown source segment
        unknown_source_segment = "unknown_source_segment"
        print(
            f"""[main] Moving key: {key} from unknown source segment:
              {unknown_source_segment} to {self.client_segments[1]}"""
        )
        _, err = self.coord_store.create_move_task(
            key, unknown_source_segment, self.client_segments[1]
        )
        self.assertEqual(
            err, -600, "Creating move task with unknown source segment should fail"
        )

        # 3. Move Data to unknown destination segment
        unknown_destination_segment = "unknown_destination_segment"
        print(
            f"""[main] Moving key: {key} from {self.client_segments[0]}
              to unknown destination segment: {unknown_destination_segment}"""
        )
        _, err = self.coord_store.create_move_task(
            key, self.client_segments[0], unknown_destination_segment
        )
        self.assertEqual(
            err, -600, "Creating move task with unknown destination segment should fail"
        )

    def test_copy_to_exist_segments(self):
        """
        Given:
            A valid key existing in localhost:10001 and localhost:10002
        When:
            Create a copy task to copy the key to localhost:10002 again
        Then:
            The key's replica should still reside in clients localhost:10001
            and localhost:10002 only
        """
        # 1. Put Data to the client: localhost:10001 and localhost:10002
        key = "test_key_exist_segment_" + str(random.randint(1, 10000))
        print(f"[main] Putting key: {key}")
        payload = os.urandom(2 * 1024)  # 2 kB random data
        replicate_config = ReplicateConfig()
        replicate_config.replica_num = 2
        # Prefer client1: localhost:10001 and client2: localhost:10002
        replicate_config.preferred_segments = self.client_segments[:2]
        rc = self.coord_store.put(key, payload, replicate_config)
        if rc != 0:
            raise RuntimeError(f"Put failed with rc={rc}")

        # 2. Copy Data to client localhost:10002 again
        print(f"[main] Copying key: {key} to client: {self.client_segments[1]} again")
        task_id, err = self.coord_store.create_copy_task(key, [self.client_segments[1]])
        if err != 0:
            raise RuntimeError(f"Create copy task failed with err={err}")
        print(f"[main] Created copy task with ID: {task_id}")

        # 3. Query Task
        print(f"[main] Querying task {task_id} status...")
        resp = query_task_until_complete(self.coord_store, task_id)
        print(f"[main] Task {task_id} status: {resp}")

        # 4. Verify the key is reside in clients localhost:10001 and localhost:10002 only
        transport_endpoints = get_descriptor_transport_endpoints(self.coord_store, key)
        print(f"[main] key: {key} is in transport_endpoints: {transport_endpoints}")
        self.assertListEqual(
            sorted(transport_endpoints), sorted(self.client_segments[:2])
        )

    def test_move_to_same_segment(self):
        """
        Given:
            A valid key existing in localhost:10001
        When:
            Create a move task to move the key to localhost:10001 again
        Then:
            The task creation should fail with error -600 (INVALID_PARAMS)
        """
        # 1. Put Data to the client1: localhost:10001
        key = "test_key_move_same_segment_" + str(random.randint(1, 10000))
        print(f"[main] Putting key: {key}")
        payload = os.urandom(2 * 1024)  # 2 kB random data
        replicate_config = ReplicateConfig()
        replicate_config.replica_num = 1
        # Prefer client1: localhost:10001
        replicate_config.preferred_segment = self.client_segments[0]
        rc = self.coord_store.put(key, payload, replicate_config)
        if rc != 0:
            raise RuntimeError(f"Put failed with rc={rc}")

        # 2. Move Data to client localhost:10001 again
        print(f"[main] Moving key: {key} to client: {self.client_segments[0]} again")
        _, err = self.coord_store.create_move_task(
            key, self.client_segments[0], self.client_segments[0]
        )
        self.assertEqual(
            err, -600, "Creating move task to the same segment should fail"
        )

    def put_n_keys(
        self, key_prefix: str, n: int, replica_num: int
    ) -> list[tuple[str, str]]:
        """
        Helper function to put N keys with given replica_num
        Return the list of tuple[key, segment_name]
        """
        keys: list[str] = []
        for i in range(n):
            key = key_prefix + str(i)
            payload = os.urandom(2 * 1024)  # 2 kB random data
            replicate_config = ReplicateConfig()
            replicate_config.replica_num = replica_num
            # Prefer a random client
            preferred_segment = random.choice(self.client_segments)
            replicate_config.preferred_segment = preferred_segment
            rc = self.coord_store.put(key, payload, replicate_config)
            if rc != 0:
                raise RuntimeError(f"Put failed with rc={rc}")
            keys.append((key, preferred_segment))
        return keys

    def test_concurrent_copy_operations(self):
        """
        Given:
            Multiple valid keys with replica number 1
        When:
            Create multiple copy tasks concurrently to copy the keys to all clients
        Then:
            1. All tasks should execute with success status
            2. All keys' replicas should reside in all clients
        """

        # 1. Put N keys (replica_num=1) to random clients
        num_keys = 100
        key_and_segments = self.put_n_keys(
            "concurrent_copy_key_", num_keys, replica_num=1
        )

        # 2. Create copy tasks concurrently
        key_to_task_ids: dict[str, tuple[int, int]] = {}
        with cf.ThreadPoolExecutor(max_workers=self.MAX_NUM_WORKERS) as executor:
            futures = {
                executor.submit(
                    self.coord_store.create_copy_task,
                    key,
                    list(set(self.client_segments) - {preferred_segment}),
                ): key
                for key, preferred_segment in key_and_segments
            }
            for future in cf.as_completed(futures):
                key = futures[future]
                task_id, err = future.result()
                if err != 0:
                    raise RuntimeError(
                        f"Create copy task for key {key} failed with err={err}"
                    )
                key_to_task_ids[key] = task_id

        # 3. Query all tasks until complete
        with cf.ThreadPoolExecutor(max_workers=self.MAX_NUM_WORKERS) as executor:
            futures = [
                executor.submit(query_task_until_complete, self.coord_store, task_id)
                for task_id in key_to_task_ids.values()
            ]
            cf.wait(futures)

        # 4. Verify all keys are in all clients
        for key in key_to_task_ids:
            transport_endpoints = get_descriptor_transport_endpoints(
                self.coord_store, key
            )
            self.assertListEqual(
                sorted(transport_endpoints), sorted(self.client_segments)
            )

    def test_concurrent_move_operations(self):
        """
        Given:
            Multiple valid keys with replica number 1
        When:
            Create multiple move tasks concurrently to move the keys from
            current client to other client
        Then:
            1. All tasks should execute with success status
            2. All keys' replicas should reside in the destination client only
        """
        # 1. Put N keys (replica_num=1) to random clients
        num_keys = 100
        key_and_segments = self.put_n_keys(
            "concurrent_move_key_", num_keys, replica_num=1
        )

        # 2. Create move tasks concurrently
        key_to_task_ids: dict[str, tuple[int, int]] = {}
        key_to_target_segments: dict[str, str] = {}
        with cf.ThreadPoolExecutor(max_workers=self.MAX_NUM_WORKERS) as executor:
            futures = {}
            for key, source_segment in key_and_segments:
                dest_segment = random.choice(
                    list(set(self.client_segments) - {source_segment})
                )
                futures[
                    executor.submit(
                        self.coord_store.create_move_task,
                        key,
                        source_segment,
                        dest_segment,
                    )
                ] = (key, dest_segment)

            for future in cf.as_completed(futures):
                key, dest_segment = futures[future]
                task_id, err = future.result()
                if err != 0:
                    raise RuntimeError(
                        f"Create move task for key {key} failed with err={err}"
                    )
                key_to_task_ids[key] = task_id
                key_to_target_segments[key] = dest_segment

        # 3. Query all tasks and verify results
        with cf.ThreadPoolExecutor(max_workers=self.MAX_NUM_WORKERS) as executor:
            futures = [
                executor.submit(query_task_until_complete, self.coord_store, task_id)
                for task_id in key_to_task_ids.values()
            ]
            cf.wait(futures)

        # 4. Verify all keys are in destination clients only
        for key, dest_segment in key_to_target_segments.items():
            transport_endpoints = get_descriptor_transport_endpoints(
                self.coord_store, key
            )
            dest_te_endpoint = self.client_segments[
                self.client_segments.index(dest_segment)
            ]
            self.assertEqual(len(transport_endpoints), 1)
            self.assertEqual(transport_endpoints[0], dest_te_endpoint)


if __name__ == "__main__":
    unittest.main()
