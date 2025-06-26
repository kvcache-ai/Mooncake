import random
import string

from mooncake.store import MooncakeDistributedStore

def simple_put_get(store):
    key = "testkey"
    value = "this is a test case"
    print(f"put key = {key}, value = {value}")
    store.put(key, bytes(value, 'utf-8'))

    value = store.get(key)
    print(f"get key = {key}, value = {value.decode('utf-8')}")
    print("get_size = ", store.get_size(key))
    store.remove(key)
    print(f"after remove, key = {key}, is_exist = {store.is_exist(key)}")
    print("value = ", store.get(key))
    print("get_size = ", store.get_size(key))


def multi_put_get(store, max_requests=100):
    for x in range(max_requests):
        key = f"testkey_{x}"
        value = f"this is a test case {x}"
        print(f"put key = {key}, value = {value}")
        store.put(key, bytes(value, 'utf-8'))

    for _ in range(min(5, max_requests)):
        random_key = f"testkey_{random.randint(0, max_requests - 1)}"
        stored_value = store.get(random_key)
        if stored_value:
            print(f"get key = {random_key}, value = {stored_value.decode('utf-8')}")
        else:
            print(f"key = {random_key} not found in store")

def large_put_get(store, value_sz_in_mb=100, max_requests=100):
    for x in range(max_requests):
        key = f"testkey_large{x}"
        ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        value_length = value_sz_in_mb * 1024 * 1024
        value = ran_str * (value_length // 32)
        print(f"put key = {key}, value_length = {len(value)}")
        store.put(key, bytes(value, 'utf-8'))

    for _ in range(min(5, max_requests)):
        random_key = f"testkey_large{random.randint(0, max_requests - 1)}"
        stored_value = store.get(random_key)
        if stored_value:
            print(f"get key = {random_key}, value_length = {len(stored_value.decode('utf-8'))}")
        else:
            print(f"key = {random_key} not found in store")

def batch_put_get(store, batch_size=10):
    random_keys = []
    keys = []
    values = []
    for x in range(batch_size):
        # value_length = 32 * 1024 * 1024
        # ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        # value = ran_str * (value_length // 32)
        keys.append(f"testkey_batch_put_{x}")
        values.append(bytes(f"this is a test case {x}", encoding='utf-8'))
    assert isinstance(keys, list), f"Item {keys} is not a list"
    assert isinstance(values, list), f"Item {values} is not a list"

    print(f"put batch size = {len(keys)}")
    store.put_batch(keys,values)
    random_keys = [f"testkey_batch_put_{i}" for i in random.sample(range(batch_size), min(5, batch_size))]
    batch_get(store, random_keys)

def batch_get(store, keys):
    existence = store.batch_is_exist(keys)
    all_exist = all(x == 1 for x in existence)
    if all_exist:
        print("All keys exist (all are 1).")
    else:
        print("Not all keys exist.")
    stored_values = store.get_batch(keys)
    if stored_values:
        for key, value in zip(keys, stored_values):
            print(f"Key: {key}, Value: {value.decode('utf-8')}")
    else:
        print(f"Key = {keys} not found in store")

def main():
    store = MooncakeDistributedStore()
    protocol = "tcp"
    device_name = "eth0"
    local_hostname = "localhost"
    metadata_server = "P2PHANDSHAKE"
    # The size of the Segment mounted by each node to the cluster, allocated by the Master Service after mounting, in bytes
    global_segment_size = 3200 * 1024 * 1024
    # Local buffer size registered with the Transfer Engine, in bytes
    local_buffer_size = 512 * 1024 * 1024
    # Address of the Master Service of Mooncake Store
    master_server_address = "127.0.0.1:50051"
    # Data length for each put()
    value_length = 1 * 1024 * 1024
    # Total number of requests sent
    max_requests = 100
    enable_lru = False
    enable_offload = False

    # Initialize Mooncake Store Client
    retcode = store.setup(
        local_hostname,
        metadata_server,
        global_segment_size,
        local_buffer_size,
        protocol,
        device_name,
        master_server_address
    )
    if retcode != 0:
        print(f"Failed to setup Mooncake Store. retcode = {retcode}")
        return
    simple_put_get(store)
    multi_put_get(store, max_requests)
    large_put_get(store, 32, 15)
    batch_put_get(store, 10)

if __name__ == "__main__":
    main()
