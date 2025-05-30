#!/usr/bin/env python3

import json
import requests
import sys
import time

def test_lmcache_notifications():
    print("Testing LMCache notifications...")
    
    try:
        response = requests.get('http://127.0.0.1:9090/status', timeout=5)
        if response.status_code != 200:
            print("LMCache server not responding")
            return False
        
        initial_status = response.json()
        initial_count = initial_status.get('notifications_received', 0)
        print(f"Initial notifications: {initial_count}")
        
        from mooncake.store import MooncakeDistributedStore
        store = MooncakeDistributedStore()
        
        retcode = store.setup(
            "localhost", 
            "http://127.0.0.1:8080/metadata", 
            3200 * 1024 * 1024,
            512 * 1024 * 1024, 
            "tcp", 
            "",
            "127.0.0.1:50051"
        )
        
        if retcode != 0:
            print(f"Store setup failed: {retcode}")
            return False
        
        print("Running put operations...")
        store.put("test_key_1", b"test_value_1")
        store.put("test_key_2", b"test_value_2")
        
        time.sleep(2)
        
        response = requests.get('http://127.0.0.1:9090/status', timeout=5)
        final_status = response.json()
        final_count = final_status.get('notifications_received', 0)
        
        print(f"Final notifications: {final_count}")
        print(f"New notifications: {final_count - initial_count}")
        
        if final_count > initial_count:
            print("SUCCESS: LMCache notifications working")
            for notif in final_status.get('recent_notifications', []):
                print(f"  {notif['data']}")
            return True
        else:
            print("FAILED: No new notifications received")
            return False
            
    except ImportError:
        print("FAILED: mooncake module not found")
        return False
    except Exception as e:
        print(f"FAILED: {e}")
        return False

if __name__ == '__main__':
    success = test_lmcache_notifications()
    sys.exit(0 if success else 1)
