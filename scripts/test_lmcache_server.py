#!/usr/bin/env python3

import json
import sys
import time
import zmq


class SimpleLMCacheServer:
    """Simplified LMCache test server for CI - validates JSON path correctness only"""

    def __init__(self, port=9001, timeout=30):
        self.port = port
        self.timeout = timeout
        self.received_notifications = []
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)

    def start_listening(self):
        """Listen for notifications and validate JSON structure"""
        self.socket.bind(f"tcp://127.0.0.1:{self.port}")
        print(f"LMCache test server listening on tcp://127.0.0.1:{self.port}")

        start_time = time.time()

        while time.time() - start_time < self.timeout:
            if self.socket.poll(1000):  # 1 second timeout
                message = self.socket.recv_string(zmq.NOBLOCK)
                notification_data = json.loads(message)

                self.received_notifications.append(notification_data)

                assert self.validate_notification(notification_data)

        return len(self.received_notifications) > 0

    def validate_notification(self, data):
        """Validate that notification contains expected fields"""
        required_fields = ['type', 'instance_id', 'worker_id', 'key', 'location']

        for field in required_fields:
            if field not in data:
                print(f"Missing required field: {field}")
                return False
        return True

    def cleanup(self):
        """Clean up resources"""
        self.socket.close()
        self.context.term()

    def get_stats(self):
        """Return simple statistics"""
        return {
            'total_notifications': len(self.received_notifications),
            'notifications': self.received_notifications
        }


def main():
    if len(sys.argv) > 1:
        timeout = int(sys.argv[1])
    else:
        timeout = 30

    server = SimpleLMCacheServer(timeout=timeout)

    try:
        success = server.start_listening()
        stats = server.get_stats()

        print(f"\nTest completed:")
        print(f"- Notifications received: {stats['total_notifications']}")
        print(f"- Validation result: {'PASS' if success else 'FAIL'}")

        # Exit with appropriate code for CI
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\nTest interrupted")
        sys.exit(1)
    finally:
        server.cleanup()


if __name__ == '__main__':
    main()
