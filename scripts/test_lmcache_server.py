#!/usr/bin/env python3

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse


class LMCacheTestHandler(BaseHTTPRequestHandler):
    received_notifications = []
    lock = threading.Lock()

    def do_POST(self):
        try:
            parsed_path = urlparse(self.path)

            if parsed_path.path == '/api/kv_events':
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length)

                try:
                    notification_data = json.loads(post_data.decode('utf-8'))

                    with self.lock:
                        self.received_notifications.append({
                            'timestamp': time.time(),
                            'data': notification_data
                        })

                    logging.info(f"Received: {notification_data}")

                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"status": "ok"}')

                except json.JSONDecodeError:
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"error": "invalid_json"}')

            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"error": "not_found"}')

        except Exception:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"error": "internal_error"}')

    def do_GET(self):
        try:
            parsed_path = urlparse(self.path)

            if parsed_path.path == '/status':
                with self.lock:
                    status = {
                        'status': 'running',
                        'notifications_received': len(self.received_notifications),
                        'recent_notifications': self.received_notifications[-5:]
                    }

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(status).encode('utf-8'))

            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"error": "not_found"}')

        except Exception:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"error": "internal_error"}')

    def log_message(self, *args):
        pass


def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    server_address = ('127.0.0.1', 9090)
    httpd = HTTPServer(server_address, LMCacheTestHandler)

    print("LMCache test server started on http://127.0.0.1:9090")
    print("Endpoint: /api/kv_events")
    print("Status: /status")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("Server stopped")
    finally:
        httpd.server_close()


if __name__ == '__main__':
    main()
