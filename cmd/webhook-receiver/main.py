from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import os

# --- Configuration ---
# Get the number of times to fail from an environment variable.
# Default to 2 failures if not set.
FAIL_COUNT = int(os.environ.get("FAIL_COUNT", 2))
requests_received = 0

class FlakyWebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global requests_received
        requests_received += 1

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        
        logging.info("--- INCOMING WEBHOOK (Attempt %d) ---", requests_received)
        logging.info("Headers:\n%s", str(self.headers))
        logging.info("Body:\n%s\n", post_data.decode('utf-8'))
        
        if requests_received <= FAIL_COUNT:
            logging.warning("Simulating failure number %d...", requests_received)
            self.send_response(500) # Internal Server Error
            self.end_headers()
        else:
            logging.info("Simulating success!")
            self.send_response(200) # OK
            self.end_headers()

def run(server_class=HTTPServer, handler_class=FlakyWebhookHandler, port=8000):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info(f"Starting FLAKY webhook receiver on port {port}...")
    logging.info(f"Will fail the first {FAIL_COUNT} requests.\n")
    httpd.serve_forever()

if __name__ == '__main__':
    run()