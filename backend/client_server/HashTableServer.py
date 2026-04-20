import sys
import socket
from HashTable import HashTable
import json
import base64
import time
import threading


class PolarisServer:
    def __init__(self, port, project_name=None, table_size = 300):
        self.hashtable = HashTable(table_size)
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # _, self.port = self.server.getsockname()
        self.host = "0.0.0.0"
        self.server.bind((self.host, int(port)))
        self.port = self.server.getsockname()[1]
        UDP_message = {
            "type" : "hashtable",
            "owner" : "lcao4",
            "port" : self.port,
            "project" : project_name
            }
        self.UDP_message = json.dumps(UDP_message).encode('utf-8')
        self.udp_client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_client.settimeout(2.0)
        self.udp_dest = ("catalog.cse.nd.edu", 9097)
        self.udp_client.sendto(self.UDP_message, self.udp_dest)

    @classmethod    
    def ns_connect(cls, project_name):
        return cls(port = 0, project_name = project_name)

    def start(self):
        self.hashtable.restart()
        try:
            self.hashtable.check_file()
        except Exception:
            print("file check failed")

        self.server.listen(1)
        print(f"Listening on port {self.port}")

        t = threading.Thread(target=self.send_to_ns, daemon=True)
        t.start()
        
        while True:
            conn, addr = self.server.accept()
            with conn:
                self.handle_client(conn)
                self.udp_client.sendto(self.UDP_message, self.udp_dest)

    def get_full_response(self, conn):
        full_data = b""
        while True:
            try:
                data = conn.recv(4096)
                if not data:
                    return None, None
                full_data += data
                if b"\n" in full_data:
                    message, remaining = full_data.split(b"\n", 1)
                    return message, remaining
            except Exception:
                print("client crashed")
                return None, None
            

    def handle_client(self, conn):
        last_remaining = b""
        while True:
            result = self.get_full_response(conn)
            data, remaining = result
            if data is None:
                break
            
            data = last_remaining + data
            last_remaining = remaining

            try:
                decode_string = data.decode()
                request_dict = json.loads(decode_string)
                self.process_request(conn, request_dict)
            except Exception:
                response = {"status": "invalid", "operation": "", "result": ""}
                self.send_json(conn, response)
                break

    def process_request(self, conn, request_dict):
        method = request_dict.get("method")
        key = request_dict.get("key")
        value = request_dict.get("value")
        
        try:
            response = {"status": "valid", "operation": "sucessful"} 
            if method == "get_description":
                keys = list(self.hashtable.hashtable.keys())
                if not keys:
                    response["result"] = "empty"
                else:
                    json_keys = json.dumps(keys)
                    encoded_keys = base64.b64encode(json_keys.encode('utf-8')).decode('utf-8')
                    response["result"] = encoded_keys

            if method == "insert":
                self.hashtable.insert(key, value)
            
            elif method == "lookup":
                res_value = self.hashtable.lookup(key)
                if res_value != 'empty':
                    # Ensure base64 encoding logic matches original
                    encoded = base64.b64encode(res_value).decode('utf-8')
                else:
                    encoded = "empty"
                response["result"] = encoded
            
            elif method == "remove":
                self.hashtable.remove(key)
            
            elif method == "size":
                response["result"] = self.hashtable.get_size()
            
            elif method == "query":
                response["result"] = self.hashtable.query(key)
            
            self.send_json(conn, response)

        except Exception:
            error_response = {"status": "valid", "operation": "failed"}
            self.send_json(conn, error_response)

    def send_json(self, conn, data_dict):
        response_str = json.dumps(data_dict) + "\n"
        conn.send(response_str.encode())

    def send_to_ns(self):
        while True:
            try:
                time.sleep(60)
                self.udp_client.sendto(self.UDP_message, self.udp_dest)
            except Exception as e:
                print(f"Send UDP package to name server failed: {e}")
                time.sleep(10)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python server.py <port>/project-name")
        sys.exit(1)

    # server = HashTableServer(port=sys.argv[1])
    server = HashTableServer.ns_connect(project_name=sys.argv[1])
    server.start()