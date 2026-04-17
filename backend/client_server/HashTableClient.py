import socket
import json
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import base64

class HashTableClient:
    def __init__(self, host, port, project_name):
        self.buffer = b""
        self.host = host
        self.port = port
        self.project_name = project_name
        self.retry_time = 1
        self.cap_wait = 64

    @classmethod
    def connect_through_ns(cls, project_name):
        tcp_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_client.connect(("catalog.cse.nd.edu", 9097))
        request = "GET /query.json HTTP/1.1\r\nHost: catalog.cse.nd.edu\r\nConnection: close\r\n\r\n"
        tcp_client.sendall(request.encode())
        response = b""
        while True:
            chunk = tcp_client.recv(4096)
            if not chunk:
                break
            response += chunk
        tcp_client.close()
        raw_response = response.decode()
        start_of_json = raw_response.find('[')
        json_body = raw_response[start_of_json:]
        end_of_json = json_body.rfind(']')
        json_body = json_body[:end_of_json+1]
        services = json.loads(json_body)
        matches = [s for s in services if s["type"] == "hashtable" and s["project"] == project_name]
        matches.sort(key=lambda x: x.get("lastheardfrom", 0), reverse=True) # get the one that is most recently heard from
        if matches:
            target = matches[0] 
            host = target["address"]
            port = target["port"]
            return cls(host, port, project_name)
        return cls(None, None, project_name)
    
    def client_request(self, key = None, value = None, operation = None):
        while True:
            try:
                try:
                    if self.host == None or self.port == None:
                        tcp_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        tcp_client.connect(("catalog.cse.nd.edu", 9097))
                        request = "GET /query.json HTTP/1.1\r\nHost: catalog.cse.nd.edu\r\nConnection: close\r\n\r\n"
                        tcp_client.sendall(request.encode())
                        response = b""
                        while True:
                            chunk = tcp_client.recv(4096)
                            if not chunk:
                                break
                            response += chunk
                        tcp_client.close()
                        raw_response = response.decode()
                        start_of_json = raw_response.find('[')
                        json_body = raw_response[start_of_json:]
                        end_of_json = json_body.rfind(']')
                        json_body = json_body[:end_of_json+1]
                        services = json.loads(json_body)
                        matches = [s for s in services if s["type"] == "hashtable" and s["project"] == self.project_name]
                        matches.sort(key=lambda x: x.get("lastheardfrom", 0), reverse=True) # get the one that is most recently heard from
                        if matches:
                            target = matches[0] 
                            self.host = target["address"]
                            self.port = target["port"]
                        if self.host == None or self.port == None:
                            raise ValueError("Lookup failure")
                        
                except Exception as e:
                    print("Failure to lookup the server name in the catalog.")
                    time.sleep(self.retry_time)
                    self.retry_time = min(self.retry_time * 2, self.cap_wait)
                    continue

                try:
                    self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.client.settimeout(5.0) 
                    self.client.connect((self.host, self.port))
                    description = self.get_description()
                    # self.retry_time = 1
                except Exception:
                    print("Failure to connect to the server.")
                    self.port = None
                    self.host = None
                    self._handle_backoff()
                    continue

                if not operation:
                    return description

                try:
                    self.send_request(operation, key, value)
                    # self.retry_time = 1
                except:
                    print("Failure to send a request.")
                    self.port = None
                    self.host = None
                    self._handle_backoff()
                    continue

                try:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._get_response_callback)
                    try:
                        response = future.result(timeout=5)
                        # print(self.process_request(operation, response))
                        self.retry_time = 1
                        return description, self.process_request(operation, response)
                    except TimeoutError:
                        raise RuntimeError("Lookup failure")

                except Exception as e:
                        print("Failure to read a response within five seconds.")
                        self._handle_backoff()
                    
            except Exception as e:
                self._handle_backoff()

    def _handle_backoff(self):
        self.client.close()
        self.client = None
        time.sleep(self.retry_time)
        self.retry_time = min(self.retry_time * 2, self.cap_wait)

    def get_description(self):
        self.send_description()
        response = self._get_response_callback()
        # print(response)
        return (self.process_getdesc(response), [f"{self.host}:{self.port}"])

    def process_request(self, operation, response):
        if operation == "insert":
            self.process_insert(response)
        elif operation == "lookup":
            return self.process_lookup(response)
        elif operation == "remove":
            self.process_remove(response)
        elif operation == "size":
            return self.process_size(response)
        elif operation == "query":
            return self.process_query(response)

    def _get_response_callback(self):
        while b"\n" not in self.buffer:
            data = self.client.recv(4096)
            if not data:
                raise ConnectionError("Server closed connection")
            self.buffer += data
        response, self.buffer = self.buffer.split(b"\n", 1)

        return json.loads(response.decode())           
    
    def process_getdesc(self, response):
        status = response["status"]
        operation = response["operation"]
        if status == "invalid":
            print("invalid request")
        else:
            if operation == "sucessful":
                result = response["result"]
                if result == 'empty':
                    return []
                decoded_bytes = base64.b64decode(result)
                keys_list = json.loads(decoded_bytes.decode('utf-8'))
                return keys_list
            else:
                print("get description operation failed")
        return ''

    def process_insert(self, response):
        status = response["status"]
        operation = response["operation"]
        if status == "invalid":
            print("invalid request")
        else:
            if operation == "sucessful":
                print("insertion operation sucessful")
            else:
                print("insertion operation failed")

    def process_lookup(self, response):
        status = response["status"]
        operation = response["operation"]
        if status == "invalid":
            print("invalid request")
        else:
            if operation == "sucessful":
                result = response["result"]
                if result == 'empty':
                    print("key does not exist")
                else: 
                    return result
            else:
                print("lookup operation failed")
        return ''

    def process_remove(self, response):
        status = response["status"]
        operation = response["operation"]
        if status == "invalid":
            print("invalid request")
        else:
            if operation == "sucessful":
                print("removal operation sucessful")
            else:
                print("removal operation failed")


    def process_size(self, response):
        # print(response)
        status = response["status"]
        operation = response["operation"]
        # print(status)
        if status == "invalid":
            print("invalid request")
        else:
            if operation == "sucessful":
                result = response["result"]
                return result
            else:
                print("check size operation failed")
        return ''

        # return response

    def process_query(self, response):
        status = response["status"]
        operation = response["operation"]
        result = response["result"]
        if status == "invalid":
            print("invalid request")
        else:
            if operation == "sucessful":
                if result == "exist":
                    print("key exists")
                    return True
                elif result == "do not exist":
                    print("key does not exist")
                    return False
            else:
                print("send query operation failed")
        return ''
    
    def send_description(self):
        message = {
            "method": "get_description"
        }
            
        message = json.dumps(message) + "\n"
        message = message.encode()
        self.client.sendall(message)

    def send_request(self, operation, key, value=None):
        if operation == "insert":
            message = {
                "method": "insert",
                "key": key,
                "value": value
            }
        elif operation == "lookup":
            message = {
                "method": "lookup",
                "key": key
            }
        elif operation == "remove":
            message = {
                "method": "remove",
                "key": key
            }
        elif operation == "size":
            message = {
                "method": "size"
            }
        elif operation == "query":
            message = {
                "method": "query",
                "key": key
            }
            
        message = json.dumps(message) + "\n"
        message = message.encode()
        self.client.sendall(message)

    def client_close(self):
        self.client.close()

    # def _get_response(self):
    #     while True:
    #         t = threading.Thread(target=self._get_response_callback)
    #         t.start()
    #         t.join(timeout=5)
    #         if not t.is_alive():
    #             self.retry_time = 1
    #             return
    #         print("Failure to read a response within five seconds.")
    #         time.sleep(self.retry_time)
    #         self.retry_time = min(2*self.retry_time, self.cap_wait)    