import time
import sys
from HashTableClient import HashTableClient
import json
import base64
import os

project_name = sys.argv[1]
hashtableclient = HashTableClient.connect_through_ns(project_name)


prepared_messages = []
for i in range(8):
    filepath = f"test_files/myfile{i}"
    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)

    with open(filepath, "rb") as f:
        data = f.read()
        encoded_string = base64.b64encode(data).decode('utf-8')
    
    message = {
    "key" : filename,
    "value" : { "size":filesize, "data":encoded_string }
    }
    prepared_messages.append((message["key"], message["value"]))

for key, value in prepared_messages:
    hashtableclient.client_request(key, value, operation = "insert")