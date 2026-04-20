import sys
from HashTableClient import HashTableClient
import os
import base64
import time

project_name = sys.argv[1]
hashtableclient = HashTableClient.connect_through_ns(project_name)

description = hashtableclient.client_request()

for filename in description[0]:

    time.sleep(3)

    start = time.perf_counter_ns()
    description, file_data = hashtableclient.client_request(key=filename, operation="lookup")
    end = time.perf_counter_ns()
    duration = (end-start)/1e9

    print(f"lookup {filename} take {duration} seconds")

    file_data = base64.b64decode(file_data)
    with open(f"client-disk/{filename}", "wb") as f:
        f.write(file_data)