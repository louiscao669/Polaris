import random
import os
import base64
from pathlib import Path

class HashTable:
    def __init__(self, capacity = 101):
        self.capacity = capacity
        self.size = 0
        self.ckpt_path = "table.ckpt"
        self.log_path = "table.txn"
        self.log_size = 0
        self.hashtable = dict()

    # def _hash(self, filename):
    #     s = 0
    #     for c in filename:
    #         s += ord(c)
    #     index = s % self.capacity
    #     return index

    # def _rehash(self):
    #     old_capacity = self.capacity
    #     self.capacity *= 2
    #     new_ht = [[] for i in range(self.capacity)]
    #     for i in range(old_capacity):
    #         for key, entry in self.hashtable[i]:
    #             index = self._hash(key)
    #             new_ht[index].append([key, entry])
    #     self.hashtable = new_ht
    
    def compaction(self):
        new_ckpt_fp = "new.ckpt"
        with open(new_ckpt_fp, "w") as f:
            for key, entry in self.hashtable.items():
                fp = entry["file_path"]
                size = entry["size"]
                f.write(f"{key},{fp},{size}\n")
            f.flush()
            os.fsync(f.fileno())
        
        os.rename(new_ckpt_fp, self.ckpt_path)

        with open(self.log_path, "w") as f:
            f.flush()
            os.fsync(f.fileno())

        self.log_size = 0
    
    def restart(self):
        self.size = 0
        ckpt_path = Path(self.ckpt_path)
        ckpt_path.parent.mkdir(parents=True, exist_ok=True)
        ckpt_path.touch(exist_ok=True)

        log_path = Path(self.log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.touch(exist_ok=True)

        if os.path.getsize(self.ckpt_path) != 0:
            with open(self.ckpt_path, "r") as f:
                for line in f:
                    line = line.rstrip("\n")
                    key, path, size = line.split(",")
                    entry = {"file_path": path, "size": int(size)}
                    self.hashtable[key] = entry
                    self.size += 1
        if os.path.getsize(self.log_path) != 0: 
            with open(self.log_path, "r") as f:
                for line in f:
                    line = line.rstrip("\n")
                    method, key, path, size = line.split(",")
                    entry = {"file_path": path, "size": int(size)}
                    if method == "insert":
                        if key in self.hashtable:
                            self.hashtable[key] = entry
                            continue
                        self.hashtable[key] = entry
                        self.size += 1
                    elif method == "remove":
                        if key not in self.hashtable:
                            continue
                        self.hashtable.pop(key, None)
                        self.size -= 1

        # with open(self.log_path, "w") as f:
        #     f.flush()
        #     os.fsync(f.fileno())

        self.log_size = 0

    def check_file(self):
        # print(self.hashtable)
        for key, entry in self.hashtable.items():
            fp = entry["file_path"]
            size = entry["size"]
            if not os.path.exists(fp) or not os.path.isfile(fp):
                print("file not found")
                raise FileNotFoundError("the file is not found on disk")
            else:
                actual_size = os.path.getsize(fp)

                if actual_size != size:
                    print(f"actual_size: {actual_size}, size: {size}\n")
                    raise ValueError("size does not match")
        return

    def insert(self, filename, data):
        key = filename
        num = random.randint(1, 100)
        char_list = []
        [char_list.append(chr(random.randint(65, 90))) for i in range(5)]
        if "." in filename:
            type = filename.split(".")[1]
            fn = "".join(char_list) + str(num) + f".{type}"
        else:
            fn = "".join(char_list) + str(num)

        dir_path = "stored-files"
        os.makedirs(dir_path, exist_ok=True)

        file_path = f"{dir_path}/{fn}"
        size = data["size"]
        entry = {
            "file_path": file_path,
            "size": size
        }
        log_entry = f"insert,{key},{file_path},{size}\n"
        if key in self.hashtable:
            old_path = self.hashtable[key]["file_path"]
            if os.path.exists(old_path):
                os.remove(old_path)
        else:
            self.size += 1
        
        data_bytes = base64.b64decode(data["data"])
        with open(file_path, "wb") as f:
            f.write(data_bytes)
            f.flush()
            os.fsync(f.fileno())
        
        with open(self.log_path, "a") as f:
            f.write(log_entry)
            f.flush()
            os.fsync(f.fileno())
        self.log_size += 1

        self.hashtable[key] = entry

        if self.log_size >= 100:
            self.compaction()


    def lookup(self, filename):
        if filename not in self.hashtable.keys():
            return 'empty'
        else:
            entry = self.hashtable[filename]
            fp = entry['file_path']
            with open(fp, 'rb') as f:
                content = f.read()
            return content
        
    def remove(self, filename):
        if filename not in self.hashtable.keys():
            raise ValueError("key not exist")
        else:
            entry = self.hashtable[filename]
            fp = entry['file_path']   
            # print(fp)             
            if os.path.exists(fp):
                log_entry = f"remove,{filename},{fp},0\n"      
                with open(self.log_path, "a") as f:
                    f.write(log_entry)
                    f.flush()
                    os.fsync(f.fileno())
                self.log_size += 1
                os.remove(fp)
                self.hashtable.pop(filename)
                self.size -= 1
                if self.log_size >= 100:
                    self.compaction()
            else:
                self.hashtable.pop(filename)
                self.size -= 1
                print(self.hashtable, fp)
                raise FileNotFoundError(f"The file '{fp}' does not exist.")

        

    def get_size(self):
        return self.size

    def query(self, filename):
        if filename in self.hashtable.keys():
            return "exist"
        else:
            return "do not exist"
