import hashlib
import socket

nodes = {}

k = 1 # replication factor, default value is 1

consistency = "eventual consistency" # default value is 'chain replication' -> linearizability, 2 choices "eventual consistency" and "chain replication"


def hash_function(key: str):
    return hashlib.sha1(key.encode('utf-8')).hexdigest()


def get_local_ip():
   # return the local IP address of the current machine
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip_address = sock.getsockname()[0]
        sock.close()
        return ip_address
    except Exception:
        return None

