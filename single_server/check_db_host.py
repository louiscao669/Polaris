import os
import socket
import sys


MYSQL_HOST="db8.cse.nd.edu"
MYSQL_USER="tmitch23"
MYSQL_PASSWORD="WynnedTrain12345"
MYSQL_DATABASE="tmitch23"

DEFAULT_HOST = MYSQL_HOST
DEFAULT_PORT = 3306
DEFAULT_USER = MYSQL_USER
DEFAULT_DATABASE = MYSQL_DATABASE


def main() -> int:
    host = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_HOST
    port = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PORT

    if not host:
        print("TCP connection failed: MYSQL_HOST is not set.")
        print("Set MYSQL_HOST in your environment or pass the host explicitly.")
        return 1

    print(f"Checking MySQL host reachability for {host}:{port}")
    print(f"Using MYSQL_USER={DEFAULT_USER} MYSQL_DATABASE={DEFAULT_DATABASE}")

    s = socket.socket()
    s.settimeout(5)

    try:
        s.connect((host, port))
        print(f"TCP connection succeeded to {host}:{port}")
        return 0
    except Exception as e:
        print(f"TCP connection failed to {host}:{port}: {e}")
        return 1
    finally:
        s.close()


if __name__ == "__main__":
    raise SystemExit(main())
