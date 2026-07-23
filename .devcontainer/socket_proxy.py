import socket
import sys
import threading
import os
import signal
import ctypes

def pipe(source, destination):
    try:
        while True:
            data = source.recv(4096)
            if not data:
                break
            destination.sendall(data)
    except Exception:
        pass
    finally:
        try:
            source.close()
        except Exception:
            pass
        try:
            destination.close()
        except Exception:
            pass

def handle_client(client_sock, target_path):
    target_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        target_sock.connect(target_path)
    except Exception as e:
        client_sock.close()
        target_sock.close()
        return

    t1 = threading.Thread(target=pipe, args=(client_sock, target_sock))
    t2 = threading.Thread(target=pipe, args=(target_sock, client_sock))
    t1.daemon = True
    t2.daemon = True
    t1.start()
    t2.start()

def main():
    if len(sys.argv) != 3:
        sys.exit(1)

    listen_path = sys.argv[1]
    target_path = sys.argv[2]

    # Instruct the Linux kernel to send SIGTERM if the parent bash script dies.
    # 1 is PR_SET_PDEATHSIG
    try:
        libc = ctypes.CDLL('libc.so.6')
        libc.prctl(1, signal.SIGTERM)
    except Exception:
        pass

    # Write PID to stdout so parent shell can capture it and kill it later
    print(os.getpid(), flush=True)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    if os.path.exists(listen_path):
        try:
            os.remove(listen_path)
        except Exception:
            pass
            
    server.bind(listen_path)
    # Restrict read/write access strictly to the owner (prevents agent hijacking)
    os.chmod(listen_path, 0o600)
    server.listen(5)

    def sig_handler(signum, frame):
        server.close()
        if os.path.exists(listen_path):
            try:
                os.remove(listen_path)
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    try:
        while True:
            client_sock, _ = server.accept()
            handle_client(client_sock, target_path)
    except Exception:
        pass
    finally:
        server.close()
        if os.path.exists(listen_path):
            try:
                os.remove(listen_path)
            except Exception:
                pass

if __name__ == "__main__":
    main()
