import socket
import time
from essential_generators import DocumentGenerator

gen = DocumentGenerator()

host, port = ('127.0.0.1', 9999)

print("Starting server")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((host, port))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while True:
            conn.sendall(gen.paragraph().encode())
            time.sleep(1)
