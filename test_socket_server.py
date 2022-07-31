import socket
import time
import json

HOST = '127.0.0.1'
PORT = 5050
print(socket.gethostbyname(HOST))

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print(f"Connected by { addr}")
        data = conn.recv(1024)
        print(data.decode())
        conn.sendall("Hello World!".encode())
        # data = conn.send("Hello World".encode)
    