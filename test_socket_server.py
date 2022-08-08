import socket
import time
import json

HOST = 'localhost'
PORT = 9009
while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print("Waiting for connection ...")
        conn, addr = s.accept()
        print(f"Connected by { addr}")
        conn.sendall("""{"1":1, "2":2}""".encode())
        print('sent')
        # data = conn.send("Hello World".encode)
    
