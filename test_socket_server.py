import socket
import time
import json

HOST = 'localhost'
PORT = 9009

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    
    while True:
        print("Waiting for connection ...")
        conn, addr = s.accept()
        print(f"Connected by { addr}")

        conn.send("""{"1":1, "2":2}""".encode())
        conn.close()
        print('sent')
        time.sleep(10)
        # data = conn.send("Hello World".encode)
s.close()
    
