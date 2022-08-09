import socket
import time
from binance.client import Client
import time
import warnings
warnings.filterwarnings('ignore')

# reading credentials file
creds = open('creds.txt')
creds = creds.readlines()

# getting api key and secret
api_key = creds[0].lstrip('API Key:').rstrip('\n').strip()
api_secret = creds[1].lstrip('Secret Key:').rstrip('\n').strip()

# init
client = Client(api_key, api_secret)

# configuring socket communication 
HOST = 'localhost'
PORT = 9009

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    
    while True:
        print("Waiting for connection ...")
        conn, addr = s.accept()
        print(f"Connected by {addr}")
        conn.send(client.get_symbol_ticker(symbol="BTCUSDT")['price'].encode())
        conn.close()
        print('sent')
        time.sleep(10)
        # data = conn.send("Hello World".encode)
s.close()
