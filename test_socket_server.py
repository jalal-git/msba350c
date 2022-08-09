import socket
import time
from binance.client import Client
from datetime import datetime
import time
import warnings
import requests
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
        ticks = requests.get('https://api.binance.com/api/v1/klines?symbol=BTCUSDT&interval=1m').json()
        print("Waiting for connection ...")
        conn, addr = s.accept()
        print(f"Connected by {addr}")
        conn.send(str(ticks).encode())
        conn.close()
        print('sent')
        time.sleep(60)
        
s.close()
