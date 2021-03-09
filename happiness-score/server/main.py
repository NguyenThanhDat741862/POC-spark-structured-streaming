import pandas as pd
import os
import socket
import time

df2015 = pd.read_csv('../datasets/2015.csv')[['Country', 'Region', 'Happiness Score']]
df2016 = pd.read_csv('../datasets/2016.csv')[['Country', 'Region', 'Happiness Score']]

df = pd.concat([df2015, df2016])

host, port = ('127.0.0.1', 9999)

print("Starting server")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((host, port))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while True:
            amount = 0
            for index, row in df.iterrows():
                conn.sendall(f"{row['Country']},{row['Region']},{row['Happiness Score']}\n".encode())
                amount += 1
                if amount == 20:
                    time.sleep(4)
                    amount = 0
