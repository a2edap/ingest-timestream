import pandas as pd
from datetime import datetime

filepath = 'CISO.2020.utc.csv'

df = pd.read_csv(f'{filepath}')

def convert_to_epoch(timestamp_string):
    dt = datetime.fromisoformat(timestamp_string)
    return int(dt.timestamp())

df['Date'] = df['Date'].apply(lambda x: convert_to_epoch(x))


df.to_csv(f'{filepath}', index=False, mode='w')