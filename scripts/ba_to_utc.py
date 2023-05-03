import pandas as pd
import numpy as np

filepath = 'BPAT\BPA_2020.xlsx'
original_time_name = 'Date/Time'
original_load_name = 'LOAD'
original_wind_name = 'WIND'
balancing_authority_name = 'BPAT'
year = '2020'

df = pd.read_excel(f'{filepath}','Production', usecols=[f'{original_time_name}', 
                                                                '{original_load_name}', '{original_wind_name}'])

# rename to date/time to time
df = df.rename(columns={f'{original_time_name}': 'time'})
df = df.rename(columns={f'{original_load_name}': 'load'})
df = df.rename(columns={f'{original_wind_name}': 'wind'})

# convert datetime column to datetime datatype
df['time'] = pd.to_datetime(df['time'])

# localize datetime column to Pacific time, handling ambiguous times with 'infer' option
df['time'] = df['time'].dt.tz_localize('US/Pacific', ambiguous='infer')

# convert to UTC timezone
df['time'] = df['time'].dt.tz_convert('UTC')

df['time'] = df['time'].apply(lambda x: x.timestamp())

df['ba_id'] = balancing_authority_name

df['time'] = df['time'].astype(int)

df.to_csv('{balancing_authority_name}.{year}.utc.csv', header=True, index=False)
