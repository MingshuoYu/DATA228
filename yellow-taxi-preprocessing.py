#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 4 14:10:58 2021
@author: groth
"""

import pandas as pd
import glob

files = glob.glob('yellow*.csv')
dfs = []
for f in sorted(files):
    dfs.append(pd.read_csv(f, 
    usecols=['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
              'passenger_count', 'trip_distance', 'PULocationID', 
              'DOLocationID', 'total_amount'], 
    parse_dates={'pickup_time': [1], 'dropoff_time': [2]}))
df = pd.concat(dfs)
df = df.dropna(axis = 0)

df = df[(df['pickup_time'] > '2019-12-31') \
          & (df['pickup_time'] < '2021-01-01')]

df.to_csv('yellow_tripdata_2020.csv')