#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 4 14:10:58 2021
@author: groth
"""

import pandas as pd
import glob


#### yellow taxi ALL OF 2020
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


#### for-hire vehicle JANUARY ONLY
hv = pd.read_csv('fhvhv_tripdata_2020-01.csv')
hv.SR_Flag = hv.SR_Flag.fillna(0)
hv.to_csv('fhvhv_data_2020-01.csv')

#### yellow taxi JANUARY ONLY
jan = pd.read_csv('yellow_tripdata_2020-01.csv')
jan = jan.drop(['store_and_fwd_flag', 'payment_type', 'fare_amount', 
'extra', 'mta_tax', 'tip_amount', 
'tolls_amount', 'improvement_surcharge', 'congestion_surcharge'], axis = 1)
jan = jan.dropna(axis = 0)
jan = jan[(jan['tpep_pickup_datetime'] > '2019-12-31') \
         & (jan['tpep_pickup_datetime'] < '2020-02-01')]
jan.to_csv('yellow_data_2020-01.csv')