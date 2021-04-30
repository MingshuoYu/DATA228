import openaq
import pandas as pd

api = openaq.OpenAQ()
cities = api.cities(country='US', limit=10000, df=True) # find cities in US
# The general area of New York has 3 entries: New York, NEW YORK, and New York-Northern New Jersey-Long Island
# The first entry has 3 locations (outdated), the second has 1 location (outdated), and the last has 33 locations (recent)
# Based on this city, we can find the specific locations to then pull measurements

header, locs = api.locations(city='New York-Northern New Jersey-Long Island', limit=10000)
locs = pd.json_normalize(locs['results']) # api uses deprecated version of json_normalize, so explicitly call it here
locs.head()
# now we have the name of the 33 locations inside the New York area

# the locations we have interest in are Manhattan/IS143, Bronx - IS52, Queens, Bklyn - PS274, Bronx - IS74, Queens Near-road
header, m = api.measurements(location='Manhattan/IS143', limit=100000)
manhattan = pd.json_normalize(m['results']) #(21941, 10)

header, b_is52 = api.measurements(location='Bronx - IS52', limit=100000)
bronx_is52 = pd.json_normalize(b_is52['results']) #(49144, 10)

header, q = api.measurements(location='Queens', limit=100000)
queens = pd.json_normalize(q['results']) #(45581, 10)

header, brooklyn = api.measurements(location='Bklyn - PS274', limit=100000)
brooklyn = pd.json_normalize(brooklyn['results']) # (22750, 10)

header, bronx_74 = api.measurements(location='Bronx - IS74', limit=100000)
bronx_is74 = pd.json_normalize(bronx_74['results']) #(16661, 10)

header, queens_near_road = api.measurements(location='Queens Near-road', limit=100000)
qnr = pd.json_normalize(queens_near_road['results']) #(17689, 10)

df = pd.concat([manhattan, bronx_is52, queens, brooklyn, bronx_is74, qnr])
df['date.utc'] = df['date.utc'].str.replace('Z', '').str.replace('T', ' ')
df['date.local'] = df['date.local'].str.rsplit('-', 1, expand=True).iloc[:, 0].str.replace('T', ' ')
df.to_csv('NY_openaq.csv', index=None)