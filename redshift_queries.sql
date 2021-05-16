# summary statistics
select extract(year from tpep_pickup_datetime) as yr, 
extract(month from tpep_pickup_datetime) as month, 
avg(y.trip_distance) as average_trip_distance,
sum(y.total_amount) as total_amount,
sum(y.passenger_count) as total_passengers,
sum(y.tip_amount) as total_tip
from yellow y
group by yr, month;

# most popular pickup locations for yellow taxi
select z.zone, borough, count(1) as zone_trips 
from yellow y, taxi_zones z 
where y.pulocationid = z.locationid g
group by z.zone, borough;

# most popular dropoff locations for yellow taxi
select z.zone, borough, count(1) as zone_trips 
from yellow y, taxi_zones z 
where y.dolocationid = z.locationid 
group by z.zone, borough;

# trips per day in yellow taxis
select extract(year from tpep_pickup_datetime) as yr, 
extract(month from tpep_pickup_datetime) as month, 
extract(day from tpep_pickup_datetime) as day, 
sum(y.passenger_count) as total_trips
from yellow y
group by yr, month, day;

# trips per day in FHV
select extract(year from pickup_datetime) as yr, 
extract(month from pickup_datetime) as month, 
extract(day from pickup_datetime) as day, 
count(1) as total_trips 
from hv group by yr, month, day;

# rides per borough in FHV
select borough, count(1) as total_trips 
from hv h, taxi_zones z
where h.pulocationid = z.locationid
group by borough
order by total_rides desc;

# rides per borough in yellow taxis
select borough, count(1) as total_trips 
from yellow y, taxi_zones z
where y.pulocationid = z.locationid
group by borough
order by total_rides desc;

# total rides per company in FHV
select hvfhs_license_num as company, 
count(1) as total_rides 
from hv h 
group by company 
order by total_rides desc;

# rides per company FHV
select extract(month from pickup_datetime) as month, 
extract(year from pickup_datetime) as yr, 
count(1) as total_rides, 
case when hvfhs_license_num = 'HV0002' then 'Juno' 
when hvfhs_license_num = 'HV0003' then 'Uber' 
when hvfhs_license_num = 'HV0004' then 'Via' 
when hvfhs_license_num = 'HV0005' then 'Lyft' 
end as company 
from hv h 
group by yr, month, company; 

# non-shared rides
select z.zone, borough, count(1) as zone_trips
from hv h, taxi_zones z
where h.dolocationid = z.locationid and h.sr_flag = 0
group by z.zone, borough;

# shared rides
select z.zone, borough, count(1) as zone_trips
from hv h, taxi_zones z
where h.dolocationid = z.locationid and h.sr_flag = 1
group by z.zone, borough;

# rides per hour
select extract(hour from tpep_pickup_datetime) as hour,
count(1) as total_rides
from yellow y
group by hour;

# most popular pickup locations FHV
select z.zone, borough, count(1) as zone_trips 
from hv h, taxi_zones z 
where h.pulocationid = z.locationid g
group by z.zone, borough;

# most popular dropoff locations FHV
select z.zone, borough, count(1) as zone_trips 
from hv h, taxi_zones z 
where h.dolocationid = z.locationid 
group by z.zone, borough;

# PM2.5
select extract(year from tpep_pickup_datetime) as yr, 
extract(month from tpep_pickup_datetime) as month, 
extract(day from tpep_pickup_datetime) as day, 
avg(value) as average_value,
location
from openaq_info a
group by yr, month, day, location;

# average trip duration
select extract(year from tpep_pickup_datetime) as yr, 
extract(month from tpep_pickup_datetime) as month, 
extract(day from tpep_pickup_datetime) as day, 
avg(datediff(minutes, tpep_pickup_datetime, tpep_dropoff_datetime)) as average_trip_time
from yellow y
group by yr, month, day
having average_trip_time > 0;

# air quality + taxi data
select extract(year from y.tpep_pickup_datetime) as yr,
extract(month from y.tpep_pickup_datetime) as month,
extract(day from y.tpep_pickup_datetime) as day,
extract(hour from y.tpep_pickup_datetime) as hour,
count(1) as total_rides,
avg(a.value) as average_PM25
from openaq_info a, yellow y, taxi_zones z
where y.pulocationid = z.locationid and
a.location = z.borough and
extract(year from a.date_local) = extract(year from y.tpep_pickup_datetime) and
extract(month from a.date_local) = extract(month from y.tpep_pickup_datetime) and
extract(day from a.date_local) = extract(day from y.tpep_pickup_datetime) and
extract(hour from a.date_local) = extract(hour from y.tpep_pickup_datetime)
group by yr, month, day, hour;         



