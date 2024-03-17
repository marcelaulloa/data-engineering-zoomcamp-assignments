{{ config(materialized='table') }}

with fhv_tripdata as (
    select *, 
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
),
trips_unioned2 as(
    select 
    tripid,
    service_type,
    pickup_datetime,
    dropoff_datetime,
    Pickup_locationid as pickup_locationid,
    dropoff_locationid 
    from trips_unioned
    union all
    select 
    tripid,
    service_type,
    pickup_datetime,
    dropoff_datetime,
    Pickup_locationid as pickup_locationid,
    dropoff_locationid 
    from fhv_tripdata
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned2.tripid, 
    trips_unioned2.service_type, 
    trips_unioned2.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned2.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned2.pickup_datetime, 
    trips_unioned2.dropoff_datetime, 
from trips_unioned2
inner join dim_zones as pickup_zone
on trips_unioned2.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned2.dropoff_locationid = dropoff_zone.locationid
