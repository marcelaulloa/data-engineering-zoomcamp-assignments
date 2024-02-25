{{ config(materialized='view') }}

with tripdata as (

    select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging', 'fhv_tripdata') }}
    where PUlocationID>-1
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime','PUlocationID']) }} as tripid,    
    dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as SR_Flag,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    affiliated_base_number

from tripdata
where rn=1

-- -- dbt build --select <model.sql> --vars '{'is_test_run: false}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}
