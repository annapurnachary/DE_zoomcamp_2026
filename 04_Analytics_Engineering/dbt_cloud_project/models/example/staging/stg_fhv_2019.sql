{{ config(materialized='view') }}

with tripdata as (

    select *
    from {{ source('rawdata','fhv_data_external') }}
    where dispatching_base_num is not null

),

renamed as (

    select
        -- identifiers
        dispatching_base_num,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- location ids
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,

        -- flags
        cast(SR_Flag as integer) as sr_flag,
        affiliated_base_number

    from tripdata

)

select * from renamed