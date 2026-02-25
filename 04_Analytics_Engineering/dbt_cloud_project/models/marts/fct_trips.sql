{{ config(materialized='view') }}

with trips as (

    select *
    from {{ ref('int_trips_unioned') }}

),

-- Deduplicate
deduplicated as (

    select
        *,
        row_number() over (
            partition by 
                service_type,
                vendorid,
                pickup_datetime,
                dropoff_datetime,
                pickup_locationid,
                dropoff_locationid,
                total_amount
            order by pickup_datetime
        ) as row_num
    from trips

),

clean_trips as (

    select *
    ,(case when service_type = '' then 'NA' else service_type end) as d_service_type
    ,(case when vendorid is NULL then 0 else vendorid end) as d_vendorid
    ,(case when pickup_datetime is NULL then current_timestamp() else pickup_datetime end) AS d_pickup_datetime
    ,(case when dropoff_datetime is NULL then current_timestamp() else dropoff_datetime end) AS d_dropoff_datetime
    ,(case when pickup_locationid is NULL then 0 else pickup_locationid end) as d_pickup_locationid
    ,(case when dropoff_locationid is NULL then 0 else dropoff_locationid end) as d_dropoff_locationid
    ,(case when total_amount is NULL then 0 else total_amount end) as d_total_amount
    from deduplicated
    where row_num = 1

),

-- Generate Surrogate Key
final as (
    select 
      md5(cast(d_service_type as string) || cast(d_vendorid as string) || cast(d_pickup_datetime as string) || cast(d_dropoff_datetime as string) || cast(d_pickup_locationid as string) || cast(d_dropoff_locationid as string) || cast(d_total_amount as string))  as trip_id,

        service_type,
        vendorid,
        pickup_datetime,
        dropoff_datetime,

        pickup_locationid,
        dropoff_locationid,

        ratecodeid,
        passenger_count,
        trip_distance,
        trip_type,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,

        payment_type,
       -- payment_type_description,

        store_and_fwd_flag,
        congestion_surcharge,
        airport_fee

    from clean_trips

)

select * from final