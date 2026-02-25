{{ config(materialized='view') }}

with trips as (

    select *
    from {{ ref('fct_trips') }}

),

zones as (

    select *
    from {{ ref('dim_locations') }}

),

aggregated as (

    select
        t.service_type,

        extract(year from t.pickup_datetime) as revenue_year,
        extract(month from t.pickup_datetime) as revenue_month,

        t.pickup_locationid,
        z.zone as pickup_zone,
        z.borough as pickup_borough,

        count(*) as total_trips,
        sum(t.passenger_count) as total_passengers,

        sum(t.fare_amount) as total_fare_amount,
        sum(t.extra) as total_extra,
        sum(t.mta_tax) as total_mta_tax,
        sum(t.tip_amount) as total_tip_amount,
        sum(t.tolls_amount) as total_tolls_amount,
        sum(t.improvement_surcharge) as total_improvement_surcharge,
        sum(t.total_amount) as total_revenue,

        avg(t.fare_amount) as avg_fare_amount

    from trips t
    left join zones z
        on t.pickup_locationid = z.location_id

    group by
        t.service_type,
        revenue_year,
        revenue_month,
        t.pickup_locationid,
        z.zone,
        z.borough

)

select * from aggregated