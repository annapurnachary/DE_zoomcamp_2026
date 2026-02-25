/* @bruin

name: reports.trips_report
#type: duckdb.sql
type: bq.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: tpep_pickup_datetime
  time_granularity: date

depends:
  - staging.trips

columns:
  - name: trip_date
    type: date
    primary_key: true
  - name: taxi_type
    type: string
    primary_key: true
  - name: payment_type
    type: string
    primary_key: true
  - name: trip_count
    type: bigint
    checks:
      - name: non_negative

@bruin */

SELECT
    CAST(tpep_pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_name AS payment_type,
    COUNT(*) AS trip_count,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
