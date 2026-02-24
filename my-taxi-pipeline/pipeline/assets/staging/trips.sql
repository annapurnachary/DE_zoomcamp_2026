/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: tpep_pickup_datetime

columns:
  - name: tpep_pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: tpep_dropoff_datetime
    type: timestamp
  - name: pu_location_id
    type: bigint
  - name: do_location_id
    type: bigint
  - name: fare_amount
    type: float
  - name: taxi_type
    type: string
  - name: payment_type_name
    type: string

@bruin */

SELECT
    t.tpep_pickup_datetime ,
    t.tpep_dropoff_datetime,
    t.pu_location_id,        -- Changed from pulocationid
    t.do_location_id,        -- Changed from dolocationid
    t.fare_amount, 
    'yellow' as taxi_type,
    p.payment_type_name
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
WHERE t.tpep_pickup_datetime >= '{{ start_datetime }}'
  AND t.tpep_dropoff_datetime < '{{ end_datetime }}'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.tpep_pickup_datetime, t.tpep_dropoff_datetime,
                 t.pu_location_id, t.do_location_id, t.fare_amount
    ORDER BY t.tpep_pickup_datetime
) = 1
