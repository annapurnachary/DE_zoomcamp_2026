

CREATE OR REPLACE EXTERNAL TABLE `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://module-3-yellow-2024/yellow/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE module-3-bigquery-486618.ny_taxi.yellow_trips_2024_non_partitioned AS
SELECT * FROM module-3-bigquery-486618.ny_taxi.yellow_trips_2024_external;

/*
This statement created a new table named yellow_trips_2024_non_partitioned.
Job ID
module-3-bigquery-486618:us-central1.bquxjob_3d621bba_19c3afdd6bc
Location
us-central1
Creation time
Feb 7, 2026, 6:04:00 PM UTC-8
Start time
Feb 7, 2026, 6:04:00 PM UTC-8
End time
Feb 7, 2026, 6:04:13 PM UTC-8
Duration
12 sec
Bytes processed
5.5 GB
Bytes billed
5.5 GB
Slot milliseconds
462010
*/

--Q1
select count(*) from module-3-bigquery-486618.ny_taxi.yellow_trips_2024_non_partitioned;
--20332093


--Q2

--Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

SELECT COUNT(distinct PULocationID)
FROM `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_external`;

--262
/*
Duration
0 sec
Bytes processed
155.12 MB
Bytes billed
156 MB
Slot milliseconds
3523
Job priority
INTERACTIVE
Use legacy SQL
false
*/


SELECT COUNT(distinct PULocationID)
FROM `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_non_partitioned`;
/*
End time
Feb 8, 2026, 9:12:29 AM UTC-8
Duration
0 sec
Bytes processed
155.12 MB
Bytes billed
156 MB
*/

--Q3
/*Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?
*/

SELECT COUNT(distinct PULocationID),count(DOLocationID)
FROM `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_non_partitioned`;
--262,20332093
/*Bytes processed
310.24 MB
Bytes billed
311 MB
Slot milliseconds
3572
*/

--Q4--How many records have a fare_amount of 0?

SELECT COUNT(*)
FROM `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_external`
where fare_amount =0;
--8333

--Q5
CREATE OR REPLACE TABLE module-3-bigquery-486618.ny_taxi.yellow_tripdata_2024_optimized
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM module-3-bigquery-486618.ny_taxi.yellow_tripdata_external;


--Q6
/*
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
*/

select distinct(VendorID) from `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_non_partitioned`
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15' ;
/*
VendorID
6
2
1
*/

/*
Bytes processed
310.24 MB
Bytes billed
311 MB
*/

CREATE OR REPLACE TABLE module-3-bigquery-486618.ny_taxi.yellow_trips_2024_partitioned
PARTITION BY
  DATE(tpep_dropoff_datetime) AS
SELECT * FROM module-3-bigquery-486618.ny_taxi.yellow_trips_2024_external;
--This statement created a new table named yellow_trips_2024_partitioned.



select distinct(VendorID) from `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_partitioned`
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15' ;
/*
Bytes processed
26.84 MB
Bytes billed
27 MB
*/


--Q9
--Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

select count(*) from `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_partitioned`;
select count(*) from `module-3-bigquery-486618.ny_taxi.yellow_trips_2024_non_partitioned`;

--Bytes processed
--0 B