-- Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `plucky-cascade-305020.green_taxi.external_green_taxi_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-marcela-ulloa/green_taxi_2022.parquet']
);

-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE plucky-cascade-305020.green_taxi.green_taxi_data AS
SELECT * FROM plucky-cascade-305020.green_taxi.external_green_taxi_data;

-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT(PULocationID)) FROM `plucky-cascade-305020.green_taxi.external_green_taxi_data`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `plucky-cascade-305020.green_taxi.green_taxi_data`;

-- How many records have a fare_amount of 0?
SELECT count(*) as fares_with_zero FROM `plucky-cascade-305020.green_taxi.green_taxi_data` 
WHERE fare_amount=0;


-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
CREATE OR REPLACE TABLE plucky-cascade-305020.green_taxi.green_taxi_data_partitioned_exercise
PARTITION BY
     lpep_pickup_date 
CLUSTER BY PULocationID AS
SELECT * FROM plucky-cascade-305020.green_taxi.external_green_taxi_data;

-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
SELECT COUNT(DISTINCT(PULocationID)) FROM  `plucky-cascade-305020.green_taxi.green_taxi_data`
WHERE DATE(lpep_pickup_date) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT COUNT(DISTINCT(PULocationID)) FROM  `plucky-cascade-305020.green_taxi.green_taxi_data_partitioned_exercise`
WHERE DATE(lpep_pickup_date) BETWEEN '2022-06-01' AND '2022-06-30';


-- Practicing Partitioning
-- Creating a partitioned table 1
CREATE OR REPLACE TABLE plucky-cascade-305020.green_taxi.green_taxi_data_partitioned
PARTITION BY
     part AS
SELECT DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime/1000 AS INT64))) AS part, * FROM plucky-cascade-305020.green_taxi.green_taxi_data;

SELECT DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime/1000 AS INT64))) from plucky-cascade-305020.green_taxi.green_taxi_data;

-- Creating a partitioned table 2
CREATE OR REPLACE TABLE plucky-cascade-305020.green_taxi.green_taxi_data_partitioned_2
PARTITION BY
     lpep_pickup_date AS
SELECT * FROM plucky-cascade-305020.green_taxi.external_green_taxi_data;