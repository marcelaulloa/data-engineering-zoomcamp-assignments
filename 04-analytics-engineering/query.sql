CREATE OR REPLACE EXTERNAL TABLE `plucky-cascade-305020.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://week4_bucket/fhv/*.csv']
);


CREATE OR REPLACE TABLE `plucky-cascade-305020.trips_data_all.fhv_tripdata` AS
SELECT 
*
FROM `plucky-cascade-305020.trips_data_all.external_fhv_tripdata`;