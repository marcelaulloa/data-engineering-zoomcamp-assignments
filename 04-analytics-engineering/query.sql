-- Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `plucky-cascade-305020.green_taxi.external_green_taxi_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-marcela-ulloa/green_taxi_2022.parquet']
);