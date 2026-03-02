-- irs_group_lakebase.public

DROP TABLE pg_yellow_taxi_trip_data;

CREATE TABLE pg_yellow_taxi_trip_data (
  VendorID INTEGER,
  tpep_pickup_datetime TEXT,
  tpep_dropoff_datetime TEXT,
  passenger_count INTEGER,
  trip_distance DOUBLE PRECISION,
  RatecodeID INTEGER,
  store_and_fwd_flag VARCHAR(10),
  PULocationID INTEGER,
  DOLocationID INTEGER,
  payment_type INTEGER,
  fare_amount DOUBLE PRECISION,
  extra DOUBLE PRECISION,
  mta_tax DOUBLE PRECISION,
  tip_amount DOUBLE PRECISION,
  tolls_amount DOUBLE PRECISION,
  improvement_surcharge DOUBLE PRECISION,
  total_amount DOUBLE PRECISION,
  congestion_surcharge DOUBLE PRECISION
);

GRANT ALL PRIVILEGES ON TABLE pg_yellow_taxi_trip_data TO irs_lakebase_user;

select count(*) from pg_yellow_taxi_trip_data;

TRUNCATE TABLE pg_yellow_taxi_trip_data;
select count(*) from pg_yellow_taxi_trip_data;