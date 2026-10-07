CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS refined;


CREATE TABLE IF NOT EXISTS raw.yellow_tripdata (
    vendor_id INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    ratecode_id DOUBLE PRECISION,
    store_and_fwd_flag VARCHAR(10),
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION
);


CREATE TABLE IF NOT EXISTS trusted.yellow_tripdata (
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    ratecode_id DOUBLE PRECISION,
    store_and_fwd_flag VARCHAR(10),
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION
);


CREATE TABLE IF NOT EXISTS refined.yellow_tripdata (
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    ratecode_id DOUBLE PRECISION,
    store_and_fwd_flag VARCHAR(10),
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION
);