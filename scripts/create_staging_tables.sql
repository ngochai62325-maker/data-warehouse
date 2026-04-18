-- Create Staging Table for Yellow Taxi
CREATE TABLE stg_YellowTaxi (
    VendorID VARCHAR(255),
    tpep_pickup_datetime VARCHAR(255),
    tpep_dropoff_datetime VARCHAR(255),
    passenger_count VARCHAR(255),
    trip_distance VARCHAR(255),
    RatecodeID VARCHAR(255),
    store_and_fwd_flag VARCHAR(255),
    PULocationID VARCHAR(255),
    DOLocationID VARCHAR(255),
    payment_type VARCHAR(255),
    fare_amount VARCHAR(255),
    extra VARCHAR(255),
    mta_tax VARCHAR(255),
    tip_amount VARCHAR(255),
    tolls_amount VARCHAR(255),
    improvement_surcharge VARCHAR(255),
    total_amount VARCHAR(255),
    congestion_surcharge VARCHAR(255),
    airport_fee VARCHAR(255)
);
GO

-- Create Staging Table for Green Taxi
CREATE TABLE stg_GreenTaxi (
    VendorID VARCHAR(255),
    lpep_pickup_datetime VARCHAR(255),
    lpep_dropoff_datetime VARCHAR(255),
    store_and_fwd_flag VARCHAR(255),
    RatecodeID VARCHAR(255),
    PULocationID VARCHAR(255),
    DOLocationID VARCHAR(255),
    passenger_count VARCHAR(255),
    trip_distance VARCHAR(255),
    fare_amount VARCHAR(255),
    extra VARCHAR(255),
    mta_tax VARCHAR(255),
    tip_amount VARCHAR(255),
    tolls_amount VARCHAR(255),
    improvement_surcharge VARCHAR(255),
    total_amount VARCHAR(255),
    payment_type VARCHAR(255),
    trip_type VARCHAR(255),
    congestion_surcharge VARCHAR(255),
	ehail_fee VARCHAR(255)
);
GO

-- Create Staging Table for FHV (for high vehicle)
CREATE TABLE stg_FHV (
	dispatching_base_num VARCHAR(255),
	pickup_datetime VARCHAR(255),
	dropOff_datetime VARCHAR(255),
	PUlocationID VARCHAR(255),
	DOlocationID VARCHAR(255),
	SR_Flag VARCHAR(255),
	Affiliated_base_number VARCHAR(255)
)

-- Create Staging Table for High Volume FHV (Uber/Lyft)
CREATE TABLE stg_HVFHV (
    hvfhs_license_num VARCHAR(255),
    dispatching_base_num VARCHAR(255),
    originating_base_num VARCHAR(255),
    request_datetime VARCHAR(255),
    on_scene_datetime VARCHAR(255),
    pickup_datetime VARCHAR(255),
    dropoff_datetime VARCHAR(255),
    PULocationID VARCHAR(255),
    DOLocationID VARCHAR(255),
    trip_miles VARCHAR(255),
    trip_time VARCHAR(255),
    base_passenger_fare VARCHAR(255),
    tolls VARCHAR(255),
    bcf VARCHAR(255),
    sales_tax VARCHAR(255),
    congestion_surcharge VARCHAR(255),
    airport_fee VARCHAR(255),
    tips VARCHAR(255),
    driver_pay VARCHAR(255),
    shared_request_flag VARCHAR(255),
    shared_match_flag VARCHAR(255),
    access_a_ride_flag VARCHAR(255),
    wav_request_flag VARCHAR(255),
    wav_match_flag VARCHAR(255)
);
GO