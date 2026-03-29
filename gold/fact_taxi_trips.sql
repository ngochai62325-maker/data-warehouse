WITH combined AS (

    SELECT
        'green' AS taxi_type,
        *
    FROM {{ ref('silver_green_taxi') }}

    UNION ALL

    SELECT
        'yellow' AS taxi_type,
        *
    FROM {{ ref('silver_yellow_taxi') }}

)

SELECT
    GENERATE_UUID() AS trip_id,

    taxi_type,
    vendor_id,

    pickup_datetime,
    dropoff_datetime,

    DATE(pickup_datetime) AS pickup_date,
    DATE(dropoff_datetime) AS dropoff_date,

    pickup_location_id,
    dropoff_location_id,

    ratecode_id,
    payment_type,
    trip_type,

    passenger_count,
    trip_distance,

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    total_amount

FROM combined