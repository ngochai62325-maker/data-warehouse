SELECT
    pickup_date,
    pickup_location_id,
    taxi_type,

    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_revenue,

    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count,

    ROUND(AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)), 2) AS avg_duration_minutes,

    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(SUM(tip_amount), 2) AS total_tip,
    ROUND(SUM(tolls_amount), 2) AS total_tolls,
    ROUND(SUM(extra), 2) AS total_extra,
    ROUND(SUM(mta_tax), 2) AS total_tax,
    ROUND(SUM(improvement_surcharge), 2) AS total_improvement_surcharge,
    ROUND(SUM(congestion_surcharge), 2) AS total_congestion_surcharge

FROM {{ ref('fact_taxi_trips') }}
GROUP BY 
    pickup_date,
    pickup_location_id,
    taxi_type

---
