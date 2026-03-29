SELECT
    date AS date,

    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,

    FORMAT_DATE('%A', date) AS day_name,
    FORMAT_DATE('%B', date) AS month_name,

    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,

    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM UNNEST(
    GENERATE_DATE_ARRAY('2019-01-01', '2020-12-31')
) AS date