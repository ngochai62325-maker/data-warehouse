-- ============================================================
--  NGƯỜI 3 - THE LOADER: FULL SQL SCRIPT
--  NYC TLC Trip Data Warehouse (Oct 2020 - Mar 2021)
--  Schema dựa theo thiết kế Star Schema trong hình
-- ============================================================

USE master;
GO

-- Tạo database nếu chưa có
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'NYC_TLC_DWH')
BEGIN
    CREATE DATABASE NYC_TLC_DWH;
END
GO

USE NYC_TLC_DWH;
GO

-- ============================================================
-- BƯỚC 0: XÓA BẢNG CŨ (nếu cần chạy lại từ đầu)
-- ============================================================
IF OBJECT_ID('fact_trip_financials',    'U') IS NOT NULL DROP TABLE fact_trip_financials;
IF OBJECT_ID('fact_dispatch_lifecycle', 'U') IS NOT NULL DROP TABLE fact_dispatch_lifecycle;
IF OBJECT_ID('fact_ride_request_event', 'U') IS NOT NULL DROP TABLE fact_ride_request_event;
IF OBJECT_ID('dim_date',                'U') IS NOT NULL DROP TABLE dim_date;
IF OBJECT_ID('dim_time',                'U') IS NOT NULL DROP TABLE dim_time;
IF OBJECT_ID('dim_location',            'U') IS NOT NULL DROP TABLE dim_location;
IF OBJECT_ID('dim_service_provider',    'U') IS NOT NULL DROP TABLE dim_service_provider;
IF OBJECT_ID('dim_rate_code',           'U') IS NOT NULL DROP TABLE dim_rate_code;
IF OBJECT_ID('dim_payment',             'U') IS NOT NULL DROP TABLE dim_payment;
IF OBJECT_ID('stg_raw_yellow',          'U') IS NOT NULL DROP TABLE stg_raw_yellow;
IF OBJECT_ID('stg_raw_green',           'U') IS NOT NULL DROP TABLE stg_raw_green;
IF OBJECT_ID('stg_raw_hvfhv',           'U') IS NOT NULL DROP TABLE stg_raw_hvfhv;
IF OBJECT_ID('stg_clean_trips',         'U') IS NOT NULL DROP TABLE stg_clean_trips;
IF OBJECT_ID('tbl_etl_log',             'U') IS NOT NULL DROP TABLE tbl_etl_log;
GO

-- ============================================================
-- BƯỚC 1A: RAW STAGING TABLES (cho Người 1 đổ vào)
-- ============================================================

-- Raw Staging: Yellow Taxi
CREATE TABLE stg_raw_yellow (
    raw_id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
    VendorID                NVARCHAR(10),
    tpep_pickup_datetime    NVARCHAR(30),
    tpep_dropoff_datetime   NVARCHAR(30),
    passenger_count         NVARCHAR(10),
    trip_distance           NVARCHAR(20),
    RatecodeID              NVARCHAR(10),
    store_and_fwd_flag      NVARCHAR(5),
    PULocationID            NVARCHAR(10),
    DOLocationID            NVARCHAR(10),
    payment_type            NVARCHAR(10),
    fare_amount             NVARCHAR(20),
    extra                   NVARCHAR(20),
    mta_tax                 NVARCHAR(20),
    tip_amount              NVARCHAR(20),
    tolls_amount            NVARCHAR(20),
    improvement_surcharge   NVARCHAR(20),
    total_amount            NVARCHAR(20),
    congestion_surcharge    NVARCHAR(20),
    airport_fee             NVARCHAR(20),
    source_file             NVARCHAR(200),
    load_datetime           DATETIME DEFAULT GETDATE()
);
GO

-- Raw Staging: Green Taxi
CREATE TABLE stg_raw_green (
    raw_id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
    VendorID                NVARCHAR(10),
    lpep_pickup_datetime    NVARCHAR(30),
    lpep_dropoff_datetime   NVARCHAR(30),
    store_and_fwd_flag      NVARCHAR(5),
    RatecodeID              NVARCHAR(10),
    PULocationID            NVARCHAR(10),
    DOLocationID            NVARCHAR(10),
    passenger_count         NVARCHAR(10),
    trip_distance           NVARCHAR(20),
    fare_amount             NVARCHAR(20),
    extra                   NVARCHAR(20),
    mta_tax                 NVARCHAR(20),
    tip_amount              NVARCHAR(20),
    tolls_amount            NVARCHAR(20),
    improvement_surcharge   NVARCHAR(20),
    total_amount            NVARCHAR(20),
    payment_type            NVARCHAR(10),
    trip_type               NVARCHAR(5),
    congestion_surcharge    NVARCHAR(20),
    source_file             NVARCHAR(200),
    load_datetime           DATETIME DEFAULT GETDATE()
);
GO

-- Raw Staging: HVFHV (Uber, Lyft, Via, Juno)
CREATE TABLE stg_raw_hvfhv (
    raw_id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
    hvfhs_license_num       NVARCHAR(20),
    dispatching_base_num    NVARCHAR(20),
    originating_base_num    NVARCHAR(20),
    request_datetime        NVARCHAR(30),
    on_scene_datetime       NVARCHAR(30),
    pickup_datetime         NVARCHAR(30),
    dropoff_datetime        NVARCHAR(30),
    PULocationID            NVARCHAR(10),
    DOLocationID            NVARCHAR(10),
    trip_miles              NVARCHAR(20),
    trip_time               NVARCHAR(20),
    base_passenger_fare     NVARCHAR(20),
    tolls                   NVARCHAR(20),
    bcf                     NVARCHAR(20),
    sales_tax               NVARCHAR(20),
    congestion_surcharge    NVARCHAR(20),
    airport_fee             NVARCHAR(20),
    tips                    NVARCHAR(20),
    driver_pay              NVARCHAR(20),
    shared_request_flag     NVARCHAR(5),
    shared_match_flag       NVARCHAR(5),
    access_a_ride_flag      NVARCHAR(5),
    wav_request_flag        NVARCHAR(5),
    wav_match_flag          NVARCHAR(5),
    source_file             NVARCHAR(200),
    load_datetime           DATETIME DEFAULT GETDATE()
);
GO

-- ============================================================
-- BƯỚC 1B: CLEAN STAGING TABLE (Người 2 đổ vào sau khi làm sạch)
-- ============================================================
CREATE TABLE stg_clean_trips (
    clean_id                BIGINT IDENTITY(1,1) PRIMARY KEY,
    -- ID định danh duy nhất cho MERGE (CDC)
    trip_id                 NVARCHAR(100) NOT NULL,
    -- Loại xe
    vehicle_type            NVARCHAR(20)  NOT NULL,   -- 'yellow','green','hvfhv'
    -- Thời gian
    pickup_datetime         DATETIME,
    dropoff_datetime        DATETIME,
    request_datetime        DATETIME,                  -- chỉ HVFHV
    on_scene_datetime       DATETIME,                  -- chỉ HVFHV
    -- Địa điểm
    PULocationID            INT,
    DOLocationID            INT,
    -- Nhà cung cấp
    hvfhs_license_num       NVARCHAR(20),
    dispatching_base_num    NVARCHAR(20),
    VendorID                INT,
    -- Chuyến đi
    passenger_count         INT,
    trip_distance           DECIMAL(10,2),
    trip_type               NVARCHAR(20),              -- 'Street-hail','Dispatch',NULL
    booking_method          NVARCHAR(20),              -- 'metered','app'
    RatecodeID              INT,
    payment_type            INT,
    -- Tài chính
    base_fare_amount        DECIMAL(10,2),
    tip_amount              DECIMAL(10,2),
    tolls_amount            DECIMAL(10,2),
    extra_surcharges        DECIMAL(10,2),
    mta_tax                 DECIMAL(10,2),
    improvement_surcharge   DECIMAL(10,2),
    black_car_fund          DECIMAL(10,2),
    sales_tax               DECIMAL(10,2),
    congestion_surcharge    DECIMAL(10,2),
    airport_fee             DECIMAL(10,2),
    total_amount            DECIMAL(10,2),
    driver_pay              DECIMAL(10,2),
    -- Cờ chia sẻ (HVFHV)
    is_shared_requested     BIT,
    is_shared_matched       BIT,
    is_wav_requested        BIT,
    is_wav_matched          BIT,
    -- Metadata
    source_file             NVARCHAR(200),
    load_datetime           DATETIME DEFAULT GETDATE()
);
GO

-- ============================================================
-- BƯỚC 2: DIMENSION TABLES
-- ============================================================

-- DIM_DATE
CREATE TABLE dim_date (
    date_key        INT IDENTITY(1,1) PRIMARY KEY,
    full_date       DATE             NOT NULL UNIQUE,
    year            INT              NOT NULL,
    quarter         INT              NOT NULL,
    month           INT              NOT NULL,
    month_name      NVARCHAR(20)     NOT NULL,
    day_of_month    INT              NOT NULL,
    day_of_week     INT              NOT NULL,   -- 1=Sunday ... 7=Saturday
    week_of_year    INT              NOT NULL,
    is_weekend      BIT              NOT NULL
);
GO

-- DIM_TIME
CREATE TABLE dim_time (
    time_key            INT IDENTITY(1,1) PRIMARY KEY,
    hour                INT             NOT NULL,
    minute              INT             NOT NULL,
    time_of_day_category NVARCHAR(20)  NOT NULL   
    -- 'Morning'(6-12),'Afternoon'(12-18),'Evening'(18-22),'Night'(22-6)
);
GO

-- DIM_LOCATION
CREATE TABLE dim_location (
    location_key    INT IDENTITY(1,1) PRIMARY KEY,
    location_id     INT              NOT NULL UNIQUE,
    borough         NVARCHAR(50),
    zone            NVARCHAR(100),
    service_zone    NVARCHAR(50)
);
GO

-- DIM_SERVICE_PROVIDER
CREATE TABLE dim_service_provider (
    service_provider_key    INT IDENTITY(1,1) PRIMARY KEY,
    vehicle_type            NVARCHAR(20)     NOT NULL,   -- 'yellow','green','hvfhv'
    app_company_affiliation NVARCHAR(50),                -- 'Uber','Lyft','Juno','Via',NULL
    base_name               NVARCHAR(100),
    dispatching_base_num    NVARCHAR(20)
);
GO

-- DIM_RATE_CODE
CREATE TABLE dim_rate_code (
    rate_code_key       INT IDENTITY(1,1) PRIMARY KEY,
    rate_code_id        INT              NOT NULL UNIQUE,
    rate_description    NVARCHAR(100)    NOT NULL
);
GO

-- DIM_PAYMENT
CREATE TABLE dim_payment (
    payment_type_key    INT IDENTITY(1,1) PRIMARY KEY,
    payment_type_id     INT              NOT NULL UNIQUE,
    payment_description NVARCHAR(50)     NOT NULL
);
GO

-- ============================================================
-- BƯỚC 3: FACT TABLES
-- ============================================================

-- FACT_TRIP_FINANCIALS (bảng chính - điểm MERGE/CDC)
CREATE TABLE fact_trip_financials (
    trip_financial_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    trip_id                 NVARCHAR(100)    NOT NULL UNIQUE,  -- dùng cho MERGE
    -- Foreign Keys
    pickup_date_key         INT FOREIGN KEY REFERENCES dim_date(date_key),
    pickup_time_key         INT FOREIGN KEY REFERENCES dim_time(time_key),
    dropoff_date_key        INT FOREIGN KEY REFERENCES dim_date(date_key),
    dropoff_time_key        INT FOREIGN KEY REFERENCES dim_time(time_key),
    pickup_location_key     INT FOREIGN KEY REFERENCES dim_location(location_key),
    dropoff_location_key    INT FOREIGN KEY REFERENCES dim_location(location_key),
    service_provider_key    INT FOREIGN KEY REFERENCES dim_service_provider(service_provider_key),
    rate_code_key           INT FOREIGN KEY REFERENCES dim_rate_code(rate_code_key),
    payment_type_key        INT FOREIGN KEY REFERENCES dim_payment(payment_type_key),
    -- Measures
    trip_distance           DECIMAL(10,2),
    base_fare_amount        DECIMAL(10,2),
    tip_amount              DECIMAL(10,2),
    tolls_amount            DECIMAL(10,2),
    extra_surcharges        DECIMAL(10,2),
    mta_tax                 DECIMAL(10,2),
    improvement_surcharge   DECIMAL(10,2),
    black_car_fund          DECIMAL(10,2),
    sales_tax               DECIMAL(10,2),
    congestion_surcharge    DECIMAL(10,2),
    airport_fee             DECIMAL(10,2),
    total_amount            DECIMAL(10,2),
    driver_pay              DECIMAL(10,2),
    booking_method          NVARCHAR(20),
    -- Audit
    dw_insert_datetime      DATETIME DEFAULT GETDATE(),
    dw_update_datetime      DATETIME
);
GO

-- FACT_DISPATCH_LIFECYCLE (chỉ HVFHV)
CREATE TABLE fact_dispatch_lifecycle (
    dispatch_event_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    trip_id                 NVARCHAR(100)    NOT NULL UNIQUE,
    -- Foreign Keys
    service_provider_key    INT FOREIGN KEY REFERENCES dim_service_provider(service_provider_key),
    pickup_location_key     INT FOREIGN KEY REFERENCES dim_location(location_key),
    dropoff_location_key    INT FOREIGN KEY REFERENCES dim_location(location_key),
    request_date_key        INT FOREIGN KEY REFERENCES dim_date(date_key),
    request_time_key        INT FOREIGN KEY REFERENCES dim_time(time_key),
    on_scene_date_key       INT FOREIGN KEY REFERENCES dim_date(date_key),
    on_scene_time_key       INT FOREIGN KEY REFERENCES dim_time(time_key),
    pickup_date_key         INT FOREIGN KEY REFERENCES dim_date(date_key),
    pickup_time_key         INT FOREIGN KEY REFERENCES dim_time(time_key),
    dropoff_date_key        INT FOREIGN KEY REFERENCES dim_date(date_key),
    dropoff_time_key        INT FOREIGN KEY REFERENCES dim_time(time_key),
    -- Measures
    wait_time_seconds       INT,
    travel_time_seconds     INT,
    booking_method          NVARCHAR(20),
    -- Audit
    dw_insert_datetime      DATETIME DEFAULT GETDATE()
);
GO

-- FACT_RIDE_REQUEST_EVENT (chỉ HVFHV)
CREATE TABLE fact_ride_request_event (
    request_event_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
    trip_id                 NVARCHAR(100)    NOT NULL UNIQUE,
    -- Foreign Keys
    event_date_key          INT FOREIGN KEY REFERENCES dim_date(date_key),
    event_time_key          INT FOREIGN KEY REFERENCES dim_time(time_key),
    location_key            INT FOREIGN KEY REFERENCES dim_location(location_key),
    service_provider_key    INT FOREIGN KEY REFERENCES dim_service_provider(service_provider_key),
    -- Measures / Flags
    is_shared_requested     BIT,
    is_shared_matched       BIT,
    is_wav_requested        BIT,
    is_wav_matched          BIT,
    event_count             INT DEFAULT 1,
    -- Audit
    dw_insert_datetime      DATETIME DEFAULT GETDATE()
);
GO

-- ETL Log Table
CREATE TABLE tbl_etl_log (
    log_id          INT IDENTITY(1,1) PRIMARY KEY,
    log_datetime    DATETIME DEFAULT GETDATE(),
    step_name       NVARCHAR(200),
    status          NVARCHAR(20),    -- 'SUCCESS','FAILED','INFO'
    rows_affected   INT,
    message         NVARCHAR(1000)
);
GO

PRINT '✅ BƯỚC 1-3: Tất cả bảng đã được tạo thành công!';
GO

-- ============================================================
-- BƯỚC 4: SEED DATA CHO DIMENSION TABLES (dữ liệu tĩnh)
-- ============================================================

-- Seed dim_rate_code
INSERT INTO dim_rate_code (rate_code_id, rate_description) VALUES
(1,  'Standard rate'),
(2,  'JFK'),
(3,  'Newark'),
(4,  'Nassau or Westchester'),
(5,  'Negotiated fare'),
(6,  'Group ride'),
(99, 'Unknown/Null');
GO

-- Seed dim_payment
INSERT INTO dim_payment (payment_type_id, payment_description) VALUES
(0, 'Flex Fare'),
(1, 'Credit Card'),
(2, 'Cash'),
(3, 'No Charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided Trip'),
(-1,'Not Applicable');   -- dùng cho HVFHV không có payment_type
GO

-- Seed dim_service_provider
INSERT INTO dim_service_provider (vehicle_type, app_company_affiliation, base_name, dispatching_base_num) VALUES
('yellow', NULL,   'Yellow Taxi - CMT',    NULL),
('yellow', NULL,   'Yellow Taxi - Curb',   NULL),
('green',  NULL,   'Green Taxi - CMT',     NULL),
('green',  NULL,   'Green Taxi - Curb',    NULL),
('hvfhv',  'Juno', 'Juno',                'HV0002'),
('hvfhv',  'Uber', 'Uber',                'HV0003'),
('hvfhv',  'Via',  'Via',                 'HV0004'),
('hvfhv',  'Lyft', 'Lyft',               'HV0005');
GO

PRINT '✅ BƯỚC 4: Seed data Dimension đã xong!';
GO

-- ============================================================
-- BƯỚC 5: STORED PROCEDURES - LOAD DIMENSION TABLES
-- ============================================================

-- SP: Load dim_date (sinh ra từ dữ liệu trong stg_clean_trips)
CREATE OR ALTER PROCEDURE sp_Load_Dim_Date
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @rows INT = 0;

    -- Thu thập tất cả các ngày duy nhất từ clean staging
    WITH all_dates AS (
        SELECT CAST(pickup_datetime  AS DATE) AS d FROM stg_clean_trips WHERE pickup_datetime  IS NOT NULL
        UNION
        SELECT CAST(dropoff_datetime AS DATE)       FROM stg_clean_trips WHERE dropoff_datetime IS NOT NULL
        UNION
        SELECT CAST(request_datetime AS DATE)       FROM stg_clean_trips WHERE request_datetime IS NOT NULL
        UNION
        SELECT CAST(on_scene_datetime AS DATE)      FROM stg_clean_trips WHERE on_scene_datetime IS NOT NULL
    )
    INSERT INTO dim_date (full_date, year, quarter, month, month_name, day_of_month, day_of_week, week_of_year, is_weekend)
    SELECT
        d,
        YEAR(d),
        DATEPART(QUARTER, d),
        MONTH(d),
        DATENAME(MONTH, d),
        DAY(d),
        DATEPART(WEEKDAY, d),
        DATEPART(WEEK, d),
        CASE WHEN DATEPART(WEEKDAY, d) IN (1,7) THEN 1 ELSE 0 END
    FROM all_dates
    WHERE d NOT IN (SELECT full_date FROM dim_date);

    SET @rows = @@ROWCOUNT;
    INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
    VALUES ('sp_Load_Dim_Date', 'SUCCESS', @rows, 'dim_date loaded');
END;
GO

-- SP: Load dim_time (1440 phút/ngày, chạy 1 lần duy nhất)
CREATE OR ALTER PROCEDURE sp_Load_Dim_Time
AS
BEGIN
    SET NOCOUNT ON;
    IF (SELECT COUNT(*) FROM dim_time) > 0
    BEGIN
        PRINT 'dim_time already seeded, skipping.';
        RETURN;
    END

    WITH hours AS (
        SELECT 0 AS h UNION ALL SELECT h+1 FROM hours WHERE h < 23
    ),
    minutes AS (
        SELECT 0 AS m UNION ALL SELECT m+1 FROM minutes WHERE m < 59
    )
    INSERT INTO dim_time (hour, minute, time_of_day_category)
    SELECT
        h.h,
        m.m,
        CASE
            WHEN h.h BETWEEN  6 AND 11 THEN 'Morning'
            WHEN h.h BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN h.h BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END
    FROM hours h CROSS JOIN minutes m
    OPTION (MAXRECURSION 100);

    INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
    VALUES ('sp_Load_Dim_Time', 'SUCCESS', @@ROWCOUNT, 'dim_time seeded with 1440 rows');
END;
GO

-- SP: Load dim_location (cần file taxi_zone_lookup.csv - Người 1 load)
-- Giả sử Người 1 đã tạo bảng stg_raw_location từ taxi_zone_lookup.csv
CREATE OR ALTER PROCEDURE sp_Load_Dim_Location
AS
BEGIN
    SET NOCOUNT ON;

    -- Nếu có bảng lookup từ file taxi_zone_lookup.csv
    IF OBJECT_ID('stg_raw_location', 'U') IS NOT NULL
    BEGIN
        INSERT INTO dim_location (location_id, borough, zone, service_zone)
        SELECT
            CAST(LocationID  AS INT),
            Borough,
            Zone,
            service_zone
        FROM stg_raw_location
        WHERE CAST(LocationID AS INT) NOT IN (SELECT location_id FROM dim_location);
    END
    ELSE
    BEGIN
        -- Fallback: lấy location_id duy nhất từ stg_clean_trips (không có tên)
        INSERT INTO dim_location (location_id, borough, zone, service_zone)
        SELECT DISTINCT PULocationID, 'Unknown', 'Unknown', 'Unknown'
        FROM stg_clean_trips
        WHERE PULocationID NOT IN (SELECT location_id FROM dim_location)
          AND PULocationID IS NOT NULL;

        INSERT INTO dim_location (location_id, borough, zone, service_zone)
        SELECT DISTINCT DOLocationID, 'Unknown', 'Unknown', 'Unknown'
        FROM stg_clean_trips
        WHERE DOLocationID NOT IN (SELECT location_id FROM dim_location)
          AND DOLocationID IS NOT NULL;
    END

    INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
    VALUES ('sp_Load_Dim_Location', 'SUCCESS', @@ROWCOUNT, 'dim_location loaded');
END;
GO

PRINT '✅ BƯỚC 5: Stored Procedures Load Dim đã sẵn sàng!';
GO

-- ============================================================
-- BƯỚC 6: STORED PROCEDURE - CDC MERGE VÀO FACT TABLES
--         *** ĐÂY LÀ PHẦN ĐIỂM RUBRIC QUAN TRỌNG NHẤT ***
-- ============================================================

CREATE OR ALTER PROCEDURE sp_Load_Fact_Trip_Financials
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE fact_trip_financials AS TARGET
        USING (
            SELECT
                s.trip_id,
                -- Lookup Date Keys
                pd.date_key                     AS pickup_date_key,
                pt.time_key                     AS pickup_time_key,
                dd.date_key                     AS dropoff_date_key,
                dt.time_key                     AS dropoff_time_key,
                -- Lookup Location Keys
                pu_loc.location_key             AS pickup_location_key,
                do_loc.location_key             AS dropoff_location_key,
                -- Lookup Service Provider Key
                sp.service_provider_key,
                -- Lookup Rate Code Key
                rc.rate_code_key,
                -- Lookup Payment Key
                pay.payment_type_key,
                -- Measures
                s.trip_distance,
                s.base_fare_amount,
                s.tip_amount,
                s.tolls_amount,
                s.extra_surcharges,
                s.mta_tax,
                s.improvement_surcharge,
                s.black_car_fund,
                s.sales_tax,
                s.congestion_surcharge,
                s.airport_fee,
                s.total_amount,
                s.driver_pay,
                s.booking_method
            FROM stg_clean_trips s
            -- Join Dim_Date (pickup)
            LEFT JOIN dim_date    pd      ON pd.full_date = CAST(s.pickup_datetime  AS DATE)
            -- Join Dim_Time (pickup)
            LEFT JOIN dim_time    pt      ON pt.hour = DATEPART(HOUR,   s.pickup_datetime)
                                         AND pt.minute = DATEPART(MINUTE, s.pickup_datetime)
            -- Join Dim_Date (dropoff)
            LEFT JOIN dim_date    dd      ON dd.full_date = CAST(s.dropoff_datetime AS DATE)
            -- Join Dim_Time (dropoff)
            LEFT JOIN dim_time    dt      ON dt.hour = DATEPART(HOUR,   s.dropoff_datetime)
                                         AND dt.minute = DATEPART(MINUTE, s.dropoff_datetime)
            -- Join Dim_Location
            LEFT JOIN dim_location pu_loc ON pu_loc.location_id = s.PULocationID
            LEFT JOIN dim_location do_loc ON do_loc.location_id = s.DOLocationID
            -- Join Dim_Service_Provider
            LEFT JOIN dim_service_provider sp
                   ON sp.vehicle_type = s.vehicle_type
                  AND (
                      (s.vehicle_type = 'hvfhv' AND sp.dispatching_base_num = s.hvfhs_license_num)
                   OR (s.vehicle_type IN ('yellow','green') AND sp.app_company_affiliation IS NULL)
                  )
            -- Join Dim_Rate_Code
            LEFT JOIN dim_rate_code rc    ON rc.rate_code_id = ISNULL(s.RatecodeID, 99)
            -- Join Dim_Payment
            LEFT JOIN dim_payment pay     ON pay.payment_type_id = ISNULL(s.payment_type, -1)
        ) AS SOURCE
        ON TARGET.trip_id = SOURCE.trip_id   -- Điều kiện CDC: so khớp theo trip_id

        -- *** WHEN MATCHED → UPDATE (chuyến đi đã tồn tại, cập nhật lại) ***
        WHEN MATCHED THEN UPDATE SET
            TARGET.pickup_date_key          = SOURCE.pickup_date_key,
            TARGET.pickup_time_key          = SOURCE.pickup_time_key,
            TARGET.dropoff_date_key         = SOURCE.dropoff_date_key,
            TARGET.dropoff_time_key         = SOURCE.dropoff_time_key,
            TARGET.pickup_location_key      = SOURCE.pickup_location_key,
            TARGET.dropoff_location_key     = SOURCE.dropoff_location_key,
            TARGET.service_provider_key     = SOURCE.service_provider_key,
            TARGET.rate_code_key            = SOURCE.rate_code_key,
            TARGET.payment_type_key         = SOURCE.payment_type_key,
            TARGET.trip_distance            = SOURCE.trip_distance,
            TARGET.base_fare_amount         = SOURCE.base_fare_amount,
            TARGET.tip_amount               = SOURCE.tip_amount,
            TARGET.tolls_amount             = SOURCE.tolls_amount,
            TARGET.extra_surcharges         = SOURCE.extra_surcharges,
            TARGET.mta_tax                  = SOURCE.mta_tax,
            TARGET.improvement_surcharge    = SOURCE.improvement_surcharge,
            TARGET.black_car_fund           = SOURCE.black_car_fund,
            TARGET.sales_tax                = SOURCE.sales_tax,
            TARGET.congestion_surcharge     = SOURCE.congestion_surcharge,
            TARGET.airport_fee              = SOURCE.airport_fee,
            TARGET.total_amount             = SOURCE.total_amount,
            TARGET.driver_pay               = SOURCE.driver_pay,
            TARGET.booking_method           = SOURCE.booking_method,
            TARGET.dw_update_datetime       = GETDATE()

        -- *** WHEN NOT MATCHED → INSERT (chuyến đi mới) ***
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            trip_id, pickup_date_key, pickup_time_key,
            dropoff_date_key, dropoff_time_key,
            pickup_location_key, dropoff_location_key,
            service_provider_key, rate_code_key, payment_type_key,
            trip_distance, base_fare_amount, tip_amount, tolls_amount,
            extra_surcharges, mta_tax, improvement_surcharge,
            black_car_fund, sales_tax, congestion_surcharge,
            airport_fee, total_amount, driver_pay, booking_method,
            dw_insert_datetime
        )
        VALUES (
            SOURCE.trip_id, SOURCE.pickup_date_key, SOURCE.pickup_time_key,
            SOURCE.dropoff_date_key, SOURCE.dropoff_time_key,
            SOURCE.pickup_location_key, SOURCE.dropoff_location_key,
            SOURCE.service_provider_key, SOURCE.rate_code_key, SOURCE.payment_type_key,
            SOURCE.trip_distance, SOURCE.base_fare_amount, SOURCE.tip_amount, SOURCE.tolls_amount,
            SOURCE.extra_surcharges, SOURCE.mta_tax, SOURCE.improvement_surcharge,
            SOURCE.black_car_fund, SOURCE.sales_tax, SOURCE.congestion_surcharge,
            SOURCE.airport_fee, SOURCE.total_amount, SOURCE.driver_pay, SOURCE.booking_method,
            GETDATE()
        );

        DECLARE @rowsAffected INT = @@ROWCOUNT;
        COMMIT TRANSACTION;

        INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
        VALUES ('sp_Load_Fact_Trip_Financials', 'SUCCESS', @rowsAffected,
                'MERGE CDC completed: ' + CAST(@rowsAffected AS NVARCHAR) + ' rows');

        PRINT '✅ MERGE fact_trip_financials thành công: ' + CAST(@rowsAffected AS NVARCHAR) + ' rows';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @errMsg NVARCHAR(1000) = ERROR_MESSAGE();
        INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
        VALUES ('sp_Load_Fact_Trip_Financials', 'FAILED', 0, @errMsg);
        RAISERROR(@errMsg, 16, 1);
    END CATCH
END;
GO

-- SP: Load fact_dispatch_lifecycle (chỉ HVFHV)
CREATE OR ALTER PROCEDURE sp_Load_Fact_Dispatch_Lifecycle
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE fact_dispatch_lifecycle AS TARGET
        USING (
            SELECT
                s.trip_id,
                sp.service_provider_key,
                pu_loc.location_key         AS pickup_location_key,
                do_loc.location_key         AS dropoff_location_key,
                rd.date_key                 AS request_date_key,
                rt.time_key                 AS request_time_key,
                od.date_key                 AS on_scene_date_key,
                ot.time_key                 AS on_scene_time_key,
                pd.date_key                 AS pickup_date_key,
                pt.time_key                 AS pickup_time_key,
                dd.date_key                 AS dropoff_date_key,
                dt.time_key                 AS dropoff_time_key,
                DATEDIFF(SECOND, s.request_datetime,  s.pickup_datetime)  AS wait_time_seconds,
                DATEDIFF(SECOND, s.pickup_datetime,   s.dropoff_datetime) AS travel_time_seconds,
                s.booking_method
            FROM stg_clean_trips s
            LEFT JOIN dim_service_provider sp  ON sp.vehicle_type = 'hvfhv'
                                              AND sp.dispatching_base_num = s.hvfhs_license_num
            LEFT JOIN dim_location pu_loc      ON pu_loc.location_id = s.PULocationID
            LEFT JOIN dim_location do_loc      ON do_loc.location_id = s.DOLocationID
            LEFT JOIN dim_date  rd ON rd.full_date = CAST(s.request_datetime  AS DATE)
            LEFT JOIN dim_time  rt ON rt.hour = DATEPART(HOUR,   s.request_datetime)
                                  AND rt.minute = DATEPART(MINUTE, s.request_datetime)
            LEFT JOIN dim_date  od ON od.full_date = CAST(s.on_scene_datetime AS DATE)
            LEFT JOIN dim_time  ot ON ot.hour = DATEPART(HOUR,   s.on_scene_datetime)
                                  AND ot.minute = DATEPART(MINUTE, s.on_scene_datetime)
            LEFT JOIN dim_date  pd ON pd.full_date = CAST(s.pickup_datetime   AS DATE)
            LEFT JOIN dim_time  pt ON pt.hour = DATEPART(HOUR,   s.pickup_datetime)
                                  AND pt.minute = DATEPART(MINUTE, s.pickup_datetime)
            LEFT JOIN dim_date  dd ON dd.full_date = CAST(s.dropoff_datetime  AS DATE)
            LEFT JOIN dim_time  dt ON dt.hour = DATEPART(HOUR,   s.dropoff_datetime)
                                  AND dt.minute = DATEPART(MINUTE, s.dropoff_datetime)
            WHERE s.vehicle_type = 'hvfhv'
        ) AS SOURCE
        ON TARGET.trip_id = SOURCE.trip_id

        WHEN MATCHED THEN UPDATE SET
            TARGET.wait_time_seconds   = SOURCE.wait_time_seconds,
            TARGET.travel_time_seconds = SOURCE.travel_time_seconds

        WHEN NOT MATCHED BY TARGET THEN INSERT (
            trip_id, service_provider_key, pickup_location_key, dropoff_location_key,
            request_date_key, request_time_key, on_scene_date_key, on_scene_time_key,
            pickup_date_key, pickup_time_key, dropoff_date_key, dropoff_time_key,
            wait_time_seconds, travel_time_seconds, booking_method
        )
        VALUES (
            SOURCE.trip_id, SOURCE.service_provider_key, SOURCE.pickup_location_key, SOURCE.dropoff_location_key,
            SOURCE.request_date_key, SOURCE.request_time_key, SOURCE.on_scene_date_key, SOURCE.on_scene_time_key,
            SOURCE.pickup_date_key, SOURCE.pickup_time_key, SOURCE.dropoff_date_key, SOURCE.dropoff_time_key,
            SOURCE.wait_time_seconds, SOURCE.travel_time_seconds, SOURCE.booking_method
        );

        COMMIT TRANSACTION;
        INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
        VALUES ('sp_Load_Fact_Dispatch_Lifecycle', 'SUCCESS', @@ROWCOUNT, 'MERGE CDC completed');
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
        VALUES ('sp_Load_Fact_Dispatch_Lifecycle', 'FAILED', 0, ERROR_MESSAGE());
        RAISERROR(ERROR_MESSAGE(), 16, 1);
    END CATCH
END;
GO

-- SP: Load fact_ride_request_event (chỉ HVFHV)
CREATE OR ALTER PROCEDURE sp_Load_Fact_Ride_Request_Event
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE fact_ride_request_event AS TARGET
        USING (
            SELECT
                s.trip_id,
                ed.date_key                 AS event_date_key,
                et.time_key                 AS event_time_key,
                loc.location_key,
                sp.service_provider_key,
                s.is_shared_requested,
                s.is_shared_matched,
                s.is_wav_requested,
                s.is_wav_matched,
                1 AS event_count
            FROM stg_clean_trips s
            LEFT JOIN dim_date  ed  ON ed.full_date = CAST(s.request_datetime AS DATE)
            LEFT JOIN dim_time  et  ON et.hour = DATEPART(HOUR,   s.request_datetime)
                                   AND et.minute = DATEPART(MINUTE, s.request_datetime)
            LEFT JOIN dim_location loc ON loc.location_id = s.PULocationID
            LEFT JOIN dim_service_provider sp ON sp.vehicle_type = 'hvfhv'
                                             AND sp.dispatching_base_num = s.hvfhs_license_num
            WHERE s.vehicle_type = 'hvfhv'
        ) AS SOURCE
        ON TARGET.trip_id = SOURCE.trip_id

        WHEN MATCHED THEN UPDATE SET
            TARGET.is_shared_requested = SOURCE.is_shared_requested,
            TARGET.is_shared_matched   = SOURCE.is_shared_matched,
            TARGET.is_wav_requested    = SOURCE.is_wav_requested,
            TARGET.is_wav_matched      = SOURCE.is_wav_matched

        WHEN NOT MATCHED BY TARGET THEN INSERT (
            trip_id, event_date_key, event_time_key, location_key, service_provider_key,
            is_shared_requested, is_shared_matched, is_wav_requested, is_wav_matched, event_count
        )
        VALUES (
            SOURCE.trip_id, SOURCE.event_date_key, SOURCE.event_time_key, SOURCE.location_key,
            SOURCE.service_provider_key, SOURCE.is_shared_requested, SOURCE.is_shared_matched,
            SOURCE.is_wav_requested, SOURCE.is_wav_matched, SOURCE.event_count
        );

        COMMIT TRANSACTION;
        INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
        VALUES ('sp_Load_Fact_Ride_Request_Event', 'SUCCESS', @@ROWCOUNT, 'MERGE CDC completed');
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
        VALUES ('sp_Load_Fact_Ride_Request_Event', 'FAILED', 0, ERROR_MESSAGE());
        RAISERROR(ERROR_MESSAGE(), 16, 1);
    END CATCH
END;
GO

PRINT '✅ BƯỚC 6: Tất cả Stored Procedures MERGE/CDC đã sẵn sàng!';
GO

-- ============================================================
-- BƯỚC 7: SP MASTER - GỌI TẤT CẢ (Người 1 gọi SP này trong SSIS)
-- ============================================================
CREATE OR ALTER PROCEDURE sp_Master_Load_DWH
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '🚀 Bắt đầu load Data Warehouse...';

    -- Bước 1: Load Dim
    EXEC sp_Load_Dim_Time;
    EXEC sp_Load_Dim_Date;
    EXEC sp_Load_Dim_Location;

    -- Bước 2: Load Fact (MERGE/CDC)
    EXEC sp_Load_Fact_Trip_Financials;
    EXEC sp_Load_Fact_Dispatch_Lifecycle;
    EXEC sp_Load_Fact_Ride_Request_Event;

    -- Bước 3: Dọn dẹp staging
    TRUNCATE TABLE stg_clean_trips;

    INSERT INTO tbl_etl_log (step_name, status, rows_affected, message)
    VALUES ('sp_Master_Load_DWH', 'SUCCESS', 0, 'Full DWH load pipeline completed');

    PRINT '✅ Data Warehouse load hoàn tất!';
END;
GO

PRINT '✅ BƯỚC 7: Master SP đã sẵn sàng!';
PRINT '';
PRINT '============================================';
PRINT ' TỔNG KẾT - BÀN GIAO CHO NGƯỜI 1 & 2:';
PRINT '============================================';
PRINT ' • Staging Tables  : stg_raw_yellow, stg_raw_green, stg_raw_hvfhv';
PRINT ' • Clean Staging   : stg_clean_trips';
PRINT ' • Dim Tables      : dim_date, dim_time, dim_location,';
PRINT '                     dim_service_provider, dim_rate_code, dim_payment';
PRINT ' • Fact Tables     : fact_trip_financials, fact_dispatch_lifecycle,';
PRINT '                     fact_ride_request_event';
PRINT ' • Gọi từ SSIS     : EXEC sp_Master_Load_DWH';
PRINT '============================================';
GO
