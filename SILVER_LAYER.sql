CREATE SCHEMA VALIDATION_SCHEMA;
USE SCHEMA VALIDATION_SCHEMA;

CREATE OR REPLACE TABLE VALIDATION_SCHEMA.STATIONS_VALID AS
WITH CLEANED AS (
    SELECT
        station_id,
        -- NULL handling
        TRIM(station_name) AS station_name,
        -- Fix latitude/longitude precision (6 decimal places)
        ROUND(latitude, 6) AS latitude,
        ROUND(longitude, 6) AS longitude,
        -- Capacity check (set invalid to NULL)
        CASE 
               WHEN TRY_TO_NUMBER(capacity) >= 0 THEN TRY_TO_NUMBER(capacity)
            ELSE NULL
        END AS capacity,
        neighborhood,
        -- Standardize city_zone (upper + trim)
        UPPER(TRIM(city_zone)) AS city_zone,
        TRY_TO_DATE(install_date) AS install_date,,

        -- Standardize status values
        CASE 
            WHEN LOWER(status) IN ('active', 'in_service') THEN 'ACTIVE'
            WHEN LOWER(status) IN ('inactive', 'out_of_service') THEN 'INACTIVE'
            ELSE 'UNKNOWN'
        END AS status,
        -- For deduplication
         _load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY station_id 
            ORDER BY install_date DESC
        ) AS rn
    FROM RAW_SCHEMA.STATIONS
),
DEDUPED AS (
    SELECT *
    FROM CLEANED
    WHERE rn = 1
),

FINAL AS (
    SELECT
        station_id,
        station_name,
        latitude,
        longitude,
        capacity,
        neighborhood,
        city_zone,
        install_date,
        status,
        -- Data Quality Status
        CASE 
            WHEN station_id IS NULL THEN 'INVALID'
            WHEN station_name IS NULL THEN 'INVALID'
            WHEN latitude IS NULL OR longitude IS NULL THEN 'INVALID'
            WHEN capacity IS NULL THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,
        -- Data Quality Reason
        CASE 
            WHEN station_id IS NULL THEN 'MISSING_ID'
            WHEN station_name IS NULL THEN 'MISSING_NAME'
            WHEN latitude IS NULL OR longitude IS NULL THEN 'INVALID_GEO'
            WHEN capacity IS NULL THEN 'INVALID_CAPACITY'
            ELSE 'OK'
        END AS dq_reason,
        CURRENT_TIMESTAMP() AS validated_at
    FROM DEDUPED
)

SELECT * FROM FINAL;

CREATE OR REPLACE TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.BIKES_VALID AS

WITH CLEANED AS (
    SELECT
        bike_id,
        -- Standardize bike_type
        CASE 
            WHEN LOWER(TRIM(bike_type)) IN ('classic', 'regular') THEN 'CLASSIC'
            WHEN LOWER(TRIM(bike_type)) IN ('ebike', 'electric') THEN 'EBIKE'
            ELSE 'UNKNOWN'
        END AS bike_type,
        -- Standardize status
        CASE 
            WHEN LOWER(status) IN ('available', 'active', 'in_service') THEN 'ACTIVE'
            WHEN LOWER(status) IN ('inactive', 'out_of_service', 'repair') THEN 'INACTIVE'
            ELSE 'UNKNOWN'
        END AS status,
        TRY_TO_DATE(purchase_date) AS purchase_date,
        TRY_TO_DATE(last_service_date) AS last_service_date,
        -- Odometer validation
        CASE 
            WHEN odometer_km >= 0 THEN odometer_km
            ELSE NULL
        END AS odometer_km,
        -- Battery validation (only meaningful for ebikes)
        CASE 
            WHEN battery_level BETWEEN 0 AND 100 THEN battery_level
            ELSE NULL
        END AS battery_level,
        firmware_version,
        _load_timestamp,
        -- Deduplication logic
        ROW_NUMBER() OVER (
            PARTITION BY bike_id
            ORDER BY last_service_date DESC NULLS LAST
        ) AS rn
    FROM HACKATHON_BIKE_RIDES.RAW_SCHEMA.BIKES
),
DEDUPED AS (
    SELECT *
    FROM CLEANED
    WHERE rn = 1
),
FINAL AS (
    SELECT
        bike_id,
        bike_type,
        status,
        purchase_date,
        last_service_date,
        odometer_km,
        battery_level,
        firmware_version,
        CASE 
            WHEN bike_id IS NULL THEN 'INVALID'
            WHEN bike_type = 'UNKNOWN' THEN 'INVALID'
            WHEN odometer_km IS NULL THEN 'INVALID'
            WHEN bike_type = 'EBIKE' AND battery_level IS NULL THEN 'INVALID'
            WHEN last_service_date < purchase_date THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,
        CASE 
            WHEN bike_id IS NULL THEN 'MISSING_ID'
            WHEN bike_type = 'UNKNOWN' THEN 'INVALID_TYPE'
            WHEN odometer_km IS NULL THEN 'INVALID_ODOMETER'
            WHEN bike_type = 'EBIKE' AND battery_level IS NULL THEN 'INVALID_BATTERY'
            WHEN last_service_date < purchase_date THEN 'INVALID_SERVICE_DATE'
            ELSE 'OK'
        END AS dq_reason,
        CASE 
            WHEN bike_type = 'EBIKE' AND battery_level < 15 THEN 1
            ELSE 0
        END AS low_battery_flag,
        -- Very high odometer (possible overuse)
        CASE 
            WHEN odometer_km > 50000 THEN 1
            ELSE 0
        END AS high_usage_flag,

        -- Service overdue (more than 6 months)
        CASE 
            WHEN last_service_date IS NOT NULL 
                 AND DATEDIFF('day', last_service_date, CURRENT_DATE()) > 180
            THEN 1
            ELSE 0
        END AS service_overdue_flag,
        -- Invalid firmware anomaly
        CASE 
            WHEN firmware_version IS NULL OR TRIM(firmware_version) = '' THEN 1
            ELSE 0
        END AS firmware_issue_flag,
        CURRENT_TIMESTAMP() AS validated_at
    FROM DEDUPED
)
SELECT * FROM BIKES_VALID;

CREATE OR REPLACE TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.USERS_VALID AS
WITH CLEANED AS (
    SELECT
        user_id,
        TRIM(customer_name) AS customer_name,
        dob,
        -- Standardize gender
        CASE 
            WHEN LOWER(gender) IN ('male', 'm') THEN 'MALE'
            WHEN LOWER(gender) IN ('female', 'f') THEN 'FEMALE'
            ELSE 'OTHER'
        END AS gender,
        -- Email validation
        CASE 
            WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
            THEN LOWER(TRIM(email))
            ELSE NULL
        END AS email,
        -- Phone validation (10 digits basic check)
        CASE 
    WHEN phone IS NOT NULL THEN
        RIGHT(
            TO_VARCHAR(TRY_TO_NUMBER(phone)),
            10
        )
    ELSE NULL
END AS phone,
        address,
        city,
        state,
        region,
        -- Normalize KYC status
        CASE 
            WHEN LOWER(kyc_status) IN ('verified', 'approved') THEN 'VERIFIED'
            WHEN LOWER(kyc_status) IN ('pending') THEN 'PENDING'
            WHEN LOWER(kyc_status) IN ('rejected', 'failed') THEN 'REJECTED'
            ELSE 'UNKNOWN'
        END AS kyc_status,
        registration_date,
        is_student,
        corporate_id,
        -- Deduplication
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY registration_date DESC NULLS LAST
        ) AS rn
    FROM HACKATHON_BIKE_RIDES.RAW_SCHEMA.USERS_TABLE
),
DEDUPED AS (
    SELECT *
    FROM CLEANED
    WHERE rn = 1
),
FINAL AS (
    SELECT
        user_id,
        customer_name,
        dob,
        gender,
        email,
        phone,
        address,
        city,
        state,
        region,
        kyc_status,
        registration_date,
        is_student,
        corporate_id,
        DATEDIFF('year', dob, CURRENT_DATE()) AS age,
        CASE 
            WHEN user_id IS NULL THEN 'INVALID'
            WHEN customer_name IS NULL THEN 'INVALID'
            WHEN dob IS NULL THEN 'INVALID'
            WHEN DATEDIFF('year', dob, CURRENT_DATE()) <= 10 THEN 'INVALID'
            WHEN email IS NULL THEN 'INVALID'
            WHEN phone IS NULL THEN 'INVALID'
            WHEN kyc_status = 'UNKNOWN' THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,
        CASE 
            WHEN user_id IS NULL THEN 'MISSING_ID'
            WHEN customer_name IS NULL THEN 'MISSING_NAME'
            WHEN dob IS NULL THEN 'MISSING_DOB'
            WHEN DATEDIFF('year', dob, CURRENT_DATE()) <= 10 THEN 'INVALID_AGE'
            WHEN email IS NULL THEN 'INVALID_EMAIL'
            WHEN phone IS NULL THEN 'INVALID_PHONE'
            WHEN kyc_status = 'UNKNOWN' THEN 'INVALID_KYC'
            ELSE 'OK'
        END AS dq_reason,
        CASE 
            WHEN corporate_id IS NOT NULL THEN 1 ELSE 0
        END AS corporate_user_flag,
        CASE 
            WHEN is_student = TRUE THEN 1 ELSE 0
        END AS student_flag,
        CURRENT_TIMESTAMP() AS validated_at
    FROM DEDUPED
)
SELECT * FROM FINAL;
CREATE OR REPLACE TABLE RENTALS_VALID AS
WITH CLEANED AS (
    SELECT
        rental_id,
        user_id,
        bike_id,
        start_station_id,
        end_station_id,

        TRY_TO_TIMESTAMP(start_time) AS start_time,
        TRY_TO_TIMESTAMP(end_time) AS end_time,

        TRY_TO_NUMBER(duration_sec) AS duration_sec,
        TRY_TO_DOUBLE(distance_km) AS distance_km,
        TRY_TO_DOUBLE(price) AS price,

        channel,
        start_gps,
        end_gps,

        _load_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY rental_id
            ORDER BY _load_timestamp DESC
        ) AS rn

    FROM HACKATHON_BIKE_RIDES.RAW_SCHEMA.RAW_RENTALS
),

DEDUPED AS (
    SELECT * FROM CLEANED WHERE rn = 1
),

ENRICHED AS (
    SELECT r.*
    FROM DEDUPED r

    INNER JOIN USERS_VALID u ON r.user_id = u.user_id AND u.dq_status = 'VALID'
    INNER JOIN BIKES_VALID b ON r.bike_id = b.bike_id AND b.dq_status = 'VALID'
    INNER JOIN STATIONS_VALID s ON r.start_station_id = s.station_id AND s.dq_status = 'VALID'
),

FINAL AS (
    SELECT *,
        CASE 
            WHEN duration_sec > 0 
            THEN distance_km / (duration_sec / 3600.0)
        END AS speed_kmh,

        CASE WHEN duration_sec < 60 THEN 1 ELSE 0 END AS ultra_short_trip_flag,

        CASE 
            WHEN duration_sec > 0 
            AND (distance_km / (duration_sec / 3600.0)) > 60
            THEN 1 ELSE 0
        END AS unrealistic_speed_flag,

        CASE 
            WHEN end_time < start_time THEN 'INVALID'
            WHEN duration_sec < 0 OR distance_km < 0 THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,

        CURRENT_TIMESTAMP() AS validated_at
    FROM ENRICHED
)

SELECT * FROM FINAL;




