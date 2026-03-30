CREATE SCHEMA VALIDATION_SCHEMA;
USE SCHEMA VALIDATION_SCHEMA;

CREATE OR REPLACE TABLE VALIDATION_SCHEMA.STATIONS_VALID AS
WITH CLEANED AS (
    SELECT
        station_id ,

        -- NULL handling
        TRIM(station_name) AS station_name,

        -- Fix latitude/longitude precision (6 decimal places)
        ROUND(latitude, 6) AS latitude ,
        ROUND(longitude, 6) AS longitude ,

        -- Capacity check (set invalid to NULL)
        CASE 
            WHEN capacity >= 0 THEN capacity
            ELSE NULL
        END AS capacity,

        neighborhood,

        -- Standardize city_zone (upper + trim)
        UPPER(TRIM(city_zone)) AS city_zone,

        install_date,

        -- Standardize status values
        CASE 
            WHEN LOWER(status) IN ('active', 'in_service') THEN 'ACTIVE'
            WHEN LOWER(status) IN ('inactive', 'out_of_service') THEN 'INACTIVE'
            ELSE 'UNKNOWN'
        END AS status,

        -- For deduplication
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



SELECT * FROM STATIONS_VALID;

DESC TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.STATIONS_VALID;


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

        purchase_date,
        last_service_date,

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

        -- =========================
        -- DATA QUALITY STATUS
        -- =========================
        CASE 
            WHEN bike_id IS NULL THEN 'INVALID'
            WHEN bike_type = 'UNKNOWN' THEN 'INVALID'
            WHEN odometer_km IS NULL THEN 'INVALID'
            WHEN bike_type = 'EBIKE' AND battery_level IS NULL THEN 'INVALID'
            WHEN last_service_date < purchase_date THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,

        -- =========================
        -- DQ REASON
        -- =========================
        CASE 
            WHEN bike_id IS NULL THEN 'MISSING_ID'
            WHEN bike_type = 'UNKNOWN' THEN 'INVALID_TYPE'
            WHEN odometer_km IS NULL THEN 'INVALID_ODOMETER'
            WHEN bike_type = 'EBIKE' AND battery_level IS NULL THEN 'INVALID_BATTERY'
            WHEN last_service_date < purchase_date THEN 'INVALID_SERVICE_DATE'
            ELSE 'OK'
        END AS dq_reason,

        -- =========================
        -- ANOMALY FLAGS 
        -- =========================
        
        -- Low battery anomaly
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

SELECT * FROM FINAL;

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

        -- =========================
        -- AGE VALIDATION
        -- =========================
        DATEDIFF('year', dob, CURRENT_DATE()) AS age,

        -- =========================
        -- DATA QUALITY STATUS
        -- =========================
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

        -- =========================
        -- DQ REASON
        -- =========================
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

        -- =========================
        -- OPTIONAL FLAGS ( useful later)
        -- =========================
        
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

-----DROP TABLE USERS_VALID;

SELECT * FROM USERS_VALID;





CREATE OR REPLACE TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID AS

WITH CLEANED AS (
    SELECT
        rental_id,
        user_id,
        bike_id,
        start_station_id,
        end_station_id,

        -- =========================
        -- TIMESTAMP → UTC STANDARDIZATION
        -- =========================
        CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP(start_time)) AS start_time_utc,
        CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP(end_time)) AS end_time_utc,

        -- NUMERIC STANDARDIZATION
        TRY_TO_NUMBER(duration_sec) AS duration_sec,
        TRY_TO_NUMBER(distance_km) AS distance_km,
        TRY_TO_NUMBER(price) AS price,

        channel,

        -- DEDUPLICATION
        ROW_NUMBER() OVER (
            PARTITION BY rental_id
            ORDER BY TRY_TO_TIMESTAMP(start_time) DESC NULLS LAST
        ) AS rn

    FROM HACKATHON_BIKE_RIDES.RAW_SCHEMA.RENTALS
),

DEDUPED AS (
    SELECT * FROM CLEANED WHERE rn = 1
),

-- =========================
-- STRICT REFERENTIAL INTEGRITY (FACT TABLE RULE)
-- =========================
ENRICHED AS (
    SELECT
        r.*,
        b.bike_type
    FROM DEDUPED r

    INNER JOIN HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.USERS_VALID u
        ON r.user_id = u.user_id AND u.dq_status = 'VALID'

    INNER JOIN HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.BIKES_VALID b
        ON r.bike_id = b.bike_id AND b.dq_status = 'VALID'

    INNER JOIN HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.STATIONS_VALID s1
        ON r.start_station_id = s1.station_id AND s1.dq_status = 'VALID'

    INNER JOIN HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.STATIONS_VALID s2
        ON r.end_station_id = s2.station_id AND s2.dq_status = 'VALID'
),

FINAL AS (
    SELECT
        rental_id,
        user_id,
        bike_id,
        start_station_id,
        end_station_id,

        start_time_utc AS start_time,
        end_time_utc AS end_time,

        duration_sec,
        distance_km,
        price,
        channel,

        -- =========================
        -- DERIVED METRIC: SPEED
        -- =========================
        CASE 
            WHEN duration_sec > 0 
            THEN distance_km / (duration_sec / 3600.0)
        END AS speed_kmh,

        -- =========================
        -- ANOMALY FLAGS (CORE KPI)
        -- =========================

        -- Ultra short trip
        CASE WHEN duration_sec < 60 THEN 1 ELSE 0 END 
        AS ultra_short_trip_flag,

        -- Unrealistic speed
        CASE 
            WHEN duration_sec > 0 
                 AND (distance_km / (duration_sec / 3600.0)) > 60
            THEN 1 ELSE 0
        END AS unrealistic_speed_flag,

        -- =========================
        -- DATA QUALITY STATUS
        -- =========================
        CASE 
            WHEN end_time_utc < start_time_utc THEN 'INVALID'
            WHEN duration_sec IS NULL OR duration_sec < 0 THEN 'INVALID'
            WHEN distance_km IS NULL OR distance_km < 0 THEN 'INVALID'
            WHEN price IS NULL OR price < 0 THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,

        -- =========================
        -- DQ REASON
        -- =========================
        CASE 
            WHEN end_time_utc < start_time_utc THEN 'INVALID_TIME'
            WHEN duration_sec < 0 THEN 'INVALID_DURATION'
            WHEN distance_km < 0 THEN 'INVALID_DISTANCE'
            WHEN price < 0 THEN 'INVALID_PRICE'
            ELSE 'OK'
        END AS dq_reason,

        CURRENT_TIMESTAMP() AS validated_at

    FROM ENRICHED
)

SELECT * FROM FINAL;


SELECT * FROM HACKATHON_BIKE_RIDES.RAW_SCHEMA.RENTALS;


ALTER TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.USERS_VALID
ADD CONSTRAINT PK_USERS PRIMARY KEY (user_id);

ALTER TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.BIKES_VALID
ADD CONSTRAINT PK_BIKES PRIMARY KEY (bike_id);

ALTER TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.STATIONS_VALID
ADD CONSTRAINT PK_STATIONS PRIMARY KEY (station_id);

ALTER TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID
ADD CONSTRAINT FK_RENTALS_USERS
FOREIGN KEY (user_id)
REFERENCES HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.USERS_VALID(user_id);

ALTER TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID
ADD CONSTRAINT FK_RENTALS_BIKES
FOREIGN KEY (bike_id)
REFERENCES HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.BIKES_VALID(bike_id);

ALTER TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID
ADD CONSTRAINT FK_RENTALS_START_STATION
FOREIGN KEY (start_station_id)
REFERENCES HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.STATIONS_VALID(station_id);

ALTER TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID
ADD CONSTRAINT FK_RENTALS_END_STATION
FOREIGN KEY (end_station_id)
REFERENCES HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.STATIONS_VALID(station_id);

create or replace table rentals_valid clone raw_schema.rentals;

select * from rentals_valid;






CREATE OR REPLACE PROCEDURE apply_anomaly_rules()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- Ultra-short trips
UPDATE rentals_final
SET is_flagged = TRUE
WHERE duration_sec < 60;

-- Unrealistic speed
UPDATE rentals_final
SET is_flagged = TRUE
WHERE distance_km / NULLIF(duration_sec/3600,0) > 80;

-- GPS mismatch
UPDATE rentals_final r
SET is_flagged = TRUE
FROM stations s
WHERE r.start_station_id = s.station_id
AND ABS(r.start_lat - s.latitude) > 0.01;

RETURN 'Anomaly rules applied';

END;
$$;


MERGE INTO rentals_final t
USING rentals_valid s
ON t.rental_id = s.rental_id

WHEN MATCHED THEN UPDATE SET
    t.price = s.price,
    t.duration_sec = s.duration_sec,
    t.ingestion_ts = s.ingestion_ts

WHEN NOT MATCHED THEN
INSERT (
    rental_id, user_id, bike_id, start_station_id, end_station_id,
    start_time, end_time, duration_sec, distance_km, price, channel, ingestion_ts
)
VALUES (
    s.rental_id, s.user_id, s.bike_id, s.start_station_id, s.end_station_id,
    s.start_time, s.end_time, s.duration_sec, s.distance_km, s.price, s.channel, s.ingestion_ts
);


MERGE INTO dim_users t
USING users_valid s
ON t.user_id = s.user_id AND t.is_current = TRUE

WHEN MATCHED AND (
    t.email <> s.email OR t.city <> s.city
)
THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date = CURRENT_DATE

WHEN NOT MATCHED THEN
INSERT (
    user_id, email, city, start_date, end_date, is_current
)
VALUES (
    s.user_id, s.email, s.city, CURRENT_DATE, NULL, TRUE
);


CREATE OR REPLACE TASK rentals_task
WAREHOUSE = compute_wh
SCHEDULE = '5 MINUTE'
AS

MERGE INTO rentals_final t
USING rentals_stream s
ON t.rental_id = s.rental_id

WHEN MATCHED THEN UPDATE SET
    t.price = s.price,
    t.duration_sec = s.duration_sec

WHEN NOT MATCHED THEN
INSERT VALUES (
    s.rental_id, s.user_id, s.bike_id, s.start_station_id, s.end_station_id,
    s.start_time, s.end_time, s.duration_sec, s.distance_km, s.price, s.channel, s.ingestion_ts
);



CREATE OR REPLACE MASKING POLICY mask_email
AS (val STRING) RETURNS STRING ->
CASE 
    WHEN CURRENT_ROLE() IN ('ADMIN') THEN val
    ELSE '***MASKED***'
END;
--APPLY MASKING:
ALTER TABLE users_valid MODIFY COLUMN email
SET MASKING POLICY mask_email;


CREATE OR REPLACE ROW ACCESS POLICY region_policy
AS (region STRING) RETURNS BOOLEAN ->
CURRENT_ROLE() = region;
--APPLY POLICY:
ALTER TABLE users_valid
ADD ROW ACCESS POLICY region_policy ON (region);


CREATE OR REPLACE TABLE audit_log (
    table_name STRING,
    load_time TIMESTAMP,
    row_count INT,
    status STRING
);
-- INSERT AUDIT RECORD:
INSERT INTO audit_log
SELECT
    'rentals_final',
    CURRENT_TIMESTAMP(),
    COUNT(*),
    'SUCCESS'
FROM rentals_valid;
--COPY INTO AUDIT RECORD:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'raw_schema.RENTALS',
    START_TIME=>DATEADD('HOUR',-1,CURRENT_TIMESTAMP())
));


-- =========================
-- KPI 1: Anomalous Rental Probability Score
-- =========================
CREATE OR REPLACE VIEW HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_ANOMALOUS_RENTAL_SCORE AS
SELECT
    COUNT(CASE WHEN IS_FLAGGED = 'True' THEN 1 END) AS high_risk_count,
    COUNT(*) AS total_rentals,
    ROUND(
        (COUNT(CASE WHEN IS_FLAGGED = 'True' THEN 1 END) / NULLIF(COUNT(*), 0)) * 100,
        2
    ) AS anomalous_rental_probability_score
FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID;


-- =========================
-- KPI 2: Station Availability Score
-- =========================
CREATE OR REPLACE VIEW HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_STATION_AVAILABILITY AS
WITH station_usage AS (
    SELECT
        s.STATION_ID,
        s.CAPACITY,
        COUNT(DISTINCT r_start.RENTAL_ID) AS bikes_currently_out,
        COUNT(DISTINCT r_end.RENTAL_ID) AS bikes_returned
    FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.STATIONS_VALID s
    LEFT JOIN HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID r_start
        ON s.STATION_ID = r_start.START_STATION_ID
    LEFT JOIN HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID r_end
        ON s.STATION_ID = r_end.END_STATION_ID
    WHERE s.DQ_STATUS = 'VALID'
    GROUP BY s.STATION_ID, s.CAPACITY
),
availability AS (
    SELECT
        STATION_ID,
        TRY_TO_NUMBER(CAPACITY) AS capacity,
        GREATEST(bikes_returned - bikes_currently_out, 0) AS est_bikes_available,
        CASE
            WHEN TRY_TO_NUMBER(CAPACITY) > 0
                 AND GREATEST(bikes_returned - bikes_currently_out, 0) >= 1
                 AND (TRY_TO_NUMBER(CAPACITY) - GREATEST(bikes_returned - bikes_currently_out, 0)) >= 1
            THEN 1 ELSE 0
        END AS is_available
    FROM station_usage
)
SELECT
    COUNT(CASE WHEN is_available = 1 THEN 1 END) AS available_stations,
    COUNT(*) AS total_stations,
    ROUND(
        (COUNT(CASE WHEN is_available = 1 THEN 1 END) / NULLIF(COUNT(*), 0)) * 100,
        2
    ) AS station_availability_score
FROM availability;


-- =========================
-- KPI 3: Active Rider Engagement Ratio
-- =========================
CREATE OR REPLACE VIEW HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_ACTIVE_RIDER_ENGAGEMENT AS
WITH active_riders AS (
    SELECT COUNT(DISTINCT r.USER_ID) AS active_users
    FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID r
    WHERE TRY_TO_TIMESTAMP(r.START_TIME) >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
),
total_riders AS (
    SELECT COUNT(*) AS registered_users
    FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.USERS_VALID
    WHERE DQ_STATUS = 'VALID'
)
SELECT
    a.active_users,
    t.registered_users,
    ROUND(
        (a.active_users / NULLIF(t.registered_users, 0)) * 100,
        2
    ) AS active_rider_engagement_ratio
FROM active_riders a, total_riders t;


-- =========================
-- KPI 4: Fleet Maintenance Health Index
-- =========================
CREATE OR REPLACE VIEW HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_FLEET_HEALTH AS
WITH bike_health AS (
    SELECT
        BIKE_ID,
        BIKE_TYPE,
        STATUS,
        TRY_TO_NUMBER(BATTERY_LEVEL) AS battery_level,
        TRY_TO_NUMBER(ODOMETER_KM) AS odometer_km,
        DATEDIFF('DAY', TRY_TO_DATE(LAST_SERVICE_DATE), CURRENT_DATE()) AS days_since_service,
        CASE
            WHEN STATUS NOT IN ('ACTIVE', 'UNKNOWN') THEN 0
            WHEN BIKE_TYPE = 'EBIKE' AND TRY_TO_NUMBER(BATTERY_LEVEL) < 25 THEN 0
            WHEN DATEDIFF('DAY', TRY_TO_DATE(LAST_SERVICE_DATE), CURRENT_DATE()) > 180 THEN 0
            WHEN TRY_TO_NUMBER(ODOMETER_KM) > 8000 THEN 0
            ELSE 1
        END AS is_healthy
    FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.BIKES_VALID
    WHERE DQ_STATUS = 'VALID'
)
SELECT
    COUNT(CASE WHEN is_healthy = 1 THEN 1 END) AS healthy_bikes,
    COUNT(*) AS total_bikes,
    ROUND(
        (COUNT(CASE WHEN is_healthy = 1 THEN 1 END) / NULLIF(COUNT(*), 0)) * 100,
        2
    ) AS fleet_maintenance_health_index
FROM bike_health;


-- =========================
-- KPI 5: Average Rental Revenue (ARR) by Channel
-- =========================
CREATE OR REPLACE VIEW HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_AVG_REVENUE_BY_CHANNEL AS
SELECT
    CHANNEL,
    COUNT(*) AS total_rentals,
    ROUND(AVG(TRY_TO_DOUBLE(PRICE)), 2) AS avg_rental_revenue
FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.RENTALS_VALID
GROUP BY CHANNEL;


-- =========================
-- RUN ALL KPI VIEWS
-- =========================
SELECT * FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_ANOMALOUS_RENTAL_SCORE;
SELECT * FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_STATION_AVAILABILITY;
SELECT * FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_ACTIVE_RIDER_ENGAGEMENT;
SELECT * FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_FLEET_HEALTH;
SELECT * FROM HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.KPI_AVG_REVENUE_BY_CHANNEL;

SELECT * FROM BIKES_VALID;

SELECT * FROM RENTALS_VALID BEFORE(STATEMENT=>'01c35f7a-3202-853d-0015-e38e00047eb6')
