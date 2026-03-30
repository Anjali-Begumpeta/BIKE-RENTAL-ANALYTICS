--KPI1
CREATE OR REPLACE VIEW KPI_ANOMALOUS_RENTAL_SCORE AS
SELECT
    COUNT(CASE 
        WHEN ultra_short_trip_flag = 1 OR unrealistic_speed_flag = 1 THEN 1 
    END) AS anomalies,
    COUNT(*) AS total,
    ROUND((COUNT(*) * 100.0) / NULLIF(COUNT(*),0),2) AS anomaly_percentage
FROM RENTALS_VALID;
--KP2
CREATE OR REPLACE VIEW KPI_AVG_REVENUE AS
SELECT
    channel,
    COUNT(*) AS total_rentals,
    ROUND(AVG(price),2) AS avg_revenue
FROM RENTALS_VALID
GROUP BY channel;

CREATE OR REPLACE VIEW CURATED_SCHEMA.KPI_ACTIVE_RIDER_ENGAGEMENT AS
--KP3
WITH active_users AS (
    SELECT COUNT(DISTINCT user_id) AS active_count
    FROM CURATED_SCHEMA.RENTALS_FINAL
    WHERE start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
),

total_users AS (
    SELECT COUNT(DISTINCT user_id) AS total_count
    FROM VALIDATION_SCHEMA.USERS_VALID
    WHERE dq_status = 'VALID'
)

SELECT
    a.active_count,
    t.total_count,
    ROUND(
        (a.active_count * 100.0) / NULLIF(t.total_count, 0),
        2
    ) AS active_rider_engagement_ratio
FROM active_users a, total_users t;
--KPI4
CREATE OR REPLACE VIEW CURATED_SCHEMA.KPI_FLEET_HEALTH AS

WITH bike_health AS (
    SELECT
        bike_id,

        CASE
            WHEN status <> 'ACTIVE' THEN 0
            WHEN battery_level IS NOT NULL AND battery_level < 25 THEN 0
            WHEN last_service_date IS NOT NULL 
                 AND DATEDIFF(DAY, last_service_date, CURRENT_DATE()) > 180 THEN 0
            WHEN odometer_km > 8000 THEN 0
            ELSE 1
        END AS is_healthy

    FROM VALIDATION_SCHEMA.BIKES_VALID
    WHERE dq_status = 'VALID'
)

SELECT
    COUNT(CASE WHEN is_healthy = 1 THEN 1 END) AS healthy_bikes,
    COUNT(*) AS total_bikes,
    ROUND(
        (COUNT(CASE WHEN is_healthy = 1 THEN 1 END) * 100.0) / NULLIF(COUNT(*), 0),
        2
    ) AS fleet_health_index
FROM bike_health;
--KPI 5
CREATE OR REPLACE VIEW CURATED_SCHEMA.KPI_AVG_REVENUE_BY_CHANNEL AS

SELECT
    channel,
    COUNT(*) AS total_rentals,
    ROUND(AVG(price), 2) AS avg_rental_revenue,
    ROUND(SUM(price), 2) AS total_revenue
FROM CURATED_SCHEMA.RENTALS_FINAL
WHERE dq_status = 'VALID'
GROUP BY channel;
