

-- ============================================================================
-- HIVE PIPELINE: THEME PARK WAITING TIMES ANALYSIS
-- ============================================================================

-- Set up Hive configuration for optimization
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;
SET mapreduce.map.memory.mb = 4096;
SET mapreduce.reduce.memory.mb = 4096;

-- ============================================================================
-- STEP 1: CREATE EXTERNAL TABLE FOR RAW DATA
-- ============================================================================

DROP TABLE IF EXISTS waiting_times_raw;

CREATE EXTERNAL TABLE waiting_times_raw (
    work_date STRING,
    deb_time STRING,
    deb_time_hour INT,
    fin_time STRING,
    entity_description_short STRING,
    wait_time_max INT,
    nb_units DOUBLE,
    guest_carried DOUBLE,
    capacity DOUBLE,
    adjust_capacity DOUBLE,
    open_time INT,
    up_time INT,
    downtime INT,
    nb_max_unit DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://themepark-data-bdm123/data/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Verify data loaded
SELECT COUNT(*) as total_records FROM waiting_times_raw;

-- ============================================================================
-- STEP 2: CREATE CLEANED AND TRANSFORMED TABLE
-- ============================================================================

DROP TABLE IF EXISTS waiting_times_cleaned;

CREATE TABLE waiting_times_cleaned
STORED AS ORC
AS
SELECT
    work_date,
    deb_time,
    deb_time_hour,
    fin_time,
    entity_description_short,
    wait_time_max,
    nb_units,
    guest_carried,
    capacity,
    adjust_capacity,
    open_time,
    up_time,
    downtime,
    nb_max_unit,

    -- Temporal features
    YEAR(work_date) as year,
    MONTH(work_date) as month,
    DAY(work_date) as day,
    DAYOFWEEK(work_date) as day_of_week,
    QUARTER(work_date) as quarter,

    -- Weekend flag
    CASE
        WHEN DAYOFWEEK(work_date) IN (1, 7) THEN 1
        ELSE 0
    END as is_weekend,

    -- Derived metrics
    CASE
        WHEN capacity > 0 THEN (guest_carried / capacity) * 100
        ELSE 0
    END as utilization_rate,

    CASE
        WHEN open_time > 0 THEN (up_time / open_time) * 100
        ELSE 0
    END as efficiency_score,

    CASE
        WHEN open_time > 0 THEN (downtime / open_time) * 100
        ELSE 0
    END as downtime_rate

FROM waiting_times_raw
WHERE wait_time_max >= 0
  AND capacity >= 0;

-- Verify cleaned data
SELECT COUNT(*) as cleaned_records FROM waiting_times_cleaned;

-- ============================================================================
-- ANALYSIS 1: AVERAGE WAIT TIMES BY ATTRACTION
-- ============================================================================

DROP TABLE IF EXISTS hive_avg_wait_by_attraction;

CREATE TABLE hive_avg_wait_by_attraction
STORED AS ORC
AS
SELECT
    entity_description_short AS attraction,
    ROUND(AVG(wait_time_max), 2) AS avg_wait_time,
    ROUND(MAX(wait_time_max), 2) AS max_wait_time,
    COUNT(*) AS total_observations,
    ROUND(AVG(utilization_rate), 2) AS avg_utilization
FROM waiting_times_cleaned
WHERE wait_time_max > 0
GROUP BY entity_description_short
ORDER BY avg_wait_time DESC
LIMIT 10;

-- Export to CSV
INSERT OVERWRITE DIRECTORY 'gs://themepark-data-bdm123/hive-output/avg_wait_by_attraction'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 'attraction', 'avg_wait_time', 'max_wait_time', 'total_observations', 'avg_utilization'
UNION ALL
SELECT
    CAST(attraction AS STRING),
    CAST(avg_wait_time AS STRING),
    CAST(max_wait_time AS STRING),
    CAST(total_observations AS STRING),
    CAST(avg_utilization AS STRING)
FROM hive_avg_wait_by_attraction;

-- ============================================================================
-- ANALYSIS 2: WAIT TIMES BY HOUR OF DAY
-- ============================================================================

DROP TABLE IF EXISTS hive_wait_by_hour;

CREATE TABLE hive_wait_by_hour
STORED AS ORC
AS
SELECT
    deb_time_hour AS hour,
    ROUND(AVG(wait_time_max), 2) AS avg_wait_time,
    ROUND(PERCENTILE_APPROX(wait_time_max, 0.5), 2) AS median_wait_time,
    COUNT(*) AS observations
FROM waiting_times_cleaned
WHERE wait_time_max > 0
GROUP BY deb_time_hour
ORDER BY hour;

-- Export to CSV
INSERT OVERWRITE DIRECTORY 'gs://themepark-data-bdm123/hive-output/wait_by_hour'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 'hour', 'avg_wait_time', 'median_wait_time', 'observations'
UNION ALL
SELECT
    CAST(hour AS STRING),
    CAST(avg_wait_time AS STRING),
    CAST(median_wait_time AS STRING),
    CAST(observations AS STRING)
FROM hive_wait_by_hour;

-- ============================================================================
-- ANALYSIS 3: SEASONAL PATTERNS
-- ============================================================================

DROP TABLE IF EXISTS hive_seasonal_pattern;

CREATE TABLE hive_seasonal_pattern
STORED AS ORC
AS
SELECT
    year,
    quarter,
    month,
    ROUND(AVG(wait_time_max), 2) AS avg_wait_time,
    COUNT(DISTINCT work_date) AS days_analyzed,
    SUM(guest_carried) AS total_guests
FROM waiting_times_cleaned
GROUP BY year, quarter, month
ORDER BY year, month;

-- Export to CSV
INSERT OVERWRITE DIRECTORY 'gs://themepark-data-bdm123/hive-output/seasonal_pattern'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 'year', 'quarter', 'month', 'avg_wait_time', 'days_analyzed', 'total_guests'
UNION ALL
SELECT
    CAST(year AS STRING),
    CAST(quarter AS STRING),
    CAST(month AS STRING),
    CAST(avg_wait_time AS STRING),
    CAST(days_analyzed AS STRING),
    CAST(total_guests AS STRING)
FROM hive_seasonal_pattern;

-- ============================================================================
-- ANALYSIS 4: WEEKEND VS WEEKDAY
-- ============================================================================

DROP TABLE IF EXISTS hive_weekend_analysis;

CREATE TABLE hive_weekend_analysis
STORED AS ORC
AS
SELECT
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(AVG(wait_time_max), 2) AS avg_wait_time,
    ROUND(AVG(utilization_rate), 2) AS avg_utilization,
    COUNT(*) AS observations
FROM waiting_times_cleaned
WHERE wait_time_max > 0
GROUP BY is_weekend;

-- Export to CSV
INSERT OVERWRITE DIRECTORY 'gs://themepark-data-bdm123/hive-output/weekend_vs_weekday'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 'day_type', 'avg_wait_time', 'avg_utilization', 'observations'
UNION ALL
SELECT
    CAST(day_type AS STRING),
    CAST(avg_wait_time AS STRING),
    CAST(avg_utilization AS STRING),
    CAST(observations AS STRING)
FROM hive_weekend_analysis;

-- ============================================================================
-- ANALYSIS 5: OPERATIONAL EFFICIENCY
-- ============================================================================

DROP TABLE IF EXISTS hive_efficiency_analysis;

CREATE TABLE hive_efficiency_analysis
STORED AS ORC
AS
SELECT
    entity_description_short AS attraction,
    ROUND(AVG(efficiency_score), 2) AS avg_efficiency,
    ROUND(AVG(downtime_rate), 2) AS avg_downtime_rate,
    SUM(downtime) AS total_downtime_mins,
    COUNT(*) AS observations
FROM waiting_times_cleaned
WHERE open_time > 0
GROUP BY entity_description_short
ORDER BY avg_efficiency DESC
LIMIT 10;

-- Export to CSV
INSERT OVERWRITE DIRECTORY 'gs://themepark-data-bdm123/hive-output/operational_efficiency'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 'attraction', 'avg_efficiency', 'avg_downtime_rate', 'total_downtime_mins', 'observations'
UNION ALL
SELECT
    CAST(attraction AS STRING),
    CAST(avg_efficiency AS STRING),
    CAST(avg_downtime_rate AS STRING),
    CAST(total_downtime_mins AS STRING),
    CAST(observations AS STRING)
FROM hive_efficiency_analysis;

-- ============================================================================
-- ANALYSIS 6: PEAK HOURS BY ATTRACTION
-- ============================================================================

DROP TABLE IF EXISTS hive_peak_hours;

CREATE TABLE hive_peak_hours
STORED AS ORC
AS
SELECT
    entity_description_short AS attraction,
    deb_time_hour AS peak_hour,
    avg_wait_time
FROM (
    SELECT
        entity_description_short,
        deb_time_hour,
        ROUND(AVG(wait_time_max), 2) AS avg_wait_time,
        ROW_NUMBER() OVER (PARTITION BY entity_description_short ORDER BY AVG(wait_time_max) DESC) AS rn
    FROM waiting_times_cleaned
    WHERE wait_time_max > 0
    GROUP BY entity_description_short, deb_time_hour
) ranked
WHERE rn = 1
ORDER BY avg_wait_time DESC
LIMIT 10;

-- Export to CSV
INSERT OVERWRITE DIRECTORY 'gs://themepark-data-bdm123/hive-output/peak_hours_by_attraction'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 'attraction', 'peak_hour', 'avg_wait_time'
UNION ALL
SELECT
    CAST(attraction AS STRING),
    CAST(peak_hour AS STRING),
    CAST(avg_wait_time AS STRING)
FROM hive_peak_hours;

-- ============================================================================
-- PIPELINE COMPLETE
-- ============================================================================