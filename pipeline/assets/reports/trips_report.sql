/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Asset definition for trip counts by vendor and day
name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  

columns:
  - name: vendor_id
    type: VARCHAR
    description: "Taxi company/vendor identifier"
    primary_key: true
  - name: trip_date
    type: DATE
    description: "Date of trip pickup"
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: "Number of trips on that date for the vendor"
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    -- dimensions
    DATE_TRUNC('day', pickup_datetime)  AS pickup_date,
    vendor_id,
    taxi_type,
    payment_type_name,
    pu_location_id,
    do_location_id,

    -- trip metrics
    COUNT(*)                            AS trip_count,
    SUM(passenger_count)                AS total_passengers,
    ROUND(AVG(trip_distance), 2)        AS avg_trip_distance_miles,
    ROUND(AVG(
        EPOCH(dropoff_datetime) - EPOCH(pickup_datetime)
    ) / 60, 2)                          AS avg_trip_duration_mins,

    -- revenue metrics
    ROUND(SUM(fare_amount), 2)          AS total_fare,
    ROUND(AVG(fare_amount), 2)          AS avg_fare,
    ROUND(SUM(tip_amount), 2)           AS total_tips,
    ROUND(AVG(tip_amount), 2)           AS avg_tip,
    ROUND(SUM(total_amount), 2)         AS total_revenue,
    ROUND(AVG(total_amount), 2)         AS avg_revenue_per_trip,

    -- surcharges
    ROUND(SUM(congestion_surcharge), 2) AS total_congestion_surcharge,
    ROUND(SUM(airport_fee), 2)          AS total_airport_fees

FROM staging.trips

WHERE
    pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime <  '{{ end_datetime }}'

GROUP BY
    DATE_TRUNC('day', pickup_datetime),
    vendor_id,
    taxi_type,
    payment_type_name,
    pu_location_id,
    do_location_id
