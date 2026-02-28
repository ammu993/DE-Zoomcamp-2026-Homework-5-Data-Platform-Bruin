/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table


# TODO: Define output columns, mark primary keys, and add a few checks.

  

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Ensure that the staging table has at least one row
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM staging.trips
    value: 1

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

SELECT
    ROW_NUMBER() OVER (
        PARTITION BY vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id
        ORDER BY extracted_at DESC
    ) AS _row_num,

    CAST(tpep_pickup_datetime AS TIMESTAMP)  AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    vendor_id,
    taxi_type,

    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,

    t.payment_type,
    p.payment_type_name,

    CAST(extracted_at AS TIMESTAMP) AS extracted_at

FROM ingestion.trips t

LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id

WHERE
    vendor_id IS NOT NULL
    AND trip_distance > 0
    AND fare_amount > 0
    AND passenger_count > 0
    AND pu_location_id IS NOT NULL
    AND do_location_id IS NOT NULL
    AND CAST(tpep_pickup_datetime AS TIMESTAMP) >= '{{ start_datetime }}'
    AND CAST(tpep_pickup_datetime AS TIMESTAMP) <  '{{ end_datetime }}'

QUALIFY _row_num = 1
