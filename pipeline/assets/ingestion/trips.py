"""@bruin

name: ingestion.trips
connection: duckdb-default
type: python
image: python:3.11

materialization:
  type: table
  strategy: append



@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import json
import os
from io import BytesIO

import pandas as pd
import requests


def materialize():
    # --- Date window ---
    start = os.getenv("BRUIN_START_DATE")
    end = os.getenv("BRUIN_END_DATE")
    if not start or not end:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be provided")

    # --- Pipeline variables ---
    vars_obj = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = vars_obj.get("taxi_types", ["yellow"])

    # --- Build list of (year, month, taxi_type) combinations in the window ---
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    current = start_dt
    dfs = []

    while current < end_dt:
        year, month = current.year, current.month
        for taxi in taxi_types:
            url = (
                f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"{taxi}_tripdata_{year}-{month:02d}.parquet"
            )
            resp = requests.get(url, timeout=120)
            if resp.status_code == 404:
                print(f"No data found for {taxi} {year}-{month:02d}, skipping.")
                continue
            resp.raise_for_status()

            df = pd.read_parquet(BytesIO(resp.content))
            df["taxi_type"] = taxi  # track which type this row came from
            dfs.append(df)

        current = (current + pd.DateOffset(months=1)).replace(day=1)

    # --- Concatenate all fetched data ---
    if not dfs:
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True, sort=False)

    # --- Lineage column ---
    result["extracted_at"] = pd.Timestamp.utcnow().isoformat()

    # --- Strip tz-aware columns (PyArrow on Windows has no IANA tzdata) ---
    for col in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = result[col].astype(str)

    return result