"""
etl/01_ingest.py
----------------
Ingestion layer — simulates reading raw multi-source files into a
structured staging area, mimicking an HDFS ingestion pipeline.

In a production environment, this layer would:
  - Pull files from HDFS paths (hdfs://namenode/data/fleet/raw/)
  - Register them as external Impala tables
  - Track ingestion metadata (file size, row count, timestamp)

Here we replicate that logic using the local filesystem and SQLite,
preserving the same structure and audit trail a real pipeline would produce.
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime

RAW_DIR     = "data/raw"
STAGING_DIR = "data/staging"
DB_PATH     = "data/fleet_safety.db"

os.makedirs(STAGING_DIR, exist_ok=True)

SOURCE_FILES = {
    "trips":       "trips.csv",
    "drivers":     "drivers.csv",
    "vehicles":    "vehicles.csv",
    "environment": "environment.csv",
    "incidents":   "incidents.csv",
}

EXPECTED_SCHEMAS = {
    "trips":       ["trip_id", "driver_id", "vehicle_id", "trip_date", "time_of_day",
                    "distance_km", "duration_min", "route_risk_rating", "speed_violation"],
    "drivers":     ["driver_id", "driver_age", "experience_years", "fatigue_score",
                    "license_class", "region"],
    "vehicles":    ["vehicle_id", "vehicle_age", "maintenance_score",
                    "cargo_weight_compliance", "vehicle_type", "mileage"],
    "environment": ["trip_id", "weather_condition", "road_type",
                    "visibility_score", "road_condition"],
    "incidents":   ["incident_id", "trip_id", "incident_type", "severity",
                    "injury_reported", "damage_cost_usd"],
}


def log_ingestion(conn, source, rows, cols, status, notes=""):
    conn.execute("""
        INSERT INTO ingestion_log (source, rows_ingested, columns, status, ingested_at, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (source, rows, cols, status, datetime.now().isoformat(), notes))
    conn.commit()


def setup_log_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            source       TEXT,
            rows_ingested INTEGER,
            columns      INTEGER,
            status       TEXT,
            ingested_at  TEXT,
            notes        TEXT
        )
    """)
    conn.commit()


def ingest_source(source_name, filename, conn):
    filepath = os.path.join(RAW_DIR, filename)

    if not os.path.exists(filepath):
        print(f"  [SKIP] {filename} not found — run generate_data.py first.")
        log_ingestion(conn, source_name, 0, 0, "SKIPPED", "File not found")
        return None

    df = pd.read_csv(filepath)

    # Schema check
    expected = set(EXPECTED_SCHEMAS[source_name])
    actual   = set(df.columns)
    missing  = expected - actual
    if missing:
        note = f"Missing columns: {missing}"
        print(f"  [WARN] {source_name}: {note}")
        log_ingestion(conn, source_name, len(df), len(df.columns), "WARNING", note)
    else:
        log_ingestion(conn, source_name, len(df), len(df.columns), "SUCCESS")

    # Write to staging
    staging_path = os.path.join(STAGING_DIR, f"{source_name}_staged.parquet")
    df.to_parquet(staging_path, index=False)

    # Write to SQLite (simulates Impala external table registration)
    df.to_sql(f"stg_{source_name}", conn, if_exists="replace", index=False)

    print(f"  [OK]   {source_name:<15} {len(df):>10,} rows  |  {len(df.columns)} columns")
    return df


def run():
    print("=" * 55)
    print("FLEET SAFETY PIPELINE — LAYER 1: INGESTION")
    print("=" * 55)
    print(f"  Source directory : {RAW_DIR}")
    print(f"  Staging directory: {STAGING_DIR}")
    print(f"  Database         : {DB_PATH}")
    print()

    conn = sqlite3.connect(DB_PATH)
    setup_log_table(conn)

    ingested = {}
    for source_name, filename in SOURCE_FILES.items():
        df = ingest_source(source_name, filename, conn)
        if df is not None:
            ingested[source_name] = df

    print()
    print(f"Ingestion complete. {len(ingested)}/{len(SOURCE_FILES)} sources loaded.")
    print(f"Ingestion log written to: {DB_PATH} > ingestion_log")
    conn.close()


if __name__ == "__main__":
    run()
