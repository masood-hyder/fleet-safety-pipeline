"""
etl/03_transform.py
-------------------
Transformation layer — joins all staged sources into a unified analytical
dataset, engineers the 12 risk factor features, and writes query-optimized
partitioned tables ready for analytics consumption.

In a production HDFS/Impala pipeline, this layer would:
  - Execute JOIN queries directly in Impala for in-cluster performance
  - Write output to HDFS partitioned by year/month (Parquet format)
  - Register the final table as an Impala analytic table

Here we replicate that logic using pandas and SQLite, preserving the same
partitioning structure and output schema.
"""

import os
import sqlite3
import pandas as pd

DB_PATH    = "data/fleet_safety.db"
OUTPUT_DIR = "data/analytical"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_staged(conn):
    print("  Loading staged tables...")
    trips       = pd.read_sql("SELECT * FROM stg_trips",       conn)
    drivers     = pd.read_sql("SELECT * FROM stg_drivers",     conn)
    vehicles    = pd.read_sql("SELECT * FROM stg_vehicles",    conn)
    environment = pd.read_sql("SELECT * FROM stg_environment", conn)
    incidents   = pd.read_sql("SELECT * FROM stg_incidents",   conn)

    print(f"    trips:       {len(trips):>10,} rows")
    print(f"    drivers:     {len(drivers):>10,} rows")
    print(f"    vehicles:    {len(vehicles):>10,} rows")
    print(f"    environment: {len(environment):>10,} rows")
    print(f"    incidents:   {len(incidents):>10,} rows")

    return trips, drivers, vehicles, environment, incidents


def join_sources(trips, drivers, vehicles, environment, incidents):
    print("\n  Joining sources...")

    df = (
        trips
        .merge(drivers,     on="driver_id",  how="left")
        .merge(vehicles,    on="vehicle_id", how="left")
        .merge(environment, on="trip_id",    how="left")
        .merge(
            incidents[["trip_id", "incident_type", "severity", "injury_reported", "damage_cost_usd"]],
            on="trip_id", how="left"
        )
    )

    df["had_incident"] = df["incident_type"].notna().astype(int)
    print(f"    Unified dataset: {len(df):,} rows  |  {len(df.columns)} columns")
    return df


def engineer_risk_features(df):
    """
    Engineers the 12 risk factor columns used in the dashboard analysis.
    Each column is normalized to a 0-10 scale where higher = higher risk.
    """
    print("\n  Engineering 12 risk features...")

    # 1. Driver age risk (very young and older drivers carry higher risk)
    df["risk_driver_age"] = df["driver_age"].apply(
        lambda x: 8 if x < 25 else (6 if x > 55 else 3)
    )

    # 2. Experience risk (less experience = higher risk)
    df["risk_experience"] = (10 - df["experience_years"].clip(0, 10)).round(2)

    # 3. Fatigue risk (direct score, already 0-10)
    df["risk_fatigue"] = df["fatigue_score"]

    # 4. Vehicle age risk
    df["risk_vehicle_age"] = (df["vehicle_age"] / 20 * 10).clip(0, 10).round(2)

    # 5. Maintenance risk (inverse of maintenance score)
    df["risk_maintenance"] = (10 - df["maintenance_score"]).round(2)

    # 6. Cargo compliance risk
    df["risk_cargo"] = df["cargo_weight_compliance"].apply(lambda x: 0 if x else 8)

    # 7. Weather risk
    weather_risk = {"Clear": 1, "Windy": 3, "Rainy": 6, "Foggy": 7, "Snowy": 8, "Stormy": 10}
    df["risk_weather"] = df["weather_condition"].map(weather_risk).fillna(5)

    # 8. Road type risk
    road_risk = {"Highway": 4, "Urban": 5, "Rural": 6, "Coastal": 7, "Mountain": 9}
    df["risk_road_type"] = df["road_type"].map(road_risk).fillna(5)

    # 9. Visibility risk (inverse of visibility score)
    df["risk_visibility"] = (10 - df["visibility_score"]).round(2)

    # 10. Speed violation risk
    df["risk_speed"] = df["speed_violation"].apply(lambda x: 9 if x else 1)

    # 11. Route risk (direct score, already 0-10)
    df["risk_route"] = df["route_risk_rating"]

    # 12. Time of day risk
    time_risk = {"Morning": 3, "Afternoon": 2, "Evening": 6, "Night": 9}
    df["risk_time_of_day"] = df["time_of_day"].map(time_risk).fillna(5)

    risk_cols = [c for c in df.columns if c.startswith("risk_")]
    print(f"    Risk features created: {risk_cols}")
    return df


def create_partitions(df, conn):
    """
    Simulates HDFS partitioning by year and month.
    In production: hdfs://namenode/warehouse/fleet_safety/year=2023/month=01/
    """
    print("\n  Writing partitioned tables...")

    df["trip_date"] = pd.to_datetime(df["trip_date"])
    df["year"]      = df["trip_date"].dt.year
    df["month"]     = df["trip_date"].dt.month

    for (year, month), partition in df.groupby(["year", "month"]):
        path = os.path.join(OUTPUT_DIR, f"year={year}", f"month={str(month).zfill(2)}")
        os.makedirs(path, exist_ok=True)
        partition.to_parquet(os.path.join(path, "data.parquet"), index=False)

    partitions = df.groupby(["year", "month"]).size().reset_index(name="row_count")
    print(f"    Partitions written: {len(partitions)}")
    print(f"    Output directory  : {OUTPUT_DIR}/")


def write_analytical_table(df, conn):
    print("\n  Writing analytical table to database...")
    df.to_sql("fleet_risk_analytical", conn, if_exists="replace", index=False)

    # Also write a summary risk table (the one Impala queries would serve)
    risk_cols  = [c for c in df.columns if c.startswith("risk_")]
    risk_summary = df[risk_cols + ["had_incident"]].copy()
    risk_summary.to_sql("risk_factor_summary", conn, if_exists="replace", index=False)
    print(f"    fleet_risk_analytical : {len(df):,} rows")
    print(f"    risk_factor_summary   : {len(risk_summary):,} rows  |  {len(risk_cols)} risk features")


def run():
    print("=" * 55)
    print("FLEET SAFETY PIPELINE — LAYER 3: TRANSFORMATION")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)

    trips, drivers, vehicles, environment, incidents = load_staged(conn)
    df = join_sources(trips, drivers, vehicles, environment, incidents)
    df = engineer_risk_features(df)
    create_partitions(df, conn)
    write_analytical_table(df, conn)

    print(f"\nTransformation complete.")
    print(f"  Analytical table ready in : {DB_PATH} > fleet_risk_analytical")
    print(f"  Proceed to: analysis/risk_analysis.py")

    conn.close()


if __name__ == "__main__":
    run()
