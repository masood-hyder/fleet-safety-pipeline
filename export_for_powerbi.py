"""
export_for_powerbi.py
---------------------
Exports all analytical tables from the pipeline database into clean CSVs
ready for direct import into Power BI Desktop.

Tables exported:
  - powerbi/fleet_risk_analytical.csv   : Full trip-level dataset with all 12 risk features
  - powerbi/risk_factor_summary.csv     : Risk scores + incident flag per trip
  - powerbi/incident_summary.csv        : Incident-level detail
  - powerbi/weather_summary.csv         : Incident rate aggregated by weather
  - powerbi/time_road_summary.csv       : Incident rate by time of day and road type
  - powerbi/risk_correlations.csv       : All 12 risk factors ranked by correlation

Usage:
  python export_for_powerbi.py
  (Run AFTER run_pipeline.py has completed successfully)
"""

import os
import sqlite3
import pandas as pd
import numpy as np

DB_PATH    = "data/fleet_safety.db"
OUTPUT_DIR = "powerbi"
os.makedirs(OUTPUT_DIR, exist_ok=True)

RISK_LABELS = {
    "risk_driver_age":  "Driver Age",
    "risk_experience":  "Driver Experience",
    "risk_fatigue":     "Fatigue Score",
    "risk_vehicle_age": "Vehicle Age",
    "risk_maintenance": "Maintenance",
    "risk_cargo":       "Cargo Compliance",
    "risk_weather":     "Weather Condition",
    "risk_road_type":   "Road Type",
    "risk_visibility":  "Visibility",
    "risk_speed":       "Speed Violation",
    "risk_route":       "Route Risk Rating",
    "risk_time_of_day": "Time of Day",
}


def export_table(conn, query, filename, label):
    df = pd.read_sql(query, conn)
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"  [OK]  {label:<45} {len(df):>10,} rows  →  {filename}")
    return df


def build_risk_correlations(conn):
    df        = pd.read_sql("SELECT * FROM fleet_risk_analytical", conn)
    risk_cols = list(RISK_LABELS.keys())
    corr      = df[risk_cols + ["had_incident"]].corr()["had_incident"].drop("had_incident")
    corr      = corr.abs().sort_values(ascending=False).reset_index()
    corr.columns = ["risk_column", "correlation"]
    corr["risk_label"] = corr["risk_column"].map(RISK_LABELS)
    corr["rank"]       = range(1, len(corr) + 1)
    corr["is_top_3"]   = corr["rank"].apply(lambda x: "Top 3" if x <= 3 else "Other")
    corr["correlation"] = corr["correlation"].round(4)

    path = os.path.join(OUTPUT_DIR, "risk_correlations.csv")
    corr.to_csv(path, index=False)
    print(f"  [OK]  {'Risk correlations (12 factors ranked)':<45} {len(corr):>10,} rows  →  risk_correlations.csv")
    return corr


def build_weather_summary(conn):
    df = pd.read_sql("SELECT * FROM fleet_risk_analytical", conn)
    summary = (
        df.groupby("weather_condition")
        .agg(
            total_trips    = ("trip_id", "count"),
            total_incidents= ("had_incident", "sum"),
            avg_damage_cost= ("damage_cost_usd", "mean"),
        )
        .reset_index()
    )
    summary["incident_rate_pct"] = (summary["total_incidents"] / summary["total_trips"] * 100).round(2)
    summary["avg_damage_cost"]   = summary["avg_damage_cost"].round(2).fillna(0)
    summary = summary.sort_values("incident_rate_pct", ascending=False)

    path = os.path.join(OUTPUT_DIR, "weather_summary.csv")
    summary.to_csv(path, index=False)
    print(f"  [OK]  {'Incident rate by weather condition':<45} {len(summary):>10,} rows  →  weather_summary.csv")


def build_time_road_summary(conn):
    df = pd.read_sql("SELECT * FROM fleet_risk_analytical", conn)
    summary = (
        df.groupby(["time_of_day", "road_type"])
        .agg(
            total_trips     = ("trip_id", "count"),
            total_incidents = ("had_incident", "sum"),
        )
        .reset_index()
    )
    summary["incident_rate_pct"] = (summary["total_incidents"] / summary["total_trips"] * 100).round(2)
    summary = summary.sort_values("incident_rate_pct", ascending=False)

    path = os.path.join(OUTPUT_DIR, "time_road_summary.csv")
    summary.to_csv(path, index=False)
    print(f"  [OK]  {'Incident rate by time of day + road type':<45} {len(summary):>10,} rows  →  time_road_summary.csv")


def build_incident_detail(conn):
    query = """
        SELECT
            f.trip_id,
            f.trip_date,
            f.time_of_day,
            f.distance_km,
            f.duration_min,
            f.driver_age,
            f.experience_years,
            f.fatigue_score,
            f.vehicle_age,
            f.maintenance_score,
            f.weather_condition,
            f.road_type,
            f.visibility_score,
            f.speed_violation,
            f.incident_type,
            f.severity,
            f.injury_reported,
            f.damage_cost_usd,
            f.year,
            f.month
        FROM fleet_risk_analytical f
        WHERE f.had_incident = 1
    """
    df   = pd.read_sql(query, conn)
    path = os.path.join(OUTPUT_DIR, "incident_detail.csv")
    df.to_csv(path, index=False)
    print(f"  [OK]  {'Incident detail records':<45} {len(df):>10,} rows  →  incident_detail.csv")


def build_kpi_summary(conn):
    df     = pd.read_sql("SELECT * FROM fleet_risk_analytical", conn)
    inc_df = df[df["had_incident"] == 1]

    kpis = pd.DataFrame([
        {"metric": "Total Trips",             "value": len(df),                                          "format": "number"},
        {"metric": "Total Incidents",          "value": len(inc_df),                                      "format": "number"},
        {"metric": "Overall Incident Rate",    "value": round(len(inc_df) / len(df) * 100, 2),            "format": "percent"},
        {"metric": "Total Damage Cost (USD)",  "value": round(inc_df["damage_cost_usd"].sum(), 0),        "format": "currency"},
        {"metric": "Avg Damage Cost (USD)",    "value": round(inc_df["damage_cost_usd"].mean(), 0),       "format": "currency"},
        {"metric": "Injury Reported",          "value": int(inc_df["injury_reported"].sum()),             "format": "number"},
        {"metric": "High Severity Incidents",  "value": int((inc_df["severity"] == "High").sum()),        "format": "number"},
        {"metric": "Speed Violation Rate",     "value": round(df["speed_violation"].mean() * 100, 2),     "format": "percent"},
    ])

    path = os.path.join(OUTPUT_DIR, "kpi_summary.csv")
    kpis.to_csv(path, index=False)
    print(f"  [OK]  {'KPI summary metrics':<45} {len(kpis):>10,} rows  →  kpi_summary.csv")


def run():
    print("=" * 65)
    print("FLEET SAFETY PIPELINE — POWER BI EXPORT")
    print("=" * 65)
    print(f"  Source database : {DB_PATH}")
    print(f"  Output folder   : {OUTPUT_DIR}/")
    print()

    if not os.path.exists(DB_PATH):
        print("[ERROR] Database not found. Run run_pipeline.py first.")
        return

    conn = sqlite3.connect(DB_PATH)

    export_table(
        conn,
        "SELECT * FROM fleet_risk_analytical",
        "fleet_risk_analytical.csv",
        "Full analytical dataset (all trips + risk features)"
    )

    build_risk_correlations(conn)
    build_weather_summary(conn)
    build_time_road_summary(conn)
    build_incident_detail(conn)
    build_kpi_summary(conn)

    conn.close()

    files = os.listdir(OUTPUT_DIR)
    total_size = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, f)) for f in files
    ) / (1024 * 1024)

    print(f"\nExport complete.")
    print(f"  Files exported : {len(files)}")
    print(f"  Total size     : {total_size:.1f} MB")
    print(f"  Output folder  : {OUTPUT_DIR}/")
    print(f"\n  Next step: Open Power BI Desktop and connect to the")
    print(f"  CSV files in the '{OUTPUT_DIR}/' folder.")


if __name__ == "__main__":
    run()
