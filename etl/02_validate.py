"""
etl/02_validate.py
------------------
Validation layer — runs data quality checks across all staged sources
before transformation. Flags nulls, duplicates, referential integrity
issues, and out-of-range values.

In a production HDFS/Impala pipeline, this layer would:
  - Run validation queries directly against Impala staging tables
  - Write a validation report to HDFS for audit purposes
  - Halt the pipeline if critical checks fail

Here we replicate that logic using pandas and SQLite.
"""

import sqlite3
import pandas as pd

DB_PATH = "data/fleet_safety.db"

RANGE_CHECKS = {
    "stg_drivers": {
        "driver_age":       (16, 80),
        "experience_years": (0, 60),
        "fatigue_score":    (0, 10),
    },
    "stg_vehicles": {
        "vehicle_age":        (0, 30),
        "maintenance_score":  (0, 10),
        "mileage":            (0, 500_000),
    },
    "stg_trips": {
        "distance_km":       (0, 2000),
        "duration_min":      (1, 1440),
        "route_risk_rating": (0, 10),
    },
    "stg_environment": {
        "visibility_score": (0, 10),
    },
    "stg_incidents": {
        "damage_cost_usd": (0, 200_000),
    },
}

REFERENTIAL_CHECKS = [
    ("stg_trips",       "driver_id",  "stg_drivers",  "driver_id"),
    ("stg_trips",       "vehicle_id", "stg_vehicles",  "vehicle_id"),
    ("stg_environment", "trip_id",    "stg_trips",     "trip_id"),
    ("stg_incidents",   "trip_id",    "stg_trips",     "trip_id"),
]

results = []


def check(conn, name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    icon   = "✓" if passed else "✗"
    print(f"  [{icon}] {name:<50} {status}  {detail}")
    results.append({"check": name, "status": status, "detail": detail})


def run_null_checks(conn):
    print("\n── Null Checks ──")
    for table in ["stg_trips", "stg_drivers", "stg_vehicles", "stg_environment", "stg_incidents"]:
        df = pd.read_sql(f"SELECT * FROM {table} LIMIT 1000", conn)
        null_counts = df.isnull().sum()
        nulls = null_counts[null_counts > 0]
        if nulls.empty:
            check(conn, f"{table}: no nulls", True)
        else:
            for col, count in nulls.items():
                check(conn, f"{table}.{col}: null values", False, f"{count} nulls found")


def run_duplicate_checks(conn):
    print("\n── Duplicate Checks ──")
    pk_map = {
        "stg_trips":       "trip_id",
        "stg_drivers":     "driver_id",
        "stg_vehicles":    "vehicle_id",
        "stg_incidents":   "incident_id",
    }
    for table, pk in pk_map.items():
        query = f"""
            SELECT COUNT(*) - COUNT(DISTINCT {pk}) AS dupes
            FROM {table}
        """
        dupes = pd.read_sql(query, conn).iloc[0, 0]
        check(conn, f"{table}: duplicate {pk}s", dupes == 0, f"{dupes} duplicates" if dupes else "")


def run_range_checks(conn):
    print("\n── Range Checks ──")
    for table, cols in RANGE_CHECKS.items():
        for col, (lo, hi) in cols.items():
            query = f"""
                SELECT COUNT(*) AS out_of_range
                FROM {table}
                WHERE {col} < {lo} OR {col} > {hi}
            """
            try:
                count = pd.read_sql(query, conn).iloc[0, 0]
                pct   = ""
                if count > 0:
                    total = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
                    pct   = f"{count:,} rows ({100*count/total:.1f}%)"
                check(conn, f"{table}.{col}: range [{lo}, {hi}]", count == 0, pct)
            except Exception as e:
                check(conn, f"{table}.{col}: range check", False, str(e))


def run_referential_checks(conn):
    print("\n── Referential Integrity Checks ──")
    for child_table, child_col, parent_table, parent_col in REFERENTIAL_CHECKS:
        query = f"""
            SELECT COUNT(*) AS orphans
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
            WHERE p.{parent_col} IS NULL
        """
        try:
            orphans = pd.read_sql(query, conn).iloc[0, 0]
            detail  = f"{orphans:,} orphan rows" if orphans else ""
            check(conn, f"{child_table}.{child_col} → {parent_table}", orphans == 0, detail)
        except Exception as e:
            check(conn, f"{child_table} → {parent_table}", False, str(e))


def save_report(conn):
    df = pd.DataFrame(results)
    df.to_sql("validation_report", conn, if_exists="replace", index=False)
    passed = (df["status"] == "PASS").sum()
    failed = (df["status"] == "FAIL").sum()
    print(f"\n── Summary ──")
    print(f"  Total checks : {len(df)}")
    print(f"  Passed       : {passed}")
    print(f"  Failed       : {failed}")
    print(f"  Report saved : {DB_PATH} > validation_report")
    if failed > 0:
        print(f"\n  [!] {failed} check(s) failed. Review before proceeding to transform layer.")
    else:
        print(f"\n  All checks passed. Safe to proceed to transform layer.")


def run():
    print("=" * 55)
    print("FLEET SAFETY PIPELINE — LAYER 2: VALIDATION")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)

    run_null_checks(conn)
    run_duplicate_checks(conn)
    run_range_checks(conn)
    run_referential_checks(conn)
    save_report(conn)

    conn.close()


if __name__ == "__main__":
    run()
