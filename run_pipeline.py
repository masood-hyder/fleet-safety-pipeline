"""
run_pipeline.py
---------------
Master runner — executes all pipeline layers in sequence:
  1. Generate synthetic data
  2. Ingest raw sources
  3. Validate data quality
  4. Transform and build analytical tables
  5. Run risk analysis and generate charts

Usage:
  python run_pipeline.py
"""

import subprocess
import sys
import time

STEPS = [
    ("Generating synthetic data",    "generate_data.py"),
    ("Ingesting raw sources",         "etl/01_ingest.py"),
    ("Validating data quality",       "etl/02_validate.py"),
    ("Transforming data",             "etl/03_transform.py"),
    ("Running risk analysis",         "analysis/risk_analysis.py"),
]


def run_step(label, script):
    print(f"\n{'='*55}")
    print(f"STEP: {label}")
    print(f"{'='*55}")
    start  = time.time()
    result = subprocess.run([sys.executable, script], capture_output=False)
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"\n[ERROR] {script} failed. Stopping pipeline.")
        sys.exit(1)
    print(f"\n  Completed in {elapsed:.1f}s")
    return elapsed


def main():
    print("\n" + "="*55)
    print("  FLEET SAFETY & RISK ANALYTICS PIPELINE")
    print("="*55)
    print("  Running full pipeline end-to-end...\n")

    total_start = time.time()
    timings = []

    for label, script in STEPS:
        elapsed = run_step(label, script)
        timings.append((label, elapsed))

    total = time.time() - total_start

    print(f"\n{'='*55}")
    print("  PIPELINE COMPLETE")
    print(f"{'='*55}")
    for label, elapsed in timings:
        print(f"  {label:<35} {elapsed:>5.1f}s")
    print(f"  {'─'*42}")
    print(f"  {'Total':<35} {total:>5.1f}s")
    print(f"\n  Output files:")
    print(f"    data/fleet_safety.db       — SQLite database")
    print(f"    data/analytical/           — Partitioned Parquet files")
    print(f"    analysis/outputs/          — Charts and visualizations")


if __name__ == "__main__":
    main()
