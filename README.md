# Fleet Safety & Risk Analytics Pipeline

A scalable ETL pipeline that ingests, validates, and processes **500,000+ multi-source fleet records** to uncover the top incident drivers across 12 risk dimensions. Built to mirror a production-grade HDFS/Impala architecture using Python, pandas, and SQLite.

---

## Project Overview

Fleet operators generate large volumes of data across trips, drivers, vehicles, and environmental conditions — but this data lives in silos. This pipeline consolidates those sources, engineers 12 risk features, and delivers a ranked analysis of which factors most strongly predict incidents.

**Key findings from the analysis:**
- Identified the **top 3 incident drivers** out of 12 risk variables
- Uncovered incident rate patterns across weather conditions, road types, and time of day
- Quantified total damage cost exposure across 500K+ trips

---

## Architecture

```
Raw Sources (5 CSVs)
       │
       ▼
┌─────────────────┐
│  01 — Ingest    │  Schema checks · Row counts · Audit logging
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  02 — Validate  │  Null checks · Duplicates · Range checks · Referential integrity
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  03 — Transform │  Source joins · 12 risk features · Partitioned Parquet output
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Analysis       │  Correlation analysis · Top 3 drivers · Heatmaps · Charts
└─────────────────┘
```

> In a production environment, ingestion and transformation layers execute as Impala queries against data stored in HDFS (partitioned Parquet). This project replicates that architecture locally using pandas and SQLite, preserving the same pipeline structure, audit logic, and output schema.

---

## Data Sources

Synthetic data generated using the `faker` library — 500,000+ records across 5 sources:

| Source | Records | Key Fields |
|--------|---------|------------|
| `trips.csv` | 500,000 | trip_id, driver_id, vehicle_id, distance, duration, speed_violation |
| `drivers.csv` | 2,000 | driver_age, experience_years, fatigue_score |
| `vehicles.csv` | 1,500 | vehicle_age, maintenance_score, cargo_compliance |
| `environment.csv` | 500,000 | weather_condition, road_type, visibility_score |
| `incidents.csv` | ~60,000 | incident_type, severity, damage_cost_usd |

---

## The 12 Risk Variables

| # | Variable | Description |
|---|----------|-------------|
| 1 | Driver Age | Risk adjusted for very young and older drivers |
| 2 | Driver Experience | Inverse of years of experience |
| 3 | Fatigue Score | Self-reported fatigue at trip start |
| 4 | Vehicle Age | Older vehicles carry higher mechanical risk |
| 5 | Maintenance Score | Inverse of last maintenance rating |
| 6 | Cargo Compliance | Whether vehicle is within weight limits |
| 7 | Weather Condition | Clear → Stormy risk scale |
| 8 | Road Type | Highway → Mountain risk scale |
| 9 | Visibility Score | Inverse of visibility at trip time |
| 10 | Speed Violation | Whether trip recorded a speed violation |
| 11 | Route Risk Rating | Pre-assigned route difficulty score |
| 12 | Time of Day | Morning → Night risk scale |

---

## Project Structure

```
fleet-safety-pipeline/
│
├── generate_data.py          # Synthetic data generation (500K+ records)
├── run_pipeline.py           # Master runner — executes all steps in sequence
├── requirements.txt
│
├── etl/
│   ├── 01_ingest.py          # Ingestion layer with audit logging
│   ├── 02_validate.py        # Data quality validation
│   └── 03_transform.py       # Feature engineering + partitioned output
│
├── analysis/
│   ├── risk_analysis.py      # Full risk factor analysis + chart generation
│   └── outputs/              # Generated charts (created on run)
│
└── data/                     # Created on run (excluded from git)
    ├── raw/
    ├── staging/
    ├── analytical/
    └── fleet_safety.db
```

---

## Getting Started

**1. Clone the repository**
```bash
git clone https://github.com/masood-hyder/fleet-safety-pipeline.git
cd fleet-safety-pipeline
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Run the full pipeline**
```bash
python run_pipeline.py
```

This runs all five steps end-to-end and saves charts to `analysis/outputs/`.

**Or run individual layers:**
```bash
python generate_data.py          # Generate synthetic data
python etl/01_ingest.py          # Ingest raw sources
python etl/02_validate.py        # Validate data quality
python etl/03_transform.py       # Transform and engineer features
python analysis/risk_analysis.py # Run analysis and generate charts
```

---

## Output Charts

All charts are saved to `analysis/outputs/` after running the pipeline:

| File | Description |
|------|-------------|
| `01_risk_correlations.png` | Horizontal bar chart — all 12 risk factors ranked by correlation with incident occurrence. Top 3 highlighted. |
| `02_incident_by_weather.png` | Incident rate by weather condition |
| `03_incident_heatmap.png` | Heatmap — incident rate by time of day vs road type |
| `04_severity_distribution.png` | Severity breakdown and incident type frequency |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Generation | Python, Faker |
| ETL & Transformation | Python, pandas, pyarrow |
| Storage (local) | SQLite, Parquet |
| Production equivalent | HDFS, Apache Impala |
| Analysis | pandas, NumPy |
| Visualization | Matplotlib |
| Dashboard (production) | Power BI |

---

## Author

**Masood Hyder**  
MS Business Analytics & AI — UT Dallas  
[linkedin.com/in/masoodh](https://linkedin.com/in/masoodh) · [masood-hyder.github.io](https://masood-hyder.github.io)
