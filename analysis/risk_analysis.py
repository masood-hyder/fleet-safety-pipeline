"""
analysis/risk_analysis.py
--------------------------
Reads the analytical table produced by the ETL pipeline and performs
a full risk factor analysis across all 12 risk dimensions.

Key outputs:
  - Correlation of each risk factor with incident occurrence
  - Top 3 incident drivers (ranked by impact)
  - Incident rate breakdown by weather, road type, time of day
  - Severity and cost distribution
  - All charts saved to analysis/outputs/

This mirrors the analysis layer that feeds the Power BI dashboard.
"""

import os
import sqlite3
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

DB_PATH    = "data/fleet_safety.db"
OUTPUT_DIR = "analysis/outputs"
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

PALETTE = {
    "primary":    "#f0a500",
    "secondary":  "#e05c2a",
    "tertiary":   "#3a7bd5",
    "bg":         "#0f1117",
    "bg2":        "#1a1d27",
    "text":       "#e8e6e0",
    "muted":      "#6b7080",
    "grid":       "#2a2d38",
}


def style_plot(ax, title="", xlabel="", ylabel=""):
    ax.set_facecolor(PALETTE["bg2"])
    ax.figure.patch.set_facecolor(PALETTE["bg"])
    ax.tick_params(colors=PALETTE["muted"], labelsize=9)
    ax.xaxis.label.set_color(PALETTE["muted"])
    ax.yaxis.label.set_color(PALETTE["muted"])
    ax.title.set_color(PALETTE["text"])
    for spine in ax.spines.values():
        spine.set_edgecolor(PALETTE["grid"])
    ax.grid(True, color=PALETTE["grid"], linewidth=0.5, alpha=0.7)
    if title:   ax.set_title(title, fontsize=12, pad=12, color=PALETTE["text"])
    if xlabel:  ax.set_xlabel(xlabel, fontsize=10)
    if ylabel:  ax.set_ylabel(ylabel, fontsize=10)


def load_data(conn):
    print("  Loading analytical table...")
    df = pd.read_sql("SELECT * FROM fleet_risk_analytical", conn)
    print(f"  Loaded {len(df):,} rows  |  {len(df.columns)} columns")
    return df


def analyze_risk_correlations(df):
    print("\n── Risk Factor Correlations ──")
    risk_cols    = list(RISK_LABELS.keys())
    correlations = df[risk_cols + ["had_incident"]].corr()["had_incident"].drop("had_incident")
    correlations = correlations.abs().sort_values(ascending=False)

    print(f"\n  {'Risk Factor':<30} {'Correlation':>12}")
    print(f"  {'-'*42}")
    for col, val in correlations.items():
        label = RISK_LABELS.get(col, col)
        bar   = "█" * int(val * 40)
        print(f"  {label:<30} {val:>12.4f}  {bar}")

    top3 = correlations.head(3)
    print(f"\n  TOP 3 INCIDENT DRIVERS:")
    for i, (col, val) in enumerate(top3.items(), 1):
        print(f"    {i}. {RISK_LABELS[col]:<28} (r = {val:.4f})")

    return correlations


def plot_risk_correlations(correlations):
    labels = [RISK_LABELS[c] for c in correlations.index]
    values = correlations.values
    colors = [PALETTE["primary"] if i < 3 else PALETTE["muted"] for i in range(len(values))]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(labels[::-1], values[::-1], color=colors[::-1], height=0.65)

    for bar, val in zip(bars, values[::-1]):
        ax.text(bar.get_width() + 0.002, bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}", va="center", ha="left",
                color=PALETTE["text"], fontsize=8)

    style_plot(ax, "Risk Factor Correlation with Incident Occurrence",
               "Absolute Correlation Coefficient", "Risk Factor")

    top3_patch  = mpatches.Patch(color=PALETTE["primary"], label="Top 3 incident drivers")
    rest_patch  = mpatches.Patch(color=PALETTE["muted"],   label="Other risk factors")
    ax.legend(handles=[top3_patch, rest_patch], facecolor=PALETTE["bg2"],
              labelcolor=PALETTE["text"], fontsize=9, loc="lower right")

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "01_risk_correlations.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\n  Chart saved: {path}")


def plot_incident_by_weather(df):
    weather_stats = (
        df.groupby("weather_condition")["had_incident"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"mean": "incident_rate", "count": "trips"})
        .sort_values("incident_rate", ascending=False)
    )

    fig, ax = plt.subplots(figsize=(9, 5))
    colors  = [PALETTE["secondary"] if r > 0.15 else PALETTE["tertiary"]
               for r in weather_stats["incident_rate"]]
    bars    = ax.bar(weather_stats["weather_condition"],
                     weather_stats["incident_rate"] * 100,
                     color=colors, width=0.6)

    for bar, (_, row) in zip(bars, weather_stats.iterrows()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f"{row['incident_rate']*100:.1f}%\n({row['trips']:,} trips)",
                ha="center", va="bottom", color=PALETTE["muted"], fontsize=8)

    style_plot(ax, "Incident Rate by Weather Condition",
               "Weather Condition", "Incident Rate (%)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "02_incident_by_weather.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved: {path}")


def plot_incident_heatmap(df):
    pivot = df.pivot_table(
        values="had_incident",
        index="time_of_day",
        columns="road_type",
        aggfunc="mean"
    )

    order = ["Morning", "Afternoon", "Evening", "Night"]
    pivot = pivot.reindex([o for o in order if o in pivot.index])

    fig, ax = plt.subplots(figsize=(9, 5))
    im = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto")

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, color=PALETTE["text"], fontsize=10)
    ax.set_yticklabels(pivot.index,   color=PALETTE["text"], fontsize=10)

    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.values[i, j]
            ax.text(j, i, f"{val:.1%}", ha="center", va="center",
                    color="black" if val > 0.12 else "white", fontsize=9, fontweight="bold")

    cbar = plt.colorbar(im, ax=ax)
    cbar.ax.tick_params(colors=PALETTE["muted"])
    cbar.set_label("Incident Rate", color=PALETTE["muted"], fontsize=9)

    style_plot(ax, "Incident Rate Heatmap — Time of Day vs Road Type")
    ax.set_facecolor(PALETTE["bg"])

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "03_incident_heatmap.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved: {path}")


def plot_severity_distribution(df):
    inc_df   = df[df["had_incident"] == 1].copy()
    severity = inc_df["severity"].value_counts()
    types    = inc_df["incident_type"].value_counts()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    colors1 = [PALETTE["secondary"], PALETTE["primary"], PALETTE["tertiary"]]
    ax1.pie(severity.values, labels=severity.index, autopct="%1.1f%%",
            colors=colors1, startangle=140,
            textprops={"color": PALETTE["text"], "fontsize": 10})
    ax1.set_title("Incident Severity Distribution",
                  color=PALETTE["text"], fontsize=11, pad=10)
    ax1.set_facecolor(PALETTE["bg2"])
    ax1.figure.patch.set_facecolor(PALETTE["bg"])

    colors2 = [PALETTE["primary"], PALETTE["secondary"], PALETTE["tertiary"],
               PALETTE["muted"], "#7b5ea7"]
    bars = ax2.barh(types.index, types.values, color=colors2, height=0.6)
    for bar, val in zip(bars, types.values):
        ax2.text(bar.get_width() + 50, bar.get_y() + bar.get_height() / 2,
                 f"{val:,}", va="center", color=PALETTE["muted"], fontsize=9)
    style_plot(ax2, "Incident Type Frequency", "Count", "Incident Type")

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "04_severity_distribution.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved: {path}")


def print_summary_stats(df):
    inc_df = df[df["had_incident"] == 1]
    print("\n── Summary Statistics ──")
    print(f"  Total trips analyzed  : {len(df):,}")
    print(f"  Total incidents       : {len(inc_df):,}")
    print(f"  Overall incident rate : {len(inc_df)/len(df)*100:.2f}%")
    print(f"  Avg damage cost (USD) : ${inc_df['damage_cost_usd'].mean():,.0f}")
    print(f"  Total damage cost     : ${inc_df['damage_cost_usd'].sum():,.0f}")
    print(f"  Injury reported       : {inc_df['injury_reported'].sum():,} incidents")


def run():
    print("=" * 55)
    print("FLEET SAFETY PIPELINE — ANALYSIS LAYER")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    df   = load_data(conn)

    print_summary_stats(df)
    correlations = analyze_risk_correlations(df)

    print("\n── Generating Charts ──")
    plot_risk_correlations(correlations)
    plot_incident_by_weather(df)
    plot_incident_heatmap(df)
    plot_severity_distribution(df)

    print(f"\nAnalysis complete. All charts saved to: {OUTPUT_DIR}/")
    conn.close()


if __name__ == "__main__":
    run()
