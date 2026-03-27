"""
analysis.py
AML Fraud Detection — Python Analysis & Visualizations
Generates charts saved to dashboard/ folder for Power BI / Tableau / GitHub README
"""

import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os
import warnings
warnings.filterwarnings("ignore")

# ── Style ──────────────────────────────────────────────────────────────────────
NAVY   = "#1B3A6B"
BLUE   = "#2E75B6"
RED    = "#C0392B"
ORANGE = "#E67E22"
GREEN  = "#27AE60"
GRAY   = "#7F8C8D"
LIGHT  = "#EAF0FB"

sns.set_theme(style="whitegrid", font="DejaVu Sans")
plt.rcParams.update({"figure.dpi": 130, "axes.titlesize": 14, "axes.titleweight": "bold"})

os.makedirs("dashboard", exist_ok=True)

# ── Load data ──────────────────────────────────────────────────────────────────
conn = sqlite3.connect("data/aml_fraud.db")
txn  = pd.read_sql("SELECT * FROM fact_transactions_enriched", conn)
cust = pd.read_sql("SELECT * FROM dim_customers", conn)
alerts = pd.read_sql("SELECT * FROM aml_alerts", conn)
conn.close()

txn["date"] = pd.to_datetime(txn["date"])
txn["year_month"] = txn["date"].dt.to_period("M").astype(str)

print(f"Loaded {len(txn):,} transactions | Fraud rate: {txn['is_fraud'].mean()*100:.1f}%")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 1 — Fraud Rate by Transaction Type
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(10, 5))
fraud_by_type = txn.groupby("transaction_type")["is_fraud"].mean().sort_values(ascending=False) * 100
colors = [RED if v > 8 else BLUE for v in fraud_by_type.values]
bars = ax.bar(fraud_by_type.index, fraud_by_type.values, color=colors, edgecolor="white", linewidth=0.8)
ax.set_title("Fraud Rate by Transaction Type", pad=14)
ax.set_ylabel("Fraud Rate (%)")
ax.set_xlabel("")
for bar, val in zip(bars, fraud_by_type.values):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2, f"{val:.1f}%",
            ha="center", va="bottom", fontsize=10, fontweight="bold")
ax.set_ylim(0, fraud_by_type.max() * 1.3)
ax.tick_params(axis="x", rotation=15)
plt.tight_layout()
plt.savefig("dashboard/01_fraud_by_txn_type.png")
plt.close()
print("✅ Chart 1 saved")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 2 — Monthly Fraud Trend
# ══════════════════════════════════════════════════════════════════════════════
monthly = txn.groupby("year_month").agg(
    total=("is_fraud", "count"),
    fraud=("is_fraud", "sum")
).reset_index()
monthly["fraud_rate"] = monthly["fraud"] / monthly["total"] * 100

fig, ax1 = plt.subplots(figsize=(12, 5))
ax2 = ax1.twinx()
ax1.bar(monthly["year_month"], monthly["total"], color=LIGHT, edgecolor=BLUE, linewidth=0.7, label="Total Txns")
ax2.plot(monthly["year_month"], monthly["fraud_rate"], color=RED, marker="o", linewidth=2.5, markersize=6, label="Fraud Rate %")
ax1.set_title("Monthly Transaction Volume & Fraud Rate", pad=14)
ax1.set_ylabel("Total Transactions", color=BLUE)
ax2.set_ylabel("Fraud Rate (%)", color=RED)
ax1.tick_params(axis="x", rotation=45)
ax1.tick_params(axis="y", colors=BLUE)
ax2.tick_params(axis="y", colors=RED)
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
plt.tight_layout()
plt.savefig("dashboard/02_monthly_fraud_trend.png")
plt.close()
print("✅ Chart 2 saved")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 3 — Fraud by Country (Heatmap style)
# ══════════════════════════════════════════════════════════════════════════════
country_stats = txn.groupby("country").agg(
    fraud_count=("is_fraud", "sum"),
    total=("is_fraud", "count"),
    fraud_rate=("is_fraud", "mean")
).reset_index().sort_values("fraud_rate", ascending=True)
country_stats["fraud_rate_pct"] = country_stats["fraud_rate"] * 100

fig, ax = plt.subplots(figsize=(10, 6))
cmap_colors = [RED if r > 15 else ORANGE if r > 10 else BLUE for r in country_stats["fraud_rate_pct"]]
bars = ax.barh(country_stats["country"], country_stats["fraud_rate_pct"], color=cmap_colors, edgecolor="white")
for bar, val in zip(bars, country_stats["fraud_rate_pct"]):
    ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
            f"{val:.1f}%", va="center", fontsize=10)
ax.set_title("Fraud Rate by Country", pad=14)
ax.set_xlabel("Fraud Rate (%)")
ax.set_xlim(0, country_stats["fraud_rate_pct"].max() * 1.25)
plt.tight_layout()
plt.savefig("dashboard/03_fraud_by_country.png")
plt.close()
print("✅ Chart 3 saved")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 4 — Fraud by Hour of Day
# ══════════════════════════════════════════════════════════════════════════════
txn["hour"] = txn["date"].dt.hour
hourly = txn.groupby("hour")["is_fraud"].mean() * 100

fig, ax = plt.subplots(figsize=(12, 5))
colors_h = [RED if h < 6 or h > 22 else BLUE for h in hourly.index]
ax.bar(hourly.index, hourly.values, color=colors_h, edgecolor="white")
ax.set_title("Fraud Rate by Hour of Day  (Red = Odd Hours: 11pm–6am)", pad=14)
ax.set_xlabel("Hour of Day")
ax.set_ylabel("Fraud Rate (%)")
ax.set_xticks(range(24))
plt.tight_layout()
plt.savefig("dashboard/04_fraud_by_hour.png")
plt.close()
print("✅ Chart 4 saved")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 5 — Fraud Amount Distribution
# ══════════════════════════════════════════════════════════════════════════════
fraud_txns = txn[txn["is_fraud"] == 1]["amount"]
legit_txns = txn[txn["is_fraud"] == 0]["amount"]

fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(np.log1p(legit_txns), bins=60, alpha=0.6, color=BLUE, label="Legitimate")
ax.hist(np.log1p(fraud_txns), bins=60, alpha=0.7, color=RED, label="Fraud")
ax.set_title("Transaction Amount Distribution (Log Scale)", pad=14)
ax.set_xlabel("Log(Amount + 1)")
ax.set_ylabel("Frequency")
ax.legend()
plt.tight_layout()
plt.savefig("dashboard/05_amount_distribution.png")
plt.close()
print("✅ Chart 5 saved")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 6 — Risk Tier vs Fraud Rate
# ══════════════════════════════════════════════════════════════════════════════
risk_fraud = txn.groupby("risk_tier")["is_fraud"].agg(["mean", "count"]).reset_index()
risk_fraud["fraud_rate"] = risk_fraud["mean"] * 100
risk_fraud = risk_fraud[risk_fraud["risk_tier"].isin(["Low", "Medium", "High"])]

fig, ax = plt.subplots(figsize=(7, 5))
tier_colors = {"Low": GREEN, "Medium": ORANGE, "High": RED}
bars = ax.bar(risk_fraud["risk_tier"], risk_fraud["fraud_rate"],
              color=[tier_colors.get(t, BLUE) for t in risk_fraud["risk_tier"]],
              edgecolor="white", width=0.5)
for bar, val in zip(bars, risk_fraud["fraud_rate"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
            f"{val:.1f}%", ha="center", fontweight="bold")
ax.set_title("Fraud Rate by Customer Risk Tier", pad=14)
ax.set_ylabel("Fraud Rate (%)")
ax.set_ylim(0, risk_fraud["fraud_rate"].max() * 1.35)
plt.tight_layout()
plt.savefig("dashboard/06_risk_tier_fraud.png")
plt.close()
print("✅ Chart 6 saved")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 7 — Merchant Category Risk
# ══════════════════════════════════════════════════════════════════════════════
merch = txn.groupby("merchant_category")["is_fraud"].mean().sort_values(ascending=False) * 100
fig, ax = plt.subplots(figsize=(10, 5))
colors_m = [RED if v > 8 else ORANGE if v > 5 else BLUE for v in merch.values]
ax.bar(merch.index, merch.values, color=colors_m, edgecolor="white")
ax.set_title("Fraud Rate by Merchant Category", pad=14)
ax.set_ylabel("Fraud Rate (%)")
ax.tick_params(axis="x", rotation=20)
for i, val in enumerate(merch.values):
    ax.text(i, val + 0.15, f"{val:.1f}%", ha="center", fontsize=9, fontweight="bold")
ax.set_ylim(0, merch.max() * 1.3)
plt.tight_layout()
plt.savefig("dashboard/07_merchant_fraud.png")
plt.close()
print("✅ Chart 7 saved")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 8 — Structuring Detection
# ══════════════════════════════════════════════════════════════════════════════
struct = txn[txn["is_structuring"] == 1]
fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(struct["amount"], bins=40, color=ORANGE, edgecolor="white")
ax.axvline(9000, color=RED, linestyle="--", linewidth=2, label="$9,000 threshold")
ax.axvline(10000, color=NAVY, linestyle="--", linewidth=2, label="$10,000 reporting limit")
ax.set_title("Structuring Detection — Transactions $9,000–$10,000", pad=14)
ax.set_xlabel("Transaction Amount ($)")
ax.set_ylabel("Count")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax.legend()
plt.tight_layout()
plt.savefig("dashboard/08_structuring_detection.png")
plt.close()
print("✅ Chart 8 saved")

print("\n🎉 All 8 charts saved to dashboard/ folder.")
print("   Ready for Power BI / Tableau / GitHub README.")
