"""
etl_pipeline.py
ETL Pipeline: Extract → Transform → Load (SQLite) for AML Fraud Analytics
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("etl/etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_PATH = "data/aml_fraud.db"

# ══════════════════════════════════════════════════════════════════════════════
# EXTRACT
# ══════════════════════════════════════════════════════════════════════════════
def extract():
    log.info("EXTRACT — Loading raw CSV files...")
    customers = pd.read_csv("data/customers.csv")
    transactions = pd.read_csv("data/transactions.csv")
    log.info(f"  Customers: {len(customers)} rows | Transactions: {len(transactions)} rows")
    return customers, transactions


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════
def transform(customers: pd.DataFrame, transactions: pd.DataFrame):
    log.info("TRANSFORM — Cleaning and enriching data...")

    # ── Customers: clean & enrich ──────────────────────────────────────────
    customers = customers.drop_duplicates(subset="customer_id")
    customers["risk_tier"] = pd.cut(
        customers["risk_score"],
        bins=[0, 30, 60, 100],
        labels=["Low", "Medium", "High"]
    )
    customers["kyc_status"] = customers["kyc_verified"].map({1: "Verified", 0: "Unverified"})
    customers["is_high_risk_country"] = customers["country"].isin(["NG", "RU", "AE"]).astype(int)

    # ── Transactions: clean & enrich ──────────────────────────────────────
    transactions["date"] = pd.to_datetime(transactions["date"])
    transactions = transactions.drop_duplicates(subset="transaction_id")
    transactions = transactions.dropna(subset=["amount", "customer_id"])
    transactions["amount"] = transactions["amount"].clip(lower=0)

    # Feature engineering
    transactions["hour"] = transactions["date"].dt.hour
    transactions["day_of_week"] = transactions["date"].dt.day_name()
    transactions["month"] = transactions["date"].dt.month
    transactions["is_weekend"] = transactions["date"].dt.dayofweek.isin([5, 6]).astype(int)
    transactions["is_odd_hour"] = ((transactions["hour"] < 6) | (transactions["hour"] > 22)).astype(int)
    transactions["is_high_risk_country"] = transactions["country"].isin(["NG", "RU", "AE"]).astype(int)
    transactions["is_structuring"] = (
        (transactions["amount"] >= 9000) & (transactions["amount"] < 10000)
    ).astype(int)
    transactions["amount_bucket"] = pd.cut(
        transactions["amount"],
        bins=[0, 1000, 5000, 10000, 50000, float("inf")],
        labels=["<1K", "1K-5K", "5K-10K", "10K-50K", ">50K"]
    )

    # ── Merge: enrich transactions with customer data ──────────────────────
    merged = transactions.merge(
        customers[["customer_id", "risk_score", "risk_tier", "kyc_status",
                   "is_pep", "account_age_days", "is_high_risk_country"]],
        on="customer_id", how="left", suffixes=("_txn", "_cust")
    )
    merged.rename(columns={"is_high_risk_country_txn": "txn_high_risk_country",
                            "is_high_risk_country_cust": "cust_high_risk_country"}, inplace=True)

    # ── Alert table: high-risk transactions ───────────────────────────────
    alerts = merged[
        (merged["is_fraud"] == 1) |
        (merged["is_structuring"] == 1) |
        (merged["fraud_score"] > 75)
    ].copy()
    alerts["alert_reason"] = ""
    alerts.loc[alerts["is_fraud"] == 1, "alert_reason"] += "FRAUD_FLAG;"
    alerts.loc[alerts["is_structuring"] == 1, "alert_reason"] += "STRUCTURING;"
    alerts.loc[alerts["fraud_score"] > 75, "alert_reason"] += "HIGH_RISK_SCORE;"

    # ── Summary: customer-level aggregates ────────────────────────────────
    cust_summary = merged.groupby("customer_id").agg(
        total_transactions=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        avg_amount=("amount", "mean"),
        max_amount=("amount", "max"),
        fraud_count=("is_fraud", "sum"),
        fraud_rate=("is_fraud", "mean"),
        structuring_count=("is_structuring", "sum"),
        high_risk_txn_count=("txn_high_risk_country", "sum"),
    ).reset_index()

    log.info(f"  Cleaned transactions: {len(merged)} | Alerts generated: {len(alerts)}")
    return customers, transactions, merged, alerts, cust_summary


# ══════════════════════════════════════════════════════════════════════════════
# LOAD
# ══════════════════════════════════════════════════════════════════════════════
def load(customers, transactions, merged, alerts, cust_summary):
    log.info(f"LOAD — Writing to SQLite: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
    transactions.to_sql("fact_transactions", conn, if_exists="replace", index=False)
    merged.to_sql("fact_transactions_enriched", conn, if_exists="replace", index=False)
    alerts.to_sql("aml_alerts", conn, if_exists="replace", index=False)
    cust_summary.to_sql("customer_summary", conn, if_exists="replace", index=False)

    conn.close()
    log.info("  ✅ All tables loaded successfully.")
    log.info("  Tables: dim_customers | fact_transactions | fact_transactions_enriched | aml_alerts | customer_summary")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    os.makedirs("etl", exist_ok=True)
    log.info("=" * 60)
    log.info("AML FRAUD DETECTION — ETL PIPELINE STARTED")
    log.info("=" * 60)

    customers_raw, transactions_raw = extract()
    customers, transactions, merged, alerts, cust_summary = transform(customers_raw, transactions_raw)
    load(customers, transactions, merged, alerts, cust_summary)

    log.info("=" * 60)
    log.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
    log.info("=" * 60)
