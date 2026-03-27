"""
generate_data.py
Generates synthetic AML/Fraud transaction dataset for analysis.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

np.random.seed(42)
random.seed(42)

N_CUSTOMERS = 500
N_TRANSACTIONS = 20000

COUNTRIES = ["US", "UK", "DE", "CN", "NG", "RU", "BR", "MX", "IN", "AE"]
HIGH_RISK_COUNTRIES = ["NG", "RU", "AE"]
TRANSACTION_TYPES = ["Wire Transfer", "ACH", "Cash Deposit", "Cash Withdrawal", "POS", "Online Transfer"]
HIGH_RISK_TYPES = ["Wire Transfer", "Cash Deposit", "Cash Withdrawal"]
MERCHANT_CATEGORIES = ["Retail", "Gaming", "Crypto Exchange", "Money Transfer", "Restaurant", "Travel", "Healthcare"]

# ── Customers ──────────────────────────────────────────────────────────────────
def generate_customers():
    customers = []
    for i in range(N_CUSTOMERS):
        risk_score = np.random.beta(2, 5) * 100  # skewed low, some high
        customers.append({
            "customer_id": f"CUST{str(i+1).zfill(5)}",
            "name": f"Customer_{i+1}",
            "country": random.choices(COUNTRIES, weights=[30,15,10,10,5,5,8,7,7,3])[0],
            "account_age_days": random.randint(30, 3650),
            "kyc_verified": random.choices([1, 0], weights=[85, 15])[0],
            "risk_score": round(risk_score, 2),
            "is_pep": random.choices([0, 1], weights=[95, 5])[0],  # politically exposed person
        })
    return pd.DataFrame(customers)

# ── Transactions ───────────────────────────────────────────────────────────────
def generate_transactions(customers_df):
    start_date = datetime(2023, 1, 1)
    transactions = []

    for i in range(N_TRANSACTIONS):
        cust = customers_df.sample(1).iloc[0]
        txn_type = random.choice(TRANSACTION_TYPES)
        country = random.choices(COUNTRIES, weights=[25,12,10,10,8,8,9,8,7,3])[0]
        amount = np.random.lognormal(mean=6, sigma=2)  # realistic skewed amounts
        amount = round(min(amount, 500000), 2)

        date = start_date + timedelta(days=random.randint(0, 364),
                                      hours=random.randint(0, 23),
                                      minutes=random.randint(0, 59))

        # ── Fraud flag logic (rule-based with randomness) ──────────────────
        fraud_score = 0
        if txn_type in HIGH_RISK_TYPES:           fraud_score += 20
        if country in HIGH_RISK_COUNTRIES:         fraud_score += 25
        if cust["risk_score"] > 70:                fraud_score += 20
        if amount > 9000 and amount < 10000:       fraud_score += 30  # structuring
        if amount > 50000:                         fraud_score += 15
        if cust["kyc_verified"] == 0:              fraud_score += 20
        if cust["is_pep"] == 1:                    fraud_score += 10
        if date.hour < 5 or date.hour > 22:        fraud_score += 10  # odd hours

        fraud_score += random.gauss(0, 10)
        is_fraud = 1 if fraud_score > 60 else 0

        # Add noise: ~2% random fraud
        if random.random() < 0.02:
            is_fraud = 1

        transactions.append({
            "transaction_id": f"TXN{str(i+1).zfill(7)}",
            "customer_id": cust["customer_id"],
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "currency": "USD",
            "transaction_type": txn_type,
            "country": country,
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "is_fraud": is_fraud,
            "fraud_score": round(min(max(fraud_score, 0), 100), 2)
        })

    return pd.DataFrame(transactions)

if __name__ == "__main__":
    print("Generating customers...")
    customers_df = generate_customers()

    print("Generating transactions...")
    transactions_df = generate_transactions(customers_df)

    os.makedirs("data", exist_ok=True)
    customers_df.to_csv("data/customers.csv", index=False)
    transactions_df.to_csv("data/transactions.csv", index=False)

    print(f"✅ Generated {len(customers_df)} customers and {len(transactions_df)} transactions.")
    print(f"   Fraud rate: {transactions_df['is_fraud'].mean()*100:.1f}%")
    print("   Saved to data/customers.csv and data/transactions.csv")
