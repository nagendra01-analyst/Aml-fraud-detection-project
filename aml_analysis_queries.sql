-- ============================================================
-- AML Fraud Detection — SQL Analysis Queries
-- Database: aml_fraud.db (SQLite)
-- Author: Nagendra Babu Gugulothu
-- ============================================================


-- ── 1. Overall Fraud Summary ─────────────────────────────────────────────────
SELECT
    COUNT(*)                                        AS total_transactions,
    SUM(is_fraud)                                   AS total_fraud_cases,
    ROUND(AVG(is_fraud) * 100, 2)                   AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud=1 THEN amount END), 2) AS total_fraud_amount,
    ROUND(AVG(CASE WHEN is_fraud=1 THEN amount END), 2) AS avg_fraud_amount
FROM fact_transactions;


-- ── 2. Fraud by Transaction Type ─────────────────────────────────────────────
SELECT
    transaction_type,
    COUNT(*)                            AS total_txns,
    SUM(is_fraud)                       AS fraud_count,
    ROUND(AVG(is_fraud) * 100, 2)       AS fraud_rate_pct,
    ROUND(SUM(amount), 2)               AS total_amount
FROM fact_transactions
GROUP BY transaction_type
ORDER BY fraud_rate_pct DESC;


-- ── 3. High-Risk Country Analysis ────────────────────────────────────────────
SELECT
    country,
    COUNT(*)                            AS total_txns,
    SUM(is_fraud)                       AS fraud_count,
    ROUND(AVG(is_fraud) * 100, 2)       AS fraud_rate_pct,
    ROUND(AVG(amount), 2)               AS avg_txn_amount
FROM fact_transactions
GROUP BY country
ORDER BY fraud_rate_pct DESC;


-- ── 4. Structuring Detection (Transactions just below $10,000) ───────────────
SELECT
    customer_id,
    COUNT(*)                            AS structuring_txns,
    ROUND(SUM(amount), 2)               AS total_structured_amount,
    ROUND(AVG(amount), 2)               AS avg_amount,
    MIN(date)                           AS first_occurrence,
    MAX(date)                           AS last_occurrence
FROM fact_transactions
WHERE amount >= 9000 AND amount < 10000
GROUP BY customer_id
HAVING COUNT(*) >= 2
ORDER BY structuring_txns DESC
LIMIT 20;


-- ── 5. Top High-Risk Customers ───────────────────────────────────────────────
SELECT
    cs.customer_id,
    cs.total_transactions,
    ROUND(cs.total_amount, 2)           AS total_amount,
    cs.fraud_count,
    ROUND(cs.fraud_rate * 100, 2)       AS fraud_rate_pct,
    cs.structuring_count,
    dc.risk_tier,
    dc.kyc_status,
    dc.is_pep
FROM customer_summary cs
JOIN dim_customers dc ON cs.customer_id = dc.customer_id
WHERE cs.fraud_count > 0
ORDER BY cs.fraud_rate DESC, cs.fraud_count DESC
LIMIT 25;


-- ── 6. Monthly Fraud Trend ───────────────────────────────────────────────────
SELECT
    SUBSTR(date, 1, 7)                  AS year_month,
    COUNT(*)                            AS total_txns,
    SUM(is_fraud)                       AS fraud_count,
    ROUND(AVG(is_fraud) * 100, 2)       AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud=1 THEN amount END), 2) AS fraud_amount
FROM fact_transactions
GROUP BY year_month
ORDER BY year_month;


-- ── 7. Fraud by Hour of Day (Anomaly Detection) ──────────────────────────────
SELECT
    CAST(SUBSTR(date, 12, 2) AS INTEGER) AS hour_of_day,
    COUNT(*)                             AS total_txns,
    SUM(is_fraud)                        AS fraud_count,
    ROUND(AVG(is_fraud) * 100, 2)        AS fraud_rate_pct
FROM fact_transactions
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- ── 8. Merchant Category Risk Analysis ──────────────────────────────────────
SELECT
    merchant_category,
    COUNT(*)                            AS total_txns,
    SUM(is_fraud)                       AS fraud_count,
    ROUND(AVG(is_fraud) * 100, 2)       AS fraud_rate_pct,
    ROUND(AVG(amount), 2)               AS avg_amount
FROM fact_transactions
GROUP BY merchant_category
ORDER BY fraud_rate_pct DESC;


-- ── 9. AML Alert Summary by Alert Reason ────────────────────────────────────
SELECT
    alert_reason,
    COUNT(*)                            AS alert_count,
    ROUND(SUM(amount), 2)               AS total_amount,
    ROUND(AVG(fraud_score), 2)          AS avg_fraud_score
FROM aml_alerts
GROUP BY alert_reason
ORDER BY alert_count DESC;


-- ── 10. KYC Unverified High-Value Transactions ───────────────────────────────
SELECT
    ft.transaction_id,
    ft.customer_id,
    ft.date,
    ROUND(ft.amount, 2)                 AS amount,
    ft.transaction_type,
    ft.country,
    ft.is_fraud,
    dc.kyc_status,
    dc.risk_tier
FROM fact_transactions ft
JOIN dim_customers dc ON ft.customer_id = dc.customer_id
WHERE dc.kyc_verified = 0
  AND ft.amount > 10000
ORDER BY ft.amount DESC
LIMIT 30;
