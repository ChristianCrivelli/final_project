-- Tables: curated.facts_table, curated.dim_merchant
-- Note: No dim_date yet (grouping by year/month extracted from date)

-- 1. Monthly revenue
-- Q: What is our total revenue by month?
CREATE OR ALTER VIEW curated.mart_finance_monthly_revenue AS
SELECT
    YEAR(f.date)                                        AS year,
    MONTH(f.date)                                       AS month,
    DATENAME(MONTH, f.date)                             AS month_name,
    COUNT(*)                                            AS total_transactions,
    SUM(f.amount)                                       AS total_revenue,
    SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END)   AS total_refunds,
    SUM(CASE WHEN f.is_refund = 1 THEN f.amount ELSE 0 END) AS refund_amount
FROM curated.facts_table f
WHERE f.is_refund = 0
GROUP BY YEAR(f.date), MONTH(f.date), DATENAME(MONTH, f.date);
GO

-- 2. Refund rate
-- Q: What percentage of transactions are refunds?
CREATE OR ALTER VIEW curated.mart_finance_refund_rate AS
SELECT
    YEAR(f.date)                                        AS year,
    MONTH(f.date)                                       AS month,
    DATENAME(MONTH, f.date)                             AS month_name,
    COUNT(*)                                            AS total_transactions,
    SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END)   AS refund_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        2
    )                                                   AS refund_rate_pct
FROM curated.facts_table f
GROUP BY YEAR(f.date), MONTH(f.date), DATENAME(MONTH, f.date);
GO

-- 3. Revenue by state
-- Q: Which states generate the most revenue?
CREATE OR ALTER VIEW curated.mart_finance_revenue_by_state AS
SELECT
    m.state,
    m.country,
    COUNT(*)                                            AS total_transactions,
    SUM(f.amount)                                       AS total_revenue,
    AVG(f.amount)                                       AS avg_transaction_value
FROM curated.facts_table f
JOIN curated.dim_merchant m ON f.merchant_id = m.id
WHERE f.is_refund = 0
  AND m.state IS NOT NULL
GROUP BY m.state, m.country;
GO

-- 4. Revenue by MCC category
-- Q: Which merchant categories drive the highest spending?
CREATE OR ALTER VIEW curated.mart_finance_revenue_by_category AS
SELECT
    m.mcc_id,
    m.description                                       AS mcc_description,
    COUNT(*)                                            AS total_transactions,
    SUM(f.amount)                                       AS total_revenue,
    AVG(f.amount)                                       AS avg_transaction_value,
    SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END)   AS refund_count
FROM curated.facts_table f
JOIN curated.dim_merchant m ON f.merchant_id = m.id
WHERE m.description IS NOT NULL
GROUP BY m.mcc_id, m.description;
GO
