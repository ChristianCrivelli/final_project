-- 1. Monthly revenue
CREATE OR ALTER VIEW mart.monthly_revenue AS
SELECT
    YEAR(f.date) AS year,
    MONTH(f.date) AS month,
    DATENAME(MONTH, f.date) AS month_name,
    COUNT(*) AS total_transactions,
    SUM(f.amount) AS total_revenue,
    SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END) AS total_refunds,
    SUM(CASE WHEN f.is_refund = 1 THEN f.amount ELSE 0 END) AS refund_amount
FROM curated.facts_table f
WHERE f.is_refund = 0
GROUP BY YEAR(f.date), MONTH(f.date), DATENAME(MONTH, f.date);
GO

-- 2. Refund rate
CREATE OR ALTER VIEW mart.finance_refund_rate AS
SELECT
    YEAR(f.date) AS year,
    MONTH(f.date) AS month,
    DATENAME(MONTH, f.date) AS month_name,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END) AS refund_count,
    CAST(
    ROUND(
        100.0 * SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        2
    ) AS DECIMAL(5,2)
) AS refund_rate
FROM curated.facts_table f
GROUP BY YEAR(f.date), MONTH(f.date), DATENAME(MONTH, f.date);
GO

-- 3. Revenue by state
CREATE OR ALTER VIEW mart.finance_revenue_by_state AS
SELECT
    m.state,
    m.country,
    COUNT(*) AS total_transactions,
    SUM(f.amount) AS total_revenue,
    AVG(f.amount) AS avg_transaction_value
FROM curated.facts_table f
JOIN curated.dim_merchant m ON f.merchant_id = m.id
WHERE f.is_refund = 0
  AND m.state IS NOT NULL
GROUP BY m.state, m.country;
GO

-- 4. Revenue by category
CREATE OR ALTER VIEW mart.finance_revenue_by_category AS
SELECT
    m.id AS category_id,
    m.description AS mcc_description,
    COUNT(*) AS total_transactions,
    SUM(f.amount) AS total_revenue,
    AVG(f.amount) AS avg_transaction_value,
    SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END) AS refund_count
FROM curated.facts_table f
JOIN curated.dim_merchant m ON f.merchant_id = m.id
WHERE m.description IS NOT NULL
GROUP BY m.id, m.description;
GO