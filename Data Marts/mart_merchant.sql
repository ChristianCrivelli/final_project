-- Tables: curated.facts_table, curated.dim_merchant

-- 1. Merchant transaction volume
-- Q: Which merchants generate the highest transaction volume?
CREATE OR ALTER VIEW curated.mart_merchant_volume AS
SELECT
    m.id                                                AS merchant_id,
    m.county,
    m.state,
    m.country,
    m.description                                       AS mcc_description,
    COUNT(*)                                            AS total_transactions,
    SUM(f.amount)                                       AS total_revenue,
    AVG(f.amount)                                       AS avg_transaction_value,
    SUM(CASE WHEN f.is_refund = 1 THEN 1 ELSE 0 END)   AS refund_count,
    COUNT(DISTINCT f.client_id)                         AS unique_customers
FROM curated.facts_table f
JOIN curated.dim_merchant m ON f.merchant_id = m.id
GROUP BY m.id, m.city, m.state, m.country, m.description;
GO

-- 2. Industry growth month over month
-- Q: What industries are growing the fastest?
CREATE OR ALTER VIEW curated.mart_merchant_industry_growth AS
WITH monthly AS (
    SELECT
        m.mcc_id,
        m.description                                   AS mcc_description,
        YEAR(f.date)                                    AS year,
        MONTH(f.date)                                   AS month,
        SUM(f.amount)                                   AS monthly_revenue,
        COUNT(*)                                        AS monthly_transactions
    FROM curated.facts_table f
    JOIN curated.dim_merchant m ON f.merchant_id = m.id
    WHERE f.is_refund = 0
      AND m.description IS NOT NULL
    GROUP BY m.mcc_id, m.description, YEAR(f.date), MONTH(f.date)
)
SELECT
    mcc_id,
    mcc_description,
    year,
    month,
    monthly_revenue,
    monthly_transactions,
    LAG(monthly_revenue) OVER (
        PARTITION BY mcc_id ORDER BY year, month
    )                                                   AS prev_month_revenue,
    ROUND(
        100.0 * (monthly_revenue
            - LAG(monthly_revenue) OVER (PARTITION BY mcc_id ORDER BY year, month))
            / NULLIF(LAG(monthly_revenue) OVER (PARTITION BY mcc_id ORDER BY year, month), 0),
        2
    )                                                   AS mom_growth_pct
FROM monthly;
GO

-- 3. Merchant error rates
-- Q: Which merchants have the highest error rates?
CREATE OR ALTER VIEW curated.mart_merchant_error_rates AS
SELECT
    m.id                                                AS merchant_id,
    m.city,
    m.state,
    m.description                                       AS mcc_description,
    COUNT(*)                                            AS total_transactions,
    SUM(CASE WHEN t.errors IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    ROUND(
        100.0 * SUM(CASE WHEN t.errors IS NOT NULL THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        2
    )                                                   AS error_rate_pct
FROM curated.facts_table f
JOIN curated.dim_merchant  m ON f.merchant_id = m.id   -- errors stored in clean.transactions, not in facts_table
JOIN clean.transactions    t ON f.id          = t.id   -- join back for errors field
GROUP BY m.id, m.city, m.state, m.description;
GO

-- 4. Geographic revenue distribution
-- Q: How is revenue distributed geographically?
CREATE OR ALTER VIEW curated.mart_merchant_geo_revenue AS
SELECT
    m.country,
    m.state,
    m.city,
    COUNT(DISTINCT m.id)                                AS merchant_count,
    COUNT(*)                                            AS total_transactions,
    SUM(f.amount)                                       AS total_revenue,
    AVG(f.amount)                                       AS avg_transaction_value,
    ROUND(
        100.0 * SUM(f.amount)
              / NULLIF(SUM(SUM(f.amount)) OVER (), 0),
        4
    )                                                   AS pct_of_total_revenue
FROM curated.facts_table f
JOIN curated.dim_merchant m ON f.merchant_id = m.id
WHERE f.is_refund = 0 
  AND m.country IS NOT NULL
GROUP BY m.country, m.state, m.city;
GO
