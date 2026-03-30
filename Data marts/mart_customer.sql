-- Tables: curated.facts_table, curated.dim_users, curated.dim_cards

-- 1. Customer lifetime value
-- Q: What is the lifetime value of each customer?

CREATE OR ALTER VIEW mart.customer_ltv AS
SELECT
    u.user_key,
    u.id                                                AS user_id,
    u.gender,
    u.employment_status,
    u.education_level,
    u.credit_score,
    u.yearly_income,
    COUNT(*)                                            AS total_transactions,
    SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END) AS lifetime_spend,
    SUM(CASE WHEN f.is_refund = 1 THEN f.amount ELSE 0 END) AS total_refunded,
    AVG(CASE WHEN f.is_refund = 0 THEN f.amount END)   AS avg_transaction_value,
    MIN(f.date)                                         AS first_transaction_date,
    MAX(f.date)                                         AS last_transaction_date
FROM curated.facts_table f
JOIN curated.dim_users u ON f.client_id = u.id
GROUP BY
    u.user_key, u.id, u.gender,
    u.employment_status, u.education_level,
    u.credit_score, u.yearly_income;
GO

-- 2. Online vs in-store behaviour
-- Q: How do customers behave online vs in-store?
CREATE OR ALTER VIEW mart.customer_channel_behaviour AS
SELECT
    u.user_key,
    u.id                                                AS user_id,
    f.use_chip                                          AS channel,
    COUNT(*)                                            AS transaction_count,
    SUM(f.amount)                                       AS total_spend,
    AVG(f.amount)                                       AS avg_spend,
    ROUND(
        100.0 * COUNT(*)
              / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY u.user_key), 0),
        2
    )                                                   AS channel_pct
FROM curated.facts_table f
JOIN curated.dim_users u ON f.client_id = u.id
WHERE f.is_refund = 0
GROUP BY u.user_key, u.id, f.use_chip;
GO

-- 3. Active cards per customer
-- Q: How many active cards does a typical customer have?
CREATE OR ALTER VIEW mart.customer_active_cards AS
SELECT
    u.user_key,
    u.id                                                AS user_id,
    u.num_credit_cards                                  AS reported_num_cards,
    COUNT(DISTINCT c.id)                                AS cards_with_transactions,
    COUNT(DISTINCT CASE
        WHEN c.card_on_dark_web = 'No'  THEN c.id
    END)                                                AS safe_cards,
    COUNT(DISTINCT CASE
        WHEN c.card_on_dark_web = 'Yes' THEN c.id
    END)                                                AS compromised_cards
FROM curated.dim_users u
JOIN curated.dim_cards c     ON c.client_id = u.id
JOIN curated.facts_table f   ON f.card_id   = c.id
GROUP BY u.user_key, u.id, u.num_credit_cards;
GO

-- 4. Suspicious transaction patterns
-- Q: Can we identify suspicious transaction patterns?

CREATE OR ALTER VIEW mart.customer_suspicious_transactions AS -- statistical outlier + compromised card + error recorded
WITH customer_stats AS (
    SELECT
        f.client_id,
        AVG(f.amount)   AS avg_amount,
        STDEV(f.amount) AS stdev_amount
    FROM curated.facts_table f
    WHERE f.is_refund = 0
    GROUP BY f.client_id
)
SELECT
    f.transaction_key,
    f.client_id,
    f.card_id,
    f.merchant_id,
    f.date,
    f.amount,
    c.card_on_dark_web,
    cs.avg_amount,
    cs.stdev_amount,
    CASE WHEN f.amount > cs.avg_amount + 2 * cs.stdev_amount
         THEN 1 ELSE 0 END                              AS is_amount_outlier,
    CASE WHEN c.card_on_dark_web = 'Yes'
         THEN 1 ELSE 0 END                              AS is_card_compromised,
    -- composite risk score 0-2 (no errors column in facts_table)
    CASE WHEN f.amount > cs.avg_amount + 2 * cs.stdev_amount
         THEN 1 ELSE 0 END
    + CASE WHEN c.card_on_dark_web = 'Yes'
           THEN 1 ELSE 0 END                            AS risk_score
FROM curated.facts_table f
JOIN curated.dim_cards c    ON f.card_id    = c.id
JOIN customer_stats cs      ON f.client_id  = cs.client_id
WHERE f.is_refund = 0;
GO