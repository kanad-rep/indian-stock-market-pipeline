{{ config(materialized='table') }}

WITH monthly_metrics AS (
    SELECT
        symbol,
        sector,
        -- Truncates the date to the first of the month
        DATE_TRUNC(trade_date, MONTH) AS month_date,
        AVG(close_price) AS avg_monthly_close,
        SUM(volume) AS total_monthly_volume
    FROM {{ ref('stg_nse') }}
    GROUP BY 1, 2, 3
)

SELECT
    *,
    -- 1-Month Moving Average (Current Month's Average)
    AVG(avg_monthly_close) OVER(PARTITION BY symbol ORDER BY month_date ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS ma_1_month,
    
    -- 3-Month Moving Average (Average of current + last 2 months)
    AVG(avg_monthly_close) OVER(PARTITION BY symbol ORDER BY month_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma_3_month,
    
    -- 6-Month Moving Average (Average of current + last 5 months)
    AVG(avg_monthly_close) OVER(PARTITION BY symbol ORDER BY month_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ma_6_month
FROM monthly_metrics