{{ config(materialized='table') }}

WITH daily_metrics AS (
    SELECT 
        *,
        -- 7-day and 30-day moving averages
        AVG(close_price) OVER(PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7,
        AVG(close_price) OVER(PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30
    FROM {{ ref('stg_nse') }}
)

SELECT 
    *,
    ROUND(close_price - prev_close, 2) AS daily_change,
    ROUND(((close_price - prev_close) / NULLIF(prev_close, 0)) * 100, 2) AS daily_pct_change
FROM daily_metrics