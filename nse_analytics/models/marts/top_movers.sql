{{ config(materialized='table') }}

SELECT
    symbol,
    sector,
    trade_date,
    close_price,
    prev_close,
    ROUND(close_price - prev_close, 2) AS price_change,
    ROUND(((close_price - prev_close) / NULLIF(prev_close, 0)) * 100, 2) AS pct_change,
    volume,
    turnover
FROM {{ ref('stg_nse') }}
-- Usually, we want the most recent day's movers for a dashboard
WHERE trade_date = (SELECT MAX(trade_date) FROM {{ ref('stg_nse') }})