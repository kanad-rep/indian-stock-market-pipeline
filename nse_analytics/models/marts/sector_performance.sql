{{ config(materialized='table') }}

SELECT
    sector,
    trade_date,
    ROUND(AVG(close_price), 2) AS avg_close,
    ROUND(SUM(turnover), 2) AS total_turnover_lacs,
    SUM(volume) AS total_volume,
    COUNT(DISTINCT symbol) AS stock_count
FROM {{ ref('stg_nse') }}
GROUP BY 1, 2