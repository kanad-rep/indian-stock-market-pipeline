{{ config(materialized='view') }}

SELECT
    TRIM(SYMBOL) AS symbol,
    TRIM(SERIES) AS series,

    TRADE_DATE AS trade_date,

    CAST(PREV_CLOSE AS FLOAT64) AS prev_close,
    CAST(OPEN_PRICE AS FLOAT64) AS open_price,
    CAST(HIGH_PRICE AS FLOAT64) AS high_price,
    CAST(LOW_PRICE AS FLOAT64) AS low_price,
    CAST(CLOSE_PRICE AS FLOAT64) AS close_price,

    CAST(TTL_TRD_QNTY AS INT64) AS volume,
    CAST(TURNOVER_LACS AS FLOAT64) AS turnover,

    CAST(NO_OF_TRADES AS INT64) AS trades,
    CAST(DELIV_QTY AS INT64) AS delivery_qty,
    CAST(DELIV_PER AS FLOAT64) AS delivery_pct,

    TRIM(SECTOR) AS sector,

    {{ dbt_utils.generate_surrogate_key(['symbol', 'trade_date']) }} AS nse_id

-- Use the source function instead of a hardcoded string
FROM {{ source('staging', 'nse_processed_external') }}
WHERE TRIM(SERIES) = 'EQ'