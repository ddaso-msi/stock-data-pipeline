SELECT
    trade_date,
    ticker,
    sector,
    close_price,
    LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_close,
    ROUND(
        (((close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date)) 
        / LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date)) * 100)::numeric, 
    2) AS daily_return_pct,
    volume
FROM {{ ref('stg_stock_prices') }}