SELECT
    trade_date,
    sector,
    COUNT(DISTINCT ticker) AS num_stocks,
    ROUND(AVG(daily_return_pct)::numeric, 2) AS avg_return_pct,
    ROUND(MIN(daily_return_pct)::numeric, 2) AS worst_return_pct,
    ROUND(MAX(daily_return_pct)::numeric, 2) AS best_return_pct,
    SUM(volume) AS total_volume
FROM {{ ref('daily_returns') }}
WHERE daily_return_pct IS NOT NULL
GROUP BY trade_date, sector
ORDER BY trade_date DESC, sector