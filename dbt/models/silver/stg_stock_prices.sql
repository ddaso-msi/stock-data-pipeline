SELECT
    "Date"::date AS trade_date,
    ticker,
    sector,
    "Open" AS open_price,
    "High" AS high_price,
    "Low" AS low_price,
    "Close" AS close_price,
    "Volume" AS volume
FROM public.raw_stock_prices