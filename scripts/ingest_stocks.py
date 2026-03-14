"""
Step 1: Historical Data Backfill
Pull 3 years of daily OHLCV data for selected NSE stocks using yfinance.
Saves raw data as parquet files partitioned by ticker.
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
OUTPUT_DIR = Path("raw_data/stock_prices")
PERIOD = "3y"

TICKERS = {
    # IT
    "TCS.NS": "IT",
    "INFY.NS": "IT",
    "WIPRO.NS": "IT",
    "HCLTECH.NS": "IT",
    # Banking
    "HDFCBANK.NS": "Banking",
    "ICICIBANK.NS": "Banking",
    "SBIN.NS": "Banking",
    "KOTAKBANK.NS": "Banking",
    # Pharma
    "SUNPHARMA.NS": "Pharma",
    "DRREDDY.NS": "Pharma",
    "CIPLA.NS": "Pharma",
    "GRANULES.NS": "Pharma",
    # FMCG
    "HINDUNILVR.NS": "FMCG",
    "ITC.NS": "FMCG",
    "NESTLEIND.NS": "FMCG",
    # Auto
    "MARUTI.NS": "Auto",
    "TATAMOTORS.NS": "Auto",
    "M&M.NS": "Auto",
    # Infra / Conglomerate
    "RELIANCE.NS": "Conglomerate",
    "LT.NS": "Infra",
    "ADANIENT.NS": "Infra",
    "BHARTIARTL.NS": "Telecom",
    # Index
    "^NSEI": "Index",  # Nifty 50
}


def fetch_stock_data(ticker: str, period: str) -> pd.DataFrame | None:
    """Fetch OHLCV data for a single ticker."""
    try:
        data = yf.download(ticker, period=period, progress=False)
        if data.empty:
            print(f"  WARNING: No data returned for {ticker}")
            return None
        
        # yfinance returns MultiIndex columns when downloading single ticker too
        # Flatten if needed
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        data = data.reset_index()
        data["ticker"] = ticker
        data["sector"] = TICKERS[ticker]
        data["ingested_at"] = datetime.utcnow().isoformat()
        
        return data
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_data = []
    success, failed = 0, 0
    
    print(f"Fetching {len(TICKERS)} tickers | Period: {PERIOD}\n")
    
    for ticker in TICKERS:
        print(f"  Fetching {ticker}...", end=" ")
        df = fetch_stock_data(ticker, PERIOD)
        
        if df is not None:
            rows = len(df)
            all_data.append(df)
            success += 1
            print(f"OK ({rows} rows)")
        else:
            failed += 1
            print("FAILED")
    
    # Combine and save
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        
        # Save as single parquet (backfill)
        out_path = OUTPUT_DIR / "backfill_3y.parquet"
        combined.to_parquet(out_path, index=False)
        
        # Also save a CSV for quick inspection
        csv_path = OUTPUT_DIR / "backfill_3y.csv"
        combined.to_csv(csv_path, index=False)
        
        print(f"\n--- SUMMARY ---")
        print(f"Tickers:  {success} OK, {failed} failed")
        print(f"Rows:     {len(combined)}")
        print(f"Date range: {combined['Date'].min()} to {combined['Date'].max()}")
        print(f"Parquet:  {out_path}")
        print(f"CSV:      {csv_path}")
        print(f"\nSample:\n{combined.head()}")
    else:
        print("No data fetched.")


if __name__ == "__main__":
    main()