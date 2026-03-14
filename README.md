# Stock Data Pipeline

End-to-end data pipeline for Indian stock market data, built with modern data engineering tools and best practices.

## Architecture

```
yfinance API → MinIO (S3) → PostgreSQL → DBT (Medallion) → Analytics
                                ↑
                          Apache Airflow (Orchestration)
```

**Data Flow:**
1. **Ingestion** — Python scripts pull daily OHLCV data for 23 NSE stocks via yfinance
2. **Raw Storage** — Parquet files land in MinIO (S3-compatible object store)
3. **Warehouse Load** — Data is loaded into PostgreSQL
4. **Transformation** — DBT transforms raw data through medallion architecture:
   - **Bronze:** `raw_stock_prices` — raw ingested data
   - **Silver:** `stg_stock_prices` — cleaned, typed, renamed columns
   - **Gold:** `daily_returns` — per-stock daily percentage returns | `sector_daily_summary` — sector-level aggregated performance
5. **Testing** — DBT runs automated data quality checks after every transformation
6. **Orchestration** — Airflow DAG runs the full pipeline daily at 4 PM IST (weekdays)

## Tech Stack

| Component | Tool | Purpose |
|-----------|------|---------|
| Ingestion | yfinance | Pull NSE stock data |
| Raw Storage | MinIO | S3-compatible object store |
| Warehouse | PostgreSQL | Analytical data warehouse |
| Transformation | DBT | SQL-based medallion architecture |
| Orchestration | Apache Airflow | Pipeline scheduling & monitoring |
| Admin UI | pgAdmin | Database query interface |
| Infrastructure | Docker Compose | Containerized deployment |

## Stocks Covered

23 tickers across 8 sectors:

| Sector | Stocks |
|--------|--------|
| IT | TCS, Infosys, Wipro, HCL Tech |
| Banking | HDFC Bank, ICICI Bank, SBI, Kotak Bank |
| Pharma | Sun Pharma, Dr. Reddy's, Cipla, Granules India |
| FMCG | Hindustan Unilever, ITC, Nestle India |
| Auto | Maruti, Tata Motors, M&M |
| Conglomerate | Reliance |
| Infra | L&T, Adani Enterprises |
| Telecom | Bharti Airtel |
| Index | Nifty 50 |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+

### 1. Clone & Start Infrastructure
```bash
git clone git@github.com:ddaso-msi/stock-data-pipeline.git
cd stock-data-pipeline
docker compose up -d
```

This starts MinIO, PostgreSQL, pgAdmin, and Airflow.

### 2. Run Backfill (One-Time)
```bash
pip install yfinance pandas pyarrow boto3 psycopg2-binary sqlalchemy
cd scripts
python ingest_stocks.py
python upload_to_minio.py
python load_to_postgres.py
```

### 3. Run DBT Models
```bash
pip install dbt-postgres
cd dbt
dbt run
dbt test
```

### 4. Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO | http://localhost:9001 | admin / admin123 |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin123 |

## DBT Models

### Silver Layer (Views)
- **stg_stock_prices** — Cleans raw data: renames columns, casts date types, standardizes field names

### Gold Layer (Tables)
- **daily_returns** — Calculates daily percentage return per stock using LAG window function
- **sector_daily_summary** — Aggregates daily performance by sector: avg/min/max returns, total volume

### Data Tests
- Not-null checks on trade_date, ticker, close_price, sector
- Accepted values validation on sector column
- Tests run automatically after each DBT transformation via Airflow

## Airflow DAG

**`stock_pipeline`** — Runs weekdays at 4 PM IST

```
ingest_daily_data → dbt_run → dbt_test
```

1. **ingest_daily_data** — Pulls latest stock data, uploads to MinIO, appends to Postgres
2. **dbt_run** — Runs all silver and gold transformations
3. **dbt_test** — Validates data quality

## Project Structure

```
stock-data-pipeline/
├── dags/
│   ├── scripts/
│   │   └── daily_ingest.py          # Daily ingestion logic
│   └── stock_pipeline_dag.py        # Airflow DAG definition
├── dbt/
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_stock_prices.sql
│   │   │   └── schema.yml
│   │   └── gold/
│   │       ├── daily_returns.sql
│   │       ├── sector_daily_summary.sql
│   │       └── schema.yml
│   └── dbt_project.yml
├── dbt_profiles/
│   └── profiles.yml
├── scripts/
│   ├── ingest_stocks.py              # One-time backfill
│   ├── upload_to_minio.py            # Upload to MinIO
│   └── load_to_postgres.py           # Load to Postgres
├── docker-compose.yml
├── Dockerfile.airflow
└── README.md
```

## Author

**Debrishi Das** — ML Engineer & Data Engineer  
[LinkedIn](https://www.linkedin.com/in/debrishidas/) | [GitHub](https://github.com/ddaso-msi)