from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/dags/scripts")

default_args = {
    "owner": "debrishi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_pipeline",
    default_args=default_args,
    description="Daily stock data pipeline",
    schedule="0 16 * * 1-5",
    start_date=datetime(2026, 3, 14),
    catchup=False,
) as dag:

    def ingest_daily():
        from daily_ingest import run
        run()

    ingest = PythonOperator(
        task_id="ingest_daily_data",
        python_callable=ingest_daily,
    )

    transform = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt_profiles",
    )

    test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt_profiles",
    )

    ingest >> transform >> test