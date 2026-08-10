from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

if '/opt/airflow' not in sys.path:
  sys.path.insert(0, '/opt/airflow')

from scripts.bronze_bmkg import run_bronze_bmkg
from scripts.gold_bmkg import run_gold_bmkg
from scripts.silver_bmkg import run_silver_bmkg
from scripts.load_snowflake import run_load_snowflake
from scripts.notifications import on_failure_callback

default_args = {
    "owner": "shakila",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="bmkg_realtime_disaster_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*0 * * * *",  
    catchup=False,
    tags=["BMKG", 'earthquake', 'snowflake',"production"]
) as dag:

  bronze_task = PythonOperator(
      task_id="bronze_task", python_callable=run_bronze_bmkg
  )

  silver_task = PythonOperator(
      task_id="silver_task", python_callable=run_silver_bmkg
  )

  gold_task = PythonOperator(
      task_id="gold_task", python_callable=run_gold_bmkg
  )
  snowflake_task = PythonOperator(
        task_id='snowflake_task',
        python_callable=run_load_snowflake,
    )

  bronze_task >> silver_task >> gold_task >> snowflake_task