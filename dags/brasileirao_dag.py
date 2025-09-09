from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Direct imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'scr'))

# DAG metadata
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DOC_MD = """
### Brasileirão Odds Analysis
Pipeline:
1. fetch_odds   -> Pull odds and append to S3 Parquet.
2. fetch_results-> Pull match results and append to S3 Parquet.
3. join_data    -> Join odds + results and write joined Parquet.

All credentials & bucket come from Airflow Variables:
- BRASILEIRAO_DATA_BUCKET
- ODDS_API_KEY
- FOOTBALL_DATA_API_KEY
"""

dag = DAG(
    dag_id='brasileirao_odds_analysis',
    default_args=default_args,
    description='Fetch and process Brasileirão odds and results',
    schedule='0 02 * * 2',   # Tuesdays 02:00 UTC
    catchup=False,
    doc_md=DOC_MD,
    tags=['brasileirao', 'odds', 'football']
)

# Task callables (lazy imports & variable access)
def fetch_odds_task():
    from airflow.models import Variable
    from scr.fetch_odds import fetch_odds
    bucket = Variable.get("BRASILEIRAO_DATA_BUCKET")
    api_key = Variable.get("ODDS_API_KEY")
    odds_path = f"s3://{bucket}/data/raw/odds/brasileirao_odds.parquet"
    fetch_odds(api_key, odds_path)
    return None

def fetch_results_task():
    from airflow.models import Variable
    from scr.fetch_results import fetch_brasileirao_results
    bucket = Variable.get("BRASILEIRAO_DATA_BUCKET")
    fd_key = Variable.get("FOOTBALL_DATA_API_KEY")
    os.environ["FOOTBALL_DATA_API_KEY"] = fd_key  # fetch_results reads env
    results_path = f"s3://{bucket}/data/raw/results/brasileirao_results.parquet"
    fetch_brasileirao_results(only_finished=False, output_file=results_path)
    return None

def join_data_task():
    from airflow.models import Variable
    from scr.joining_data import join_data
    bucket = Variable.get("BRASILEIRAO_DATA_BUCKET")
    odds_path = f"s3://{bucket}/data/raw/odds/brasileirao_odds.parquet"
    results_path = f"s3://{bucket}/data/raw/results/brasileirao_results.parquet"
    joined_path = f"s3://{bucket}/data/processed/brasileirao_odds_results.parquet"
    join_data(odds_path, results_path, joined_path)
    return None

t_fetch_odds = PythonOperator(
    task_id='fetch_odds',
    python_callable=fetch_odds_task,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

t_fetch_results = PythonOperator(
    task_id='fetch_results',
    python_callable=fetch_results_task,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

t_join = PythonOperator(
    task_id='join_data',
    python_callable=join_data_task,
    dag=dag,
    execution_timeout=timedelta(minutes=10)
)

[t_fetch_odds, t_fetch_results] >> t_join