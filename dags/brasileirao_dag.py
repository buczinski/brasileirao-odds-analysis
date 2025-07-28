from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the project root to Python path at the beginning of the file
import sys
import os
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

# Now import your modules
try:
    from src.data_collection.fetch_odds import fetch_odds
    from src.data_collection.fetch_results import fetch_brasileirao_results
    from src.data_processing.joining_data import join_data
    print("Imports successful")
except ImportError as e:
    print(f"Import error: {e}")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'brasileirao_odds_analysis',
    default_args=default_args,
    description='Fetch and process Brasileirão odds and results',
    schedule='0 23 * * 1',  # Run weekly on Mondays at 23:00 UTC
    catchup=False
)

# Define API key (consider using Airflow Variables or environment variables)
API_KEY = 'e791c01b3f81d55c9bf9c4134503fe56'

# Define task functions with proper paths for Airflow
def fetch_odds_task():
    data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'data/raw')
    data_dir = os.path.expanduser(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    return fetch_odds(API_KEY, f'{data_dir}/brasileirao_odds.csv')

def fetch_results_task():
    data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'data/raw')
    data_dir = os.path.expanduser(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    return fetch_brasileirao_results(f'{data_dir}/brasileirao_results.csv')

def join_data_task():
    raw_dir = '/opt/airflow/data/raw'
    processed_dir = '/opt/airflow/data/processed'
    os.makedirs(processed_dir, exist_ok=True)
    return join_data(
        f'{raw_dir}/brasileirao_odds.csv',
        f'{raw_dir}/brasileirao_results.csv',
        f'{processed_dir}/brasileirao_odds_results.csv'
    )

# Create tasks
t1 = PythonOperator(
    task_id='fetch_odds',
    python_callable=fetch_odds_task,
    dag=dag,
)

t2 = PythonOperator(
    task_id='fetch_results',
    python_callable=fetch_results_task,
    dag=dag,
)

t3 = PythonOperator(
    task_id='join_data',
    python_callable=join_data_task,
    dag=dag,
)

# Set dependencies
[t1, t2] >> t3  # t3 depends on both t1 and t2