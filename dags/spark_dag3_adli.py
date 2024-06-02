from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pathlib import Path
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Load environment variables
def load_env():
    dotenv_path = Path('../.env')
    load_dotenv(dotenv_path=dotenv_path)
    print('dotenv loaded')

# Initialize the DAG
dag = DAG(
    dag_id='Assignment_BatchProcessing',
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    description='Assignment Batch Processing Spark',
    schedule_interval=None,
    start_date=days_ago(1),
)

# Task to load environment variables
load_env_task = PythonOperator(
    task_id='load_env',
    python_callable=load_env,
    dag=dag,
)

# Task to run the PySpark script
Extract = SparkSubmitOperator(
    application='/spark-scripts/Assignment_Spark_BatchProcessing.py',
    conn_id="spark_main",
    task_id="spark_task",
    dag=dag
)

# Set up task dependencies
load_env_task >> Extract
