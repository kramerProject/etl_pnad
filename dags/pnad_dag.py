from datetime import timedelta
from email.policy import default
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from pipeline import extract_from_mongo, load_raw_to_s3, transform_data, load_to_dw


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'pnad_dag',
    default_args=default_args,
    description='pnad etl'
)


extract = PythonOperator(
    task_id='extract_from_mongo',
    python_callable=extract_from_mongo,
    dag=dag
)



load_raw = PythonOperator(
    task_id='load_raw',
    python_callable=load_raw_to_s3,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_dw = PythonOperator(
    task_id='load_dw',
    python_callable=load_to_dw,
    dag=dag
)

extract >> load_raw >> transform >> load_dw