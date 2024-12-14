from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from code.extract_reddit_api import extract_reddit_data, transform_reddit_data
from code.load_to_s3 import load_to_s3

with DAG(
    dag_id="reddit_dag",
    description="Reddit extract data & load into AWS S3",
    start_date=datetime(2024, 12, 14),
    schedule_interval='@daily',
) as dag:
    
    extract_reddit_api = PythonOperator(
        task_id = 'extract_reddit_api',
        python_callable=extract_reddit_data,
        dag=dag
    )

    transform_reddit_api = PythonOperator(
        task_id = 'transform_reddit_api',
        python_callable=transform_reddit_data,
        dag=dag
    )

    load_to_s3_api = PythonOperator(
        task_id = 'load_to_s3_api',
        python_callable=load_to_s3,
        dag=dag
    )

extract_reddit_api >> transform_reddit_api >> load_to_s3_api