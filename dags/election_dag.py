from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime,timedelta
default_args={

    'owner':'aditya',
    'retry':2,
    'retry_delay':timedelta(seconds=20)


}





with DAG(
    dag_id='election_dag_v3',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024,5,24)
) as dag:
    start_pipeline=DummyOperator(
        task_id='start-pipeline'

    )

    python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/tranform.py",
    )


    start_pipeline>>python_job



