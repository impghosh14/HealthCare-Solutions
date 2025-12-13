from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.s3_utils.s3_to_snowflake import ingest

with DAG("EXT_S3_HCS_DOCTOR",
         start_date=datetime(2024,1,1),
         schedule_interval="*/5 * * * *",
         catchup=False) as dag:

    PythonOperator(
        task_id="doctor_ingest",
        python_callable=ingest,
        op_args=["doctor","HCS_DOCTOR_CORE"]
    )
