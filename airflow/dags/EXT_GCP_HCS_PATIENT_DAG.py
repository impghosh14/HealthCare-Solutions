from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.gcp_utils.gcp_to_snowflake import ingest

with DAG(
    dag_id="EXT_GCP_HCS_PATIENT",
    start_date=datetime(2024,1,1),
    schedule_interval="*/5 * * * *",
    catchup=False
) as dag:

    PythonOperator(
        task_id="gcp_doctor_ingest",
        python_callable=ingest,
        op_args=["patient","HCS_PATIENT_CORE"]
    )
