from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.blob_utils.azure_blob_to_snowflake import ingest

with DAG(
    dag_id="EXT_BLOB_HCS_DISEASE",
    start_date=datetime(2024,1,1),
    schedule_interval="*/5 * * * *",
    catchup=False
) as dag:

    PythonOperator(
        task_id="blob_disease_ingest",
        python_callable=ingest,
        op_args=["disease","HCS_DISEASE_CORE"]
    )
