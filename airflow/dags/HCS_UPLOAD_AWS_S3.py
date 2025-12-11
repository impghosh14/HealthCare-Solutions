# airflow/dags/HCS_UPLOAD_AWS_S3.py
from datetime import datetime
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Import uploader (adjust import path to your project structure)
from src.s3_utils.upload_s3 import upload_file

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "hcs",
    "depends_on_past": False,
    "retries": 0
}

def run_upload(**context):
    """
    Expects DAG run conf (JSON) with keys:
    - local_path: absolute or container-visible path to file (required)
    - s3_folder: folder inside bucket (optional)
    - s3_bucket: optional override of bucket (falls back to .env S3_BUCKET)
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else {}) or context.get("params") or {}

    logger.info("Received conf: %s", conf)
    local_path = conf.get("local_path")
    s3_folder = conf.get("s3_folder", "")
    s3_bucket = conf.get("s3_bucket", None)

    if not local_path:
        raise AirflowFailException("Missing required parameter 'local_path' in DAG run conf.")

    try:
        s3_key = upload_file(local_path=local_path, s3_bucket=s3_bucket, s3_folder=s3_folder)
        # push result to XCom for other tasks if needed
        context['ti'].xcom_push(key='s3_key', value=s3_key)
        logger.info("Upload done, s3_key=%s", s3_key)
    except Exception as e:
        logger.exception("Upload failed: %s", e)
        # fail DAG so Airflow registers failure
        raise

with DAG(
    dag_id="HCS_UPLOAD_AWS_S3",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,   # manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["HCS", "upload", "s3"]
) as dag:

    t_upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=run_upload,
        provide_context=True
    )

    t_upload
