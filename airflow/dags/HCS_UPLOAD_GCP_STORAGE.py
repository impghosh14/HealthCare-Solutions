# airflow/dags/HCS_UPLOAD_GCP_STORAGE.py
from datetime import datetime
import json
import logging
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from src.gcp_utils.upload_gcp import upload_file as upload_gcp_file

logger = logging.getLogger(__name__)
DEFAULT_ARGS = {"owner": "hcs", "depends_on_past": False, "retries": 0}
PAYLOAD_DIR = Path("/opt/airflow/app/payload")

def _load_payload_for_dag(dag_id: str) -> dict:
    if not PAYLOAD_DIR.exists():
        raise AirflowFailException(f"Payload directory not found: {PAYLOAD_DIR}")
    exact = PAYLOAD_DIR / f"{dag_id}_default.json"
    if exact.exists():
        p = exact
    else:
        candidates = sorted(PAYLOAD_DIR.glob(f"{dag_id}*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        if candidates:
            p = candidates[0]
        else:
            raise AirflowFailException(f"No payload found for DAG '{dag_id}' in {PAYLOAD_DIR}.")
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        raise AirflowFailException(f"Failed to parse payload {p}: {e}")

def run_upload(local_path: str = None, gcs_prefix: str = None, **kwargs):
    dag_run = kwargs.get("dag_run")
    conf = (dag_run.conf if dag_run else {}) or {}
    dag_id = kwargs.get("dag").dag_id if kwargs.get("dag") else "HCS_UPLOAD_GCP_STORAGE"

    payload = {}
    try:
        payload = _load_payload_for_dag(dag_id)
        logger.info("Loaded payload for %s: %s", dag_id, payload)
    except AirflowFailException as e:
        logger.warning("Payload load failed: %s", e)
        payload = {}

    final_local = conf.get("local_path") or payload.get("local_path") or local_path
    final_prefix = conf.get("gcs_prefix") or payload.get("gcs_prefix") or gcs_prefix or ""
    final_bucket = conf.get("bucket_name") or payload.get("bucket_name") or None

    if not final_local:
        raise AirflowFailException("Missing required 'local_path' (path inside container).")

    try:
        uri = upload_gcp_file(local_path=final_local, bucket_name=final_bucket, gcs_prefix=final_prefix)
        kwargs["ti"].xcom_push(key="gcs_uri", value=uri)
        logger.info("Upload succeeded: %s", uri)
    except Exception:
        logger.exception("GCP upload failed")
        raise

with DAG(
    dag_id="HCS_UPLOAD_GCP_STORAGE",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["HCS", "upload", "gcp"]
) as dag:

    t_upload = PythonOperator(
        task_id="upload_to_gcp_storage",
        python_callable=run_upload,
        provide_context=True,
    )

    t_upload
