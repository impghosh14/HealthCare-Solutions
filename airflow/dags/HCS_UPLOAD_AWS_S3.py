from datetime import datetime
import json
import logging
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from src.s3_utils.upload_s3 import upload_file

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "hcs",
    "depends_on_past": False,
    "retries": 0
}

# Path inside container where payload files are mounted
PAYLOAD_DIR = Path("/opt/airflow/app/payload")

def _load_payload_for_dag(dag_id: str) -> dict:
    """
    Load JSON payload for dag_id from payload directory.
    Search order:
      1) exact {dag_id}_default.json
      2) any file starting with {dag_id}*.json (choose the newest)
    Returns dict parsed from JSON.
    Raises AirflowFailException if no valid payload found or JSON invalid.
    """
    if not PAYLOAD_DIR.exists():
        raise AirflowFailException(f"Payload directory not found inside container: {PAYLOAD_DIR}")

    exact = PAYLOAD_DIR / f"{dag_id}_default.json"
    candidates = []

    if exact.exists():
        payload_path = exact
    else:
        # find other candidate files starting with dag_id
        for p in sorted(PAYLOAD_DIR.glob(f"{dag_id}*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
            if p.is_file():
                candidates.append(p)
        if candidates:
            payload_path = candidates[0]
        else:
            raise AirflowFailException(f"No payload found for DAG '{dag_id}' in {PAYLOAD_DIR}. "
                                       f"Expected {exact} or {dag_id}*.json")

    try:
        with payload_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as e:
        raise AirflowFailException(f"Failed to read/parse payload file {payload_path}: {e}")

    logger.info("Loaded payload from %s: %s", payload_path, payload)
    return payload

def run_upload(local_path: str = None, s3_folder: str = None, s3_bucket: str = None, **kwargs):
    """
    Main task callable.
    Resolution order for parameters:
      1) dag_run.conf overrides everything (if present)
      2) payload file under /opt/airflow/app/payload/{dag_id}_default.json or {dag_id}*.json
      3) op_kwargs defaults (not used in this version)
    Required final keys: local_path
    """
    dag_run = kwargs.get("dag_run")
    conf = (dag_run.conf if dag_run else {}) or {}
    dag_id = kwargs.get("dag").dag_id if kwargs.get("dag") else "HCS_UPLOAD_AWS_S3"

    # first, attempt to load payload file (if present)
    payload = {}
    try:
        payload = _load_payload_for_dag(dag_id)
    except AirflowFailException as e:
        # If no payload file found, we'll allow conf to supply values; otherwise re-raise later
        logger.warning("Payload not found or invalid: %s", e)
        payload = {}

    # merge precedence: dag_run.conf (highest) > payload file > op_kwargs defaults (none here)
    final_local_path = conf.get("local_path") or payload.get("local_path") or local_path
    final_s3_folder = conf.get("s3_folder") or payload.get("s3_folder") or s3_folder or ""
    final_s3_bucket = conf.get("s3_bucket") or payload.get("s3_bucket") or s3_bucket

    if not final_local_path:
        raise AirflowFailException(
            "Missing required 'local_path'. Provide it via DAG run conf or payload file (see payload schema)."
        )

    # perform the upload
    try:
        s3_uri = upload_file(local_path=final_local_path, s3_bucket=final_s3_bucket, s3_folder=final_s3_folder)
        kwargs["ti"].xcom_push(key="s3_uri", value=s3_uri)
        logger.info("Upload completed. s3_uri=%s", s3_uri)
    except Exception:
        logger.exception("Upload task failed")
        raise

with DAG(
    dag_id="HCS_UPLOAD_AWS_S3",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["HCS", "upload", "s3"]
) as dag:

    t_upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=run_upload,
        provide_context=True,  # keeps compatibility; kwargs include dag/dag_run/ti
    )

    t_upload
