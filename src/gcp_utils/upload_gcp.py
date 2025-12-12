# src/gcp_utils/upload_gcp.py
import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

ALLOWED_EXTS = {".json", ".csv", ".parquet"}
GCP_BUCKET = os.getenv("GCP_BUCKET")
GCP_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # path to mounted json inside container

def _timestamped_filename(original_name: str) -> str:
    p = Path(original_name)
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    return f"{p.stem}_{ts}{p.suffix}"

def _create_gcs_client():
    # if GOOGLE_APPLICATION_CREDENTIALS is set to a path, google lib will pick it up automatically
    if GCP_SERVICE_ACCOUNT_JSON:
        return storage.Client.from_service_account_json(GCP_SERVICE_ACCOUNT_JSON)
    return storage.Client()

def validate_local_file(local_path: str):
    p = Path(local_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    if p.suffix.lower() not in ALLOWED_EXTS:
        raise ValueError(f"Unsupported extension '{p.suffix}'. Allowed: {ALLOWED_EXTS}")
    return p

def upload_file(local_path: str, bucket_name: str = None, gcs_prefix: str = "") -> str:
    """
    Uploads local file to GCS bucket. Returns gs://bucket/object path.
    """
    bucket_name = bucket_name or GCP_BUCKET
    if not bucket_name:
        raise ValueError("GCP bucket not configured. Set GCP_BUCKET or pass bucket_name.")

    p = validate_local_file(local_path)
    dest_name = _timestamped_filename(p.name)
    gcs_prefix = (gcs_prefix or "").strip().strip("/")
    if any(part == ".." for part in gcs_prefix.split("/")):
        raise ValueError("Invalid gcs_prefix: contains ..")
    blob_name = f"{gcs_prefix}/{dest_name}" if gcs_prefix else dest_name

    client = _create_gcs_client()
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        logger.info("Uploading %s -> gs://%s/%s", p, bucket_name, blob_name)
        blob.upload_from_filename(str(p))
        uri = f"gs://{bucket_name}/{blob_name}"
        logger.info("GCP upload succeeded: %s", uri)
        return uri
    except gcp_exceptions.GoogleAPIError as e:
        logger.exception("GCP upload failed: %s", e)
        raise
