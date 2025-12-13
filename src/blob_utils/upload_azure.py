import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import AzureError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Load .env from repo root
env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

# Allowed extensions
ALLOWED_EXTS = {".json", ".csv", ".parquet"}

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")
AZURE_DEFAULT_CONTAINER = os.getenv("AZURE_CONTAINER", None)
AZURE_REGION = os.getenv("AZURE_REGION", None)

def _timestamped_filename(original_name: str) -> str:
    p = Path(original_name)
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    return f"{p.stem}_{ts}{p.suffix}"

def _create_blob_service_client():
    if AZURE_CONNECTION_STRING:
        return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    elif AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY:
        account_url = f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=AZURE_ACCOUNT_KEY)
    else:
        # Allow default credentials (managed identity) if running in Azure
        return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING or "")

def validate_local_file(local_path: str):
    p = Path(local_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    if p.suffix.lower() not in ALLOWED_EXTS:
        raise ValueError(f"Unsupported extension '{p.suffix}'. Allowed: {ALLOWED_EXTS}")
    return p

def upload_file(local_path: str, container_name: str = None, blob_dir: str = "") -> str:
    """
    Upload local file to Azure Blob Storage.
    Returns the blob URL on success.
    """
    container_name = container_name or AZURE_DEFAULT_CONTAINER
    if not container_name:
        raise ValueError("Azure container name not configured. Set AZURE_CONTAINER in .env or pass container_name.")

    p = validate_local_file(local_path)
    dest_name = _timestamped_filename(p.name)
    blob_dir = (blob_dir or "").strip().strip("/")
    if any(part == ".." for part in blob_dir.split("/")):
        raise ValueError("Invalid blob_dir: contains ..")
    blob_path = f"{blob_dir}/{dest_name}" if blob_dir else dest_name

    client = _create_blob_service_client()
    try:
        container_client = client.get_container_client(container_name)
        # create container if not exists (idempotent)
        try:
            container_client.create_container()
        except Exception:
            pass
        logger.info("Uploading %s -> container=%s blob=%s", p, container_name, blob_path)
        # Let azure auto-detect content-type, or set based on extension
        content_settings = None
        if p.suffix.lower() == ".json":
            content_settings = ContentSettings(content_type="application/json")

        with p.open("rb") as fh:
            container_client.upload_blob(name=blob_path, data=fh, overwrite=True, content_settings=content_settings)

        blob_url = f"https://{client.account_name}.blob.core.windows.net/{container_name}/{blob_path}"
        logger.info("Azure upload succeeded: %s", blob_url)
        return blob_url
    except AzureError as e:
        logger.exception("Azure upload failed: %s", e)
        raise
