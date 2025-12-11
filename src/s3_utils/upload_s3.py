# src/s3_utils/upload_s3.py
import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# load .env from repo root (two levels up from this file)
env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

ALLOWED_EXTS = {".json", ".csv", ".parquet"}

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET = os.getenv("S3_BUCKET")

def _timestamped_filename(original_name: str) -> str:
    p = Path(original_name)
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    return f"{p.stem}_{ts}{p.suffix}"

def create_s3_client():
    session_kwargs = {}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        session_kwargs['aws_access_key_id'] = AWS_ACCESS_KEY_ID
        session_kwargs['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            session_kwargs['aws_session_token'] = AWS_SESSION_TOKEN

    session = boto3.Session(region_name=AWS_REGION, **({} if not session_kwargs else session_kwargs))
    return session.client("s3")

def validate_local_file(local_path: str) -> Path:
    p = Path(local_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    if p.suffix.lower() not in ALLOWED_EXTS:
        raise ValueError(f"Unsupported extension '{p.suffix}'. Allowed: {ALLOWED_EXTS}")
    return p

def upload_file(local_path: str, s3_bucket: str = None, s3_folder: str = "") -> str:
    """
    Upload local file to s3://bucket/folder/<name>_<timestamp>.<ext>
    Returns the full s3:// URI on success.
    """
    s3_bucket = s3_bucket or S3_BUCKET
    if not s3_bucket:
        raise ValueError("S3 bucket not configured. Set S3_BUCKET in .env or pass s3_bucket.")

    p = validate_local_file(local_path)
    dest_name = _timestamped_filename(p.name)

    # sanitize folder
    s3_folder = (s3_folder or "").strip().strip("/")
    if any(part == ".." for part in s3_folder.split("/")):
        raise ValueError("Invalid s3_folder: contains ..")

    s3_key = f"{s3_folder}/{dest_name}" if s3_folder else dest_name

    s3 = create_s3_client()
    try:
        logger.info("Uploading %s -> s3://%s/%s", p, s3_bucket, s3_key)
        s3.upload_file(str(p), s3_bucket, s3_key)
        s3_uri = f"s3://{s3_bucket}/{s3_key}"
        logger.info("Upload succeeded: %s", s3_uri)
        return s3_uri
    except NoCredentialsError as e:
        logger.exception("AWS credentials not found or invalid: %s", e)
        raise
    except ClientError as e:
        logger.exception("Failed to upload file to S3: %s", e)
        raise
