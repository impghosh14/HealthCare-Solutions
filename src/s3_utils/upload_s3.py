# src/s3_utils/upload_s3.py
import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Load .env from project root (adjust path if needed)
env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()  # fallback to environment

# Allowed extensions (you can add more)
ALLOWED_EXTS = {".json", ".csv", ".parquet"}

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")  # optional
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")  # or get from payload if preferred

if not S3_BUCKET:
    logger.warning("S3_BUCKET not found in .env; DAG payload must supply bucket name under 's3_bucket' key.")

def _timestamped_filename(original_name: str) -> str:
    """Return filename with timestamp appended before extension."""
    p = Path(original_name)
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())  # UTC timestamp
    return f"{p.stem}_{ts}{p.suffix}"

def create_s3_client():
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs.update({
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY
        })
        if AWS_SESSION_TOKEN:
            kwargs["aws_session_token"] = AWS_SESSION_TOKEN
    # boto3 will fallback to instance/profile credentials if env ones are not provided
    return boto3.client("s3", **kwargs)

def validate_local_file(local_path: str):
    p = Path(local_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    if p.suffix.lower() not in ALLOWED_EXTS:
        raise ValueError(f"Unsupported file extension '{p.suffix}'. Allowed: {ALLOWED_EXTS}")
    return p

def upload_file(local_path: str, s3_bucket: str = None, s3_folder: str = "") -> str:
    """
    Upload local file to s3://s3_bucket/s3_folder/<filename>_<timestamp>.<ext>
    Returns the S3 key of uploaded object.
    """
    s3_bucket = s3_bucket or S3_BUCKET
    if not s3_bucket:
        raise ValueError("S3 bucket not configured. Set S3_BUCKET in .env or pass s3_bucket argument.")

    p = validate_local_file(local_path)

    dest_name = _timestamped_filename(p.name)
    # sanitize folder: remove leading/trailing slashes and disallow .. segments
    s3_folder = (s3_folder or "").strip().strip("/")
    if ".." in s3_folder.split("/"):
        raise ValueError("Invalid s3_folder: contains disallowed path segments")

    s3_key = f"{s3_folder}/{dest_name}" if s3_folder else dest_name

    s3 = create_s3_client()
    try:
        logger.info("Uploading %s to s3://%s/%s", p, s3_bucket, s3_key)
        s3.upload_file(str(p), s3_bucket, s3_key)
        logger.info("Upload successful: s3://%s/%s", s3_bucket, s3_key)
        return s3_key
    except ClientError as e:
        logger.exception("Failed to upload file to S3: %s", e)
        raise
