from google.cloud import storage
import pandas as pd
import snowflake.connector
import os, uuid
from datetime import datetime

def ingest(entity, table):
    client = storage.Client()
    bucket = client.bucket(os.getenv("GCP_BUCKET"))

    raw_prefix = f"raw/{entity}/"
    processed_prefix = f"processed/{entity}/"
    archive_prefix = f"archive/{entity}/"

    for blob in bucket.list_blobs(prefix=raw_prefix):
        file_name = os.path.basename(blob.name)
        local = f"/tmp/{file_name}"

        try:
            blob.download_to_filename(local)

            if local.endswith(".csv"):
                df = pd.read_csv(local)
            elif local.endswith(".json"):
                df = pd.read_json(local)
            elif local.endswith(".parquet"):
                df = pd.read_parquet(local)
            else:
                raise Exception("Unsupported format")

            df["SOURCE_FILE"] = file_name
            df["INGESTION_ID"] = str(uuid.uuid4())
            df["INGESTION_TS"] = datetime.utcnow()

            load_to_snowflake(df, table)

            bucket.copy_blob(blob, bucket, processed_prefix + file_name)
            blob.delete()

        except Exception as e:
            log_error(entity, file_name, str(e))
            bucket.copy_blob(blob, bucket, archive_prefix + file_name)
            blob.delete()

def load_to_snowflake(df, table):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cur = conn.cursor()
    cur.executemany(
        f"INSERT INTO {table} ({','.join(df.columns)}) VALUES ({','.join(['%s']*len(df.columns))})",
        df.values.tolist()
    )
    conn.commit()
    cur.close()
    conn.close()

def log_error(entity, file, msg):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO HCS_INGESTION_ERROR_LOG VALUES (%s,%s,%s,%s)",
        (entity, file, msg, datetime.utcnow())
    )
    conn.commit()
    cur.close()
    conn.close()
