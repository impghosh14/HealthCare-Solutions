import boto3
import pandas as pd
import snowflake.connector
import os
import uuid
from datetime import datetime

def ingest(entity, table):
    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET")

    raw_prefix = f"raw/{entity}/"
    processed_prefix = f"processed/{entity}/"

    # 🔴 archive is used as reject
    archive_prefix = f"archive/{entity}/"

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=raw_prefix
    )

    if "Contents" not in response:
        return

    for obj in response["Contents"]:
        key = obj["Key"]

        if key.endswith("/"):
            continue

        file_name = os.path.basename(key)
        local_file = f"/tmp/{file_name}"

        try:
            # -------------------
            # Download file
            # -------------------
            s3.download_file(bucket, key, local_file)

            # -------------------
            # Detect file format
            # -------------------
            if local_file.endswith(".csv"):
                df = pd.read_csv(local_file)

            elif local_file.endswith(".json"):
                df = pd.read_json(local_file)

            elif local_file.endswith(".parquet"):
                df = pd.read_parquet(local_file)

            else:
                raise Exception("Unsupported file format")

            # -------------------
            # Add metadata columns
            # -------------------
            df["SOURCE_FILE"] = file_name
            df["INGESTION_ID"] = str(uuid.uuid4())
            df["INGESTION_TS"] = datetime.utcnow()

            # -------------------
            # Load into Snowflake
            # -------------------
            load_to_snowflake(df, table)

            # -------------------
            # Move file: raw → processed
            # -------------------
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=key.replace("raw/", "processed/")
            )

            s3.delete_object(Bucket=bucket, Key=key)

        except Exception as e:
            # -------------------
            # Log error to Snowflake
            # -------------------
            log_error(
                entity=entity,
                file_name=file_name,
                error_msg=str(e)
            )

            # -------------------
            # Move file: raw → archive (reject)
            # -------------------
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=key.replace("raw/", "archive/")
            )

            s3.delete_object(Bucket=bucket, Key=key)


def load_to_snowflake(df, table):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    columns = ",".join(df.columns)
    values = ",".join(["%s"] * len(df.columns))

    insert_sql = f"""
        INSERT INTO {table} ({columns})
        VALUES ({values})
    """

    cursor.executemany(insert_sql, df.values.tolist())
    conn.commit()

    cursor.close()
    conn.close()


def log_error(entity, file_name, error_msg):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO HCS_INGESTION_ERROR_LOG
        (ENTITY, FILE_NAME, ERROR_MESSAGE, ERROR_TS)
        VALUES (%s, %s, %s, %s)
        """,
        (entity, file_name, error_msg, datetime.utcnow())
    )

    conn.commit()
    cursor.close()
    conn.close()
