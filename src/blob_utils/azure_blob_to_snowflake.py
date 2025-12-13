from azure.storage.blob import BlobServiceClient
import pandas as pd, snowflake.connector, os, uuid
from datetime import datetime

def ingest(entity, table):
    blob_service = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    container = blob_service.get_container_client(os.getenv("AZURE_CONTAINER"))

    raw_prefix = f"raw/{entity}/"
    processed_prefix = f"processed/{entity}/"
    archive_prefix = f"archive/{entity}/"

    for blob in container.list_blobs(name_starts_with=raw_prefix):
        file_name = blob.name.split("/")[-1]
        local = f"/tmp/{file_name}"

        try:
            with open(local, "wb") as f:
                f.write(container.download_blob(blob.name).readall())

            df = read_file(local)
            add_metadata(df, file_name)
            load_to_snowflake(df, table)

            container.upload_blob(
                processed_prefix + file_name,
                open(local, "rb"),
                overwrite=True
            )
            container.delete_blob(blob.name)

        except Exception as e:
            log_error(entity, file_name, str(e))
            container.upload_blob(
                archive_prefix + file_name,
                open(local, "rb"),
                overwrite=True
            )
            container.delete_blob(blob.name)

def read_file(path):
    if path.endswith(".csv"):
        return pd.read_csv(path)
    if path.endswith(".json"):
        return pd.read_json(path)
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    raise Exception("Unsupported format")

def add_metadata(df, file):
    df["SOURCE_FILE"] = file
    df["INGESTION_ID"] = str(uuid.uuid4())
    df["INGESTION_TS"] = datetime.utcnow()

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
    cols = ",".join(df.columns)
    vals = ",".join(["%s"] * len(df.columns))
    cur.executemany(f"INSERT INTO {table} ({cols}) VALUES ({vals})", df.values.tolist())
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
    conn.cursor().execute(
        "INSERT INTO HCS_INGESTION_ERROR_LOG VALUES (%s,%s,%s,%s)",
        (entity, file, msg, datetime.utcnow())
    )
    conn.commit()
    conn.close()
