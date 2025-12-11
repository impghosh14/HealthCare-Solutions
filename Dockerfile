# Dockerfile (repo root)
FROM apache/airflow:2.10.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    vim \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir boto3 python-dotenv

COPY airflow/dags/ /opt/airflow/dags/
COPY src/ /opt/airflow/app/src/
COPY sql/ /opt/airflow/app/sql/
COPY sample_data/ /opt/airflow/app/sample_data/

ENV PYTHONPATH=/opt/airflow/app:/opt/airflow/app/src
