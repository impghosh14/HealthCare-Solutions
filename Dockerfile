# Dockerfile (repo root)
FROM apache/airflow:2.10.0-python3.11

USER root
# small apt packages you might need
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       vim \
       gcc \
       g++ \
       make \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# copy requirements and install
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# copy dags, src, sample data, etc into image
COPY airflow/dags/ /opt/airflow/dags/
COPY src/ /opt/airflow/app/src/
COPY sql/ /opt/airflow/app/sql/
COPY sample_data/ /opt/airflow/app/sample_data/

# set python path to include your app code
ENV PYTHONPATH=/opt/airflow/app:/opt/airflow/app/src

# expose airflow webserver port (docker compose does the mapping)
EXPOSE 8080
