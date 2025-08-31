FROM apache/airflow:3.0.4-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

COPY --chown=airflow:root postgres_con.json minio_con.json /opt/airflow/

WORKDIR /opt/airflow