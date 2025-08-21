import json
import logging

from airflow.models import Variable
from airflow.exceptions import AirflowException


def config_loader(minio_conn_id: str):
    """
    Этот метод это костыли, все конфиги можно задать через ui airflow
    """
    path = "../minio_con.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
            Variable.set(minio_conn_id, config, serialize_json=True)
            logging.info("MinIO config loaded successfully")
    except Exception as e:
        logging.error(f"Failed to load MinIO config: {str(e)}")
        raise AirflowException(f"Failed to load MinIO config: {str(e)}")