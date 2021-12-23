import json
import logging
from airflow import models
from airflow.utils import db

JUPYTERLAB_CONFIG = json.dumps({
    "key_file": "/usr/local/airflow/dags/id_rsa"
})

def add_connection(conn_id, conn_type, host, port, extra=None):
    logging.info(f"Adding connection: {conn_id}")
    m = models.Connection(conn_id=conn_id, conn_type=conn_type, extra=extra, host=host, port=port, login="root")
    db.merge_conn(m)

add_connection('jupyterlab', 'ssh', 'jupyterlab', '22', JUPYTERLAB_CONFIG)