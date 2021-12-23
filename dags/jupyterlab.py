from airflow import DAG
from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'pwc',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 22),
    'schedule_interval': '@daily',
    'email': ['esaphogus@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

sshHook = SSHHook(ssh_conn_id="jupyterlab")

dag = DAG('jupyterlab', 
            default_args=default_args
            )

t1 = SSHOperator(
    task_id="task1",
    command="cd /opt/workspace && spark-submit --jars jars/sqljdbc42.jar,jars/mysql-connector-java-5.1.49.jar odbc.py",
    ssh_hook=sshHook,
    dag=dag)