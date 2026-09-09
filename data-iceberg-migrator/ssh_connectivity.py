from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'data-migration',
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('ssh_connectivity', default_args=default_args, tags=['migration', 'mapr', 's3', 'hive'], schedule=None) as dag:
    SSHOperator(
        task_id='test_ssh_remotely',
        ssh_conn_id='ssh_test_edl',
        command='echo "Testing SSH connectivity"',
    )
