from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator


POSTGRES_CONN_ID = "postgres_conn_curr"

conn = BaseHook.get_connection(POSTGRES_CONN_ID)


with DAG(
    dag_id="postgres_currency_backup",
    start_date=datetime(2026, 7, 5),
    schedule="0 2 * * *",
    catchup=False,
    tags=["postgres", "backup", "currency"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:


    backup_currency_db = BashOperator(
        task_id="backup_currency_db",

        bash_command="/airflow/scripts/postgres_backup.sh",

        env={
            "DB_HOST": conn.host,
            "DB_PORT": str(conn.port or 5432),
            "DB_USER": conn.login,
            "DB_NAME": conn.schema,
            "PGPASSWORD": conn.password,
        },
    )