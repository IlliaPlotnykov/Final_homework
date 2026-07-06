from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from currencies_classes.postgres_replicator import PostgresReplicator


SOURCE_CONN_ID = "postgres_currency"
TARGET_CONN_ID = "postgres_currency_replica"


def replicate_currency_rates():
    replicator = PostgresReplicator(
        source_conn_id=SOURCE_CONN_ID,
        target_conn_id=TARGET_CONN_ID
    )

    replicator.replicate()


def validate_currency_rates_replication():
    replicator = PostgresReplicator(
        source_conn_id=SOURCE_CONN_ID,
        target_conn_id=TARGET_CONN_ID
    )

    replicator.validate_replication()


default_args = {
    "owner": "Illia_Plotnykov",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="currency_rates_replication",
    description="Replicate currency rates from primary PostgreSQL DB to replica PostgreSQL DB",
    start_date=datetime(2026, 7, 1),
    schedule="0 3 * * *,
    catchup=False,
    default_args=default_args,
    tags=["currency", "postgres", "replication"],
) as dag:

    replicate_task = PythonOperator(
        task_id="replicate_currency_rates_to_replica_db",
        python_callable=replicate_currency_rates,
    )

    validate_task = PythonOperator(
        task_id="validate_currency_rates_replication",
        python_callable=validate_currency_rates_replication,
    )

    replicate_task >> validate_task