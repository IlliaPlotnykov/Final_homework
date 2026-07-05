from datetime import datetime, timedelta
import sys

sys.path.append("/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

from currencies_classes.request_api import RequestAPI
from currencies_classes.postgres_writer import PostgresWriter


POSTGRES_CONN_ID = "postgres_conn_curr"


def load_currency_rates(): #get exchange rates for the current day and loading them into PostgreSQL

    today = datetime.now()

    df = RequestAPI(
        start_date=today,
        end_date=today
    ).get_data()

    writer = PostgresWriter(POSTGRES_CONN_ID)
    writer.write_to_postgres(df)


default_args = {
    "owner": "Illia_Plotnykov",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="currency_exchange_daily_load",
    description="Daily loading of NBU currency exchange rates into PostgreSQL",
    start_date=datetime(2026, 6, 25),
    schedule="0 1 * * *",          # Every Day at 01:00
    catchup=False,
    default_args=default_args,
    tags=["api", "currency", "postgres"],
) as dag:

    load_currency_rates_task = PythonOperator(
        task_id="load_currency_rates",
        python_callable=load_currency_rates,
    )

    load_currency_rates_task