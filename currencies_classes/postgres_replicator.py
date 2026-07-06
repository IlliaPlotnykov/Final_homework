from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


class PostgresReplicator:
    def __init__(self, source_conn_id: str, target_conn_id: str):
        self.source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        self.target_hook = PostgresHook(postgres_conn_id=target_conn_id)

        self.source_engine = self.source_hook.get_sqlalchemy_engine()
        self.target_engine = self.target_hook.get_sqlalchemy_engine()

    def get_last_replicated_date(self):
        with self.target_engine.begin() as target_conn:
            last_replicated_date = target_conn.execute(
                text("""
                    SELECT COALESCE(MAX(exchange_date), DATE '1900-01-01')
                    FROM public.currency_rates_replica
                """)
            ).scalar()

        return last_replicated_date

    def get_source_rows(self, last_replicated_date):
        with self.source_engine.begin() as source_conn:
            rows = source_conn.execute(
                text("""
                    SELECT
                        currency_code,
                        currency_name,
                        rate,
                        exchange_date,
                        created_at
                    FROM public.currency_rates
                    WHERE exchange_date >= :last_replicated_date
                    ORDER BY exchange_date, currency_code
                """),
                {"last_replicated_date": last_replicated_date}
            ).fetchall()

        return rows

    def upsert_to_replica(self, rows):
        if not rows:
            print("No data to replicate")
            return

        with self.target_engine.begin() as target_conn:
            for row in rows:
                target_conn.execute(
                    text("""
                        INSERT INTO public.currency_rates_replica (
                            currency_code,
                            currency_name,
                            rate,
                            exchange_date,
                            created_at,
                            replicated_at
                        )
                        VALUES (
                            :currency_code,
                            :currency_name,
                            :rate,
                            :exchange_date,
                            :created_at,
                            CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (currency_code, exchange_date)
                        DO UPDATE SET
                            currency_name = EXCLUDED.currency_name,
                            rate = EXCLUDED.rate,
                            created_at = EXCLUDED.created_at,
                            replicated_at = CURRENT_TIMESTAMP
                    """),
                    {
                        "currency_code": row.currency_code,
                        "currency_name": row.currency_name,
                        "rate": row.rate,
                        "exchange_date": row.exchange_date,
                        "created_at": row.created_at,
                    }
                )

        print(f"Replication completed. Rows processed: {len(rows)}")

    def replicate(self):
        last_replicated_date = self.get_last_replicated_date()

        print(f"Last replicated date: {last_replicated_date}")

        rows = self.get_source_rows(last_replicated_date)

        print(f"Rows selected from source: {len(rows)}")

        self.upsert_to_replica(rows)

    def validate_replication(self):
        with self.source_engine.begin() as source_conn:
            source_stats = source_conn.execute(
                text("""
                    SELECT
                        COUNT(*) AS row_count,
                        MAX(exchange_date) AS max_exchange_date
                    FROM public.currency_rates
                """)
            ).fetchone()

        with self.target_engine.begin() as target_conn:
            target_stats = target_conn.execute(
                text("""
                    SELECT
                        COUNT(*) AS row_count,
                        MAX(exchange_date) AS max_exchange_date
                    FROM public.currency_rates_replica
                """)
            ).fetchone()

        source_count = source_stats.row_count
        target_count = target_stats.row_count

        source_max_date = source_stats.max_exchange_date
        target_max_date = target_stats.max_exchange_date

        print(f"Source count: {source_count}")
        print(f"Target count: {target_count}")
        print(f"Source max date: {source_max_date}")
        print(f"Target max date: {target_max_date}")

        if source_count != target_count:
            raise ValueError(
                f"Replication validation failed: "
                f"source_count={source_count}, target_count={target_count}"
            )

        if source_max_date != target_max_date:
            raise ValueError(
                f"Replication validation failed: "
                f"source_max_date={source_max_date}, target_max_date={target_max_date}"
            )

        print("Replication validation passed")