# Currency Exchange Data Pipeline

Personal Data Engineering project for collecting, processing, storing, backing up, and replicating currency exchange rates.

The project uses data from the public National Bank of Ukraine API and demonstrates a simple but realistic batch data pipeline with Python, PostgreSQL, Apache Airflow, staging tables, incremental loading, backup/restore, and replication between PostgreSQL databases.

---

## Project Overview

This project collects currency exchange rates from a public REST API, transforms the received JSON data into a structured tabular format, and stores the results for further analysis.

The project started as a local Python script that exported currency rates to Excel, but was extended into a more complete Data Engineering workflow with:

* API data extraction
* Pandas-based transformation
* PostgreSQL storage
* Staging table loading
* Upsert logic into the target table
* Apache Airflow orchestration
* PostgreSQL backup and restore
* Incremental replication between PostgreSQL databases
* Basic data validation and duplicate protection

The goal of the project is to practice core Data Engineering concepts on a small but realistic use case.

---

## Data Source

The project uses the National Bank of Ukraine public API:

```text
https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange
```

The pipeline requests exchange rates by currency code and date.

Example currencies:

```text
USD, EUR, PLN, GBP
```

---

## Technologies

* Python
* Pandas
* Requests
* PostgreSQL
* SQLAlchemy
* Apache Airflow
* Airflow Postgres Provider
* DBeaver
* Linux / Ubuntu Server
* Bash
* Cron
* Git / GitHub
* openpyxl

---

## Main Features

### API Extraction

The project sends requests to the NBU API for selected currencies and dates.

The API response is converted into a Pandas DataFrame.

### Data Transformation

Raw API fields are renamed and prepared for database loading.

Example mapping:

```text
cc           -> currency_code
txt          -> currency_name
rate         -> rate
exchangedate -> exchange_date
```

### Excel Export

The first version of the project supported local export to Excel.

This is useful for quick local testing and checking the extracted data before loading it into a database.

### PostgreSQL Storage

The project stores currency rates in PostgreSQL.

Main target table:

```sql
public.currency_rates
```

Example table structure:

```sql
CREATE TABLE public.currency_rates (
    id serial4 NOT NULL,
    currency_code varchar(10),
    currency_name text,
    rate numeric,
    exchange_date date,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT currency_rates_pkey PRIMARY KEY (id),
    CONSTRAINT currency_rates_currency_code_exchange_date_key UNIQUE (currency_code, exchange_date)
);
```

The unique constraint on:

```text
currency_code, exchange_date
```

protects the table from duplicate currency rates for the same day.

### Staging Table

Before inserting data into the final table, the pipeline loads data into a staging table:

```sql
public.currency_rates_stg
```

The staging table is used as an intermediate layer between the raw API result and the final PostgreSQL table.

This makes the pipeline safer and easier to debug.

### Upsert Logic

After loading data into the staging table, the pipeline inserts rows into the final table.

If the same currency and exchange date already exist, the pipeline can update the existing row instead of inserting a duplicate.

This is handled through PostgreSQL conflict logic based on the unique key:

```text
currency_code + exchange_date
```

### Apache Airflow Orchestration

The project was extended with Apache Airflow to automate the daily loading process.

Airflow is responsible for:

* scheduling the pipeline
* running Python tasks
* retrying failed tasks
* storing task logs
* managing database connections
* monitoring pipeline status through the Airflow UI

Example DAG idea:

```text
currency_exchange_daily_load
```

Typical flow:

```text
Airflow DAG
    -> request data from API
    -> transform data with Pandas
    -> load data to PostgreSQL staging table
    -> upsert data into final table
```

### PostgreSQL Backup

The project includes PostgreSQL backup logic using `pg_dump`.

Backups are stored as dump files and can be used to restore the database if needed.

Example backup command:

```bash
pg_dump -U postgres -d currency_db -F c -f /backups/currency_exchange/currency_db_YYYY-MM-DD_HH-MM-SS.dump
```

### PostgreSQL Restore

The project supports restoring a backup into a separate PostgreSQL database.

Example restore flow:

```bash
createdb -U postgres currency_db_restore

pg_restore -U postgres \
  -d currency_db_restore \
  /backups/currency_exchange/currency_db_YYYY-MM-DD_HH-MM-SS.dump
```

This allows testing backup files without overwriting the main production-like database.

### Backup Retention

Old backup files can be removed automatically using a scheduled cleanup command or cron job.

Example idea:

```bash
find /backups/currency_exchange -type f -name "*.dump" -mtime +14 -delete
```

This keeps only recent backups and prevents disk space issues.

### PostgreSQL Replication Logic

The project also includes custom Python-based replication logic between two PostgreSQL databases.

Replication idea:

```text
source PostgreSQL database
        -> read only new rows
        -> insert/update rows in target PostgreSQL database
```

The replica table can be used for testing backup, restore, data synchronization, or separating source and target environments.

Example replica table:

```sql
public.currency_rates_replica
```

Incremental replication is based on the latest replicated date:

```sql
SELECT COALESCE(MAX(exchange_date), DATE '1900-01-01')
FROM public.currency_rates_replica;
```

Then the pipeline reads only newer rows from the source database and inserts them into the target database.

---

## Project Architecture

```text
NBU API
  |
  v
Python / Requests
  |
  v
Pandas DataFrame
  |
  v
PostgreSQL staging table
  |
  v
PostgreSQL final table
  |
  +--> Backup with pg_dump
  |
  +--> Restore with pg_restore
  |
  +--> Replica database / replica table
```

With Airflow:

```text
Apache Airflow DAG
  |
  v
Python task
  |
  v
API extraction
  |
  v
Data transformation
  |
  v
PostgreSQL load
  |
  v
Backup / replication tasks
```

---

## Project Structure

Current and planned project structure:

```text
Currencies_Exchange/
├── config/
│   ├── global_config.py
│   └── list_currencies.xlsx
│
├── currencies_classes/
│   ├── __init__.py
│   ├── file_reader.py
│   ├── file_writer_excel.py
│   ├── filename.py
│   ├── request_api.py
│   ├── postgres_writer.py
│   └── postgres_replicator.py
│
├── dags/
│   ├── currency_exchange_daily_load.py
│   ├── postgres_currency_backup.py
│   └── postgres_currency_replication.py
│
├── sql/
│   ├── create_currency_rates.sql
│   └── create_currency_rates_replica.sql
│
├── main.py
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Database Tables

### `currency_rates`

Main table for storing exchange rates.

Purpose:

* stores final cleaned currency rates
* prevents duplicates by currency and date
* keeps historical exchange rate data

### `currency_rates_stg`

Staging table.

Purpose:

* temporary loading layer
* receives fresh data from Pandas
* used before inserting into the final table

### `currency_rates_replica`

Replica table.

Purpose:

* stores copied data from the source database
* used for replication practice
* helps simulate source/target database synchronization

---

## How to Run Locally

### 1. Clone the repository

```bash
git clone https://github.com/IlliaPlotnykov/Currencies_Exchange.git
cd Currencies_Exchange
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it:

Windows:

```bash
venv\Scripts\activate
```

Linux / macOS:

```bash
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the local script

```bash
python main.py
```

The local script extracts currency rates from the API and saves the result to an Excel file.

---

## Airflow Setup

The project can also be executed through Apache Airflow.

Airflow is used for scheduled daily loading and operational control of the pipeline.

Typical Airflow components:

```text
airflow webserver
airflow scheduler
PostgreSQL metadata database
Airflow DAGs
Airflow Connections
```

Required Airflow connection:

```text
postgres_default
```

or another custom connection ID used inside the DAG.

Example connection target:

```text
PostgreSQL database: currency_db
```

---

## Backup and Restore Example

### Create backup

```bash
pg_dump -U postgres \
  -d currency_db \
  -F c \
  -f /backups/currency_exchange/currency_db_$(date +"%Y-%m-%d_%H-%M-%S").dump
```

### Restore backup into a separate database

```bash
createdb -U postgres currency_db_restore

pg_restore -U postgres \
  -d currency_db_restore \
  /backups/currency_exchange/currency_db_YYYY-MM-DD_HH-MM-SS.dump
```

After restore, the restored database can be checked in DBeaver or through `psql`.

---

## Data Engineering Concepts Practiced

This project demonstrates the following Data Engineering concepts:

* Batch data processing
* REST API ingestion
* DataFrame transformation
* PostgreSQL table design
* Primary keys
* Unique constraints
* Staging tables
* Upsert logic
* Idempotent loading
* Airflow DAG orchestration
* Airflow retries
* Airflow connections
* Database backup
* Database restore
* Backup retention
* Incremental replication
* Source and target database separation

---

## Current Status

Implemented:

* API extraction
* Currency list handling
* Pandas DataFrame creation
* Excel export
* PostgreSQL table design
* PostgreSQL loading logic
* Staging table approach
* Airflow orchestration
* PostgreSQL backup
* PostgreSQL restore
* Backup cleanup idea
* PostgreSQL replication logic

Planned improvements:

* Add Docker support
* Add more structured logging
* Add pytest tests for transformation logic
* Add data quality checks
* Add Parquet export
* Add SQL scripts for table creation

---

## Author

Illia Plotnykov

Personal Data Engineering learning project.
