from pathlib import Path

import pandas as pd
from airflow.sdk import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime


DATA_DIR = Path("/opt/airflow/data")
SQL_DIR = Path("/opt/airflow/sql")

POSTGRES_CONN_ID = "postgres_nyc"


def load_raw():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    files = sorted(DATA_DIR.glob("yellow_tripdata_2022-*.parquet.gz"))

    if not files:
        raise FileNotFoundError("Nenhum arquivo Parquet encontrado.")

    for file in files:
        print(f"Processando: {file.name}")

        df = pd.read_parquet(file)

        df.columns = [
            "vendor_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "ratecode_id",
            "store_and_fwd_flag",
            "pu_location_id",
            "do_location_id",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ]

        df.to_sql(
            "yellow_tripdata",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi",
        )

        print(f"{file.name}: {len(df):,} registros carregados.")


def validate_data():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    result = hook.get_first(
        """
        SELECT COUNT(*)
        FROM refined.yellow_tripdata;
        """
    )

    total_records = result[0]

    if total_records == 0:
        raise ValueError("A tabela refined.yellow_tripdata está vazia.")

    print(f"Total de registros na tabela refined: {total_records:,}")


with DAG(
    dag_id="nyc_taxi_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["challenge", "nyc", "etl"],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=POSTGRES_CONN_ID,
        sql=(SQL_DIR / "create_tables.sql").read_text(),
    )

    load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw,
    )

    transform_trusted = SQLExecuteQueryOperator(
        task_id="transform_trusted",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            TRUNCATE TABLE trusted.yellow_tripdata;

            INSERT INTO trusted.yellow_tripdata
            SELECT
                vendor_id,
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance,
                ratecode_id,
                store_and_fwd_flag,
                pu_location_id,
                do_location_id,
                payment_type,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                total_amount,
                congestion_surcharge,
                airport_fee
            FROM raw.yellow_tripdata;
        """,
    )

    load_refined = SQLExecuteQueryOperator(
        task_id="load_refined",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            TRUNCATE TABLE refined.yellow_tripdata;

            INSERT INTO refined.yellow_tripdata
            SELECT *
            FROM trusted.yellow_tripdata
            WHERE pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND dropoff_datetime >= pickup_datetime
              AND trip_distance >= 0;
        """,
    )

    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=validate_data,
    )

    (
        create_tables
        >> load_raw_data
        >> transform_trusted
        >> load_refined
        >> quality_checks
    )