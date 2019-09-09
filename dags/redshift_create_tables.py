import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

redshift_conn_id = "redshift_analytics_db"

default_args = {
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 4,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 6, 1),
    'provide_context': True
}


def create_tables():
    create_glacier_dimension = """CREATE TABLE IF NOT EXISTS glacier_dimension (
        "year" INT PRIMARY KEY,
        "cumulative_mass" FLOAT NOT NULL
    ) SORTKEY (cumulative_mass);"""

    create_temperature_dimension = """CREATE TABLE IF NOT EXISTS temperature_dimension (
        "date" DATE PRIMARY KEY,
        "gcag" FLOAT NOT NULL,
        "gistemp" FLOAT NOT NULL
    ) SORTKEY (gcag, gistemp);"""

    create_population_dimension = """CREATE TABLE IF NOT EXISTS population_dimension (
        "country_code" VARCHAR(3),
        "year" INT,
        "population" FLOAT NOT NULL,
        PRIMARY KEY (country_code, year)
    ) SORTKEY (population);"""

    create_sealevel_dimension = """CREATE TABLE IF NOT EXISTS sealevel_dimension (
        "date" DATE PRIMARY KEY,
        "sealevel" BIGINT NOT NULL
    ) SORTKEY (sealevel);"""

    create_series_dimension = """CREATE TABLE IF NOT EXISTS series_dimension (
        "series_code" VARCHAR(20) PRIMARY KEY,
        "topic" VARCHAR(255) NOT NULL DISTKEY,
        "indicator_name" VARCHAR(100) NOT NULL,
        "periodicity" VARCHAR(20) NOT NULL,
        "base_period" SMALLINT NOT NULL,
        "aggregation_method" VARCHAR(40) NOT NULL
    ) SORTKEY (topic, aggregation_method, base_period);"""

    create_co2_ppm_dimension = """CREATE TABLE IF NOT EXISTS co2_ppm_dimension (
        "date" DATE PRIMARY KEY,
        "interpolated" FLOAT NOT NULL,
        "trend" FLOAT NOT NULL
    ) SORTKEY (interpolated);"""

    create_countries_dimension = """CREATE TABLE IF NOT EXISTS countries_dimension (
        "country_code" VARCHAR(3) PRIMARY KEY,
        "shortname" VARCHAR(40) NOT NULL DISTKEY,
        "alpha_code" VARCHAR(2) NOT NULL,
        "currency_unit" VARCHAR(40) NOT NULL,
        "region" VARCHAR(30) NOT NULL,
        "income_group" VARCHAR(30) NOT NULL
    ) SORTKEY (region, income_group);"""

    create_indicators_fact = """CREATE TABLE indicators_fact (
        "indicator_id" SERIAL PRIMARY KEY,
        "indicator_code" VARCHAR(20) NOT NULL DISTKEY,
        "country_code" VARCHAR(3) NOT NULL DISTKEY,
        "year" INT NOT NULL,
        "value" BIGINT NOT NULL
    ) SORTKEY (year, country_code, indicator_code);"""

    tables = [
        create_glacier_dimension,
        create_temperature_dimension,
        create_population_dimension,
        create_sealevel_dimension,
        create_series_dimension,
        create_co2_ppm_dimension,
        create_countries_dimension,
        create_indicators_fact
    ]

    for idx, table in enumerate(tables):
        logging.info("creating table {idx} of {len}".format(idx=idx + 1, len=len(tables)))
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        redshift_hook.run(table)


def drop_tables():
    """
    loop over the list of tables and drop the tables in the database if exists
    any trigger that exists for a table will be removed as well

    :return: None
    """

    # list of tables to delete
    tables = [
        "glacier_dimension",
        "temperature_dimension",
        "population_dimension",
        "sealevel_dimension",
        "series_dimension",
        "co2_ppm_dimension",
        "countries_dimension",
        "indicators_fact"
    ]

    # loop over list of tables and execute DROP TABLE IF EXISTS statement for each table in the list
    for idx, table in enumerate(tables):
        logging.info("dropping table {idx} of {len}".format(idx=idx + 1, len=len(tables)))
        query = "DROP TABLE IF EXISTS {TABLE}".format(TABLE=table)
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        redshift_hook.run(query)


dag = DAG('redshift_create_tables',
          start_date=datetime.now(),
          default_args=default_args,
          description='Will drop all existing tables and create fresh tables with triggers')

drop = PythonOperator(task_id="drop_tables",
                      python_callable=drop_tables,
                      dag=dag)

create = PythonOperator(task_id="create_tables",
                        python_callable=create_tables,
                        dag=dag)

# first, drop existing tables, than create new tables
drop >> create
