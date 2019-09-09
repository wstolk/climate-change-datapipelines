import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

redshift_conn_id = "redshift_conn"

default_args = {
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 4,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 6, 1),
    'provide_context': True
}


def create_staging_tables(**kwargs):
    create_glacier_staging = """CREATE TABLE IF NOT EXISTS glacier_staging (
        "year" INT,
        "mean_cumulative_mass_balance" FLOAT,
        "number_of_observations" INT
    );"""

    create_temperature_staging = """CREATE TABLE IF NOT EXISTS temperature_staging (
        "source" VARCHAR(10),
        "date" DATE,
        "mean" FLOAT
    );"""

    create_population_staging = """CREATE TABLE IF NOT EXISTS population_staging (
        "country_name" VARCHAR(255),
        "country_code" VARCHAR(3),
        "year" INT,
        "value" FLOAT
    );"""

    create_sealevel_staging = """CREATE TABLE IF NOT EXISTS sealevel_staging (
        "time" DATE,
        "gmsl" FLOAT,
        "uncertainty" FLOAT
    );"""

    create_series_staging = """CREATE TABLE IF NOT EXISTS series_staging (
        "series_code" VARCHAR(20),
        "topic" VARCHAR(255),
        "indicator_name" VARCHAR(100),
        "periodicity" VARCHAR(20),
        "base_period" SMALLINT,
        "aggregation_method" VARCHAR(40)
    );"""

    create_co2_ppm_staging = """CREATE TABLE IF NOT EXISTS co2_ppm_staging (
        "date" DATE,
        "decimal_date" FLOAT,
        "average" FLOAT,
        "interpolated" FLOAT,
        "trend" FLOAT,
        "number_days" INT
    );"""

    create_countries_staging = """CREATE TABLE IF NOT EXISTS countries_staging (
        "country_code" VARCHAR(3),
        "shortname" VARCHAR(40),
        "alpha_code" VARCHAR(2),
        "currency_unit" VARCHAR(40),
        "region" VARCHAR(30),
        "income_group" VARCHAR(30)
    );"""

    create_indicators_staging = """CREATE TABLE indicators_staging (
        "indicator_code" VARCHAR(20),
        "country_code" VARCHAR(3),
        "year" INT,
        "value" BIGINT
    );"""

    tables = [
        create_glacier_staging,
        create_temperature_staging,
        create_population_staging,
        create_sealevel_staging,
        create_series_staging,
        create_co2_ppm_staging,
        create_countries_staging,
        create_indicators_staging
    ]

    for idx, table in enumerate(tables):
        logging.info("creating table {idx} of {len}".format(idx=idx + 1, len=len(tables)))
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        redshift_hook.run(table)


def create_tables(**kwargs):
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
        "indicator_id" INT IDENTITY(0,1) PRIMARY KEY,
        "indicator_code" VARCHAR(20) NOT NULL DISTKEY,
        "country_code" VARCHAR(3) NOT NULL,
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


def drop_tables(**kwargs):
    """
    loop over the list of tables and drop the tables in the database if exists
    any trigger that exists for a table will be removed as well

    :return: None
    """

    # list of tables to delete
    tables = [
        "glacier_staging",
        "temperature_staging",
        "population_staging",
        "sealevel_staging",
        "series_staging",
        "co2_ppm_staging",
        "countries_staging",
        "indicators_staging",
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
                      dag=dag,
                      python_callable=drop_tables)

create_staging = PythonOperator(task_id="create_staging_tables",
                                dag=dag,
                                python_callable=create_staging_tables)

create = PythonOperator(task_id="create_tables",
                        dag=dag,
                        python_callable=create_tables)

# first, drop existing tables, than create new tables
drop >> create_staging >> create
