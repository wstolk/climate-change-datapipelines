from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.local_to_s3_plugin import UploadToS3Operator
from airflow.operators.redshift_operations_plugin import StageToRedshiftOperator
from airflow.operators.redshift_operations_plugin import LoadStagingToProduction
from airflow.operators.redshift_operations_plugin import DataQualityOperator
from helpers.sql_queries import SqlQueries

default_args = {
    'depends_on_past': False,
    'wait_for_downstream': True,
    'retries': 4,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2019, 9, 1)
}

# Default connection variables
s3_conn_id = 's3_conn'
s3_filepath = 'global_temp_monthly.csv'
s3_bucket = Variable.get('s3_bucket')
redshift_conn_id = 'redshift_conn'
redshift_schema = Variable.get('redshift_schema')
redshift_arn = Variable.get('redshift_arn')

datasets = [
    {
        "id": "country_info",
        "path": "/usr/local/airflow/data/Country.csv",
        "staging_table": "countries_staging",
        "prod_table": "countries_dimension",
        "prod_columns": ("country_code", "shortname", "alpha_code", "currency_unit", "region", "income_group"),
        "insert_query": SqlQueries.temperature_insert
    }, {
        "id": "series_info",
        "path": "/usr/local/airflow/data/Series.csv",
        "staging_table": "series_staging",
        "prod_table": "series_dimension",
        "prod_columns": ("series_code", "topic", "indicator_name", "periodicity", "base_period", "aggregation_method"),
        "insert_query": SqlQueries.temperature_insert
    }, {
        "id": "indicators",
        "path": "/usr/local/airflow/data/Series.csv",
        "staging_table": "indicators_staging",
        "prod_table": "indicators_fact",
        "prod_columns": ("indicator_code", "country_code", "year", "value"),
        "insert_query": SqlQueries.temperature_insert
    }
]


def create_dag(id, path, staging_table, prod_table, prod_columns, insert_query):
    dag = DAG(dag_id=id,
              description='processing of global temperature data to Redshift',
              schedule_interval='0 3 1 * *',
              default_args=default_args)

    store_to_s3 = UploadToS3Operator(task_id='{id}_localfile_to_s3'.format(id=id),
                                          path=path,
                                          s3_conn_id=s3_conn_id,
                                          s3_bucket=s3_bucket,
                                          s3_filepath=s3_filepath,
                                          dag=dag)

    s3_to_staging = StageToRedshiftOperator(task_id='{id}_stage_data'.format(id=id),
                                            redshift_conn_id=redshift_conn_id,
                                            redshift_table=staging_table,
                                            redshift_schema=redshift_schema,
                                            redshift_arn=redshift_arn,
                                            s3_bucket=s3_bucket,
                                            s3_key=s3_filepath,
                                            dag=dag)

    staging_to_prod = LoadStagingToProduction(task_id='{id}_staging_to_prod'.format(id=id),
                                              redshift_conn_id=redshift_conn_id,
                                              query=insert_query,
                                              schema=redshift_schema,
                                              table=prod_table,
                                              columns=prod_columns,
                                              delete_before_load=True,
                                              dag=dag)

    run_quality_checks = DataQualityOperator(task_id='run_data_quality_checks',
                                             dag=dag,
                                             redshift_conn_id=redshift_conn_id,
                                             table=prod_table)

    store_to_s3 >> s3_to_staging >> staging_to_prod >> run_quality_checks

    return dag


# Generate DAG per dataset
for set in datasets:
    globals()[set["id"]] = create_dag(set["id"],
                                      set["path"],
                                      set["staging_table"],
                                      set["prod_table"],
                                      set["prod_columns"],
                                      set["insert_query"])
