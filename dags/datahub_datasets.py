from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.datapackage_to_s3_plugin import DatapackageToS3Operator
from airflow.operators.redshift_operations_plugin import StageToRedshiftOperator

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

datasets = [{
    "id": "global_temperature",
    "url": "https://datahub.io/core/global-temp/datapackage.json",
    "resource": "monthly_csv",
    "headers": ["source", "year", "mean"],
    "staging_table": "temperature_staging"
}, {
    "id": "glacier_mass_balance",
    "url": "https://datahub.io/core/glacier-mass-balance/datapackage.json",
    "resource": "glaciers_csv",
    "headers": ["year", "mean_cumulative_mass_balance", "number_of_observations"],
    "staging_table": "glacier_staging"
}, {
    "id": "sea_level_change",
    "url": "https://datahub.io/core/sea-level-rise/datapackage.json",
    "resource": "csiro_recons_gmsl_mo_2015_csv",
    "headers": ["time", "gmsl"],
    "staging_table": "sealevel_staging"
}, {
    "id": "co2_ppm_trend",
    "url": "https://datahub.io/core/co2-ppm/datapackage.json",
    "resource": "co2-mm-mlo_csv",
    "headers": ["date", "decimal_date", "average", "interpolated", "trend", "number_days"],
    "staging_table": "co2_ppm_staging"
}, {
    "id": "population_growth",
    "url": "https://datahub.io/core/population/datapackage.json",
    "resource": "population_csv",
    "headers": ["country_name", "country_code", "year", "value"],
    "staging_table": "population_staging"
}]


def create_dag(id, url, resource, headers, staging_table):
    dag = DAG(dag_id=id,
              description='processing of global temperature data to Redshift',
              schedule_interval='0 3 1 * *',
              default_args=default_args)

    store_to_s3 = DatapackageToS3Operator(task_id='{id}_datapackage_to_s3'.format(id=id),
                                          package_url=url,
                                          resource=resource,
                                          headers=headers,
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

    store_to_s3 >> s3_to_staging

    return dag


# Generate DAG per dataset
for set in datasets:
    globals()[set["id"]] = create_dag(set["id"], set["url"], set["resource"], set["headers"], set["staging_table"])
