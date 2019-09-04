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
redshift_conn_id = 'redshift_connection'
redshift_schema = Variable.get('redshift_schema')
redshift_arn = Variable.get('redshift_arn')


dag = DAG(dag_id='global_temperature',
          description='processing of global temperature data to Redshift',
          schedule_interval='0 3 1 * *',
          default_args=default_args)

store_to_s3 = DatapackageToS3Operator(task_id='global_temp_datapackage_to_s3',
                                      package_url='https://datahub.io/core/global-temp/datapackage.json',
                                      resource='monthly_csv',
                                      s3_conn_id=s3_conn_id,
                                      s3_bucket=s3_bucket,
                                      s3_filepath=s3_filepath,
                                      dag=dag)

s3_to_staging = StageToRedshiftOperator(task_id='global_temp_stage_data',
                                        redshift_conn_id=redshift_conn_id,
                                        redshift_table='staging_global_temp',
                                        redshift_schema=redshift_schema,
                                        redshift_arn=redshift_arn,
                                        s3_bucket=s3_bucket,
                                        s3_key=s3_filepath,
                                        dag=dag)

store_to_s3 >> s3_to_staging
