from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import DatapackageToS3Operator, StageToRedshiftOperator

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
redshift_conn = 'redshift_connection'
redshift_schema = Variable.get('redschift_schema')
redshift_arn = Variable.get('redshift_arn')


dag = DAG(dag_id='global_temperature',
          description='processing of global temperature data to Redshift',
          schedule_interval='0 3 1 * *',
          default_args=default_args)

store_to_s3 = DatapackageToS3Operator(task_id='global_temp_datapackage_to_s3',
                                      dag=dag,
                                      url='https://datahub.io/core/global-temp/datapackage.json',
                                      resource='monthly_csv',
                                      s3_conn_id=s3_conn_id,
                                      s3_bucket=s3_bucket,
                                      s3_filepath=s3_filepath)

s3_to_staging = StageToRedshiftOperator(task_id='global_temp_stage_data',
                                        dag=dag,
                                        schema=redshift_schema,
                                        table='staging_global_temp',
                                        s3_bucket=s3_bucket,
                                        s3_key=s3_filepath,
                                        arn=redshift_arn,
                                        redshift_conn_id=redshift_conn)

store_to_s3 >> s3_to_staging
