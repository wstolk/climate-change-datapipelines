from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Load data from S3 to a staging table in AWS Redshift

    Will execute a query to insert data from S3 to staging tables in Redshift.
    :param redshift_conn_id:     Redshift connection ID.
    :type redshift_conn_id:      string
    :param table:                Table where the staging data should be stored.
    :type table:                 string
    :param schema:               Schema where the staging data should be stored.
    :type schema:                string
    :param s3_bucket:            S3 bucket where the source data is located.
    :type s3_bucket:             string
    :param s3_key:               S3 key where the source data is located.
    :type s3_key:                string
    :param arn:                  AWS ARN for the role to execute the query with.
    :type arn:                   string
    :param json_map:             S3 location where the JSON map can be found. Default value is auto
    :type json_map:              string
    """

    ui_color = '#358140'

    template_fields = ['redshift_conn_id', 'redshift_table', 'redshift_schema']

    @apply_defaults
    def __init__(self, redshift_conn_id, redshift_table, redshift_schema, redshift_arn, s3_bucket, s3_key,
                 json_map='auto', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_table = redshift_table
        self.redshift_schema = redshift_schema
        self.redshift_arn = redshift_arn
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_map = json_map

    def execute(self, context):
        # Setup connections
        postgres = PostgresHook(self.redshift_conn_id)

        # generate SQL query
        sql = """
        COPY {schema}.{table}
        FROM 's3://{bucket}/{key}'
        IAM_ROLE '{arn}'
        FORMAT AS json '{json_map}'
        """.format(schema=self.redshift_schema,
                   table=self.redshift_table,
                   bucket=self.s3_bucket,
                   key=self.s3_key,
                   arn=self.arn,
                   json_map=self.json_map)

        # execute SQL query in Redshift database
        self.log.info('Copying data from S3 to Redshift')
        postgres.run(sql)
        self.log.info('Copying data from S3 to Redshift completed')
