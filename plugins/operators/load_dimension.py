from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Load dimension data from staging tables operator

    Will execute a given query to select data from staging tables and insert into a dimension table.
    :param redshift_conn_id:     Redshift connection ID.
    :type redshift_conn_id:      string
    :param query:                Query to execute in the database.
    :type query:                 string
    :param schema:               Schema where the table is located the database.
    :type schema:                string
    :param table:                Table name where the data should be inserted.
    :type table:                 string
    :param columns:              Tuple of column names that will be inserted.
    :type columns:               tuple
    :param delete_before_load:   If true, the table will be truncated before inserting data.
    :type delete_before_load:    bool
    """
    ui_color = '#80BD9E'

    template_fields = ['redshift_conn_id', 'query']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 query='',
                 schema='',
                 table='',
                 columns=(),
                 delete_before_load=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.schema = schema
        self.table = table
        self.columns = columns
        self.delete_before_load = delete_before_load

    def execute(self, context):
        # Setup connections
        postgres = PostgresHook(self.redshift_conn_id)

        # Truncate table before insert (ony if delete_before_load is true)
        if self.delete_before_load:
            self.log.info("Truncating table before loading data")
            postgres.run(f"TRUNCATE TABLE {self.table};")
            self.log.info("Finished truncating table")

        # Generate SQL query
        self.log.info("Generating SQL query")
        query = """
        INSERT INTO {schema}.{table} {columns}
        {select_query}
        """.format(schema=self.schema,
                   table=self.table,
                   columns=str(self.columns),
                   select_query=self.query)

        # execute SQL query in Redshift database
        self.log.info('Copying data from staging table to dimension table')
        postgres.run(query)
        self.log.info('Copying data from staging table to dimension table completed')
