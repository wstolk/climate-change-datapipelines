from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Data Quality Operator

    Will check the tables in Redshift for records. If a table is found with zero recors,
    a ValueError will be raised.
    :param redshift_conn_id:        Redshift connection ID.
    :type redshift_conn_id:         str
    :param tables:                  List of tables to check for quality.
    :type tables:                   list
    """

    ui_color = '#89DA59'

    template_fields = ['redshift_conn_id']

    @apply_defaults
    def __init__(self, redshift_conn_id, tables, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        # Setup connections and query
        postgres = PostgresHook(self.redshift_conn_id)
        query = ""

        # Dynamically generate SQL query
        for idx, table in enumerate(self.tables):
            query += f"SELECT COUNT(*) AS rows, '{table}' AS tablename FROM {table} "

            if idx != len(self.tables) - 1:
                query += "UNION ALL "

        # Execute query and loop over results
        res = postgres.get_records(query)

        for row in res:
            if int(row[0]) < 1:
                raise ValueError("No records where found for table {table}".format(table=row[1]))
            else:
                self.log.info("Number of records for table {table}: {rec}".format(table=row[1], rec=str(row[0])))

        self.log.info("Finished data quality check, everything is alright")
