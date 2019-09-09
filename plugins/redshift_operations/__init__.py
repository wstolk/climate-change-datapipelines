from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
from redshift_operations.operators.stage_redshift import StageToRedshiftOperator
from redshift_operations.operators.staging_to_production import LoadStagingToProduction
from redshift_operations.operators.data_quality import DataQualityOperator


# Defining the plugin class
class RedshiftOperationsPlugin(AirflowPlugin):
    name = "redshift_operations_plugin"
    operators = [
        StageToRedshiftOperator,
        LoadStagingToProduction,
        DataQualityOperator
    ]
