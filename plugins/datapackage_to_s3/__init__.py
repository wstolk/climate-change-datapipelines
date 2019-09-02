import logging

from airflow.plugins_manager import AirflowPlugin
from plugins.datapackage_to_s3.operators.datapackage_to_s3 import DatapackageToS3Operator

log = logging.getLogger(__name__)


class DataPackageToS3Plugin(AirflowPlugin):
    name = "datapackage_to_s3_plugin"
    operators = [DatapackageToS3Operator]
