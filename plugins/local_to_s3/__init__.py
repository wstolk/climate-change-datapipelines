import logging

from airflow.plugins_manager import AirflowPlugin
from local_to_s3.operators.upload_to_s3 import UploadToS3Operator

log = logging.getLogger(__name__)


class LocalToS3Plugin(AirflowPlugin):
    name = "local_to_s3_plugin"
    operators = [UploadToS3Operator]
