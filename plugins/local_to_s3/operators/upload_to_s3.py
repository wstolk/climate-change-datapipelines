import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UploadToS3Operator(BaseOperator):
    """
    An Apache Airflow operator to retrieve a resource from a datapackage and upload the resulting CSV to AWS S3.

    :param path:            path to the file
    :type path:             str
    :param s3_conn_id:      name of the S3 connection ID
    :type s3_conn_id:       str
    :param s3_bucket:       name of the S3 bucket where the file will be uploaded
    :type s3_bucket:        str
    :param s3_filepath:     filepath where the file will be uploaded to (including the key of the file)
    :type s3_filepath:      str
    """

    template_fields = ['path', 's3_conn_id', 's3_bucket', 's3_filepath']

    @apply_defaults
    def __init__(self, path, s3_conn_id, s3_bucket, s3_filepath, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_filepath = s3_filepath

    def execute(self, context):
        # Upload locally stored CSV to S3
        logging.info("uploading local CSV {fn} to S3://{bucket}/{fp}".format(fn=self.path,
                                                                             bucket=self.s3_bucket,
                                                                             fp=self.s3_filepath))
        s3 = S3Hook(aws_conn_id=self.s3_conn_id)
        s3.load_file(filename=self.path,
                     key=self.s3_filepath,
                     bucket_name=self.s3_bucket,
                     replace=True)

        # Remove local file after upload
        logging.info("finished uploading to S3")
