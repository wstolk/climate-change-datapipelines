import logging
import os
import uuid

import pandas as pd

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datapackage import Package


class DatapackageToS3Operator(BaseOperator):
    """
    An Apache Airflow operator to retrieve a resource from a datapackage and upload the resulting CSV to AWS S3.

    :param url:             URL of the datapackage
    :type url:              str
    :param resource:        name of the resource in the datapackage you want to upload
    :type resource:         str
    :param s3_conn_id:      name of the S3 connection ID
    :type s3_conn_id:       str
    :param s3_bucket:       name of the S3 bucket where the file will be uploaded
    :type s3_bucket:        str
    :param s3_filepath:     filepath where the file will be uploaded to (including the key of the file)
    :type s3_filepath:      str
    """

    template_fields = ['url', 'resource', 's3_conn_id', 's3_bucket', 's3_filepath']

    @apply_defaults
    def __init__(self, package_url, resource, s3_conn_id, s3_bucket, s3_filepath, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = package_url
        self.resource = resource
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_filepath = s3_filepath

    def execute(self, context):
        # Retrieve data package
        logging.info("retrieving datapackage from URL: {url}".format(url=self.url))
        package = Package(self.url)

        # Read resource from package and store to Pandas Dataframe
        logging.info("package retrieved, reading resource {resource} into DF".format(resource=self.resource))
        resource = package.get_resource(self.resource).read()
        df = pd.DataFrame(resource)

        # Generate random unique filename for temporary storing CSV to filesystem
        fn_temp = str(uuid.uuid4())

        # Store Dataframe to locally stored CSV and upload to S3
        df.to_csv(fn_temp)
        logging.info("uploading local CSV {fn} to S3://{bucket}/{fp}".format(fn=fn_temp,
                                                                             bucket=self.s3_bucket,
                                                                             fp=self.s3_filepath))
        s3 = S3Hook(aws_conn_id=self.s3_conn_id)
        s3.load_file(filename=fn_temp,
                     key=self.s3_filepath,
                     bucket_name=self.s3_bucket,
                     replace=True)

        # Remove local file after upload
        logging.info("finished uploading to S3, removing local CSV")
        os.remove(fn_temp)
