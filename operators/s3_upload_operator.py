"""
S3 Upload Operator — delivers IIIF images and manifests to an S3 bucket.

Replaces the SFTP-based FileTransferOperator for cloud-native deployments.
AWS credentials are resolved by boto3 in the standard order:
  env vars → ~/.aws/credentials → IAM instance / ECS task role.

Required Airflow Variable (or environment variable):
  DELIVERY_S3_BUCKET  — target bucket for all uploaded artefacts

Optional:
  DELIVERY_S3_PREFIX  — key prefix applied to every object (default: 'iiif')
  AWS_DEFAULT_REGION  — AWS region (default: 'eu-west-1')
"""

import os
import boto3
from airflow.models import BaseOperator

from operators.processed_books import update_book_status


_CONTENT_TYPES = {
    '.jpg':  'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png':  'image/png',
    '.tif':  'image/tiff',
    '.tiff': 'image/tiff',
    '.json': 'application/json',
    '.js':   'application/javascript',
    '.html': 'text/html',
}


def _content_type(path: str) -> str:
    return _CONTENT_TYPES.get(os.path.splitext(path)[1].lower(), 'application/octet-stream')


class S3UploadOperator(BaseOperator):
    """
    Upload IIIF artefacts (images + manifests) to a delivery S3 bucket.

    Each item in the upload list (pulled from XCom) must have:
      source_path  — absolute local path to the file
      s3_key       — object key relative to the bucket root (prefix applied on top)
      book_number  — used for status tracking
      file_type    — status flag written on success/failure (e.g. 'images_uploaded')

    :param upload_list_task_id: Task that pushes the upload list to XCom
    :param bucket:              Override DELIVERY_S3_BUCKET Variable/env var
    :param prefix:              Override DELIVERY_S3_PREFIX Variable/env var
    """

    def __init__(
        self,
        upload_list_task_id: str,
        bucket: str = '',
        prefix: str = '',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.upload_list_task_id = upload_list_task_id
        self._bucket_override = bucket
        self._prefix_override = prefix

    def execute(self, context):
        from airflow.models import Variable

        bucket = (
            self._bucket_override
            or Variable.get('DELIVERY_S3_BUCKET', default_var=os.getenv('DELIVERY_S3_BUCKET', ''))
        )
        if not bucket:
            raise ValueError(
                "DELIVERY_S3_BUCKET must be set as an Airflow Variable or environment variable"
            )

        prefix = (
            self._prefix_override
            or Variable.get('DELIVERY_S3_PREFIX', default_var=os.getenv('DELIVERY_S3_PREFIX', 'iiif'))
        ).rstrip('/')

        region = Variable.get(
            'AWS_DEFAULT_REGION', default_var=os.getenv('AWS_DEFAULT_REGION', 'eu-west-1')
        )

        upload_list = context['ti'].xcom_pull(task_ids=self.upload_list_task_id) or []
        if not upload_list:
            self.log.warning("No files to upload — skipping")
            return {'success': 0, 'failed': 0, 'book_results': {}}

        self.log.info(
            "S3 upload starting: %d file(s) → s3://%s/%s [region=%s]",
            len(upload_list), bucket, prefix, region,
        )

        s3 = boto3.client('s3', region_name=region)
        success = 0
        failed = 0
        book_results = {}

        for idx, item in enumerate(upload_list, 1):
            source_path = item['source_path']
            raw_key = item['s3_key'].lstrip('/')
            s3_key = f"{prefix}/{raw_key}" if prefix else raw_key
            book_number = item.get('book_number')
            file_type = item.get('file_type')

            try:
                self.log.info(
                    "Upload %d/%d: %s → s3://%s/%s",
                    idx, len(upload_list), source_path, bucket, s3_key,
                )
                s3.upload_file(
                    source_path,
                    bucket,
                    s3_key,
                    ExtraArgs={'ContentType': _content_type(source_path)},
                )
                success += 1
                self._track(book_results, book_number, file_type, True)
            except Exception as exc:
                self.log.error("Upload failed: %s → s3://%s/%s: %s", source_path, bucket, s3_key, exc)
                failed += 1
                self._track(book_results, book_number, file_type, False, str(exc))

        self.log.info("S3 Upload Summary: %d succeeded, %d failed", success, failed)

        results = {'success': success, 'failed': failed, 'book_results': book_results}
        context['ti'].xcom_push(key='upload_results', value=results)
        return results

    def _track(self, book_results, book_number, file_type, success, error=None):
        if not (book_number and file_type):
            return
        book_results.setdefault(book_number, {})[file_type] = success
        if not success and error:
            book_results[book_number][f'{file_type}_error'] = error
        update_book_status(book_number, file_type, success, error)
