"""
Storage abstraction layer — local filesystem or AWS S3.

Backend is selected (in priority order) by:
  1. Airflow Variable  PIPELINE_STORAGE_BACKEND = 'local' | 's3'
  2. Env var           PIPELINE_STORAGE_BACKEND          (default: 'local')

S3 Variables / env vars (when backend = 's3'):
  PIPELINE_S3_BUCKET   – required   e.g. 'your-pipeline-bucket'
  PIPELINE_S3_PREFIX   – optional   e.g. 'airflow/data'  (no leading slash)
  AWS_DEFAULT_REGION   – optional   (default: 'us-east-1')

AWS credentials are resolved by boto3 in the standard order:
  env vars → ~/.aws/credentials → IAM instance / ECS task role.
  You can also set them as the Airflow Connection 'aws_default'.

Usage (operators):
    from utils.storage import get_storage
    storage = get_storage()
    files = storage.list_files('/opt/airflow/data/descriptions')
    local  = storage.local_path('/opt/airflow/data/descriptions/book.docx')
    ...

Both backends expose the same interface so operators are backend-agnostic.
"""

import io
import logging
import os
import tempfile
from pathlib import Path
from typing import List, Optional

log = logging.getLogger(__name__)

# Local data root inside the container — stripped when building S3 keys.
_LOCAL_DATA_ROOT = '/opt/airflow/data'


def _airflow_var(name: str, default: str = '') -> str:
    """Read an Airflow Variable, falling back to an environment variable."""
    try:
        from airflow.models import Variable
        return Variable.get(name, default_var=os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)


def get_storage():
    """Return the configured storage backend (LocalStorage or S3Storage)."""
    backend = _airflow_var('PIPELINE_STORAGE_BACKEND', 'local').strip().lower()
    if backend == 's3':
        return S3Storage()
    return LocalStorage()


# ─── Local filesystem ────────────────────────────────────────────────────────

class LocalStorage:
    """Local / NFS / EFS filesystem storage (default, single-host or shared mount)."""

    def list_files(self, directory: str) -> List[str]:
        """Return basenames of files directly inside *directory*."""
        if not os.path.exists(directory):
            return []
        return [f for f in os.listdir(directory)
                if os.path.isfile(os.path.join(directory, f))]

    def list_dirs(self, directory: str) -> List[str]:
        """Return basenames of subdirectories directly inside *directory*."""
        if not os.path.exists(directory):
            return []
        return [d for d in os.listdir(directory)
                if os.path.isdir(os.path.join(directory, d))]

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def is_dir(self, path: str) -> bool:
        return os.path.isdir(path)

    def read_bytes(self, path: str) -> bytes:
        with open(path, 'rb') as f:
            return f.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data)

    def write_text(self, path: str, text: str, encoding: str = 'utf-8') -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding=encoding) as f:
            f.write(text)

    def local_path(self, path: str) -> str:
        """Return a local filesystem path (identity for LocalStorage)."""
        return path

    def as_fileobj(self, path: str) -> io.RawIOBase:
        """Return an open binary file object for *path*."""
        return open(path, 'rb')

    def makedirs(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def __repr__(self) -> str:
        return 'LocalStorage()'


# ─── AWS S3 ──────────────────────────────────────────────────────────────────

class S3Storage:
    """AWS S3 storage backend."""

    def __init__(self) -> None:
        self._bucket: str = _airflow_var('PIPELINE_S3_BUCKET')
        prefix = _airflow_var('PIPELINE_S3_PREFIX', 'airflow/data').strip('/')
        self._prefix: str = prefix
        self._region: str = _airflow_var('AWS_DEFAULT_REGION', 'us-east-1')
        self._client = None

    @property
    def client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client('s3', region_name=self._region)
        return self._client

    def _key(self, local_path: str) -> str:
        """Convert a container-local path to an S3 key."""
        path = str(local_path)
        for root in (_LOCAL_DATA_ROOT + '/', _LOCAL_DATA_ROOT, 'data/'):
            if path.startswith(root):
                path = path[len(root):]
                break
        path = path.lstrip('/')
        return f"{self._prefix}/{path}" if self._prefix else path

    def _dir_prefix(self, directory: str) -> str:
        return self._key(directory).rstrip('/') + '/'

    def list_files(self, directory: str) -> List[str]:
        """Return basenames of objects (not "folders") directly under *directory*."""
        prefix = self._dir_prefix(directory)
        paginator = self.client.get_paginator('list_objects_v2')
        result = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter='/'):
            for obj in page.get('Contents', []):
                name = obj['Key'][len(prefix):]
                if name and '/' not in name:
                    result.append(name)
        return result

    def list_dirs(self, directory: str) -> List[str]:
        """Return basenames of common prefixes (virtual "folders") under *directory*."""
        prefix = self._dir_prefix(directory)
        paginator = self.client.get_paginator('list_objects_v2')
        result = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter='/'):
            for cp in page.get('CommonPrefixes', []):
                name = cp['Prefix'][len(prefix):].rstrip('/')
                if name:
                    result.append(name)
        return result

    def exists(self, path: str) -> bool:
        key = self._key(path)
        try:
            self.client.head_object(Bucket=self._bucket, Key=key)
            return True
        except Exception:
            # Might be a "directory" (prefix with objects under it)
            try:
                resp = self.client.list_objects_v2(
                    Bucket=self._bucket,
                    Prefix=key.rstrip('/') + '/',
                    MaxKeys=1,
                )
                return bool(resp.get('Contents'))
            except Exception:
                return False

    def is_dir(self, path: str) -> bool:
        key = self._key(path).rstrip('/') + '/'
        try:
            resp = self.client.list_objects_v2(
                Bucket=self._bucket, Prefix=key, MaxKeys=1,
            )
            return bool(resp.get('Contents'))
        except Exception:
            return False

    def read_bytes(self, path: str) -> bytes:
        key = self._key(path)
        resp = self.client.get_object(Bucket=self._bucket, Key=key)
        return resp['Body'].read()

    def write_bytes(self, path: str, data: bytes) -> None:
        key = self._key(path)
        self.client.put_object(Bucket=self._bucket, Key=key, Body=data)
        log.info("S3 write: s3://%s/%s (%d bytes)", self._bucket, key, len(data))

    def write_text(self, path: str, text: str, encoding: str = 'utf-8') -> None:
        self.write_bytes(path, text.encode(encoding))

    def local_path(self, path: str) -> str:
        """Download an S3 object to a temp file and return its local path.

        The caller is responsible for unlinking the temp file when done::

            tmp = storage.local_path(s3_path)
            try:
                process(tmp)
            finally:
                os.unlink(tmp)
        """
        key = self._key(path)
        suffix = Path(path).suffix or ''
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        log.info("S3 download: s3://%s/%s → %s", self._bucket, key, tmp.name)
        self.client.download_fileobj(self._bucket, key, tmp)
        tmp.flush()
        tmp.close()
        return tmp.name

    def as_fileobj(self, path: str) -> io.BytesIO:
        """Return a BytesIO of the object (for SFTP putfo, etc.)."""
        return io.BytesIO(self.read_bytes(path))

    def makedirs(self, path: str) -> None:
        """No-op for S3 (keys don't require parent "directories")."""
        pass

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload a local file to the equivalent S3 key."""
        key = self._key(remote_path)
        log.info("S3 upload: %s → s3://%s/%s", local_path, self._bucket, key)
        self.client.upload_file(local_path, self._bucket, key)

    def __repr__(self) -> str:
        return f'S3Storage(bucket={self._bucket!r}, prefix={self._prefix!r})'
