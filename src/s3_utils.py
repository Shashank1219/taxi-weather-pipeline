"""Shared S3 upload helper, used by both ingestion scripts."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_s3_client():
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        raise EnvironmentError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY not valid"
        )
    return boto3.client("s3")


def object_exists(bucket, key):
    # Check if an S3 object already exists, without downloading it
    client = get_s3_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def upload_file_if_missing(local_path, bucket, key, force = False):
    if not force and object_exists(bucket, key):
        logger.info("s3://%s/%s already exists, skipping upload", bucket, key)
        return f"s3://{bucket}/{key}"

    logger.info("Uploading %s to s3://%s/%s", local_path, bucket, key)
    client = get_s3_client()
    client.upload_file(str(local_path), bucket, key)
    logger.info("Upload complete: s3://%s/%s", bucket, key)

    return f"s3://{bucket}/{key}"