"""
S3 Event-Driven Spark Event Log Copy Lambda

Triggered by either:
  1. S3 Event Notifications (s3:ObjectCreated:*) — batch of Records
  2. EventBridge Object Created events — single event per invocation

Copies ONLY Spark event logs (objects under a `sparklogs/` path segment) to the
destination bucket, flattening them into a single Spark History Server log directory:

    source:      .../sparklogs/eventlog_v2_<job-run-id>/events_0_<job-run-id>
    destination: logs/eventlog_v2_<job-run-id>/events_0_<job-run-id>

Works with any source prefix depth (logs/applications/..., dataproc-emr-serverless/...,
etc.) — the flatten logic finds `/sparklogs/` anywhere in the key.

Environment variables:
    DESTINATION_BUCKET  (required) - target bucket name
    DESTINATION_PREFIX  (optional) - SHS log directory prefix (default: "logs/")

Memory: 128 MB | Timeout: 60s
"""

import json
import logging
import os
import urllib.parse

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3", config=Config(retries={"max_attempts": 3, "mode": "adaptive"}))

DESTINATION_BUCKET = os.environ["DESTINATION_BUCKET"]
DESTINATION_PREFIX = os.environ.get("DESTINATION_PREFIX", "logs/")

SPARKLOGS_MARKER = "/sparklogs/"

# copy_object supports up to 5 GiB. Larger objects need multipart UploadPartCopy.
COPY_OBJECT_MAX_BYTES = 5 * 1024 * 1024 * 1024


def _normalize_events(event):
    """Yield (bucket, key, size) tuples from either S3 notification or EventBridge shape.

    S3 notification: event["Records"][*]["s3"]["bucket"]["name"] / ["object"]["key"]
      - key is URL-encoded (spaces become '+', special chars percent-encoded)
    EventBridge:     event["detail"]["bucket"]["name"] / event["detail"]["object"]["key"]
      - key is plain (not URL-encoded), single event per invocation
    """
    if "Records" in event:
        for record in event["Records"]:
            try:
                bucket = record["s3"]["bucket"]["name"]
                key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
                size = record["s3"]["object"].get("size", 0)
                yield bucket, key, size
            except KeyError:
                logger.error("Malformed S3 notification record, skipping: %s", json.dumps(record))
                yield None, None, None
    elif "detail" in event:
        detail = event["detail"]
        try:
            bucket = detail["bucket"]["name"]
            key = detail["object"]["key"]
            size = detail["object"].get("size", 0)
            yield bucket, key, size
        except KeyError:
            logger.error("Malformed EventBridge detail, skipping: %s", json.dumps(event)[:500])
            yield None, None, None
    else:
        logger.error("Unrecognized event shape: %s", json.dumps(event)[:500])


def destination_key_for(source_key):
    """Map a source key to its flattened SHS destination key, or None to skip."""
    idx = source_key.find(SPARKLOGS_MARKER)
    if idx == -1:
        return None
    return DESTINATION_PREFIX + source_key[idx + len(SPARKLOGS_MARKER):]


def lambda_handler(event, context):
    """Process S3 event notification or EventBridge Object Created events."""
    copied, skipped, failed = 0, 0, 0

    for bucket, key, size in _normalize_events(event):
        if bucket is None:
            failed += 1
            continue

        destination_key = destination_key_for(key)
        if destination_key is None:
            skipped += 1
            logger.info("Skipped (not a spark event log): s3://%s/%s", bucket, key)
            continue

        try:
            if size > COPY_OBJECT_MAX_BYTES:
                _multipart_copy(bucket, key, destination_key, size)
            else:
                s3.copy_object(
                    CopySource={"Bucket": bucket, "Key": key},
                    Bucket=DESTINATION_BUCKET,
                    Key=destination_key,
                )
            copied += 1
            logger.info(
                "Copied s3://%s/%s -> s3://%s/%s (%d bytes)",
                bucket, key, DESTINATION_BUCKET, destination_key, size,
            )
        except Exception:
            failed += 1
            logger.exception(
                "Failed to copy s3://%s/%s -> s3://%s/%s",
                bucket, key, DESTINATION_BUCKET, destination_key,
            )

    total = copied + skipped + failed
    result = {"copied": copied, "skipped": skipped, "failed": failed, "total": total}
    logger.info("Batch complete: %s", json.dumps(result))
    if failed > 0:
        raise RuntimeError(
            f"{failed}/{total} copies failed — raising to trigger async retry/DLQ"
        )
    return result


def _multipart_copy(source_bucket, source_key, destination_key, size):
    """Multipart server-side copy for objects > 5 GiB."""
    part_size = 1 * 1024 * 1024 * 1024  # 1 GiB parts
    mpu = s3.create_multipart_upload(Bucket=DESTINATION_BUCKET, Key=destination_key)
    upload_id = mpu["UploadId"]
    try:
        parts = []
        part_number = 1
        offset = 0
        while offset < size:
            last_byte = min(offset + part_size, size) - 1
            resp = s3.upload_part_copy(
                Bucket=DESTINATION_BUCKET,
                Key=destination_key,
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource={"Bucket": source_bucket, "Key": source_key},
                CopySourceRange=f"bytes={offset}-{last_byte}",
            )
            parts.append({"ETag": resp["CopyPartResult"]["ETag"], "PartNumber": part_number})
            offset = last_byte + 1
            part_number += 1
        s3.complete_multipart_upload(
            Bucket=DESTINATION_BUCKET,
            Key=destination_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        s3.abort_multipart_upload(
            Bucket=DESTINATION_BUCKET, Key=destination_key, UploadId=upload_id
        )
        raise
