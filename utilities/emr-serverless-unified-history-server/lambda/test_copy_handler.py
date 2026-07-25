"""Unit tests for copy_handler — no AWS access required (S3 client mocked)."""

import os
import unittest
from unittest.mock import MagicMock, patch

os.environ["DESTINATION_BUCKET"] = "dest-bucket"

import copy_handler


def s3_event(records):
    """Build an S3 notification event from (bucket, key, size) tuples."""
    return {"Records": [
        {"s3": {"bucket": {"name": b}, "object": {"key": k, "size": s}}}
        for b, k, s in records
    ]}


def eventbridge_event(bucket, key, size=0):
    """Build an EventBridge Object Created event."""
    return {
        "version": "0",
        "id": "example-id",
        "detail-type": "Object Created",
        "source": "aws.s3",
        "account": "123456789012",
        "time": "2026-07-08T06:29:41Z",
        "region": "us-east-1",
        "detail": {
            "version": "0",
            "bucket": {"name": bucket},
            "object": {"key": key, "size": size, "etag": "abc123"},
            "request-id": "req-1",
            "requester": "123456789012",
            "reason": "PutObject",
        },
    }


# --- Standard EMR Serverless key layout (logs/applications/...) ---
EVENTLOG_KEY = (
    "logs/applications/00g72ravf2a45t09/jobs/00g72rcefave480b/"
    "sparklogs/eventlog_v2_00g72rcefave480b/events_0_00g72rcefave480b"
)
APPSTATUS_KEY = (
    "logs/applications/00g72ravf2a45t09/jobs/00g72rcefave480b/"
    "sparklogs/eventlog_v2_00g72rcefave480b/appstatus_00g72rcefave480b"
)
STDERR_KEY = (
    "logs/applications/00g72ravf2a45t09/jobs/00g72rcefave480b/"
    "SPARK_DRIVER/stderr.gz"
)

# --- Deep-prefix layout (dataproc-emr-serverless/<namespace>/<label>/applications/...) ---
DATAPROC_EVENTLOG_KEY = (
    "dataproc-emr-serverless/team-analytics/batch-etl/applications/APP01/jobs/RUN01/"
    "sparklogs/eventlog_v2_RUN01/events_0_RUN01"
)
DATAPROC_DRIVER_KEY = (
    "dataproc-emr-serverless/team-analytics/batch-etl/applications/APP01/jobs/RUN01/"
    "SPARK_DRIVER/stderr.gz"
)


class TestS3NotificationEvents(unittest.TestCase):
    """Tests for the S3 Event Notification trigger path."""

    def setUp(self):
        self.mock_s3 = MagicMock()
        patcher = patch.object(copy_handler, "s3", self.mock_s3)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_eventlog_copied_to_flat_shs_directory(self):
        event = s3_event([("src-bucket", EVENTLOG_KEY, 337_647)])
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result, {"copied": 1, "skipped": 0, "failed": 0, "total": 1})
        self.mock_s3.copy_object.assert_called_once_with(
            CopySource={"Bucket": "src-bucket", "Key": EVENTLOG_KEY},
            Bucket="dest-bucket",
            Key="logs/eventlog_v2_00g72rcefave480b/events_0_00g72rcefave480b",
        )

    def test_appstatus_marker_also_copied(self):
        event = s3_event([("src-bucket", APPSTATUS_KEY, 2)])
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result["copied"], 1)
        self.assertEqual(
            self.mock_s3.copy_object.call_args.kwargs["Key"],
            "logs/eventlog_v2_00g72rcefave480b/appstatus_00g72rcefave480b",
        )

    def test_non_eventlog_objects_skipped(self):
        event = s3_event([
            ("src-bucket", STDERR_KEY, 5157),
            ("src-bucket", "logs/applications/app/jobs/run/job-metadata.log", 218),
            ("src-bucket", "logs/applications/app/jobs/run/SPARK_EXECUTOR/1/stdout.gz", 1806),
        ])
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result, {"copied": 0, "skipped": 3, "failed": 0, "total": 3})
        self.mock_s3.copy_object.assert_not_called()

    def test_multiple_apps_and_jobs_flatten_into_same_directory(self):
        keys = [
            ("src-bucket", "logs/applications/APP_A/jobs/RUN_1/sparklogs/eventlog_v2_RUN_1/events_0_RUN_1", 10),
            ("src-bucket", "logs/applications/APP_B/jobs/RUN_2/sparklogs/eventlog_v2_RUN_2/events_0_RUN_2", 10),
        ]
        copy_handler.lambda_handler(s3_event(keys), None)
        dest_keys = [c.kwargs["Key"] for c in self.mock_s3.copy_object.call_args_list]
        self.assertEqual(dest_keys, [
            "logs/eventlog_v2_RUN_1/events_0_RUN_1",
            "logs/eventlog_v2_RUN_2/events_0_RUN_2",
        ])

    def test_url_encoded_key_is_decoded(self):
        event = s3_event([("src-bucket", "logs/applications/a/jobs/r/sparklogs/eventlog%3Dv2/file+1", 100)])
        copy_handler.lambda_handler(event, None)
        self.assertEqual(
            self.mock_s3.copy_object.call_args.kwargs["Key"],
            "logs/eventlog=v2/file 1",
        )

    def test_error_logged_and_batch_continues(self):
        self.mock_s3.copy_object.side_effect = [Exception("AccessDenied"), None]
        event = s3_event([
            ("src-bucket", "logs/applications/a/jobs/r1/sparklogs/e1/f1", 10),
            ("src-bucket", "logs/applications/a/jobs/r2/sparklogs/e2/f2", 10),
        ])
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result, {"copied": 1, "skipped": 0, "failed": 1, "total": 2})

    def test_malformed_record_skipped(self):
        result = copy_handler.lambda_handler({"Records": [{"s3": {}}]}, None)
        self.assertEqual(result, {"copied": 0, "skipped": 0, "failed": 1, "total": 1})

    def test_large_object_uses_multipart(self):
        size = 6 * 1024**3  # 6 GiB > 5 GiB copy_object limit
        self.mock_s3.create_multipart_upload.return_value = {"UploadId": "u1"}
        self.mock_s3.upload_part_copy.return_value = {"CopyPartResult": {"ETag": "e"}}
        event = s3_event([("src-bucket", EVENTLOG_KEY, size)])
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result["copied"], 1)
        self.mock_s3.copy_object.assert_not_called()
        self.assertEqual(self.mock_s3.upload_part_copy.call_count, 6)  # 6 x 1 GiB parts
        self.mock_s3.complete_multipart_upload.assert_called_once()

    def test_multipart_aborts_on_failure(self):
        self.mock_s3.create_multipart_upload.return_value = {"UploadId": "u1"}
        self.mock_s3.upload_part_copy.side_effect = Exception("boom")
        event = s3_event([("src-bucket", EVENTLOG_KEY, 6 * 1024**3)])
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result["failed"], 1)
        self.mock_s3.abort_multipart_upload.assert_called_once()


class TestEventBridgeEvents(unittest.TestCase):
    """Tests for the EventBridge Object Created trigger path."""

    def setUp(self):
        self.mock_s3 = MagicMock()
        patcher = patch.object(copy_handler, "s3", self.mock_s3)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_eventbridge_eventlog_copied(self):
        event = eventbridge_event("src-bucket", EVENTLOG_KEY, 337_647)
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result, {"copied": 1, "skipped": 0, "failed": 0, "total": 1})
        self.mock_s3.copy_object.assert_called_once_with(
            CopySource={"Bucket": "src-bucket", "Key": EVENTLOG_KEY},
            Bucket="dest-bucket",
            Key="logs/eventlog_v2_00g72rcefave480b/events_0_00g72rcefave480b",
        )

    def test_eventbridge_non_eventlog_skipped(self):
        event = eventbridge_event("src-bucket", STDERR_KEY, 5157)
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result, {"copied": 0, "skipped": 1, "failed": 0, "total": 1})
        self.mock_s3.copy_object.assert_not_called()

    def test_eventbridge_deep_prefix_dataproc_layout(self):
        """Acceptance criterion: dataproc-emr-serverless/ns/lbl/applications/.../sparklogs/... flattens correctly."""
        event = eventbridge_event("dataproc-emr-872193204297-us-east-1", DATAPROC_EVENTLOG_KEY, 1024)
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result["copied"], 1)
        self.mock_s3.copy_object.assert_called_once_with(
            CopySource={"Bucket": "dataproc-emr-872193204297-us-east-1", "Key": DATAPROC_EVENTLOG_KEY},
            Bucket="dest-bucket",
            Key="logs/eventlog_v2_RUN01/events_0_RUN01",
        )

    def test_eventbridge_deep_prefix_driver_skipped(self):
        """Driver logs under deep prefix are skipped (no /sparklogs/ in key)."""
        event = eventbridge_event("dataproc-emr-872193204297-us-east-1", DATAPROC_DRIVER_KEY, 2048)
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result, {"copied": 0, "skipped": 1, "failed": 0, "total": 1})
        self.mock_s3.copy_object.assert_not_called()

    def test_eventbridge_key_not_url_encoded(self):
        """EventBridge keys are plain — no double-decode should happen."""
        key_with_special = "logs/applications/a/jobs/r/sparklogs/eventlog_v2_r/file name+special"
        event = eventbridge_event("src-bucket", key_with_special, 100)
        copy_handler.lambda_handler(event, None)
        self.assertEqual(
            self.mock_s3.copy_object.call_args.kwargs["Key"],
            "logs/eventlog_v2_r/file name+special",
        )

    def test_eventbridge_malformed_detail_handled(self):
        event = {"detail-type": "Object Created", "source": "aws.s3", "detail": {}}
        result = copy_handler.lambda_handler(event, None)
        self.assertEqual(result, {"copied": 0, "skipped": 0, "failed": 1, "total": 1})

    def test_unrecognized_event_shape(self):
        """Neither Records nor detail — should not crash."""
        result = copy_handler.lambda_handler({"foo": "bar"}, None)
        self.assertEqual(result, {"copied": 0, "skipped": 0, "failed": 0, "total": 0})


if __name__ == "__main__":
    unittest.main(verbosity=2)
