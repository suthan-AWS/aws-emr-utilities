# HBase Cross-Region Snapshot Copy

A utility to copy HBase snapshots from one S3 bucket to another across AWS regions while preserving data integrity using ETag checksums.

## Overview

This script automates the process of copying HBase snapshots between AWS regions using the HBase snapshot export feature with ETag checksum validation. It ensures data integrity during cross-region transfers without requiring manual S3 sync operations or disabling checksum verification.

## Features

- Data Integrity: Uses ETag checksums to validate data during transfer
- Cross-Region Support: Copy snapshots between any AWS regions
- Progress Monitoring: Real-time progress tracking of the export job
- Automatic Validation: Verifies snapshot exists before starting
- Error Handling: Comprehensive error checking and reporting

## Prerequisites

- HBase 2.x running on Amazon EMR
- Source cluster with HBase snapshot already created
- SSH access to the source EMR cluster master node
- AWS CLI configured with appropriate permissions
- IAM permissions for S3 operations on both source and destination buckets

## Usage

```bash
./hbase-cross-region-copy.sh <source_cluster_master> <source_bucket_path> <dest_bucket_path> <snapshot_name> [ssh_key]
```

### Arguments

| Argument | Description | Required |
|----------|-------------|----------|
| `source_cluster_master` | EMR master node hostname or IP address | Yes |
| `source_bucket_path` | S3 path where HBase data currently resides | Yes |
| `dest_bucket_path` | S3 path where data should be copied | Yes |
| `snapshot_name` | Name of the HBase snapshot to copy | Yes |
| `ssh_key` | SSH key to access EMR cluster | No (default: ~/.ssh/id_rsa) |

### Example

```bash
./hbase-cross-region-copy.sh \
  ec2-3-85-123-155.compute-1.amazonaws.com \
  s3://source-bucket-us-east-1/hbase/ \
  s3://dest-bucket-ap-south-1/hbase/ \
  snap_20260302_151152 \
  ~/.ssh/my-emr-key.pem
```

## How It Works

1. **Validation**: Checks if the specified snapshot exists on the source cluster
2. **Export**: Initiates HBase snapshot export with ETag checksum enabled
3. **Monitoring**: Tracks the MapReduce job progress in real-time
4. **Verification**: Confirms data was successfully copied to the destination

The script uses the `-Dfs.s3a.etag.checksum.enabled=true` flag to ensure that S3 ETags are used for checksum validation, preventing the "checksum mismatch" errors that can occur with cross-region S3 operations.

## Performance

The script uses HBase's MapReduce-based snapshot export, which runs with parallel mappers for efficient data transfer.

**Typical performance:**
- Speed: 2-3 GB/s (varies based on data size, network conditions, and number of mappers)
- Duration: ~10 minutes per 1.5 TiB of data

**Tuning for better performance:**

You can control the number of mappers to improve transfer speed by adding the `-mappers` flag:

```bash
# On the source cluster, modify the export command to use more mappers
sudo -u hbase hbase snapshot export \
  -Dfs.s3a.etag.checksum.enabled=true \
  -snapshot snap_name \
  -copy-to s3://dest-bucket/hbase/ \
  -mappers 200
```

Increasing the number of mappers can significantly improve throughput for large datasets, depending on your cluster size and available resources. Start with 100-200 mappers and adjust based on your cluster capacity.

## Next Steps After Copy

Once the copy is complete, you can:

1. Launch a new EMR cluster in the destination region
2. Configure the cluster to use the destination S3 bucket as HBase root directory
3. Import the snapshot on the new cluster:

```bash
hbase snapshot import \
  -Dfs.s3a.etag.checksum.enabled=true \
  -snapshot snap_20260302_151152 \
  -copy-from s3://dest-bucket-ap-south-1/hbase/ \
  -copy-to s3://dest-bucket-ap-south-1/hbase/
```

## Troubleshooting

### Snapshot Not Found
If the script reports that the snapshot doesn't exist:
- Verify the snapshot name is correct
- Check that you're connected to the correct cluster
- List available snapshots: `echo "list_snapshots" | hbase shell`

### Export Job Fails
If the export job fails:
- Check `/tmp/export-output.log` for detailed error messages
- Verify IAM permissions for both source and destination buckets
- Ensure the destination bucket exists and is accessible

### SSH Connection Issues
If you can't connect to the cluster:
- Verify the SSH key path is correct
- Check security group rules allow SSH access
- Ensure the master node hostname/IP is correct

## IAM Permissions Required

The EMR cluster's IAM role needs the following S3 permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::source-bucket/*",
        "arn:aws:s3:::source-bucket",
        "arn:aws:s3:::dest-bucket/*",
        "arn:aws:s3:::dest-bucket"
      ]
    }
  ]
}
```

## References

- [AWS EMR HBase Best Practices - Data Integrity](https://aws.github.io/aws-emr-best-practices/docs/bestpractices/Applications/HBase/data_integrity)
- [HBase Snapshot Documentation](https://hbase.apache.org/book.html#ops.snapshots)
- [Amazon EMR HBase on S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase-s3.html)

## License

This utility is licensed under the MIT-0 License. See the [LICENSE](../../LICENSE) file in the repository root.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
