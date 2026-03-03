#!/bin/bash
#
# HBase Cross-Region Snapshot Copy Script
# 
# This script copies an HBase snapshot from one S3 bucket to another across AWS regions
# while preserving data integrity using ETag checksums.
#
# Requirements: HBase 2.x on Amazon EMR
#
# Usage: ./hbase-cross-region-copy.sh <source_cluster_master> <source_bucket_path> <dest_bucket_path> <snapshot_name> [ssh_key]
#
# Example:
#   ./hbase-cross-region-copy.sh \
#     ec2-3-85-123-155.compute-1.amazonaws.com \
#     s3://source-bucket/hbase/ \
#     s3://dest-bucket/hbase/ \
#     snap_20260302_151152 \
#     ~/.ssh/my-key.pem

set -e

# Arguments
SOURCE_CLUSTER_MASTER="$1"
SOURCE_BUCKET_PATH="$2"
DEST_BUCKET_PATH="$3"
SNAPSHOT_NAME="$4"
SSH_KEY="${5:-~/.ssh/id_rsa}"

# Validate arguments
if [ -z "$SOURCE_CLUSTER_MASTER" ] || [ -z "$SOURCE_BUCKET_PATH" ] || [ -z "$DEST_BUCKET_PATH" ] || [ -z "$SNAPSHOT_NAME" ]; then
    echo "Usage: $0 <source_cluster_master> <source_bucket_path> <dest_bucket_path> <snapshot_name> [ssh_key]"
    echo ""
    echo "Arguments:"
    echo "  source_cluster_master  - EMR master node hostname or IP"
    echo "  source_bucket_path     - S3 path where HBase data currently resides"
    echo "  dest_bucket_path       - S3 path where data should be copied"
    echo "  snapshot_name          - Name of the HBase snapshot to copy"
    echo "  ssh_key                - SSH key to access EMR cluster (default: ~/.ssh/id_rsa)"
    echo ""
    echo "Example:"
    echo "  $0 ec2-3-85-123-155.compute-1.amazonaws.com \\"
    echo "     s3://source-bucket/hbase/ \\"
    echo "     s3://dest-bucket/hbase/ \\"
    echo "     snap_20260302_151152 \\"
    echo "     ~/.ssh/my-key.pem"
    exit 1
fi

echo "=========================================="
echo "HBase Cross-Region Snapshot Copy"
echo "=========================================="
echo "Source Cluster: $SOURCE_CLUSTER_MASTER"
echo "Source Path:    $SOURCE_BUCKET_PATH"
echo "Dest Path:      $DEST_BUCKET_PATH"
echo "Snapshot:       $SNAPSHOT_NAME"
echo "SSH Key:        $SSH_KEY"
echo "=========================================="
echo ""

# Step 1: Check if snapshot exists
echo "[1/4] Checking if snapshot exists..."
SNAPSHOT_EXISTS=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no hadoop@"$SOURCE_CLUSTER_MASTER" \
    "echo \"list_snapshots '$SNAPSHOT_NAME'\" | hbase shell 2>&1 | grep -c '$SNAPSHOT_NAME' || echo 0")

if [ "$SNAPSHOT_EXISTS" -eq 0 ]; then
    echo "ERROR: Snapshot '$SNAPSHOT_NAME' not found on source cluster"
    echo "Available snapshots:"
    ssh -i "$SSH_KEY" hadoop@"$SOURCE_CLUSTER_MASTER" "echo 'list_snapshots' | hbase shell 2>&1 | grep -A 100 SNAPSHOT"
    exit 1
fi
echo "✓ Snapshot exists"
echo ""

# Step 2: Export snapshot to destination bucket
echo "[2/4] Exporting snapshot to destination bucket..."
echo "This may take several minutes depending on data size..."
ssh -i "$SSH_KEY" hadoop@"$SOURCE_CLUSTER_MASTER" \
    "sudo -u hbase hbase snapshot export \
    -Dfs.s3a.etag.checksum.enabled=true \
    -snapshot $SNAPSHOT_NAME \
    -copy-to $DEST_BUCKET_PATH" 2>&1 | tee /tmp/export-output.log

# Check if export succeeded
if grep -q "ERROR" /tmp/export-output.log; then
    echo "ERROR: Export failed. Check /tmp/export-output.log for details"
    exit 1
fi
echo "✓ Export initiated"
echo ""

# Step 3: Monitor export progress
echo "[3/4] Monitoring export progress..."
LAST_APP_ID=""
while true; do
    JOB_STATUS=$(ssh -i "$SSH_KEY" hadoop@"$SOURCE_CLUSTER_MASTER" \
        "yarn application -list 2>&1 | grep ExportSnapshot | tail -1" || echo "")
    
    if [ -z "$JOB_STATUS" ]; then
        # Check if job completed successfully
        if [ -n "$LAST_APP_ID" ]; then
            FINAL_STATE=$(ssh -i "$SSH_KEY" hadoop@"$SOURCE_CLUSTER_MASTER" \
                "yarn application -status $LAST_APP_ID 2>&1 | grep 'Final-State' | awk '{print \$3}'")
            if [ "$FINAL_STATE" = "SUCCEEDED" ]; then
                echo "✓ Export job completed successfully"
            else
                echo "ERROR: Export job failed with state: $FINAL_STATE"
                exit 1
            fi
        fi
        break
    fi
    
    APP_ID=$(echo "$JOB_STATUS" | awk '{print $1}')
    PROGRESS=$(echo "$JOB_STATUS" | awk '{print $8}')
    LAST_APP_ID="$APP_ID"
    echo "Progress: $PROGRESS (Job: $APP_ID)"
    sleep 10
done
echo ""

# Step 4: Verify data copied
echo "[4/4] Verifying data in destination bucket..."
DEST_BUCKET=$(echo "$DEST_BUCKET_PATH" | sed 's|s3://||' | cut -d'/' -f1)
DEST_PREFIX=$(echo "$DEST_BUCKET_PATH" | sed 's|s3://||' | cut -d'/' -f2-)
DEST_REGION=$(aws s3api get-bucket-location --bucket "$DEST_BUCKET" --query 'LocationConstraint' --output text 2>/dev/null || echo "us-east-1")

if [ "$DEST_REGION" = "None" ] || [ -z "$DEST_REGION" ]; then
    DEST_REGION="us-east-1"
fi

DEST_SIZE=$(aws s3 ls "$DEST_BUCKET_PATH" --region "$DEST_REGION" --recursive --summarize 2>&1 | \
    grep "Total Size" | awk '{print $3, $4}')

if [ -z "$DEST_SIZE" ]; then
    echo "WARNING: Could not verify destination size. Please check manually."
else
    echo "✓ Data copied to destination: $DEST_SIZE"
fi
echo ""

echo "=========================================="
echo "COPY COMPLETED SUCCESSFULLY"
echo "=========================================="
echo "Source:      $SOURCE_BUCKET_PATH"
echo "Destination: $DEST_BUCKET_PATH"
echo "Snapshot:    $SNAPSHOT_NAME"
echo "Data Size:   $DEST_SIZE"
echo ""
echo "Next steps:"
echo "1. Launch EMR cluster in destination region pointing to: $DEST_BUCKET_PATH"
echo "2. Import snapshot on the new cluster using:"
echo ""
echo "   hbase snapshot import \\"
echo "     -Dfs.s3a.etag.checksum.enabled=true \\"
echo "     -snapshot $SNAPSHOT_NAME \\"
echo "     -copy-from $DEST_BUCKET_PATH \\"
echo "     -copy-to $DEST_BUCKET_PATH"
echo ""
echo "=========================================="
