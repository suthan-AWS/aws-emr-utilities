# Spark S3 Express Shuffle Manager

A drop-in Apache Spark ShuffleManager that stores shuffle data on [Amazon S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) instead of local disk. Provides **spot instance resilience** (zero FetchFailed errors on executor loss) while achieving performance close to local EBS storage.

## Features

- **Spot resilience** — Shuffle data persists on S3 Express when executors are terminated. No FetchFailed errors, no stage retries.
- **Performance** — 30 min/iter on TPC-DS 3TB vs 37 min with ShuffleDataIO approach and 15.7 min with local EBS gp2.
- **Zero errors** — Retry logic with backoff handles transient S3 Express file-not-found issues.
- **Memory efficient** — Backpressure limits in-flight data to 48MB per task. LRU stream cache (500 max) prevents memory leaks.
- **Compatible** — Works with EMR 7.x (Spark 3.5.x). No changes to application code required.

## Prerequisites

1. **EMR cluster** running EMR 7.x (tested on EMR 7.12.0)
2. **S3 Express One Zone directory bucket** in the same Availability Zone as your cluster
3. **IAM permissions** for the EMR EC2 instance profile to read/write to the directory bucket

## Quick Start

### 1. Create an S3 Express One Zone directory bucket

Create a directory bucket in the same AZ as your EMR cluster:

```bash
aws s3api create-bucket \
  --bucket my-shuffle-bucket--use1-az4--x-s3 \
  --region us-east-1 \
  --create-bucket-configuration '{
    "Location": {"Type": "AvailabilityZone", "Name": "use1-az4"},
    "Bucket": {"Type": "Directory", "DataRedundancy": "SingleAvailabilityZone"}
  }'
```

### 2. Upload the jar to S3

```bash
aws s3 cp jars/spark-s3express-shuffle-manager.jar s3://your-bucket/jars/
```

### 3. Submit your Spark application

```bash
spark-submit --deploy-mode client \
  --class your.MainClass \
  --conf "spark.driver.extraClassPath=/path/to/spark-s3express-shuffle-manager.jar:$EXISTING_CP" \
  --conf spark.yarn.dist.jars=s3://your-bucket/jars/spark-s3express-shuffle-manager.jar \
  --conf "spark.executor.extraClassPath=spark-s3express-shuffle-manager.jar:$EXISTING_CP" \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.cloud.CloudShuffleManager \
  --conf "spark.shuffle.storage.path=s3a://my-shuffle-bucket--use1-az4--x-s3/shuffle/" \
  --conf spark.shuffle.storage.s3express.enabled=true \
  --conf spark.shuffle.storage.s3express.endpoint.region=us-east-1 \
  --conf spark.shuffle.cloud.fetchParallelism=50 \
  --conf spark.shuffle.service.enabled=false \
  --conf spark.dynamicAllocation.enabled=false \
  your-app.jar
```

## Configuration Reference

### Required

| Config | Value | Description |
|--------|-------|-------------|
| `spark.shuffle.manager` | `org.apache.spark.shuffle.cloud.CloudShuffleManager` | Enables the S3 Express shuffle manager |
| `spark.shuffle.storage.path` | `s3a://<directory-bucket>/shuffle/` | S3 Express directory bucket path for shuffle data |
| `spark.shuffle.storage.s3express.enabled` | `true` | Enables S3 Express optimizations |
| `spark.shuffle.storage.s3express.endpoint.region` | e.g. `us-east-1` | Region of the directory bucket |
| `spark.shuffle.service.enabled` | `false` | Must be disabled (no external shuffle service needed) |
| `spark.dynamicAllocation.enabled` | `false` | Recommended for consistent performance; dynamic allocation is supported since shuffle data persists on S3 |

### Recommended

| Config | Default | Recommended | Description |
|--------|---------|-------------|-------------|
| `spark.shuffle.cloud.fetchParallelism` | `50` | `50` | Parallel S3 GET threads per reduce task |
| `spark.driver.memory` | — | `64g` | Driver needs large heap for shuffle metadata |
| `spark.executor.memoryOverhead` | `2G` | `4G` | Extra off-heap memory for S3 connection buffers |
| `spark.executor.cores` | — | `4` | Cores per executor |
| `spark.executor.memory` | — | `6g` | Heap memory per executor |

### S3A Tuning

| Config | Recommended | Description |
|--------|-------------|-------------|
| `spark.hadoop.fs.s3a.connection.maximum` | `450` | Max S3 connections per executor |
| `spark.hadoop.fs.s3a.threads.max` | `64` | S3A internal thread pool |
| `spark.hadoop.fs.s3a.fast.upload.buffer` | `disk` | Buffer writes to disk to reduce heap pressure |
| `spark.hadoop.fs.s3a.multipart.size` | `134217728` | 128MB multipart upload size |
| `spark.hadoop.fs.s3a.change.detection.mode` | `none` | Skip ETag checks (S3 Express is strongly consistent) |
| `spark.hadoop.fs.s3a.endpoint.region` | e.g. `us-east-1` | Region for S3A |
| `spark.hadoop.fs.s3a.attempts.maximum` | `10` | Max retry attempts |
| `spark.hadoop.fs.s3a.retry.limit` | `10` | Retry limit |
| `spark.hadoop.fs.s3a.connection.establish.timeout` | `5000` | Connection timeout (ms) |
| `spark.hadoop.fs.s3a.readahead.range` | `4194304` | 4MB readahead |
| `spark.hadoop.fs.s3a.bucket.probe` | `0` | Skip bucket existence check |
| `spark.hadoop.fs.s3a.select.enabled` | `false` | Disable S3 Select |

### Timeout Settings

| Config | Recommended | Description |
|--------|-------------|-------------|
| `spark.network.timeout` | `2000` | Network timeout (seconds) |
| `spark.executor.heartbeatInterval` | `300s` | Heartbeat interval |

## Full Example Configuration

```bash
spark-submit --deploy-mode client \
  --class com.example.MyApp \
  --conf "spark.driver.extraClassPath=/usr/lib/spark/jars/spark-s3express-shuffle-manager.jar" \
  --conf spark.yarn.dist.jars=s3://my-bucket/jars/spark-s3express-shuffle-manager.jar \
  --conf "spark.executor.extraClassPath=spark-s3express-shuffle-manager.jar" \
  --conf spark.driver.cores=4 \
  --conf spark.driver.memory=64g \
  --conf spark.driver.memoryOverhead=4000 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=6g \
  --conf spark.executor.instances=47 \
  --conf spark.executor.memoryOverhead=4G \
  --conf spark.network.timeout=2000 \
  --conf spark.executor.heartbeatInterval=300s \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=false \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.cloud.CloudShuffleManager \
  --conf "spark.shuffle.storage.path=s3a://my-shuffle-bucket--use1-az4--x-s3/shuffle/" \
  --conf spark.shuffle.storage.s3express.enabled=true \
  --conf spark.shuffle.storage.s3express.endpoint.region=us-east-1 \
  --conf spark.shuffle.cloud.fetchParallelism=50 \
  --conf spark.hadoop.fs.s3a.change.detection.mode=none \
  --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
  --conf spark.hadoop.fs.s3a.select.enabled=false \
  --conf spark.hadoop.fs.s3a.bucket.probe=0 \
  --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk \
  --conf spark.hadoop.fs.s3a.multipart.size=134217728 \
  --conf spark.hadoop.fs.s3a.connection.maximum=450 \
  --conf spark.hadoop.fs.s3a.attempts.maximum=10 \
  --conf spark.hadoop.fs.s3a.retry.limit=10 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=5000 \
  --conf spark.hadoop.fs.s3a.readahead.range=4194304 \
  --conf spark.hadoop.fs.s3a.threads.max=64 \
  s3://my-bucket/app.jar
```

## Performance

Benchmarked with TPC-DS 3TB on EMR 7.12.0 (4× m7g.12xlarge, 47 executors):

| Shuffle Backend | Iter 1 | Iter 2 | Iter 3 | Spot Resilient |
|-----------------|--------|--------|--------|----------------|
| Local EBS gp2 (default) | 15.7 min | 15.7 min | 15.7 min | ❌ |
| S3 Express ShuffleDataIO | 37 min | 37 min | 37 min | ❌ |
| **S3 Express ShuffleManager** | **30.8 min** | **29.7 min** | **30.6 min** | **✅** |

### Spot Resilience Test

With AWS Fault Injection Service (FIS) terminating 3 out of 5 core nodes mid-query:

| Metric | Default Spark | S3 Express ShuffleManager |
|--------|--------------|---------------------------|
| FetchFailed errors | 14 | **0** |
| Stage retries | 3 | **0** |
| Queries completed | 5/5 | **5/5** |

## How It Works

The plugin replaces Spark's `SortShuffleManager` with `CloudShuffleManager` which:

1. **Writes** shuffle data to S3 Express using Spark's optimized `ExternalSorter.writePartitionedMapOutput()` path
2. **Reads** shuffle data with 50-thread parallel S3 GETs per reduce task, with batched reads (1 GET per mapper covering all needed partitions)
3. **Tracks** shuffle metadata on the driver via a custom `CloudMapOutputTracker` that never clears metadata on executor loss — this is the spot resilience mechanism
4. **Manages memory** with backpressure (48MB max in-flight per task) and LRU stream cache (500 max open S3 streams)

## Compatibility

| Component | Tested Version |
|-----------|---------------|
| EMR | 7.12.0 |
| Spark | 3.5.6 |
| Hadoop | 3.4.1 |
| Instance types | m7g.12xlarge (ARM/Graviton) |

## Limitations

- Performance is ~2x local EBS gp2 based on TPC-DS 3TB benchmark (30 min vs 15.7 min per iteration)
- S3 Express directory bucket must be in the same Availability Zone as the cluster

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
