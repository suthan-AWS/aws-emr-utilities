# EMR Serverless Config Advisor

An automated pipeline that analyzes Spark event logs from EMR on EC2 or EMR Serverless applications and generates optimized EMR Serverless configuration recommendations.

## Overview

This utility processes Spark event logs to extract performance metrics and automatically recommends:
- Worker type (Small/Medium/Large/XLarge)
- Executor count (min/max for dynamic allocation)
- Memory and vCPU allocation
- Optimized shuffle partitions
- Complete Spark configuration for EMR Serverless

## Features

- **Automated Pipeline**: End-to-end processing from event logs to recommendations
- **Dual Optimization Modes**: Cost-optimized vs Performance-optimized configurations
- **Parallel Processing**: Multi-threaded extraction with configurable workers
- **S3 Integration**: Direct S3 read/write with streaming decompression
- **Rolling Log Support**: Handles both single and rolling event logs
- **Configurable Partition Size**: Adjust shuffle parallelism (default: 1GB)
- **Job Config Format**: Optional output in deployment-ready format
- **18 Metric Extractors**: Comprehensive analysis including:
  - Input/output data volumes
  - Shuffle read/write patterns
  - Memory utilization and spilling
  - CPU utilization and idle time
  - Task execution statistics
  - Stage-level performance metrics

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PIPELINE WORKFLOW                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Event Logs (S3 or Local)                                   │
│       │                                                      │
│       ▼                                                      │
│  ┌──────────────────────────────────────┐                  │
│  │  STAGE 1: spark_processor.py         │                  │
│  │  - Discover & group event logs       │                  │
│  │  - Extract 18 metrics per app        │                  │
│  │  - Parallel processing (20 workers)  │                  │
│  │  - Output: JSON metrics (S3/Local)   │                  │
│  └──────────────────────────────────────┘                  │
│       │                                                      │
│       ▼                                                      │
│  ┌──────────────────────────────────────┐                  │
│  │  STAGE 2: emr_recommender.py         │                  │
│  │  - Load metrics (S3 or Local)        │                  │
│  │  - Dual-mode optimization            │                  │
│  │  - Calculate worker requirements     │                  │
│  │  - Optimize shuffle partitions       │                  │
│  │  - Generate Spark configs            │                  │
│  │  - Output: Cost + Perf JSON          │                  │
│  └──────────────────────────────────────┘                  │
│       │                                                      │
│       ▼                                                      │
│  EMR Serverless Recommendations                             │
│  - Cost-optimized configs                                   │
│  - Performance-optimized configs                            │
│  - Optional: Job config format                              │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Algorithm

### Optimization Modes

#### Cost-Optimized Mode (Default)
Conservative executor allocation based on resource pressure:
```python
pressure = mem_utilization * 0.4 + cpu_utilization * 0.4 + spill_ratio * 0.2
scaling_factor = 0.5 + (pressure / 100) * 1.0  # Range: 0.5 to 1.5
max_executors = base_requirement * scaling_factor
```

**Best for:** Budget-conscious workloads, development/testing environments

#### Performance-Optimized Mode
Aggressive scaling for high memory pressure (>75%):
```python
if memory_utilization > 75%:
    scaling_factor = 1.0 + ((memory_utilization - 75) / 25) * 0.5  # Range: 1.0 to 1.5
else:
    # Use standard pressure calculation
```

**Best for:** Production SLA-critical workloads, jobs with memory pressure

**Impact:** 5-21% more executors for memory-stressed jobs (>75% utilization)

### Stage 1: Metric Extraction (`spark_processor.py`)

1. **Event Log Discovery**
   - Scans S3 prefix for event logs (`.zst`, `.gz`, `.lz4`, uncompressed)
   - Groups logs by application (handles rolling logs)
   - Identifies application names from log metadata

2. **Parallel Processing**
   - Spawns worker threads (default: 20)
   - Streams and decompresses logs from S3
   - Parses JSON events line-by-line

3. **Metric Extraction** (18 extractors):
   - **Data Volume**: Input bytes, output bytes, shuffle read/write
   - **Memory**: Executor memory, JVM heap, memory spilled
   - **CPU**: Executor cores, task CPU time, idle percentage
   - **Tasks**: Task count, duration, success/failure rates
   - **Stages**: Stage count, duration, shuffle dependencies
   - **Partitions**: Input partitions, output partitions
   - **Spill**: Memory spill, disk spill
   - **Time**: Application duration, stage durations

4. **Output**
   - Writes JSON per application to S3 staging area
   - Two outputs: `task_stage_summary/` and `spark_config_extract/`

### Stage 2: Recommendation Generation (`emr_recommender.py`)

1. **Load Metrics**
   - Reads JSON files from S3 staging area
   - Aggregates metrics across multiple runs (if available)

2. **Worker Type Selection**
   ```
   Memory per executor = max(
     input_data / parallelism * 1.2,
     shuffle_data / parallelism * 1.5,
     min_memory_threshold
   )
   
   Worker types:
   - Small:  2 vCPU,  16 GB  (< 20 GB needed)
   - Medium: 4 vCPU,  32 GB  (< 40 GB needed)
   - Large:  16 vCPU, 108 GB (< 120 GB needed)
   - XLarge: 32 vCPU, 256 GB (>= 120 GB needed)
   ```

3. **Executor Count Calculation**
   ```
   max_executors = ceil(
     max(
       input_gb / 100,
       shuffle_gb / 50,
       total_tasks / 1000
     )
   )
   
   min_executors = max(1, max_executors / 2)
   ```

4. **Shuffle Partition Optimization**
   ```
   target_partition_size = 1 GB (for shuffle-heavy workloads)
   
   shuffle_partitions = max(
     200,  # Spark default minimum
     ceil(shuffle_total_gb / target_partition_size)
   )
   ```

5. **Configuration Generation**
   - Generates complete Spark config for EMR Serverless
   - Includes driver sizing, dynamic allocation, shuffle settings
   - Adds EMR Serverless-specific configs (disk, network timeouts)

## Prerequisites

- Python 3.7+
- AWS credentials configured
- S3 bucket with Spark event logs
- Required Python packages:
  ```bash
  pip install boto3 zstandard pandas
  ```

## Usage

### Recommended: Dual-Mode Recommender with Local/S3 Support

Generate both cost-optimized and performance-optimized recommendations from local or S3 metrics:

```bash
# Local filesystem
python3 emr_recommender.py \
  --input-path /path/to/metrics/ \
  --output-cost recommendations_cost.json \
  --output-perf recommendations_perf.json

# S3 path
python3 emr_recommender.py \
  --input-path s3://YOUR_BUCKET/staging/ \
  --output-cost recommendations_cost.json \
  --output-perf recommendations_perf.json
```

**Features:**
- Works with both local filesystem and S3 paths
- No S3 credentials needed for local files
- Generates both cost and performance recommendations
- Optional job config format output
- Configurable shuffle partition size

**Parameters:**
- `--input-path`: Local directory or S3 path (s3://bucket/prefix) - **required**
- `--output-cost`: Output file for cost-optimized recommendations
- `--output-perf`: Output file for performance-optimized recommendations
- `--limit`: Max applications to process (default: 100)
- `--target-partition-size`: Shuffle partition size in MiB (default: 1024)
- `--format-job-config`: Generate deployment-ready job configs
- `--region`: AWS region (only for S3 paths)

**Advanced Options:**

```bash
# Custom partition size (smaller = more parallelism)
python3 emr_recommender.py \
  --input-path s3://YOUR_BUCKET/staging/ \
  --target-partition-size 512 \
  --limit 100

# Generate deployment-ready job configs
python3 emr_recommender.py \
  --input-path s3://YOUR_BUCKET/staging/ \
  --format-job-config \
  --limit 100
```

**Partition Size Impact:**

| Size (MiB) | Effect | Use Case |
|------------|--------|----------|
| 2048 | Fewer partitions, fewer executors | Large data, less parallelism |
| 1024 | Default (1 GB per partition) | Balanced (recommended) |
| 512 | 2x partitions, 2x executors | Shuffle-heavy workloads |
| 256 | 4x partitions, 4x executors | Extreme parallelism needed |

**Example:** With 512 MiB partitions, a job with 1008 partitions becomes 2016 partitions, and executors increase from 22 to 44.

### Option 2: Automated Pipeline

Run the complete pipeline with a single command:

```bash
# Local filesystem
python3 pipeline_wrapper.py \
  --input-path /path/to/event-logs/ \
  --output-path /path/to/output/ \
  --output recommendations.json \
  --limit 10 \
  --target-partition-size 1024 \
  --format-job-config

# S3 paths
python3 pipeline_wrapper.py \
  --input-path s3://YOUR_BUCKET/event-logs/ \
  --output-path s3://YOUR_BUCKET/staging/ \
  --output recommendations.json \
  --limit 10 \
  --target-partition-size 1024 \
  --format-job-config

# Legacy S3 format (backward compatible)
python3 pipeline_wrapper.py \
  --input-bucket YOUR_BUCKET \
  --input-prefix event-logs/ \
  --staging-prefix staging/ \
  --output recommendations.json \
  --limit 100
```

**Parameters:**
- `--input-path`: Local directory or S3 path (s3://bucket/prefix)
- `--output-path`: Local directory or S3 path for metrics output
- `--output`: Local filename for recommendations (JSON)
- `--limit`: Maximum number of applications to process (default: 100)
- `--target-partition-size`: Shuffle partition size in MiB (default: 1024)
- `--format-job-config`: Generate deployment-ready job configs
- `--region`: AWS region (default: us-east-1)
- `--skip-extraction`: Skip stage 1, use existing metrics

**Legacy S3 Parameters (backward compatible):**
- `--input-bucket`: S3 bucket containing event logs
- `--input-prefix`: S3 prefix/folder with event logs
- `--staging-bucket`: S3 bucket for intermediate JSON
- `--staging-prefix`: S3 prefix for intermediate JSON files

**Output:**
- `recommendations_cost.json`: Cost-optimized recommendations
- `recommendations_perf.json`: Performance-optimized recommendations
- `recommendations_cost_job_config.json`: Deployment-ready cost configs (if --format-job-config)
- `recommendations_perf_job_config.json`: Deployment-ready perf configs (if --format-job-config)
- Metrics directory: Intermediate JSON metrics (task_stage_summary/, spark_config_extract/)

### Option 3: Manual Two-Stage Process

**Stage 1: Extract Metrics**

```bash
python3 spark_processor.py \
  --input-bucket YOUR_BUCKET \
  --input-prefix event-logs/ \
  --output-bucket YOUR_BUCKET \
  --output-prefix staging/
```

**Stage 2: Generate Recommendations**

```bash
python3 emr_recommender.py \
  --s3-path s3://YOUR_BUCKET/staging/ \
  --region us-east-1 \
  --output recommendations.json \
  --limit 100
```

### Option 3: Run on EMR Cluster

For large-scale processing, run on an EMR cluster with high memory:

```bash
# Upload scripts to S3
aws s3 cp pipeline_wrapper.py s3://YOUR_BUCKET/scripts/
aws s3 cp spark_processor.py s3://YOUR_BUCKET/scripts/
aws s3 cp emr_recommender.py s3://YOUR_BUCKET/scripts/

# SSH to EMR master node
ssh -i your-key.pem hadoop@your-emr-master

# Download scripts
aws s3 cp s3://YOUR_BUCKET/scripts/pipeline_wrapper.py ~/
aws s3 cp s3://YOUR_BUCKET/scripts/spark_processor.py ~/
aws s3 cp s3://YOUR_BUCKET/scripts/emr_recommender.py ~/

# Install dependencies
pip3 install zstandard pandas

# Run pipeline
nohup python3 pipeline_wrapper.py \
  --input-bucket YOUR_BUCKET \
  --input-prefix event-logs/ \
  --staging-prefix staging/ \
  --output recommendations.json \
  > pipeline.log 2>&1 &

# Monitor progress
tail -f pipeline.log
```

## Example Output

### CSV Summary
```csv
application_id,application_name,input_gb,shuffle_ratio,worker_type,vcpu,memory_gb,max_executors,shuffle_partitions
app_20260301_143022_001,data-processing-batch,20873.32,46.18,Large,16,108,191,4938
app_20260302_081234_008,etl-incremental-job,12.14,16601.48,Large,16,108,22,1008
```

### JSON Recommendation (excerpt)
```json
{
  "application_id": "app_20260301_143022_001",
  "application_name": "data-processing-batch",
  "metrics": {
    "input_gb": 20873.32,
    "shuffle_ratio_percent": 46.18,
    "duration_hours": 0.76,
    "avg_memory_utilization_percent": 80.74
  },
  "worker": {
    "type": "Large",
    "vcpu": 16,
    "memory_gb": 108,
    "max_executors": 191,
    "min_executors": 95
  },
  "spark_configs": {
    "spark.executor.cores": "16",
    "spark.executor.memory": "108g",
    "spark.dynamicAllocation.maxExecutors": "191",
    "spark.sql.shuffle.partitions": "4938"
  }
}
```

## Performance

**Test Results** (10 applications, 1,686 event log files):
- Total processing time: 5.8 minutes
- Stage 1 (extraction): 342 seconds
- Stage 2 (recommendations): 3.6 seconds
- Throughput: ~5 files/second with 20 workers

**Scalability:**
- Tested with up to 62 applications
- Handles rolling logs (6-540 files per application)
- Supports compressed formats: zstd, gzip, lz4
- Parallel S3 uploads with 20 concurrent threads

## Configuration

### Worker Thread Count
Edit `spark_processor.py`:
```python
MAX_WORKERS = 20  # Adjust based on available CPU/memory
```

### S3 Upload Concurrency
Edit `spark_processor.py`:
```python
MAX_CONCURRENT_UPLOADS = 20  # Adjust based on network bandwidth
```

### Shuffle Partition Target Size
Edit `emr_recommender.py`:
```python
TARGET_PARTITION_SIZE_MIB = 1024  # 1 GB per partition
```

## Troubleshooting

### Issue: NoCredentialsError
**Solution:** Ensure AWS credentials are configured:
```bash
aws configure
# OR
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

### Issue: Memory errors during processing
**Solution:** Reduce worker count or run on larger instance:
```python
MAX_WORKERS = 10  # Reduce from 20
```

### Issue: S3 throttling errors
**Solution:** Reduce concurrent uploads:
```python
MAX_CONCURRENT_UPLOADS = 10  # Reduce from 20
```

### Issue: Event log parsing errors
**Solution:** Check log format and compression. Supported formats:
- Uncompressed JSON
- Gzip (`.gz`)
- Zstandard (`.zst`)
- LZ4 (`.lz4`)

## Limitations

- Requires Spark event logs in JSON format
- Does not analyze Spark UI or live applications
- Recommendations based on historical patterns

## Contributing

Contributions are welcome! Please submit pull requests or open issues for:
- Additional metric extractors
- Support for new compression formats
- Cost optimization algorithms
- Performance improvements

## License

This utility is licensed under the MIT-0 License. See the LICENSE file.
