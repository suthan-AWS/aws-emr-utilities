# EMR Serverless Config Advisor

Analyzes Spark event logs and generates optimized EMR Serverless configurations. Works with both EMR on EC2 and EMR Serverless event logs.

## Quick Start

```bash
pip install boto3 zstandard pandas
```

**Option A — Full pipeline** (event logs → metrics → recommendations):
```bash
python3 pipeline_wrapper.py \
  --input-path s3://YOUR_BUCKET/event-logs/ \
  --output-path s3://YOUR_BUCKET/staging/ \
  --output recommendations.json
```

**Option B — Recommendations only** (from pre-extracted metrics):
```bash
python3 emr_recommender.py \
  --input-path s3://YOUR_BUCKET/staging/ \
  --output-cost cost.json \
  --output-perf perf.json
```

Both commands work with local paths too — just use a directory path instead of `s3://`.

---

## How It Works

```
Event Logs (S3 or Local)
        │
        ▼
┌─────────────────────────────┐
│  Stage 1: spark_processor   │  Extract 18 metrics per app
│  (20 parallel workers)      │  Supports .zst, .gz, .lz4
└─────────────────────────────┘
        │
        ▼
┌─────────────────────────────┐
│  Stage 2: emr_recommender   │  Generate Spark configs
│  (cost + performance modes) │  Worker type, executors, shuffle
└─────────────────────────────┘
        │
        ▼
  JSON recommendations
  (or Iceberg table)
```

The recommender analyzes input/output data volumes, shuffle patterns, memory utilization, CPU usage, spill ratios, and task statistics to determine:

- **Worker type**: Small (2 vCPU/16 GB) → XLarge (32 vCPU/256 GB)
- **Executor count**: min/max for dynamic allocation
- **Shuffle partitions**: Based on data volume and target partition size
- **Complete Spark config**: Ready for EMR Serverless deployment

---

## CLI Reference

### emr_recommender.py

| Flag | Description | Default |
|------|-------------|---------|
| `--input-path` | Local dir or S3 path with metrics | *required* |
| `--output-cost` | Output file for cost-optimized recs | — |
| `--output-perf` | Output file for performance-optimized recs | — |
| `--cost-optimized` | Generate only cost recommendations | both |
| `--performance-optimized` | Generate only performance recommendations | both |
| `--individual-files` | One JSON per job (`1-jobname.json`) | single file |
| `--format-job-config` | Deployment-ready format | standard |
| `--write-to-iceberg-table` | Write to Iceberg table (`catalog.db.table`) | — |
| `--target-partition-size` | Shuffle partition size in MiB | 1024 |
| `--limit` | Max applications to process | 100 |
| `--region` | AWS region (for S3/Iceberg) | us-east-1 |

### pipeline_wrapper.py

All flags from `emr_recommender.py` plus:

| Flag | Description | Default |
|------|-------------|---------|
| `--output-path` | Local dir or S3 path for metrics output | — |
| `--output` | Filename for recommendations | — |
| `--last-hours` | Only process logs modified in last N hours | all |
| `--skip-extraction` | Skip Stage 1, use existing metrics | false |

Legacy S3 flags (`--input-bucket`, `--input-prefix`, `--staging-bucket`, `--staging-prefix`) are still supported for backward compatibility.

### spark_processor.py

| Flag | Description | Default |
|------|-------------|---------|
| `--input-bucket` | S3 bucket with event logs | — |
| `--input-prefix` | S3 prefix for event logs | — |
| `--output-bucket` | S3 bucket for metrics output | — |
| `--output-prefix` | S3 prefix for metrics output | — |
| `--last-hours` | Only process logs modified in last N hours | all |

---

## Common Workflows

### Daily optimization (last 24 hours, cost-focused)
```bash
python3 pipeline_wrapper.py \
  --input-path s3://bucket/event-logs/ \
  --output-path s3://bucket/staging/ \
  --output daily.json \
  --last-hours 24 \
  --cost-optimized \
  --format-job-config \
  --individual-files
```

### Weekly review (both modes)
```bash
python3 pipeline_wrapper.py \
  --input-path s3://bucket/event-logs/ \
  --output-path s3://bucket/staging/ \
  --output weekly.json \
  --last-hours 168
```

### Write recommendations to Iceberg for tracking
```bash
python3 emr_recommender.py \
  --input-path s3://bucket/staging/ \
  --output-cost cost.json \
  --cost-optimized \
  --write-to-iceberg-table AwsDataCatalog.common.emr-serverless-config-advisor
```

### Generate individual deployment configs
```bash
python3 emr_recommender.py \
  --input-path ~/metrics/ \
  --output-cost /deploy/configs/cost.json \
  --cost-optimized \
  --format-job-config \
  --individual-files

# Output:
# /deploy/configs/1-DedupTripEvents.json
# /deploy/configs/2-UserAttributeStore.json
```

### Run on EMR cluster (large-scale)
```bash
scp *.py hadoop@your-emr-master:~/
ssh -i key.pem hadoop@your-emr-master
pip3 install zstandard pandas

nohup python3 pipeline_wrapper.py \
  --input-path s3://bucket/event-logs/ \
  --output-path s3://bucket/staging/ \
  --output recommendations.json \
  > pipeline.log 2>&1 &
```

---

## Time Filter Reference

| `--last-hours` | Duration | Use Case |
|-----------------|----------|----------|
| 1 | Last hour | Real-time monitoring |
| 24 | Last day | Daily optimization |
| 168 | Last week | Weekly review |
| 720 | Last month | Monthly analysis |

Filters by S3 `LastModified` or local file mtime (UTC-aware).

---

## Iceberg Table Output

The `--write-to-iceberg-table` flag writes recommendations to an Apache Iceberg table via Amazon Athena for historical tracking.

**Requirements:**
- Athena workgroup named `V3` (engine version 3)
- Glue database already exists

**Table format:** `catalog.database.table` (e.g., `AwsDataCatalog.common.emr-serverless-config-advisor`)

The table is created automatically with this schema:

| Column | Type | Description |
|--------|------|-------------|
| job_id | string | Spark job ID from event log |
| application_name | string | Spark application name |
| optimization_mode | string | cost / performance / minimal |
| recommendation | string | Full job config as JSON |
| created_at | timestamp | When written |

**Query examples:**
```sql
SELECT * FROM common."emr-serverless-config-advisor"
ORDER BY created_at DESC;

SELECT job_id, application_name, recommendation
FROM common."emr-serverless-config-advisor"
WHERE optimization_mode = 'cost';
```

---

## Optimization Modes

### Cost-Optimized (default)

Conservative executor allocation based on resource pressure:

```
pressure = memory_util × 0.4 + cpu_util × 0.4 + spill_ratio × 0.2
scaling_factor = 0.5 + (pressure / 100) × 1.0    → range: 0.5 to 1.5
max_executors = base_requirement × scaling_factor
```

Best for: development, testing, budget-conscious workloads.

### Performance-Optimized

Adds aggressive scaling when memory utilization exceeds 75%:

```
scaling_factor = 1.0 + ((memory_util - 75) / 25) × 0.5    → range: 1.0 to 1.5
```

Results in 5–21% more executors for memory-stressed jobs. Best for: production SLA-critical workloads.

### Worker Type Selection

| Worker | vCPU | Memory | Selected When |
|--------|------|--------|---------------|
| Small | 2 | 16 GB | < 20 GB needed |
| Medium | 4 | 32 GB | < 40 GB needed |
| Large | 16 | 108 GB | < 120 GB needed |
| XLarge | 32 | 256 GB | ≥ 120 GB needed |

### Partition Size Tuning

| `--target-partition-size` | Effect | Use Case |
|---------------------------|--------|----------|
| 256 | 4× partitions/executors | Extreme parallelism |
| 512 | 2× partitions/executors | Shuffle-heavy |
| 1024 (default) | Balanced | Most workloads |
| 2048 | Fewer partitions | Large data, less parallelism |

---

## Example Output

### Standard recommendation
```json
{
  "application_name": "data-processing-batch",
  "optimization_mode": "cost",
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
    "spark.executor.instances": "191",
    "spark.dynamicAllocation.maxExecutors": "191",
    "spark.sql.shuffle.partitions": "4938"
  }
}
```

### Job config format (`--format-job-config`)
```json
{
  "job_name": "data-processing-batch",
  "configuration": {
    "type": "spark",
    "compute_platform": "EMR_SERVERLESS",
    "spark_conf": {
      "spark.driver.cores": "8",
      "spark.driver.memory": "54G",
      "spark.executor.cores": "16",
      "spark.executor.memory": "108g",
      "spark.executor.instances": "191",
      "spark.dynamicAllocation.enabled": "true",
      "spark.dynamicAllocation.maxExecutors": "191"
    }
  }
}
```

---

## Performance

| Scenario | Input | Time | Throughput |
|----------|-------|------|------------|
| Large-scale (EMR cluster) | 5,887 logs, 62 apps | 12 min 25 sec | 474 files/min |
| Small-scale (single app) | 1 log, 1 app | 63 sec | — |
| Recommendation generation | 62 apps | 3.6 sec | — |

Supports compressed formats: zstd, gzip, lz4, and uncompressed JSON.

---

## Configuration

Tunable constants in source files:

| Setting | File | Default | Description |
|---------|------|---------|-------------|
| `MAX_WORKERS` | spark_processor.py | 20 | Parallel extraction threads |
| `MAX_CONCURRENT_UPLOADS` | spark_processor.py | 20 | S3 upload concurrency |
| `TARGET_PARTITION_SIZE_MIB` | emr_recommender.py | 1024 | Shuffle partition target |

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `NoCredentialsError` | Run `aws configure` or export `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` |
| Memory errors | Reduce `MAX_WORKERS` to 10 in spark_processor.py |
| S3 throttling | Reduce `MAX_CONCURRENT_UPLOADS` to 10 |
| Event log parse errors | Check format — must be JSON (.gz, .zst, .lz4, or uncompressed) |
| Iceberg INSERT fails | Ensure Athena workgroup `V3` exists with engine version 3 |

---

## Prerequisites

- Python 3.7+
- AWS credentials configured
- `pip install boto3 zstandard pandas`
- For Iceberg output: Athena V3 workgroup + Glue database

## Limitations

- Requires Spark event logs in JSON format
- Analyzes historical logs only (not live applications)
- Recommendations based on observed patterns

## License

MIT-0 License. See the LICENSE file.
