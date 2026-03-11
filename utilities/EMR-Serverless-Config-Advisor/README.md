# EMR Serverless Config Advisor

Analyzes Spark event logs from EMR on EC2 or EMR Serverless and generates optimized configurations with cost and performance recommendations.

## Architecture

```
┌──────────────┐         ┌──────────────────────────────────────────────────────┐
│   User /CI   │         │                    AWS Cloud                         │
│              │         │                                                      │
│  Invoke      │────────▶│  ┌─────────────────────┐                             │
│  Lambda      │         │  │  Lambda Orchestrator │                             │
│              │         │  │  (lambda_orchestrator │                             │
│              │         │  │   .py)                │                             │
│              │         │  └────────┬─────────────┘                             │
│              │         │           │ Submits 1 job per app (parallel)          │
│              │         │           ▼                                           │
│              │         │  ┌─────────────────────────────────────┐              │
│              │         │  │       EMR Serverless Application     │              │
│              │         │  │                                      │              │
│              │         │  │  ┌──────────────┐ ┌──────────────┐  │              │
│              │         │  │  │spark_extractor│ │spark_extractor│  │              │
│              │         │  │  │  (App 1)     │ │  (App 2)     │  │              │
│              │         │  │  └──────┬───────┘ └──────┬───────┘  │              │
│              │         │  │         │    ...N jobs    │          │              │
│              │         │  └─────────┼────────────────┼──────────┘              │
│              │         │            ▼                ▼                         │
│              │         │  ┌──────────────────────────────────┐                 │
│              │         │  │            Amazon S3              │                 │
│              │         │  │                                   │                 │
│              │         │  │  /event-logs/        (input)      │                 │
│              │         │  │  /task_stage_summary/ (extract)   │                 │
│              │         │  │  /spark_config/       (configs)   │                 │
│              │         │  │  /iceberg/            (table)     │                 │
│              │         │  └──────────────────────────────────┘                 │
│              │         │            │                                          │
│              │         │            ▼                                          │
│              │         │  ┌──────────────────┐  ┌───────────────────┐          │
│              │         │  │ emr_recommender.py│─▶│write_to_iceberg.py│          │
│              │         │  │ (cost + perf)     │  │ (Spark + Glue)    │          │
│              │         │  └──────────────────┘  └───────────────────┘          │
│              │         │                                                      │
└──────────────┘         └──────────────────────────────────────────────────────┘
```

**Flow:**
1. Lambda orchestrator lists event log apps in S3, submits one EMR Serverless job per app (parallel)
2. Each `spark_extractor.py` job reads compressed event logs, extracts 80+ metrics per app
3. `emr_recommender.py` reads extracted metrics, generates cost/performance Spark configs
4. `write_to_iceberg.py` (optional) writes metrics + recommendations to an Iceberg table via Spark

## Scripts

| Script | Purpose |
|--------|---------|
| `spark_extractor.py` | Extracts metrics from Spark event logs using PySpark |
| `lambda_orchestrator.py` | Lambda function that submits parallel EMR Serverless jobs |
| `orchestrator.py` | YARN-based orchestrator for EMR on EC2 clusters |
| `emr_recommender.py` | Generates cost/performance optimized Spark configurations |
| `write_to_iceberg.py` | Writes metrics + recommendations to Iceberg table via Spark |
| `format_to_job_config.py` | Formats recommendations into EMR Serverless job config format |

## Quick Start

### Option 1: Lambda + EMR Serverless (recommended)

Deploy `lambda_orchestrator.py` as a Lambda function, then invoke:

```bash
aws lambda invoke \
  --function-name your-lambda-function \
  --payload '{
    "input_path": "s3://your-bucket/event-logs/",
    "output_path": "s3://your-bucket/advisor-output/",
    "application_id": "YOUR_EMR_SERVERLESS_APP_ID",
    "execution_role": "arn:aws:iam::ACCOUNT:role/YourRole",
    "script_path": "s3://your-bucket/scripts/spark_extractor.py",
    "archives_path": "s3://your-bucket/scripts/zstandard.zip"
  }' \
  --cli-read-timeout 910 \
  output.json
```

Then generate recommendations:

```bash
python3 emr_recommender.py \
  --input-path s3://your-bucket/advisor-output/ \
  --output-cost cost.json \
  --output-perf perf.json
```

### Option 2: EMR on EC2 (YARN)

```bash
python3 orchestrator.py \
  --input s3://your-bucket/event-logs/ \
  --output s3://your-bucket/advisor-output/ \
  --num-executors 4 --executor-memory 4g
```

### Option 3: Direct spark-submit

```bash
spark-submit --master local[*] --driver-memory 32g \
  spark_extractor.py \
  --input s3://your-bucket/event-logs/ \
  --output /tmp/output/
```

## Extracted Metrics

Each app produces a JSON with these sections:

| Section | Key Fields |
|---------|------------|
| `task_summary` | total/completed/failed/killed tasks, success rate |
| `stage_summary` | per-stage shuffle read/write, spill, duration, failure reasons |
| `executor_summary` | 20 fields per executor: cores, memory, uptime, utilization, cost factor |
| `io_summary` | total input/output/shuffle read/write in GB |
| `spill_summary` | memory + disk spill totals and percentages |
| `shuffle_data_summary` | peak stage shuffle write, EMR Serverless storage eligibility (200GB limit) |
| `driver_metrics` | GC stats, off-heap memory, tasks/jobs/stages launched |
| `job_details` | per-job duration, status, stage mapping |
| `sql_metrics` | per-SQL execution plan, duration, status |

## Recommendation Modes

| Mode | Strategy | Best For |
|------|----------|----------|
| Cost | Conservative scaling (0.5–1.5× base) | Dev/test, budget workloads |
| Performance | Aggressive scaling for memory-stressed jobs | Production SLA-critical |

## Write to Iceberg Table

### Option A: Let the script create the table

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.awssdk:bundle:2.28.11,software.amazon.awssdk:url-connection-client:2.28.11 \
  write_to_iceberg.py \
  --rec-path s3://your-bucket/recommendations/cost.json \
  --extract-path s3://your-bucket/advisor-output/ \
  --table glue_catalog.your_database.config_advisor \
  --warehouse s3://your-bucket/iceberg/
```

### Option B: Create the table yourself first

Use this DDL in Athena (V3 engine) or Spark SQL:

```sql
CREATE TABLE IF NOT EXISTS your_database.config_advisor (
    job_id                          STRING,
    application_name                STRING,
    app_id                          STRING,
    optimization_mode               STRING,
    input_gb                        DOUBLE,
    shuffle_read_gb                 DOUBLE,
    shuffle_write_gb                DOUBLE,
    peak_shuffle_write_per_stage    DOUBLE,
    peak_disk_spill_per_stage       DOUBLE,
    duration_hours                  DOUBLE,
    duration_minutes                DOUBLE,
    avg_memory_utilization_percent  DOUBLE,
    avg_cpu_utilization_percent     DOUBLE,
    max_memory_utilization_percent  DOUBLE,
    idle_core_percentage            DOUBLE,
    total_memory_spilled_gb         DOUBLE,
    cost_factor                     DOUBLE,
    src_event_log_location          STRING,
    recommendation                  STRING,
    created_at                      STRING
)
USING iceberg
LOCATION 's3://your-bucket/iceberg/your_database/config_advisor/'
```

Then run `write_to_iceberg.py` — it will detect the existing table and append data.

### Query Examples

```sql
-- Latest recommendations
SELECT job_id, application_name, input_gb, duration_hours,
       peak_shuffle_write_per_stage, cost_factor
FROM your_database.config_advisor
ORDER BY created_at DESC;

-- Jobs exceeding Serverless storage limit (200GB)
SELECT job_id, application_name, peak_shuffle_write_per_stage
FROM your_database.config_advisor
WHERE peak_shuffle_write_per_stage > 200;

-- High memory utilization jobs
SELECT job_id, application_name, avg_memory_utilization_percent,
       max_memory_utilization_percent, total_memory_spilled_gb
FROM your_database.config_advisor
WHERE max_memory_utilization_percent > 85
ORDER BY total_memory_spilled_gb DESC;
```

## CLI Reference

### spark_extractor.py

| Flag | Description | Default |
|------|-------------|---------|
| `--input` | S3 path or local path to event logs | *required* |
| `--output` | Output path for extracted metrics | *required* |
| `--limit` | Max applications to process | 100 |
| `--single-app` | Input path is a single app (not a directory of apps) | false |
| `--decompress-workers` | Parallel S3 download threads | 50 |

### emr_recommender.py

| Flag | Description | Default |
|------|-------------|---------|
| `--input-path` | Path with extracted metrics (local or S3) | *required* |
| `--output-cost` | Output file for cost-optimized recs | — |
| `--output-perf` | Output file for performance-optimized recs | — |
| `--cost-optimized` | Generate only cost recommendations | both |
| `--performance-optimized` | Generate only performance recommendations | both |
| `--individual-files` | One JSON per job | single file |
| `--format-job-config` | Deployment-ready format | standard |
| `--target-partition-size` | Shuffle partition size in MiB | 1024 |
| `--limit` | Max applications | 100 |

### write_to_iceberg.py

| Flag | Description | Default |
|------|-------------|---------|
| `--rec-path` | Path to recommendation JSON | *required* |
| `--extract-path` | Path containing `task_stage_summary/` | *required* |
| `--table` | Iceberg table: `catalog.database.table` | *required* |
| `--warehouse` | S3 warehouse location | `s3://suthan-event-logs/iceberg/` |

## Prerequisites

- Python 3.7+, `pip install boto3 zstandard pandas`
- For Spark extraction: EMR cluster or EMR Serverless application
- For Iceberg: Glue Catalog access, Iceberg Spark runtime JAR

## Legacy Scripts

Previous Python-based extraction scripts are in the `legacy/` folder. The Spark extractor (`spark_extractor.py`) fully replaces `spark_processor.py` with identical output and 4× faster execution.

## License

MIT-0 License. See the LICENSE file.
