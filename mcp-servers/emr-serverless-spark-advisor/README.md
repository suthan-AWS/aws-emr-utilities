# EMR Serverless Spark Config Advisor — MCP Server

[![MCP](https://img.shields.io/badge/MCP-1.0-blue)](https://modelcontextprotocol.io)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](../../LICENSE)

An [MCP](https://modelcontextprotocol.io) server that analyzes Apache Spark event logs and generates optimized EMR Serverless Spark configurations — right-sized worker types, executor counts, memory, shuffle partitions, timeouts, and dynamic allocation settings. Also provides bottleneck detection, comparative analysis, and stage-level diagnostics.

Works with [Kiro](https://kiro.dev), Claude Desktop, Amazon Q CLI, LangGraph, and any MCP-compatible client.

## How It Works

```
You: "Analyze my Spark logs and recommend cost-optimized configs"

AI → [analyze_spark_logs] → extracts metrics from S3 event logs → generates recommendations:

  Worker: Large (16 vCPU, 108 GB)
  Max Executors: 191, Min: 95
  Shuffle Partitions: 4938
  Executor Disk: 500G (shuffle_optimized)
  Network Timeout: 3600s
  + 15 more Spark configs...

You: "Why is my job slow?"

AI → [get_bottlenecks] →
  HIGH: 203/354 executors (57%) never ran tasks
  HIGH: 99.6% idle core percentage
  HIGH: 2 TB disk spill on 0.16 GB input
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MCP Clients                              │
│  ┌──────────┐  ┌──────────────┐  ┌──────────┐  ┌────────────┐  │
│  │   Kiro   │  │    Claude    │  │ Amazon Q │  │ LangGraph  │  │
│  │   CLI    │  │   Desktop    │  │   CLI    │  │  Agents    │  │
│  └────┬─────┘  └──────┬───────┘  └────┬─────┘  └─────┬──────┘  │
│       └───────────────┼───────────────┼──────────────┘          │
│                  ┌────▼───────────────▼────┐                    │
│                  │   MCP Protocol (stdio)  │                    │
│                  └────────────┬────────────┘                    │
└───────────────────────────────┼──────────────────────────────────┘
                                │
                   ┌────────────▼────────────┐
                   │  Spark Config Advisor   │
                   │  MCP Server (local)     │
                   │                         │
                   │  12 Tools:              │
                   │  • analyze_spark_logs   │
                   │  • get_bottlenecks      │
                   │  • compare_performance  │
                   │  • list_applications    │
                   │  • ...8 more            │
                   └────────────┬────────────┘
                                │ SSH
                   ┌────────────▼────────────┐
                   │   EMR Primary Node      │
                   │                         │
                   │  ┌───────────────────┐  │
                   │  │ Spark Extractor   │  │
                   │  │ (PySpark)         │  │
                   │  │                   │  │
                   │  │ Decompress from   │  │
                   │  │ S3 (50 threads)   │  │
                   │  │       ↓           │  │
                   │  │ Extract metrics   │  │
                   │  │ (local[*])        │  │
                   │  └────────┬──────────┘  │
                   │           ↓              │
                   │  ┌───────────────────┐  │
                   │  │ EMR Recommender   │  │
                   │  │                   │  │
                   │  │ Worker sizing     │  │
                   │  │ Executor tuning   │  │
                   │  │ Shuffle partitions│  │
                   │  │ Timeouts & S3     │  │
                   │  └───────────────────┘  │
                   └────────────┬────────────┘
                                │
                   ┌────────────▼────────────┐
                   │       Amazon S3         │
                   │                         │
                   │  Spark Event Logs       │
                   │  (any source)           │
                   └─────────────────────────┘
```
```

The MCP server runs locally and SSHs into an EMR primary node to execute the pipeline:

1. **Decompress** — Downloads event log files from S3 in parallel (50 threads, bz2/zstd)
2. **Extract** — PySpark aggregates task metrics, executor utilization, stage details, and SQL plans
3. **Recommend** — Generates production-ready EMR Serverless configurations

Extracted data is cached on the EMR node. Subsequent requests skip extraction and return recommendations in ~1 second.

## Tools

| Tool | Description |
|---|---|
| **`analyze_spark_logs`** | Extract metrics from S3 event logs and generate EMR Serverless configs (cost or performance optimized). Caches results — skips re-extraction on repeat calls. |
| `list_event_log_prefixes` | Browse S3 for available event log prefixes |
| `list_applications` | List all extracted apps with summary metrics, sorted by cost |
| `get_application` | Full metrics + Spark config for one app |
| `get_bottlenecks` | Severity-ranked findings: CPU, memory, spill, idle executors, stages, failures |
| `compare_job_performance` | Side-by-side metrics between two apps with % deltas |
| `compare_job_environments` | Diff Spark configs between two apps |
| `list_slowest_stages` | Top N stages by duration with IO/shuffle/spill |
| `get_stage_details` | Deep dive into one stage |
| `get_resource_timeline` | Executor add/remove events over time — shows scaling behavior |
| `list_sql_executions` | SQL queries with duration |
| `compare_sql_execution_plans` | Diff physical plans between two SQL queries |

## Setup

### Prerequisites

- Python 3.10+ (local machine)
- An EMR on EC2 cluster (Spark pre-installed)
- SSH key access to the EMR primary node

### 1. Create an EMR Cluster

```bash
aws emr create-cluster \
  --name "spark-advisor" \
  --release-label emr-7.12.0 \
  --applications Name=Spark \
  --instance-type r8g.4xlarge \
  --instance-count 1 \
  --ec2-attributes KeyName=<your-key-pair>,SubnetId=<your-subnet-id> \
  --use-default-roles \
  --region us-east-1
```

A single-node cluster is sufficient — the extractor runs PySpark in `local[*]` mode.

Ensure the security group allows **inbound SSH (port 22)** from your IP.

### 2. Deploy to EMR Primary Node

```bash
# Get the primary node DNS
aws emr describe-cluster --cluster-id <cluster-id> \
  --query 'Cluster.MasterPublicDnsName' --output text

# Copy pipeline scripts
scp -i <your-key.pem> \
  spark_extractor.py pipeline_wrapper.py emr_recommender.py \
  hadoop@<emr-primary-dns>:~/

# SSH in and install dependencies
ssh -i <your-key.pem> hadoop@<emr-primary-dns>
pip install pandas numpy zstandard
mkdir -p /tmp/spark_advisor_output
```

### 3. Configure MCP Client (Local Machine)

Install the MCP server dependency locally:

```bash
pip install mcp
```

Then add the server to your MCP client config:

**Kiro CLI** (`~/.kiro/settings/mcp.json`):
```json
{
  "mcpServers": {
    "spark-config-advisor": {
      "command": "python3",
      "args": ["/path/to/spark_advisor_mcp.py"],
      "env": {
        "EC2_HOST": "hadoop@<emr-primary-dns>",
        "SSH_KEY": "/path/to/<your-key>.pem",
        "AWS_DEFAULT_REGION": "us-east-1"
      }
    }
  }
}
```

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "spark-config-advisor": {
      "command": "python3",
      "args": ["/path/to/spark_advisor_mcp.py"],
      "env": {
        "EC2_HOST": "hadoop@<emr-primary-dns>",
        "SSH_KEY": "/path/to/<your-key>.pem"
      }
    }
  }
}
```

**Amazon Q CLI** (`~/.aws/amazonq/mcp.json`): same format as Claude Desktop.

### 4. Verify

On the EMR node:
```bash
spark-submit --version
python3 -c "import pandas, numpy, zstandard, boto3; print('OK')"
ls ~/spark_extractor.py ~/pipeline_wrapper.py ~/emr_recommender.py
aws s3 ls s3://<your-bucket>/<your-event-log-prefix>/
```

## Example Output

### EMR Serverless Configuration Recommendation

```json
{
  "worker": {
    "type": "Large",
    "vcpu": 16,
    "memory_gb": 108,
    "max_executors": 191,
    "min_executors": 95
  },
  "spark_configs": {
    "spark.driver.cores": "8",
    "spark.driver.memory": "54G",
    "spark.executor.cores": "16",
    "spark.executor.memory": "108g",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.maxExecutors": "191",
    "spark.dynamicAllocation.minExecutors": "95",
    "spark.dynamicAllocation.initialExecutors": "95",
    "spark.sql.shuffle.partitions": "4938",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.files.maxPartitionBytes": "512m",
    "spark.emr-serverless.executor.disk": "500G",
    "spark.emr-serverless.executor.disk.type": "shuffle_optimized",
    "spark.network.timeout": "3600s",
    "spark.shuffle.io.connectionTimeout": "1800s",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",
    "spark.hadoop.fs.s3a.retry.limit": "15",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true"
  }
}
```

### Bottleneck Analysis

```json
{
  "application_id": "00g0jaejehl1980b",
  "findings": [
    {
      "severity": "HIGH",
      "category": "Executors",
      "finding": "203/354 executors (57.3%) were allocated but never ran tasks",
      "recommendation": "Tune spark.dynamicAllocation.maxExecutors or increase idle timeout"
    },
    {
      "severity": "HIGH",
      "category": "Spill",
      "finding": "Disk spill: 2088.31 GB, Memory spill: 7517.98 GB",
      "recommendation": "Increase executor memory or spark.sql.shuffle.partitions"
    }
  ]
}
```

## Instance Sizing

Memory is only needed during extraction (`analyze_spark_logs`). All other tools use negligible memory. If you extract ≤10 apps at a time, r8g.4xlarge works for any total workload.

| Workload | Instance Type | Memory | Apps per Extraction |
|---|---|---|---|
| Small (1-10 apps) | r8g.4xlarge | 122 GB | Up to 10 |
| Medium (10-50 apps) | r8g.16xlarge | 488 GB | Up to 50 |
| Large (50-100+ apps) | r8g.24xlarge | 732 GB | 100+ |

### Performance (r8g.24xlarge, 10 apps, 6.2M events)

| Phase | Time | Peak Memory |
|---|---|---|
| Decompress (1,676 files from S3) | 85s | 48 GB |
| Spark Extract (3M+ tasks) | 62s | 59 GB |
| Recommender | 1.4s | < 1 GB |
| **Total** | **151s** | **59 GB** |

## New EMR Cluster Checklist

When you create a new cluster, repeat these steps:

1. `scp` the 3 scripts to `~/` on the primary node
2. `pip install pandas numpy zstandard`
3. `mkdir -p /tmp/spark_advisor_output`
4. Update `EC2_HOST` in your MCP client config
5. Restart your MCP client

## Troubleshooting

| Issue | Fix |
|---|---|
| `Connection timed out` | Add inbound SSH (port 22) to the EMR security group |
| `Permission denied (publickey)` | Check SSH key path and use `hadoop@` as user |
| `Pipeline failed: 0 JSON files` | Deploy scripts: `scp` the 3 Python files to `~/` |
| `ModuleNotFoundError: pandas` | Run `pip install pandas numpy zstandard` on EMR node |
| `spark-submit: command not found` | Use an EMR on EC2 cluster, not a plain EC2 instance |
| `Loaded 0 JSON files` | Verify S3 path: `aws s3 ls s3://your-bucket/prefix/` |

## Project Structure

```
emr-serverless-spark-advisor/
├── spark_advisor_mcp.py      # MCP server (12 tools, runs locally)
├── spark_extractor.py        # PySpark metric extraction (runs on EMR)
├── pipeline_wrapper.py       # Orchestrates extract → recommend (runs on EMR)
├── emr_recommender.py        # EMR Serverless config generator (runs on EMR)
├── requirements.txt          # Local dependencies (mcp, boto3)
└── README.md
```

## License

Apache License 2.0 — see [LICENSE](../../LICENSE).
