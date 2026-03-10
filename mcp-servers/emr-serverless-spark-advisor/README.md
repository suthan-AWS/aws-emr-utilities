# EMR Serverless Spark Config Advisor MCP Server

[![MCP](https://img.shields.io/badge/MCP-1.0-blue)](https://modelcontextprotocol.io)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](../../LICENSE)

> **🤖 Connect AI agents to EMR Serverless for intelligent Spark configuration recommendations, job analysis, and performance optimization**

An MCP (Model Context Protocol) server that analyzes Apache Spark event logs from Amazon EMR Serverless and **generates optimized EMR Serverless Spark configurations** — right-sized worker types, executor counts, memory, and shuffle partitions. Also provides bottleneck detection, comparative analysis, and stage-level diagnostics. Works with any MCP-compatible client including [Kiro](https://kiro.dev), Claude Desktop, Amazon Q CLI, and LangGraph agents.

## 🎯 What is This?

This MCP server bridges AI agents with your EMR Serverless Spark workloads. The **primary capability** is generating production-ready EMR Serverless configurations from your actual Spark event logs:

- 🎯 **Generate optimized EMR Serverless configurations** — cost-optimized or performance-optimized worker sizing, executor counts, memory, shuffle partitions, and dynamic allocation settings
- 🔍 **Analyze Spark event logs** from S3 and extract detailed metrics
- 🚨 **Identify bottlenecks** — CPU waste, memory pressure, spill, idle executors
- 🔄 **Compare applications** — performance metrics and Spark config diffs
- 📊 **Stage-level analysis** — find the slowest stages and their root causes
- 📈 **Resource timeline** — visualize executor scaling behavior over time
- 🔎 **SQL plan analysis** — compare execution plans between runs

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MCP Clients                              │
│  ┌──────────┐  ┌──────────────┐  ┌──────────┐  ┌────────────┐  │
│  │   Kiro   │  │ Claude       │  │ Amazon Q │  │ LangGraph  │  │
│  │   CLI    │  │ Desktop      │  │ CLI      │  │ Agents     │  │
│  └────┬─────┘  └──────┬───────┘  └────┬─────┘  └─────┬──────┘  │
│       │               │               │              │          │
│       └───────────────┼───────────────┼──────────────┘          │
│                       │               │                         │
│                  ┌────▼───────────────▼────┐                    │
│                  │   MCP Protocol (stdio)  │                    │
│                  └────────────┬────────────┘                    │
└───────────────────────────────┼──────────────────────────────────┘
                                │
                   ┌────────────▼────────────┐
                   │  Spark Config Advisor   │
                   │     MCP Server          │
                   │                         │
                   │  12 Tools:              │
                   │  • analyze_spark_logs   │
                   │  • list_applications    │
                   │  • get_application      │
                   │  • get_bottlenecks      │
                   │  • compare_performance  │
                   │  • compare_environments │
                   │  • list_slowest_stages  │
                   │  • get_stage_details    │
                   │  • get_resource_timeline│
                   │  • list_sql_executions  │
                   │  • compare_sql_plans    │
                   │  • list_event_logs      │
                   └────────────┬────────────┘
                                │ SSH
                   ┌────────────▼────────────┐
                   │  EMR Primary Node       │
                   │                         │
                   │  ┌───────────────────┐  │
                   │  │ Spark Extractor   │  │
                   │  │ (PySpark)         │  │
                   │  │                   │  │
                   │  │ Phase A: Parallel  │  │
                   │  │ S3 decompress     │  │
                   │  │                   │  │
                   │  │ Phase B: Spark    │  │
                   │  │ metric extraction │  │
                   │  └────────┬──────────┘  │
                   │           │              │
                   │  ┌────────▼──────────┐  │
                   │  │ EMR Recommender   │  │
                   │  │                   │  │
                   │  │ Worker sizing     │  │
                   │  │ Executor tuning   │  │
                   │  │ Shuffle partitions│  │
                   │  └───────────────────┘  │
                   └────────────┬────────────┘
                                │
                   ┌────────────▼────────────┐
                   │     Amazon S3           │
                   │                         │
                   │  Spark Event Logs       │
                   │  (EMR Serverless)       │
                   └─────────────────────────┘
```

### Processing Pipeline

```
S3 Event Logs ──► Phase A: Decompress ──► Phase B: Spark Extract ──► Recommender
                  (50 parallel threads)    (PySpark local[*])        (Python)
                  bz2/zstd → jsonl         Per-app aggregation       Worker sizing
                                           Stage/executor/SQL        Spark configs
                                           metrics extraction        Shuffle tuning
```

**Phase A** downloads and decompresses event log files from S3 in parallel (50 threads). **Phase B** uses PySpark to aggregate task metrics, executor utilization, stage details, and SQL plans per application. The **EMR Recommender** is the core output — it generates production-ready EMR Serverless configurations including worker type, executor count, memory sizing, shuffle partitions, and dynamic allocation settings (cost-optimized or performance-optimized).

## 🛠️ Available Tools

The MCP server provides **12 specialized tools**. The core tool is `analyze_spark_logs` which generates production-ready EMR Serverless configurations; the remaining tools support interactive exploration and debugging.

### 🎯 EMR Serverless Configuration Recommendations (Core)

| Tool | Description |
|---|---|
| `analyze_spark_logs` | **Primary tool.** Full pipeline: extract metrics from S3 event logs and generate optimized EMR Serverless configurations — worker type, executor count, memory, shuffle partitions, dynamic allocation (cost or performance optimized) |
| `list_event_log_prefixes` | Browse available Spark event log application prefixes in S3 |

### 🔍 Application Querying

| Tool | Description |
|---|---|
| `list_applications` | List all extracted applications with summary metrics (sorted by cost factor) |
| `get_application` | Get detailed metrics for a specific application — executor summary, IO, spill, Spark config |

### 🚨 Bottleneck Analysis

| Tool | Description |
|---|---|
| `get_bottlenecks` | Identify performance bottlenecks with severity-ranked, actionable recommendations. Analyzes CPU, memory, executors, spill, shuffle, stages, and failures |

### 🔄 Comparative Analysis

| Tool | Description |
|---|---|
| `compare_job_performance` | Side-by-side performance metrics between two applications with percentage deltas |
| `compare_job_environments` | Diff Spark configurations between two applications — different values, unique configs |

### ⚡ Stage Analysis

| Tool | Description |
|---|---|
| `list_slowest_stages` | Get the N slowest stages sorted by duration with IO/shuffle/spill per stage |
| `get_stage_details` | Deep dive into a specific stage's metrics |

### 📈 Resource & SQL Analysis

| Tool | Description |
|---|---|
| `get_resource_timeline` | Chronological executor add/remove events with running count — shows scaling behavior |
| `list_sql_executions` | List all SQL queries with duration and plan size |
| `compare_sql_execution_plans` | Compare physical execution plans between two SQL queries across applications |

### 🤖 How LLMs Use These Tools

| User Query | Tools Selected |
|---|---|
| *"Analyze my Spark logs and recommend EMR Serverless configs"* | `analyze_spark_logs` |
| *"Optimize my EMR Serverless job for cost"* | `analyze_spark_logs` (cost-optimized mode) |
| *"Why is my job slow?"* | `get_bottlenecks` + `list_slowest_stages` |
| *"Compare today's run with yesterday's"* | `compare_job_performance` + `compare_job_environments` |
| *"What's wrong with stage 5?"* | `get_stage_details` |
| *"Show me resource usage over time"* | `get_resource_timeline` |
| *"Find my slowest SQL queries"* | `list_sql_executions` + `compare_sql_execution_plans` |
| *"List all applications sorted by cost"* | `list_applications` |

## ⚡ Quick Start

### Prerequisites

- Python 3.10+
- An EMR on EC2 cluster — see [EMR on EC2 Cluster Setup](#-emr-on-ec2-cluster-setup) below
- SSH key access to the EMR primary node
- `mcp` Python package: `pip install mcp`

### 1. Set Up the EMR Cluster

Follow the detailed [EMR on EC2 Cluster Setup](#-emr-on-ec2-cluster-setup) section below to:
- Create an EMR cluster with Spark
- Deploy the pipeline scripts
- Install dependencies
- Configure your MCP client

### 2. Start Using

```
You: "Analyze Spark logs at s3://my-bucket/event-logs/ and recommend cost-optimized configs"

AI: [calls analyze_spark_logs] → Extracts metrics from 10 applications, generates recommendations...

You: "Which app has the worst bottlenecks?"

AI: [calls list_applications, then get_bottlenecks] →
    App 00g0jaejehl1980b has 3 HIGH severity issues:
    - 57% of executors were idle (203/354 never ran tasks)
    - 99.6% idle core percentage
    - 2 TB disk spill on 0.16 GB input

You: "Compare that app's config with the healthy one"

AI: [calls compare_job_environments] → 57 config differences found...
```

## 📊 Example Output

### Configuration Recommendation for EMR Serverless

The primary output — production-ready EMR Serverless Spark configurations derived from your actual workload metrics:

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
    "spark.executor.cores": "16",
    "spark.executor.memory": "108g",
    "spark.executor.instances": "191",
    "spark.sql.shuffle.partitions": "4938",
    "spark.emr-serverless.executor.disk": "500G",
    "spark.dynamicAllocation.enabled": "true"
  }
}
```

### Bottleneck Analysis

```json
{
  "application_id": "00g0jaejehl1980b",
  "bottleneck_count": 4,
  "findings": [
    {
      "severity": "HIGH",
      "category": "Executors",
      "finding": "203/354 executors (57.3%) were allocated but never ran tasks",
      "recommendation": "Tune spark.dynamicAllocation.maxExecutors or increase idle timeout"
    },
    {
      "severity": "HIGH",
      "category": "Cores",
      "finding": "Idle core percentage is 99.61%",
      "recommendation": "Most core-hours are wasted. Reduce total cores or improve parallelism"
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

## 🔧 EMR on EC2 Cluster Setup

The extraction pipeline uses PySpark (`spark-submit`), so it must run on a node with Spark installed. The MCP server SSHs into the EMR primary node to run the pipeline. This section covers everything you need to set up on the EMR cluster.

### Step 1: Create an EMR Cluster

```bash
aws emr create-cluster \
  --name "spark-advisor-processing" \
  --release-label emr-7.0.0 \
  --applications Name=Spark \
  --instance-type r5.4xlarge \
  --instance-count 1 \
  --ec2-attributes KeyName=<your-key-pair>,SubnetId=<your-subnet-id> \
  --use-default-roles \
  --region us-east-1
```

A single-node cluster (primary only) is sufficient — the extractor runs PySpark in `local[*]` mode on the primary node.

#### Instance Sizing

The memory requirements only apply to the **batch extraction phase** (`analyze_spark_logs`). All interactive tools (list, compare, bottlenecks, etc.) use negligible memory. If you only extract up to 10 apps at a time, an r5.4xlarge is sufficient for any workload.

| Workload | Instance Type | Memory | Concurrent Apps |
|---|---|---|---|
| Small (1-10 apps) | r5.4xlarge | 128 GB | Up to 10 apps per extraction |
| Medium (10-50 apps) | r5.16xlarge | 512 GB | Up to 50 apps per extraction |
| Large (50-100+ apps) | r5.24xlarge | 768 GB | 100+ apps per extraction |

#### Security Group

Ensure the EMR primary node's security group allows **inbound SSH (port 22)** from your local machine's IP. You can find the security group in the EMR console under **Cluster > Security and access > EC2 security groups > Primary node**.

### Step 2: SSH into the EMR Primary Node

```bash
# Get the primary node DNS from the EMR console or CLI
aws emr describe-cluster --cluster-id <cluster-id> --query 'Cluster.MasterPublicDnsName' --output text

# SSH in
ssh -i <your-key.pem> hadoop@<emr-primary-dns>
```

### Step 3: Deploy Pipeline Scripts

From your **local machine**, copy the three pipeline scripts to the EMR primary node:

```bash
cd mcp-servers/emr-serverless-spark-advisor/

scp -i <your-key.pem> \
  spark_extractor.py \
  pipeline_wrapper.py \
  emr_recommender.py \
  hadoop@<emr-primary-dns>:~/
```

### Step 4: Install Python Dependencies

SSH into the EMR primary node and install the required packages. Spark and boto3 are pre-installed on EMR.

```bash
ssh -i <your-key.pem> hadoop@<emr-primary-dns>

# Install additional Python dependencies
pip install pandas numpy zstandard

# Create the output directory used by the MCP server
mkdir -p /tmp/spark_advisor_output
```

### Step 5: Verify the Setup

Run these checks on the EMR primary node to confirm everything is ready:

```bash
# Verify Spark is available
spark-submit --version

# Verify Python dependencies
python3 -c "import pandas, numpy, zstandard, boto3; print('All dependencies OK')"

# Verify the pipeline scripts are in place
ls ~/spark_extractor.py ~/pipeline_wrapper.py ~/emr_recommender.py

# Verify S3 access to your event logs
aws s3 ls s3://<your-bucket>/<your-event-log-prefix>/

# Quick test: run the extractor on a single app (optional)
spark-submit --master local[*] --conf spark.driver.memory=32g \
  ~/spark_extractor.py \
  --input s3://<your-bucket>/<your-event-log-prefix>/ \
  --output /tmp/spark_advisor_output \
  --limit 1
```

If the test succeeds, you should see output like:
```
Phase A: Decompressing 1 apps, 470 files with 50 threads
  Decompressing... 470/470 files
Phase A done: 1 apps, 1694243 events in 13.0s
Phase B: Extracting metrics from 1 apps using Spark
  [1/1] Extracting eventlog_v2_00g0dtj5r0om5o0b...
Writing 1 results to /tmp/spark_advisor_output
✅ Extraction complete: 1 applications
```

### Step 6: Configure the MCP Server (Local Machine)

Back on your **local machine**, configure the MCP server to point to the EMR cluster. Set the environment variables `EC2_HOST` and `SSH_KEY` in your MCP client config:

#### Kiro CLI (`~/.kiro/settings/mcp.json`)

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

#### Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`)

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

#### Amazon Q CLI (`~/.aws/amazonq/mcp.json`)

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

### When You Create a New EMR Cluster

Every time you spin up a new EMR cluster, you need to:

1. **Deploy the scripts** (Step 3) — `scp` the 3 Python files to `~/`
2. **Install dependencies** (Step 4) — `pip install pandas numpy zstandard`
3. **Create output directory** (Step 4) — `mkdir -p /tmp/spark_advisor_output`
4. **Update MCP config** (Step 6) — change `EC2_HOST` to the new primary node DNS
5. **Restart your MCP client** — so it picks up the new config

### Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| `Connection refused` or `Connection timed out` | Security group blocks SSH | Add inbound rule for port 22 from your IP |
| `Permission denied (publickey)` | Wrong SSH key or user | Verify key path and use `hadoop@` as the user |
| `Pipeline failed: 0 JSON files` | Scripts not deployed | Re-run Step 3 (`scp` the scripts) |
| `ModuleNotFoundError: pandas` | Dependencies not installed | Re-run Step 4 (`pip install`) |
| `spark-submit: command not found` | Not an EMR cluster | Use an EMR on EC2 cluster, not a plain EC2 instance |
| `Loaded 0 JSON files` | Wrong S3 path or no event logs | Verify path with `aws s3 ls s3://your-bucket/prefix/` |

### Performance Benchmarks (r5.24xlarge, 10 apps)

| Phase | Time | Peak Memory |
|---|---|---|
| Phase A: Decompress (1,676 files) | 85s | 48 GB |
| Phase B: Spark Extract (3M+ tasks) | 62s | 59 GB |
| Recommender | 1.4s | < 1 GB |
| **Total Pipeline** | **151s** | **59 GB** |

## 🏗️ Project Structure

```
emr-serverless-spark-advisor/
├── spark_advisor_mcp.py          # MCP server (12 tools)
├── spark_extractor.py            # PySpark-based metric extraction
├── pipeline_wrapper.py           # Orchestrates extract → recommend pipeline
├── emr_recommender.py            # EMR Serverless configuration recommender
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🤝 Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## 📄 License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

## 🔗 Related Projects

- [EMR Serverless Config Advisor](../utilities/EMR-Serverless-Config-Advisor/) — The underlying extraction and recommendation engine
