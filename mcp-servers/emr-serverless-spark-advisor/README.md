# EMR Serverless Spark Config Advisor — MCP Server

[![MCP](https://img.shields.io/badge/MCP-Compatible-blue)](https://modelcontextprotocol.io)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

An [MCP](https://modelcontextprotocol.io) server that analyzes Spark event logs and generates optimized EMR Serverless configurations. Uses EMR Serverless for extraction (no EMR EC2 cluster needed) and reads results from S3.

Works with [Kiro](https://kiro.dev), Claude Desktop, Amazon Q CLI, and any MCP-compatible client.

## How It Works

```
You: "Analyze my Spark logs at s3://my-bucket/event-logs/"

AI → [analyze_spark_logs] → submits parallel EMR Serverless jobs → extracts metrics → returns:

  12 apps analyzed in 153s
  Top cost: eg-user-attribute-store (cost_factor: 96.8, idle: 86%)
  Peak shuffle: 1278 GB/stage (exceeds 200GB Serverless storage limit)

You: "Generate EMR Serverless configuration recommendations"

AI → [generate_emr_serverless_config_recommendations] →
  Worker: Large (16 vCPU, 108 GB) | Max executors: 24
  spark.sql.shuffle.partitions: 512
  spark.executor.memory: 108g
  spark.emr-serverless.executor.disk: 1500G
  ⚠ WindowGroupLimit skew detected — excluding optimizer rule

You: "What are the bottlenecks for app 00g0jaejehl1980b?"

AI → [get_bottlenecks] →
  HIGH: 52% idle core-hours
  HIGH: 2 TB disk spill on 0.16 GB input
  HIGH: Peak stage shuffle 1278 GB — not eligible for Serverless managed storage
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
│              ┌────────▼───────────────▼──────┐                  │
│              │  streamable-http (Function URL)│                  │
│              │  or stdio (local dev)          │                  │
└──────────────┼───────────────────────────────┼──────────────────┘
               │                               │
  ┌────────────▼────────────┐     ┌────────────▼────────────┐
  │  AWS Lambda             │     │  Local (stdio)          │
  │  Function URL + Mangum  │     │  python3 spark_advisor  │
  │  ┌──────────────────┐   │     │  _mcp.py                │
  │  │ spark_advisor_mcp │   │     └────────────┬────────────┘
  │  │ 13 MCP Tools      │   │                  │
  │  └────────┬──────────┘   │                  │
  └───────────┼──────────────┘                  │
              │ boto3 API                       │
  ┌───────────▼─────────────────────────────────▼─────────────┐
  │                    AWS Cloud                               │
  │                                                            │
  │  ┌─────────────────────────────────────────────┐           │
  │  │         EMR Serverless Application           │           │
  │  │  ┌──────────────┐  ┌──────────────┐         │           │
  │  │  │spark_extractor│  │spark_extractor│  ...N  │           │
  │  │  │  (App 1)     │  │  (App 2)     │         │           │
  │  │  └──────┬───────┘  └──────┬───────┘         │           │
  │  └─────────┼─────────────────┼─────────────────┘           │
  │            ▼                 ▼                              │
  │  ┌──────────────────────────────────┐                      │
  │  │            Amazon S3             │                      │
  │  │  /event-logs/     (input)        │                      │
  │  │  /task_stage_summary/ (extract)  │                      │
  │  │  /spark_config/      (configs)   │                      │
  │  └──────────────────────────────────┘                      │
  └────────────────────────────────────────────────────────────┘
```

**Flow:**
1. MCP client sends `analyze_spark_logs` with S3 path
2. MCP server discovers apps, submits 1 EMR Serverless job per app (parallel)
3. Each job runs `spark_extractor.py` — reads compressed event logs, extracts 80+ metrics
4. MCP server reads results from S3, returns summary recommendations
5. `generate_emr_serverless_config_recommendations` produces full Spark configs with worker sizing
6. All other tools (`get_bottlenecks`, `compare_*`, etc.) read cached results from S3

**No SSH, no EMR EC2 cluster, no infrastructure to manage.**

## Tools

| Tool | Description |
|------|-------------|
| **`analyze_spark_logs`** | Extract metrics via EMR Serverless and generate summary recommendations |
| **`generate_emr_serverless_config_recommendations`** | Full Spark configs: worker sizing, shuffle tuning, disk sizing, bottleneck warnings |
| `list_event_log_prefixes` | Browse S3 for available event log apps |
| `list_applications` | List extracted apps with summary metrics |
| `get_application` | Full metrics + Spark config for one app |
| `get_bottlenecks` | Severity-ranked findings: CPU, memory, spill, idle cores, shuffle storage |
| `compare_job_performance` | Side-by-side metrics with % deltas |
| `compare_job_environments` | Diff Spark configs between two apps |
| `list_slowest_stages` | Top N stages by duration with IO/shuffle/spill |
| `get_stage_details` | Deep dive into one stage |
| `get_resource_timeline` | Executor add/remove events over time |
| `list_sql_executions` | SQL queries with duration |
| `compare_sql_execution_plans` | Diff physical plans between two SQL queries |

## Setup

### Option A: One-Command Deploy (CloudFormation)

```bash
git clone https://github.com/aws-samples/aws-emr-utilities.git
cd aws-emr-utilities/mcp-servers/emr-serverless-spark-advisor
./deploy-cfn.sh --region us-east-1
```

This builds all artifacts, uploads them to S3, and deploys a CloudFormation stack with:
- S3 bucket for advisor output
- IAM roles (least-privilege) for Lambda and EMR Serverless
- EMR Serverless Spark application
- Lambda function + layer with MCP dependencies
- Function URL (AWS_IAM auth)

Stack outputs include ready-to-paste MCP config for Kiro CLI.

### Option B: Manual Setup

#### Prerequisites

- An EMR Serverless application (Spark)
- `spark_extractor.py` uploaded to S3

#### 1. Create EMR Serverless Application

```bash
aws emr-serverless create-application \
  --name spark-advisor \
  --release-label emr-7.7.0 \
  --type SPARK \
  --region us-east-1
```

Note the `applicationId` from the output.

#### 2. Upload Extraction Script to S3

```bash
aws s3 cp legacy/spark_extractor.py s3://your-bucket/scripts/spark_extractor.py

# For zstd-compressed event logs, build and upload the zstandard archive:
pip3 install --target /tmp/zstd --platform manylinux2014_x86_64 \
  --only-binary=:all: --python-version 3.9 zstandard
cd /tmp/zstd && zip -r /tmp/zstandard.zip .
aws s3 cp /tmp/zstandard.zip s3://your-bucket/scripts/zstandard.zip
```

> **Note:** The zstandard archive must be built for Python 3.9 (EMR 7.x's Python version).

#### 3. Deploy to Lambda

```bash
# 1. Build the Lambda layer
pip3 install --target /tmp/lambda-layer/python mcp mangum
pip3 install --target /tmp/lambda-layer/python \
  --platform manylinux2014_x86_64 --only-binary=:all: --upgrade \
  pydantic-core rpds-py cffi cryptography
find /tmp/lambda-layer/python -name "*darwin*" -delete
cd /tmp/lambda-layer && zip -r /tmp/mcp-layer.zip python/

aws lambda publish-layer-version \
  --layer-name mcp-server-deps \
  --zip-file fileb:///tmp/mcp-layer.zip \
  --compatible-runtimes python3.12 python3.13 python3.14 \
  --region us-east-1

# 2. Create the Lambda function
zip /tmp/mcp-function.zip lambda_handler.py spark_advisor_mcp.py emr_recommender.py

aws lambda create-function \
  --function-name spark-config-advisor-mcp \
  --runtime python3.14 \
  --handler lambda_handler.handler \
  --zip-file fileb:///tmp/mcp-function.zip \
  --role arn:aws:iam::ACCOUNT:role/YourLambdaRole \
  --layers arn:aws:lambda:us-east-1:ACCOUNT:layer:mcp-server-deps:LATEST \
  --timeout 900 \
  --memory-size 512 \
  --environment "Variables={EMR_SERVERLESS_APP_ID=YOUR_APP_ID,EMR_EXECUTION_ROLE=arn:aws:iam::ACCOUNT:role/EMRServerlessRole,SCRIPT_S3_PATH=s3://bucket/scripts/spark_extractor.py,ARCHIVES_S3_PATH=s3://bucket/scripts/zstandard.zip,OUTPUT_S3_PATH=s3://bucket/advisor-output}" \
  --region us-east-1

# 3. Add a Function URL
aws lambda create-function-url-config \
  --function-name spark-config-advisor-mcp \
  --auth-type AWS_IAM
```

#### 4. Configure MCP Client

**Kiro CLI** (`~/.kiro/settings/mcp.json`) — stdio mode:

```json
{
  "mcpServers": {
    "spark-config-advisor": {
      "command": "python3",
      "args": ["spark_advisor_mcp.py"],
      "env": {
        "MCP_TRANSPORT": "stdio",
        "EMR_SERVERLESS_APP_ID": "YOUR_APP_ID",
        "EMR_EXECUTION_ROLE": "arn:aws:iam::ACCOUNT:role/EMRServerlessRole",
        "SCRIPT_S3_PATH": "s3://bucket/scripts/spark_extractor.py",
        "ARCHIVES_S3_PATH": "s3://bucket/scripts/zstandard.zip",
        "OUTPUT_S3_PATH": "s3://bucket/advisor-output"
      }
    }
  }
}
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `EMR_SERVERLESS_APP_ID` | Yes | EMR Serverless application ID |
| `EMR_EXECUTION_ROLE` | Yes | IAM role ARN for job execution |
| `SCRIPT_S3_PATH` | Yes | S3 path to `spark_extractor.py` |
| `ARCHIVES_S3_PATH` | No | S3 path to `zstandard.zip` (for zstd logs) |
| `OUTPUT_S3_PATH` | Yes | S3 base path for extracted output |
| `AWS_REGION` | No | AWS region (default: us-east-1). In Lambda, use `ADVISOR_AWS_REGION` instead (AWS_REGION is reserved) |
| `MCP_TRANSPORT` | No | `streamable-http` (default, for Lambda) or `stdio` (local) |

## Example Conversation

```
User: Analyze the Spark event logs at s3://my-bucket/event-logs/ and tell me
      which jobs are wasting the most resources.

AI: I'll analyze those logs now.
    [calls analyze_spark_logs(input_path="s3://my-bucket/event-logs/")]

    Extracted 12 applications in 153 seconds. Here are the top resource wasters:

    1. eg-user-attribute-store (00g0nlpd9atac00b)
       - Cost factor: 96.8 | Idle cores: 86% | Duration: 37 min
       - 1,225 GB memory spill on 20.8 TB input

    2. eg-user-attribute-store (00g0dtj5r0om5o0b)
       - Cost factor: 60.6 | Idle cores: 78% | Duration: 46 min

User: Generate EMR Serverless configuration recommendations

AI: [calls generate_emr_serverless_config_recommendations(mode="cost-optimized")]

    1. eg-user-attribute-store — Cost-optimized:
       Worker: Large (16 vCPU, 108 GB) | Max executors: 24
       spark.sql.shuffle.partitions: 512
       spark.executor.memory: 108g
       spark.emr-serverless.executor.disk: 1500G
       ⚠ WindowGroupLimit skew: excluding InferWindowGroupLimit optimizer rule

User: What are the bottlenecks for the first one?

AI: [calls get_bottlenecks(application_id="00g0nlpd9atac00b")]

    HIGH: 86% idle core-hours — most allocated cores sit unused
    HIGH: 1,225 GB memory spill — executor memory too small for data volume
    MEDIUM: Low CPU utilization (14%) — over-provisioned for compute
```

## Project Structure

```
emr-serverless-spark-advisor/
├── spark_advisor_mcp.py      # MCP server (13 tools, uses EMR Serverless + S3)
├── emr_recommender.py        # Configuration recommender (worker sizing, shuffle tuning)
├── format_to_job_config.py   # Format recommendations to EMR job config
├── lambda_handler.py         # Lambda entry point (Mangum ASGI bridge)
├── cloudformation.yaml       # CloudFormation template for one-command deploy
├── deploy-cfn.sh             # Build artifacts + deploy CloudFormation stack
├── deploy.sh                 # Docker-based deploy (ECR + Lambda)
├── requirements.txt          # Dependencies (mcp, mangum, boto3)
├── README.md
└── legacy/                   # Previous SSH-based implementation
    ├── spark_advisor_mcp.py
    ├── spark_extractor.py
    ├── pipeline_wrapper.py
    ├── emr_recommender.py
    ├── format_to_job_config.py
    └── write_to_iceberg.py
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `Missing config: EMR_SERVERLESS_APP_ID` | Set all required env vars in MCP client config |
| `No event log apps found` | Check S3 path — should contain `eventlog_v2_*` or `application_*` subdirs |
| EMR Serverless job FAILED | Check CloudWatch logs for the job run |
| `ModuleNotFoundError: zstandard` | Set `ARCHIVES_S3_PATH` to the zstandard.zip built for Python 3.9 |
| `SparkFileNotFoundException: file:/tmp/...` | Use the latest `spark_extractor.py` which stages via S3 |
| `ValidationException: memory...between 16 and 60 GB` | Executor memory + overhead exceeds limit; reduce `spark.executor.memory` |
| `Application not found` | Run `analyze_spark_logs` first to extract data |
| Slow extraction | Normal — first run takes ~150s for 12 apps. Subsequent queries use cached S3 data |

## License

Apache License 2.0 — see [LICENSE](../../LICENSE).
