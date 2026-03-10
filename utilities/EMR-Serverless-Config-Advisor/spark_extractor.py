#!/usr/bin/env python3
"""
Two-phase event log extractor:
  Phase A: Python decompresses zstd files from S3 to local disk (parallel)
  Phase B: Spark reads decompressed JSON and extracts metrics (parallel)

Usage:
    python3 spark_extractor.py \
        --input s3://bucket/event-logs/ \
        --output s3://bucket/mcp-staging/run_id/ \
        --limit 10
"""

import argparse
import gzip
import bz2
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO

import boto3
import zstandard as zstd


# ── Phase A: Python decompress ──────────────────────────────────────

def _decompress_one_file(args):
    """Download and decompress a single S3 file. Returns (app_name, lines_text)."""
    bucket, key, app_name = args
    s3_client = boto3.client("s3", region_name="us-east-1")
    dctx = zstd.ZstdDecompressor()
    try:
        raw = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith((".zstd", ".zst")):
            chunks = []
            with dctx.stream_reader(BytesIO(raw)) as reader:
                while True:
                    chunk = reader.read(1024 * 1024)
                    if not chunk:
                        break
                    chunks.append(chunk)
            text = b"".join(chunks).decode("utf-8", errors="ignore")
        elif key.endswith((".gz", ".gzip")):
            text = gzip.decompress(raw).decode("utf-8", errors="ignore")
        elif key.endswith(".bz2"):
            text = bz2.decompress(raw).decode("utf-8", errors="ignore")
        else:
            text = raw.decode("utf-8", errors="ignore")
        return app_name, text
    except Exception as e:
        print(f"  Warning: {key}: {e}", file=sys.stderr)
        return app_name, ""


def phase_a_decompress(input_path, local_base, limit, workers=50):
    """Decompress all apps from S3 to local jsonl files — flat parallelism."""
    parts = input_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3", region_name="us-east-1")

    # List application directories
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    app_prefixes = []
    for cp in resp.get("CommonPrefixes", []):
        p = cp["Prefix"]
        name = p.rstrip("/").rsplit("/", 1)[-1]
        if name.startswith("eventlog_v2_"):
            app_prefixes.append((p, name))
    app_prefixes = app_prefixes[:limit]

    # List ALL files across all apps in one pass
    all_tasks = []  # (bucket, key, app_name)
    for app_prefix, app_name in app_prefixes:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=app_prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if "/events_" in k:
                    all_tasks.append((bucket, k, app_name))

    print(f"Phase A: Decompressing {len(app_prefixes)} apps, {len(all_tasks)} files with {workers} threads")
    os.makedirs(local_base, exist_ok=True)

    # Open output files
    app_files = {}
    app_counts = {}
    for _, app_name in app_prefixes:
        d = os.path.join(local_base, app_name)
        os.makedirs(d, exist_ok=True)
        app_files[app_name] = open(os.path.join(d, "events.jsonl"), "w")
        app_counts[app_name] = 0

    start = time.time()

    # Flat parallelism — all files across all apps in one pool
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for app_name, text in pool.map(_decompress_one_file, all_tasks):
            if text:
                f = app_files[app_name]
                for line in text.splitlines():
                    line = line.strip()
                    if line:
                        f.write(line + "\n")
                        app_counts[app_name] += 1

    for f in app_files.values():
        f.close()

    elapsed = time.time() - start
    total_lines = sum(app_counts.values())
    for app_name, count in app_counts.items():
        print(f"  ✓ {app_name}: {count} events")
    print(f"Phase A done: {len(app_prefixes)} apps, {total_lines} events in {elapsed:.1f}s")
    return list(app_counts.keys())


# ── Phase B: Spark extraction ────────────────────────────────────────

def phase_b_spark_extract(app_names, local_base, output_path, limit):
    """Use Spark to read decompressed JSON and extract metrics."""
    from pyspark.sql import SparkSession, functions as F

    spark = (
        SparkSession.builder
        .appName("SparkEventLogExtractor")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    GB = 1024 ** 3
    results = []

    # Infer schema once from the first app, reuse for all others
    schema = None
    first_path = None
    for app_id in app_names[:limit]:
        p = os.path.join(local_base, app_id, "events.jsonl")
        if os.path.exists(p) and os.path.getsize(p) > 0:
            first_path = "file://" + p
            break
    if first_path:
        schema = spark.read.json(first_path).schema
        print(f"Schema inferred ({len(schema.fields)} fields), reusing for all apps")

    for app_id in app_names[:limit]:
        jsonl_path = os.path.join(local_base, app_id, "events.jsonl")
        if not os.path.exists(jsonl_path) or os.path.getsize(jsonl_path) == 0:
            print(f"  Skip {app_id}: no data")
            continue

        print(f"Processing {app_id}...")
        try:
            if schema:
                df = spark.read.schema(schema).json("file://" + jsonl_path)
            else:
                df = spark.read.json("file://" + jsonl_path)
            df.cache()
            total_events = df.count()

            # ── App Info ─────────────────────────────────────────
            app_start_row = df.filter(F.col("Event") == "SparkListenerApplicationStart").first()
            app_end_row = df.filter(F.col("Event") == "SparkListenerApplicationEnd").first()

            app_name = "N/A"
            spark_app_id = "N/A"
            start_ts = None
            end_ts = None

            if app_start_row:
                app_name = getattr(app_start_row, "App Name", None) or "N/A"
                spark_app_id = getattr(app_start_row, "App ID", None) or "N/A"
                start_ts = app_start_row["Timestamp"]

            if app_end_row:
                end_ts = app_end_row["Timestamp"]

            duration_hours = None
            duration_minutes = None
            if start_ts and end_ts:
                duration_ms = end_ts - start_ts
                duration_minutes = round(duration_ms / (1000 * 60), 2)
                duration_hours = round(duration_ms / (1000 * 60 * 60), 2)

            # ── Spark Config ─────────────────────────────────────
            executor_memory_mb = 8 * 1024
            executor_cores_cfg = 4
            job_id = spark_app_id
            cluster_id = "N/A"
            spark_config = {}

            env_row = df.filter(F.col("Event") == "SparkListenerEnvironmentUpdate").first()
            if env_row:
                try:
                    raw_props = getattr(env_row, "Spark Properties", None)
                    if raw_props:
                        if hasattr(raw_props, "asDict"):
                            spark_config = raw_props.asDict()
                        elif isinstance(raw_props, list):
                            spark_config = {r[0]: r[1] for r in raw_props if len(r) >= 2}
                        elif isinstance(raw_props, dict):
                            spark_config = raw_props

                        mem_str = spark_config.get("spark.executor.memory",
                                  spark_config.get("spark.emr.default.executor.memory", ""))
                        if mem_str:
                            if mem_str[-1].lower() == "g":
                                executor_memory_mb = int(mem_str[:-1]) * 1024
                            elif mem_str[-1].lower() == "m":
                                executor_memory_mb = int(mem_str[:-1])

                        cores_str = spark_config.get("spark.executor.cores",
                                    spark_config.get("spark.emr.default.executor.cores", ""))
                        if cores_str:
                            executor_cores_cfg = int(cores_str)

                        job_id = spark_config.get("spark.job_id", spark_app_id)
                        cluster_id = spark_config.get("spark.emr_cluster_id", "N/A")
                except Exception:
                    pass

            # ── IO + Spill aggregation from TaskEnd ──────────────
            task_ends = df.filter(F.col("Event") == "SparkListenerTaskEnd")

            io_agg = task_ends.select(
                F.col("`Task Metrics`.`Executor Run Time`").alias("run_time"),
                F.col("`Task Metrics`.`Input Metrics`.`Bytes Read`").alias("input_bytes"),
                F.col("`Task Metrics`.`Output Metrics`.`Bytes Written`").alias("output_bytes"),
                F.col("`Task Metrics`.`Shuffle Read Metrics`.`Remote Bytes Read`").alias("shuffle_remote"),
                F.col("`Task Metrics`.`Shuffle Read Metrics`.`Local Bytes Read`").alias("shuffle_local"),
                F.col("`Task Metrics`.`Shuffle Write Metrics`.`Shuffle Bytes Written`").alias("shuffle_write"),
                F.col("`Task Metrics`.`Memory Bytes Spilled`").alias("mem_spill"),
                F.col("`Task Metrics`.`Disk Bytes Spilled`").alias("disk_spill"),
                F.col("`Task Info`.`Executor ID`").alias("exec_id"),
                F.col("`Task Executor Metrics`.JVMHeapMemory").alias("jvm_heap"),
            )

            agg_result = io_agg.agg(
                F.count("*").alias("task_count"),
                F.coalesce(F.sum("input_bytes"), F.lit(0)).alias("total_input"),
                F.coalesce(F.sum("output_bytes"), F.lit(0)).alias("total_output"),
                F.coalesce(F.sum("shuffle_remote"), F.lit(0)).alias("total_shuffle_remote"),
                F.coalesce(F.sum("shuffle_local"), F.lit(0)).alias("total_shuffle_local"),
                F.coalesce(F.sum("shuffle_write"), F.lit(0)).alias("total_shuffle_write"),
                F.coalesce(F.sum("mem_spill"), F.lit(0)).alias("total_mem_spill"),
                F.coalesce(F.sum("disk_spill"), F.lit(0)).alias("total_disk_spill"),
                F.coalesce(F.sum("run_time"), F.lit(0)).alias("total_run_time"),
                F.sum(F.when(F.col("input_bytes") > 0, 1).otherwise(0)).alias("tasks_with_input"),
                F.sum(F.when(F.col("output_bytes") > 0, 1).otherwise(0)).alias("tasks_with_output"),
                F.sum(F.when((F.coalesce(F.col("shuffle_remote"), F.lit(0)) + F.coalesce(F.col("shuffle_local"), F.lit(0))) > 0, 1).otherwise(0)).alias("tasks_with_shuffle_read"),
                F.sum(F.when(F.col("shuffle_write") > 0, 1).otherwise(0)).alias("tasks_with_shuffle_write"),
                F.sum(F.when(F.col("mem_spill") > 0, 1).otherwise(0)).alias("tasks_with_mem_spill"),
                F.sum(F.when(F.col("disk_spill") > 0, 1).otherwise(0)).alias("tasks_with_disk_spill"),
            ).first()

            task_count = agg_result["task_count"]
            total_input = agg_result["total_input"]
            total_output = agg_result["total_output"]
            total_shuffle_read = agg_result["total_shuffle_remote"] + agg_result["total_shuffle_local"]
            total_shuffle_write = agg_result["total_shuffle_write"]

            # ── Executor summary ─────────────────────────────────
            exec_agg = (
                io_agg
                .filter((F.col("exec_id").isNotNull()) & (F.col("exec_id") != "driver"))
                .groupBy("exec_id")
                .agg(
                    F.count("*").alias("tasks"),
                    F.coalesce(F.sum("run_time"), F.lit(0)).alias("total_run_time_ms"),
                    F.coalesce(F.max("jvm_heap"), F.lit(0)).alias("peak_jvm_heap"),
                )
            )

            exec_added = (
                df.filter(F.col("Event") == "SparkListenerExecutorAdded")
                .select(
                    F.col("`Executor ID`").alias("added_id"),
                    F.col("Timestamp").alias("add_ts"),
                    F.col("`Executor Info`.`Total Cores`").alias("cores"),
                )
            )
            exec_removed = (
                df.filter(F.col("Event") == "SparkListenerExecutorRemoved")
                .select(
                    F.col("`Executor ID`").alias("removed_id"),
                    F.col("Timestamp").alias("remove_ts"),
                )
            )

            exec_full = (
                exec_agg
                .join(exec_added, exec_agg["exec_id"] == exec_added["added_id"], "left")
                .join(exec_removed, exec_agg["exec_id"] == exec_removed["removed_id"], "left")
                .withColumn("cores", F.coalesce(F.col("cores"), F.lit(executor_cores_cfg)))
                .withColumn("remove_ts", F.coalesce(F.col("remove_ts"), F.lit(end_ts)))
                .withColumn("uptime_ms",
                    F.when(F.col("add_ts").isNotNull() & F.col("remove_ts").isNotNull(),
                           F.col("remove_ts") - F.col("add_ts")).otherwise(F.lit(0)))
                .withColumn("mem_util",
                    F.when(F.col("peak_jvm_heap") > 0,
                           (F.col("peak_jvm_heap") / (1024.0 * 1024.0)) / executor_memory_mb * 100)
                    .otherwise(F.lit(0)))
                .withColumn("cpu_util",
                    F.when((F.col("uptime_ms") > 0) & (F.col("cores") > 0),
                           F.least(F.col("total_run_time_ms") / (F.col("uptime_ms") * F.col("cores")) * 100, F.lit(100.0)))
                    .otherwise(F.lit(0)))
                .withColumn("uptime_hours", F.col("uptime_ms") / (1000.0 * 60 * 60))
            )

            exec_summary = exec_full.agg(
                F.count("*").alias("total_executors"),
                F.coalesce(F.sum("cores"), F.lit(0)).alias("total_cores"),
                F.round(F.coalesce(F.sum("uptime_hours"), F.lit(0)), 2).alias("total_uptime_hours"),
                F.round(F.coalesce(F.avg("mem_util"), F.lit(0)), 2).alias("avg_mem_util"),
                F.round(F.coalesce(F.avg("cpu_util"), F.lit(0)), 2).alias("avg_cpu_util"),
                F.round(F.coalesce(F.max("peak_jvm_heap"), F.lit(0)) / GB, 2).alias("max_peak_gb"),
                F.round(F.coalesce(F.avg("peak_jvm_heap"), F.lit(0)) / GB, 2).alias("avg_peak_gb"),
            ).first()

            total_executors = int(exec_summary["total_executors"] or 0)
            total_cores = int(exec_summary["total_cores"] or 0)
            total_uptime = float(exec_summary["total_uptime_hours"] or 0)

            total_task_time_hours = float(agg_result["total_run_time"] or 0) / (1000 * 60 * 60)
            total_core_hours = max(total_cores * total_uptime, 1)
            idle_pct = max(0, round((1 - total_task_time_hours / total_core_hours) * 100, 2))

            executor_memory_gb = executor_memory_mb / 1024
            cost_factor = round(
                (total_cores * total_uptime * 0.05) + (total_executors * executor_memory_gb * total_uptime * 0.005), 4
            ) if total_cores else 0

            # ── Build output ─────────────────────────────────────
            task_stage_output = {
                "application_id": app_id,
                "extraction_timestamp": datetime.now().isoformat(),
                "extraction_engine": "pyspark",
                "event_count": total_events,
                "application_info": {
                    "job_id": job_id, "cluster_id": cluster_id,
                    "application_name": app_name, "app_id": spark_app_id,
                    "application_start_time": datetime.fromtimestamp(start_ts / 1000).isoformat() if start_ts else None,
                    "application_end_time": datetime.fromtimestamp(end_ts / 1000).isoformat() if end_ts else None,
                    "total_run_duration_minutes": duration_minutes,
                    "total_run_duration_hours": duration_hours,
                },
                "task_summary": {"total_tasks": task_count},
                "stage_summary": {},
                "executor_summary": {
                    "total_executors": total_executors,
                    "active_executors": total_executors, "dead_executors": 0,
                    "total_cores": total_cores,
                    "total_uptime_hours": total_uptime,
                    "max_peak_memory_gb": float(exec_summary["max_peak_gb"] or 0),
                    "avg_peak_memory_gb": float(exec_summary["avg_peak_gb"] or 0),
                    "avg_memory_utilization_percent": float(exec_summary["avg_mem_util"] or 0),
                    "avg_cpu_utilization_percent": float(exec_summary["avg_cpu_util"] or 0),
                    "idle_core_percentage": idle_pct,
                    "total_cost_factor": cost_factor,
                },
                "io_summary": {
                    "application_level": {
                        "total_input_bytes": int(total_input), "total_input_gb": round(total_input / GB, 2),
                        "total_output_bytes": int(total_output), "total_output_gb": round(total_output / GB, 2),
                        "total_shuffle_read_bytes": int(total_shuffle_read), "total_shuffle_read_gb": round(total_shuffle_read / GB, 2),
                        "total_shuffle_write_bytes": int(total_shuffle_write), "total_shuffle_write_gb": round(total_shuffle_write / GB, 2),
                        "tasks_analyzed": task_count,
                        "tasks_with_input": int(agg_result["tasks_with_input"] or 0),
                        "tasks_with_output": int(agg_result["tasks_with_output"] or 0),
                        "tasks_with_shuffle_read": int(agg_result["tasks_with_shuffle_read"] or 0),
                        "tasks_with_shuffle_write": int(agg_result["tasks_with_shuffle_write"] or 0),
                    }
                },
                "spill_summary": {
                    "total_memory_spilled_bytes": int(agg_result["total_mem_spill"]),
                    "total_memory_spilled_gb": round(agg_result["total_mem_spill"] / GB, 2),
                    "total_disk_spilled_bytes": int(agg_result["total_disk_spill"]),
                    "total_disk_spilled_gb": round(agg_result["total_disk_spill"] / GB, 2),
                    "tasks_with_memory_spill": int(agg_result["tasks_with_mem_spill"] or 0),
                    "tasks_with_disk_spill": int(agg_result["tasks_with_disk_spill"] or 0),
                    "tasks_analyzed": task_count,
                },
                "total_cost_factor": cost_factor,
            }

            config_output = {
                "application_id": app_id,
                "extraction_timestamp": datetime.now().isoformat(),
                "cluster_id": cluster_id, "job_id": job_id,
                "application_name": app_name, "app_id": spark_app_id,
                "application_start_time": task_stage_output["application_info"]["application_start_time"],
                "application_end_time": task_stage_output["application_info"]["application_end_time"],
                "total_run_duration_minutes": duration_minutes,
                "total_run_duration_hours": duration_hours,
                "total_cost_factor": cost_factor,
                "spark_configuration": spark_config,
            }

            results.append((app_id, task_stage_output, config_output))
            df.unpersist()
            print(f"  ✓ {app_id}: {task_count} tasks, {round(total_input/GB,1)} GB input, {total_executors} executors")

        except Exception as e:
            print(f"  ✗ {app_id}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

    # ── Write results ────────────────────────────────────────────────
    print(f"\nWriting {len(results)} results to {output_path}")

    if output_path.startswith("s3://"):
        parts = output_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        prefix = parts[1].rstrip("/") + "/" if len(parts) > 1 else ""
        s3 = boto3.client("s3", region_name="us-east-1")
        for aid, ts, cfg in results:
            s3.put_object(Bucket=bucket, Key=f"{prefix}task_stage_summary/{aid}.json",
                          Body=json.dumps(ts, indent=2), ContentType="application/json")
            s3.put_object(Bucket=bucket, Key=f"{prefix}spark_config_extract/{aid}.json",
                          Body=json.dumps(cfg, indent=2), ContentType="application/json")
    else:
        os.makedirs(f"{output_path}/task_stage_summary", exist_ok=True)
        os.makedirs(f"{output_path}/spark_config_extract", exist_ok=True)
        for aid, ts, cfg in results:
            with open(f"{output_path}/task_stage_summary/{aid}.json", "w") as f:
                json.dump(ts, f, indent=2)
            with open(f"{output_path}/spark_config_extract/{aid}.json", "w") as f:
                json.dump(cfg, f, indent=2)

    print(f"✅ Extraction complete: {len(results)} applications")
    spark.stop()


# ── Main ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark-based event log extractor")
    parser.add_argument("--input", required=True, help="S3 path to event logs")
    parser.add_argument("--output", required=True, help="Output path for extracted metrics")
    parser.add_argument("--limit", type=int, default=100, help="Max apps")
    parser.add_argument("--decompress-workers", type=int, default=50, help="Parallel download threads")
    args = parser.parse_args()

    LOCAL_STAGING = "/tmp/spark_extractor_staging"

    print(f"\n{'='*60}")
    print("SPARK EVENT LOG EXTRACTOR (Python decompress + Spark extract)")
    print(f"{'='*60}")
    start = time.time()

    # Phase A: Python decompress
    app_names = phase_a_decompress(args.input, LOCAL_STAGING, args.limit, args.decompress_workers)

    # Phase B: Spark extract
    phase_b_spark_extract(app_names, LOCAL_STAGING, args.output, args.limit)

    elapsed = time.time() - start
    print(f"\nTotal time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
