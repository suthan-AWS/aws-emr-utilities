#!/usr/bin/env python3
"""
EMR Serverless Recommender - Dual Mode with Local/S3 Support
Supports both local filesystem and S3 for input/output.
"""

import sys
import json
import glob
from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd
import logging

# Setup logging
logging.basicConfig(
    format="%(asctime)s %(levelname)-5s [%(name)s]  %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("dual-mode-recommender")

# Try to import S3 support
try:
    import boto3
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    log.warning("boto3 not available - S3 support disabled")


def is_s3_path(path: str) -> bool:
    """Check if path is S3."""
    return path.startswith('s3://')


def load_json_files(path: str, limit: int = 100) -> List[Dict]:
    """Load JSON files from S3 or local filesystem."""
    all_data = []
    
    if is_s3_path(path):
        if not S3_AVAILABLE:
            raise RuntimeError("boto3 required for S3 paths. Install: pip install boto3")
        
        # S3 path
        bucket, prefix = path.replace('s3://', '').split('/', 1)
        prefix = prefix.rstrip('/') + '/task_stage_summary/'
        
        s3_client = boto3.client('s3')
        log.info("Loading from S3: s3://%s/%s", bucket, prefix)
        
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('.json'):
                    continue
                
                try:
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    data = json.loads(response['Body'].read())
                    all_data.append(data)
                    
                    if len(all_data) >= limit:
                        break
                except Exception as e:
                    log.debug("Skipping %s: %s", key, e)
            
            if len(all_data) >= limit:
                break
    else:
        # Local filesystem
        search_path = Path(path) / 'task_stage_summary' / '*.json'
        log.info("Loading from local: %s", search_path)
        
        json_files = glob.glob(str(search_path))
        for json_file in json_files[:limit]:
            try:
                with open(json_file) as f:
                    data = json.load(f)
                    all_data.append(data)
            except Exception as e:
                log.debug("Skipping %s: %s", json_file, e)
    
    log.info("Loaded %d JSON files", len(all_data))
    return all_data


def _gb_to_gib(gb: float) -> float:
    return round(gb * 0.931323, 2)


def _calculate_shuffle_ratio(input_gb, read_gb, write_gb) -> float:
    if input_gb == 0:
        return 100.0 if (read_gb + write_gb) > 0 else 0.0
    return round((read_gb + write_gb) / input_gb * 100.0, 2)


def _parse_orig_cores(spark_config: dict) -> int:
    """Extract original executor cores from spark_config."""
    for key in ['spark.executor.cores', 'spark.emr.default.executor.cores']:
        val = spark_config.get(key)
        if val:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
    return 0


def _select_worker_type(input_gb: float, shuffle_ratio: float, shuffle_gb: float = 0.0) -> Tuple[str, Dict]:
    WORKERS = {
        "Small": {"vcpu": 4, "memory": 27},
        "Medium": {"vcpu": 8, "memory": 54},
        "Large": {"vcpu": 16, "memory": 108},
    }
    data_gb = max(input_gb, shuffle_gb)
    if data_gb > 2048 or shuffle_ratio > 70:
        return "Large", WORKERS["Large"]
    elif data_gb > 500 or shuffle_ratio > 40:
        return "Medium", WORKERS["Medium"]
    else:
        return "Small", WORKERS["Small"]


def _compute_exec_limits(input_gb: float, vcpu: int, partitions: int = 0,
                        mem_pct: float = 60.0, cpu_pct: float = 50.0,
                        idle_pct: float = 50.0, spill_gb: float = 0.0,
                        mode: str = "cost", active_executors: int = 0,
                        orig_cores_per_executor: int = 0) -> Tuple[int, int]:
    req_input = max(1, int(input_gb / 100))
    req_part = max(1, int((partitions / vcpu) / 3)) if partitions > 0 else 0
    if active_executors > 0:
        if orig_cores_per_executor > 0:
            # Scale by core ratio: original total vCPUs → recommended executor size
            req_actual = max(1, (active_executors * orig_cores_per_executor) // vcpu)
        else:
            # Unknown original size — conservative estimate
            divisor = 3 if mode == "cost" else 2
            req_actual = max(1, active_executors // divisor)
    else:
        req_actual = 0
    base_req = max(req_input, req_part, req_actual)
    
    if mode == "performance":
        if mem_pct > 75:
            factor = 1.0 + ((mem_pct - 75) / 25) * 0.5
        else:
            mem_pressure = mem_pct * 0.4
            cpu_pressure = cpu_pct * 0.4
            spill_ratio = (spill_gb / max(input_gb, 1)) * 100
            spill_pressure = min(20, spill_ratio * 0.2)
            pressure = max(0, min(100, mem_pressure + cpu_pressure + spill_pressure))
            factor = 0.5 + (pressure / 100) * 1.0
    else:
        mem_pressure = mem_pct * 0.4
        cpu_pressure = cpu_pct * 0.4
        spill_ratio = (spill_gb / max(input_gb, 1)) * 100
        spill_pressure = min(20, spill_ratio * 0.2)
        pressure = max(0, min(100, mem_pressure + cpu_pressure + spill_pressure))
        factor = 0.5 + (pressure / 100) * 1.0
    
    max_exec = max(2, int(base_req * factor))
    min_exec = max(1, max_exec // 2)
    
    return max_exec, min_exec


def _calculate_executor_disk(shuffle_write_gb: float, disk_spill_gb: float,
                             memory_spill_gb: float, max_executors: int) -> str:
    total_shuffle_per_exec = shuffle_write_gb / max(max_executors, 1)
    total_spill_per_exec = (disk_spill_gb + memory_spill_gb) / max(max_executors, 1)
    estimated_gb = (total_shuffle_per_exec + total_spill_per_exec) * 1.5
    disk_gb = max(500, min(2000, int(estimated_gb)))
    return f"{disk_gb}G"


def _max_partition_bytes(input_gb: float) -> str:
    if input_gb >= 1024:
        return "512m"
    return "128m"


def _get_timeout_configs(input_gb: float, duration_hours: float) -> Dict[str, str]:
    import math
    if math.isnan(input_gb): input_gb = 0
    if math.isnan(duration_hours): duration_hours = 0
    base_timeout = 600
    data_factor = int(input_gb / 1000) * 60
    duration_factor = int(duration_hours) * 120 if duration_hours else 0
    shuffle_timeout = min(max(base_timeout + data_factor + duration_factor, 600), 1800)
    network_timeout = shuffle_timeout * 2
    
    return {
        "spark.network.timeout": f"{network_timeout}s",
        "spark.shuffle.io.connectionTimeout": f"{shuffle_timeout}s",
    }


def _get_s3_retry_configs(input_gb: float) -> Dict[str, str]:
    if input_gb < 100:
        retries = "5"
    elif input_gb < 1000:
        retries = "10"
    else:
        retries = "15"
    
    return {
        "spark.hadoop.fs.s3a.retry.limit": retries,
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    }


def _get_iceberg_configs() -> Dict[str, str]:
    return {
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    }


def generate_dual_recommendations(input_path: str, limit: int = 100,
                                  target_partition_size_mib: int = 1024) -> Tuple[List[Dict], List[Dict]]:
    """Generate both cost and performance recommendations."""
    
    # Load metrics
    all_data = load_json_files(input_path, limit)
    
    # Convert to DataFrame
    flattened = []
    for data in all_data:
        # Handle both old format (io/utilization) and new format (io_summary/executor_summary)
        app_info = data.get('application_info', {})
        io_data = data.get('io', data.get('io_summary', {}).get('application_level', {}))
        util_data = data.get('utilization', data.get('executor_summary', {}))
        spill_data = data.get('spill_summary', {})
        
        flat = {
            'application_id': data.get('application_id', app_info.get('app_id')),
            'application_name': data.get('application_name', app_info.get('application_name')),
            'job_id': app_info.get('job_id'),
            'total_run_duration_hours': data.get('total_run_duration_hours', app_info.get('total_run_duration_hours')),
            'io_total_input_gb': io_data.get('total_input_gb'),
            'io_total_shuffle_read_gb': io_data.get('total_shuffle_read_gb'),
            'io_total_shuffle_write_gb': io_data.get('total_shuffle_write_gb'),
            'avg_memory_utilization_percent': util_data.get('avg_memory_utilization_percent'),
            'avg_cpu_utilization_percent': util_data.get('avg_cpu_utilization_percent'),
            'idle_core_percentage': util_data.get('idle_core_percentage'),
            'total_memory_spilled_gb': spill_data.get('total_memory_spilled_gb'),
            'total_disk_spilled_gb': spill_data.get('total_disk_spilled_gb'),
            'total_executors': util_data.get('total_executors', 0),
            'active_executors': util_data.get('active_executors', 0),
            'orig_cores_per_executor': _parse_orig_cores(data.get('spark_config', {})),
        }
        flattened.append(flat)
    
    df = pd.DataFrame(flattened)
    
    # Sanitize NaN values - replace with 0 for numeric columns
    df = df.fillna(0)
    
    # Separate apps with and without meaningful work
    has_data = (df['io_total_input_gb'] > 0) | (df['io_total_shuffle_read_gb'] > 0) | (df['io_total_shuffle_write_gb'] > 0)
    df_with_data = df[has_data].sort_values('io_total_input_gb', ascending=False).head(limit)
    df_no_data = df[~has_data].head(limit)
    
    log.info("Processing %d applications with data, %d with no input data", len(df_with_data), len(df_no_data))
    
    cost_recs = []
    perf_recs = []
    
    # Process applications with input data
    for _, row in df_with_data.iterrows():
        app_id = row.get('application_id', 'N/A')
        name = row.get('application_name', 'N/A')
        job_id = row.get('job_id')
        duration = float(row.get('total_run_duration_hours', 0) or 0)
        i_in_gb = float(row.get('io_total_input_gb', 0) or 0)
        s_in_gb = float(row.get('io_total_shuffle_read_gb', 0) or 0)
        s_out_gb = float(row.get('io_total_shuffle_write_gb', 0) or 0)
        
        mem_pct = float(row.get('avg_memory_utilization_percent', 60.0) or 60.0)
        cpu_pct = float(row.get('avg_cpu_utilization_percent', 50.0) or 50.0)
        idle_pct = float(row.get('idle_core_percentage', 50.0) or 50.0)
        spill_gb = float(row.get('total_memory_spilled_gb', 0.0) or 0.0)
        disk_spill_gb = float(row.get('total_disk_spilled_gb', 0.0) or 0.0)
        tot_executors = int(row.get('total_executors', 0) or 0)
        act_executors = int(row.get('active_executors', 0) or 0)
        orig_cores = int(row.get('orig_cores_per_executor', 0) or 0)
        
        sh_ratio = _calculate_shuffle_ratio(i_in_gb, s_in_gb, s_out_gb)
        worker_type, worker_cfg = _select_worker_type(i_in_gb, sh_ratio, max(s_in_gb, s_out_gb))
        
        shuffle_data_gb = max(s_in_gb, s_out_gb)
        shuffle_bytes = shuffle_data_gb * 1024 * 1024 * 1024
        
        # Custom partition calculation
        def auto_tune_custom(shuffle_bytes, max_executors):
            target_mib = target_partition_size_mib
            if shuffle_bytes > 0:
                partitions = max(2 * max_executors, int((shuffle_bytes / (target_mib * 1024 * 1024)) + 0.5))
            else:
                # Scale by input size (128MB per partition) instead of flat 200
                partitions = max(2, min(200, int(i_in_gb / 0.128)))
            if partitions % 2 != 0:
                partitions += 1
            return partitions, target_mib
        
        # Cost-optimized
        max_exec_cost_init, min_exec_cost = _compute_exec_limits(
            i_in_gb, worker_cfg["vcpu"], 0, mem_pct, cpu_pct, idle_pct, spill_gb, mode="cost",
            active_executors=act_executors, orig_cores_per_executor=orig_cores
        )
        sp_cost, target_mib_cost = auto_tune_custom(shuffle_bytes, max_exec_cost_init)
        max_exec_cost, min_exec_cost = _compute_exec_limits(
            i_in_gb, worker_cfg["vcpu"], sp_cost, mem_pct, cpu_pct, idle_pct, spill_gb, mode="cost",
            active_executors=act_executors, orig_cores_per_executor=orig_cores
        )
        executor_disk_cost = _calculate_executor_disk(s_out_gb, disk_spill_gb, spill_gb, max_exec_cost)
        
        # Performance-optimized
        max_exec_perf_init, min_exec_perf = _compute_exec_limits(
            i_in_gb, worker_cfg["vcpu"], 0, mem_pct, cpu_pct, idle_pct, spill_gb, mode="performance",
            active_executors=act_executors, orig_cores_per_executor=orig_cores
        )
        sp_perf, target_mib_perf = auto_tune_custom(shuffle_bytes, max_exec_perf_init)
        max_exec_perf, min_exec_perf = _compute_exec_limits(
            i_in_gb, worker_cfg["vcpu"], sp_perf, mem_pct, cpu_pct, idle_pct, spill_gb, mode="performance",
            active_executors=act_executors, orig_cores_per_executor=orig_cores
        )
        executor_disk_perf = _calculate_executor_disk(s_out_gb, disk_spill_gb, spill_gb, max_exec_perf)
        
        # Build base metrics
        base_metrics = {
            "input_gb": round(i_in_gb, 2),
            "input_gib": _gb_to_gib(i_in_gb),
            "shuffle_read_gb": round(s_in_gb, 2),
            "shuffle_write_gb": round(s_out_gb, 2),
            "shuffle_total_gb": round(s_in_gb + s_out_gb, 2),
            "shuffle_ratio_percent": sh_ratio,
            "duration_hours": round(duration, 2),
            "avg_memory_utilization_percent": round(mem_pct, 2),
            "avg_cpu_utilization_percent": round(cpu_pct, 2),
            "idle_core_percentage": round(idle_pct, 2),
            "total_memory_spilled_gb": round(spill_gb, 2),
        }
        
        # Build Spark configs
        def build_spark_cfg(max_exec, min_exec, sp, executor_disk):
            cfg = {
                "spark.driver.cores": "8",
                "spark.driver.memory": "54G",
                "spark.executor.cores": str(worker_cfg["vcpu"]),
                "spark.executor.memory": f"{worker_cfg['memory']}g",
                "spark.dynamicAllocation.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.files.maxPartitionBytes": _max_partition_bytes(i_in_gb),
                "spark.emr-serverless.executor.disk": executor_disk,
                "spark.emr-serverless.executor.disk.type": "shuffle_optimized",
                "spark.sql.shuffle.partitions": str(sp),
                "spark.dynamicAllocation.maxExecutors": str(max_exec),
                "spark.dynamicAllocation.minExecutors": str(min_exec),
                "spark.dynamicAllocation.initialExecutors": str(min_exec),
            }
            cfg.update(_get_timeout_configs(i_in_gb, duration))
            cfg.update(_get_s3_retry_configs(i_in_gb))
            cfg.update(_get_iceberg_configs())
            if sh_ratio > 30:
                cfg.update({"spark.shuffle.compress": "true", "spark.shuffle.spill.compress": "true"})
            if max_exec <= 2:
                cfg["spark.dynamicAllocation.enabled"] = "false"
                cfg["spark.executor.instances"] = str(max_exec)
                del cfg["spark.dynamicAllocation.maxExecutors"]
                del cfg["spark.dynamicAllocation.minExecutors"]
                del cfg["spark.dynamicAllocation.initialExecutors"]
            return cfg
        
        # Cost recommendation
        cost_rec = {
            "application_id": app_id,
            "application_name": name,
            "optimization_mode": "cost",
            "metrics": base_metrics,
        }
        if job_id:
            cost_rec["job_id"] = job_id
        cost_rec.update({
            "worker": {
                "type": worker_type,
                "vcpu": worker_cfg["vcpu"],
                "memory_gb": worker_cfg["memory"],
                "max_executors": max_exec_cost,
                "min_executors": min_exec_cost,
                "total_vcpu_capacity": max_exec_cost * worker_cfg["vcpu"],
                "total_memory_capacity": max_exec_cost * worker_cfg["memory"],
            },
            "spark_configs": build_spark_cfg(max_exec_cost, min_exec_cost, sp_cost, executor_disk_cost),
            "shuffle_tuned": {
                "partitions": sp_cost,
                "target_partition_size_mib": target_mib_cost,
                "auto_tuned": True,
            },
        })
        
        # Performance recommendation
        perf_rec = {
            "application_id": app_id,
            "application_name": name,
            "optimization_mode": "performance",
            "metrics": base_metrics,
        }
        if job_id:
            perf_rec["job_id"] = job_id
        perf_rec.update({
            "worker": {
                "type": worker_type,
                "vcpu": worker_cfg["vcpu"],
                "memory_gb": worker_cfg["memory"],
                "max_executors": max_exec_perf,
                "min_executors": min_exec_perf,
                "total_vcpu_capacity": max_exec_perf * worker_cfg["vcpu"],
                "total_memory_capacity": max_exec_perf * worker_cfg["memory"],
            },
            "spark_configs": build_spark_cfg(max_exec_perf, min_exec_perf, sp_perf, executor_disk_perf),
            "shuffle_tuned": {
                "partitions": sp_perf,
                "target_partition_size_mib": target_mib_perf,
                "auto_tuned": True,
            },
        })
        
        cost_recs.append(cost_rec)
        perf_recs.append(perf_rec)
    
    # Process applications with no input data - recommend minimal config
    for _, row in df_no_data.iterrows():
        app_id = row.get('application_id', 'N/A')
        name = row.get('application_name', 'N/A')
        
        max_exec_minimal = 2
        min_exec_minimal = 1
        
        minimal_rec = {
            "application_id": app_id,
            "application_name": name,
            "optimization_mode": "minimal",
            "note": "No input data detected - minimal configuration recommended",
            "metrics": {
                "input_gb": 0.0,
                "duration_hours": 0.0,
            },
            "worker": {
                "type": "Small",
                "vcpu": 1,
                "memory_gb": 2,
                "max_executors": max_exec_minimal,
                "min_executors": min_exec_minimal,
                "total_vcpu_capacity": 2,
                "total_memory_capacity": 4
            },
            "spark_configs": {
                "spark.driver.cores": "1",
                "spark.driver.memory": "2G",
                "spark.executor.cores": "1",
                "spark.executor.memory": "2g",
                "spark.executor.instances": str(max_exec_minimal),
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": str(max_exec_minimal),
                "spark.dynamicAllocation.minExecutors": str(min_exec_minimal),
                "spark.dynamicAllocation.initialExecutors": str(min_exec_minimal),
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.type": "hive",
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            }
        }
        
        cost_recs.append(minimal_rec)
        perf_recs.append(minimal_rec)
    
    log.info("Generated %d cost-optimized and %d performance-optimized recommendations",
             len(cost_recs), len(perf_recs))
    return cost_recs, perf_recs


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Dual-mode EMR Serverless recommender (S3 or Local)")
    parser.add_argument("--input-path", required=True, help="S3 path (s3://bucket/prefix) or local path")
    parser.add_argument("--region", default="us-east-1", help="AWS region (for S3 only)")
    parser.add_argument("--limit", type=int, default=100, help="Max applications")
    parser.add_argument("--target-partition-size", type=int, default=1024,
                        help="Target shuffle partition size in MiB (default: 1024 = 1GB)")
    parser.add_argument("--output-cost", default="recommendations_cost_optimized.json", help="Cost output file")
    parser.add_argument("--output-perf", default="recommendations_performance_optimized.json", help="Performance output file")
    parser.add_argument("--format-job-config", action="store_true",
                        help="Format output to job configuration format")
    parser.add_argument("--cost-optimized", action="store_true",
                        help="Generate only cost-optimized recommendations")
    parser.add_argument("--performance-optimized", action="store_true",
                        help="Generate only performance-optimized recommendations")
    parser.add_argument("--individual-files", action="store_true",
                        help="Generate individual JSON files per job (1-jobname.json, 2-jobname.json, ...)")
    parser.add_argument("--write-to-iceberg-table",
                        help="Write recommendations to Iceberg table (catalog.database.table)")
    
    args = parser.parse_args()
    
    # Generate recommendations
    cost_recs, perf_recs = generate_dual_recommendations(
        args.input_path,
        args.limit,
        args.target_partition_size
    )
    
    # Determine which recommendations to generate
    generate_cost = not args.performance_optimized  # Generate cost unless perf-only
    generate_perf = not args.cost_optimized  # Generate perf unless cost-only
    
    # Write recommendations
    if generate_cost:
        if args.format_job_config:
            from format_to_job_config import format_to_job_config
            cost_jobs = [format_to_job_config(rec) for rec in cost_recs]
            
            if args.individual_files:
                # Write individual files
                output_dir = Path(args.output_cost).parent
                output_dir.mkdir(parents=True, exist_ok=True)
                for i, job in enumerate(cost_jobs, 1):
                    job_name = job.get('job_name', f'job_{i}').replace(' ', '_').replace('-job', '')
                    filename = output_dir / f"{i}-{job_name}.json"
                    filename.write_text(json.dumps(job, indent=2))
                log.info("Cost-optimized job configs written to %d individual files in %s", len(cost_jobs), output_dir)
            else:
                # Write single file
                cost_job_file = args.output_cost.replace('.json', '_job_config.json')
                Path(cost_job_file).parent.mkdir(parents=True, exist_ok=True)
                Path(cost_job_file).write_text(json.dumps(cost_jobs, indent=2))
                log.info("Cost-optimized job config written to %s", cost_job_file)
        else:
            Path(args.output_cost).write_text(json.dumps(cost_recs, indent=2))
            log.info("Cost-optimized recommendations written to %s", args.output_cost)
    
    if generate_perf:
        if args.format_job_config:
            from format_to_job_config import format_to_job_config
            perf_jobs = [format_to_job_config(rec) for rec in perf_recs]
            
            if args.individual_files:
                # Write individual files
                output_dir = Path(args.output_perf).parent
                output_dir.mkdir(parents=True, exist_ok=True)
                for i, job in enumerate(perf_jobs, 1):
                    job_name = job.get('job_name', f'job_{i}').replace(' ', '_').replace('-job', '')
                    filename = output_dir / f"{i}-{job_name}.json"
                    filename.write_text(json.dumps(job, indent=2))
                log.info("Performance-optimized job configs written to %d individual files", len(perf_jobs))
            else:
                # Write single file
                perf_job_file = args.output_perf.replace('.json', '_job_config.json')
                Path(perf_job_file).parent.mkdir(parents=True, exist_ok=True)
                Path(perf_job_file).write_text(json.dumps(perf_jobs, indent=2))
                log.info("Performance-optimized job config written to %s", perf_job_file)
        else:
            Path(args.output_perf).write_text(json.dumps(perf_recs, indent=2))
            log.info("Performance-optimized recommendations written to %s", args.output_perf)
    
    # Write to Iceberg table if requested
    if args.write_to_iceberg_table:
        from write_to_iceberg import write_to_iceberg
        recs_to_write = []
        if generate_cost:
            recs_to_write.extend(cost_recs)
        if generate_perf:
            recs_to_write.extend(perf_recs)
        write_to_iceberg(recs_to_write, args.write_to_iceberg_table, args.region)
    
    # Print comparison (only if both modes generated)
    if generate_cost and generate_perf:
        print("\n" + "="*80)
        print("COMPARISON SUMMARY")
        print("="*80)
    print(f"{'App Name':<40} | {'Mode':<11} | {'Max Exec':>8} | {'Total vCPU':>10}")
    print("-"*80)
    for cost, perf in zip(cost_recs[:10], perf_recs[:10]):
        name = cost['application_name'][:38]
        print(f"{name:<40} | {'Cost':<11} | {cost['worker']['max_executors']:>8} | {cost['worker']['total_vcpu_capacity']:>10}")
        print(f"{'':<40} | {'Performance':<11} | {perf['worker']['max_executors']:>8} | {perf['worker']['total_vcpu_capacity']:>10}")
        diff_exec = perf['worker']['max_executors'] - cost['worker']['max_executors']
        diff_pct = (diff_exec / cost['worker']['max_executors'] * 100) if cost['worker']['max_executors'] > 0 else 0
        print(f"{'':<40} | {'Difference':<11} | {diff_exec:>+8} | {diff_pct:>+9.1f}%")
        print("-"*80)
