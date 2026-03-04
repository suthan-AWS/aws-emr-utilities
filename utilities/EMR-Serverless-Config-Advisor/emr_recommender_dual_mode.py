#!/usr/bin/env python3
"""
EMR Serverless Recommender - Dual Mode (Cost + Performance)
Generates two sets of recommendations from the same metrics.
"""

import sys
import json
from pathlib import Path

# Import from the original module
sys.path.insert(0, '/home/hadoop')
from emr_recommender_v16_s3_patched import (
    EmrRecommender, _select_worker_type, _calculate_shuffle_ratio,
    _compute_exec_limits, _auto_tune_shuffles, _calculate_executor_disk,
    _max_partition_bytes, _get_timeout_configs, _get_s3_retry_configs,
    _get_iceberg_configs, _gb_to_gib, log
)


class DualModeRecommender(EmrRecommender):
    """Extended recommender that generates both cost and performance recommendations."""
    
    def __init__(self, s3_paths, region, target_partition_size_mib=1024):
        super().__init__(s3_paths, region)
        self.target_partition_size_mib = target_partition_size_mib
        log.info("Target partition size: %d MiB", target_partition_size_mib)
    
    def generate_dual(self, limit: int = 100):
        """Generate both cost-optimized and performance-optimized recommendations."""
        df = self._load_metrics_from_s3(limit)
        
        cost_recs = []
        perf_recs = []
        
        for _, row in df.iterrows():
            app_id = row.get('application_id', 'N/A')
            name = row.get('application_name', 'N/A')
            duration = float(row.get('total_run_duration_hours', 0) or 0)
            i_in_gb = float(row.get('io_total_input_gb', 0) or 0)
            s_in_gb = float(row.get('io_total_shuffle_read_gb', 0) or 0)
            s_out_gb = float(row.get('io_total_shuffle_write_gb', 0) or 0)
            
            mem_pct = float(row.get('avg_memory_utilization_percent', 60.0) or 60.0)
            cpu_pct = float(row.get('avg_cpu_utilization_percent', 50.0) or 50.0)
            idle_pct = float(row.get('idle_core_percentage', 50.0) or 50.0)
            spill_gb = float(row.get('total_memory_spilled_gb', 0.0) or 0.0)
            disk_spill_gb = float(row.get('total_disk_spilled_gb', 0.0) or 0.0)
            
            sh_ratio = _calculate_shuffle_ratio(i_in_gb, s_in_gb, s_out_gb)
            worker_type, worker_cfg = _select_worker_type(i_in_gb, sh_ratio)
            
            shuffle_data_gb = max(s_in_gb, s_out_gb)
            shuffle_bytes = shuffle_data_gb * 1024 * 1024 * 1024
            
            # Override _auto_tune_shuffles to use configurable target size
            def auto_tune_custom(shuffle_bytes, max_executors):
                target_mib = self.target_partition_size_mib
                if shuffle_bytes > 0:
                    partitions = int((shuffle_bytes / (target_mib * 1024 * 1024)) + 0.5)
                else:
                    partitions = 200
                if partitions % 2 != 0:
                    partitions += 1
                return partitions, target_mib
            
            # Generate COST-OPTIMIZED recommendation
            max_exec_cost_init, min_exec_cost = _compute_exec_limits(
                i_in_gb, worker_cfg["vcpu"], 0, mem_pct, cpu_pct, idle_pct, spill_gb, mode="cost"
            )
            sp_cost, target_mib_cost = auto_tune_custom(shuffle_bytes, max_exec_cost_init)
            max_exec_cost, min_exec_cost = _compute_exec_limits(
                i_in_gb, worker_cfg["vcpu"], sp_cost, mem_pct, cpu_pct, idle_pct, spill_gb, mode="cost"
            )
            executor_disk_cost = _calculate_executor_disk(s_out_gb, disk_spill_gb, spill_gb, max_exec_cost)
            
            # Generate PERFORMANCE-OPTIMIZED recommendation
            max_exec_perf_init, min_exec_perf = _compute_exec_limits(
                i_in_gb, worker_cfg["vcpu"], 0, mem_pct, cpu_pct, idle_pct, spill_gb, mode="performance"
            )
            sp_perf, target_mib_perf = auto_tune_custom(shuffle_bytes, max_exec_perf_init)
            max_exec_perf, min_exec_perf = _compute_exec_limits(
                i_in_gb, worker_cfg["vcpu"], sp_perf, mem_pct, cpu_pct, idle_pct, spill_gb, mode="performance"
            )
            executor_disk_perf = _calculate_executor_disk(s_out_gb, disk_spill_gb, spill_gb, max_exec_perf)
            
            # Build base metrics (same for both)
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
            
            # Build Spark configs helper
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
                return cfg
            
            # Cost-optimized recommendation
            cost_rec = {
                "application_id": app_id,
                "application_name": name,
                "optimization_mode": "cost",
                "metrics": base_metrics,
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
            }
            
            # Performance-optimized recommendation
            perf_rec = {
                "application_id": app_id,
                "application_name": name,
                "optimization_mode": "performance",
                "metrics": base_metrics,
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
            }
            
            cost_recs.append(cost_rec)
            perf_recs.append(perf_rec)
        
        log.info("Generated %d cost-optimized and %d performance-optimized recommendations", 
                 len(cost_recs), len(perf_recs))
        return cost_recs, perf_recs


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Dual-mode EMR Serverless recommender")
    parser.add_argument("--s3-path", required=True, help="S3 path to metrics")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--limit", type=int, default=100, help="Max applications")
    parser.add_argument("--target-partition-size", type=int, default=1024, 
                        help="Target shuffle partition size in MiB (default: 1024 = 1GB)")
    parser.add_argument("--format-job-config", action="store_true",
                        help="Format output to job configuration format")
    parser.add_argument("--output-cost", default="recommendations_cost_optimized.json", help="Cost output file")
    parser.add_argument("--output-perf", default="recommendations_performance_optimized.json", help="Performance output file")
    
    args = parser.parse_args()
    
    # Convert single path to list
    s3_paths = [args.s3_path] if isinstance(args.s3_path, str) else args.s3_path
    
    recommender = DualModeRecommender(s3_paths, args.region, args.target_partition_size)
    cost_recs, perf_recs = recommender.generate_dual(args.limit)
    
    # Write cost-optimized
    Path(args.output_cost).write_text(json.dumps(cost_recs, indent=2))
    log.info("Cost-optimized recommendations written to %s", args.output_cost)
    
    # Write performance-optimized
    Path(args.output_perf).write_text(json.dumps(perf_recs, indent=2))
    log.info("Performance-optimized recommendations written to %s", args.output_perf)
    
    # Format to job config if requested
    if args.format_job_config:
        import sys
        sys.path.insert(0, '/home/hadoop')
        from format_to_job_config import format_to_job_config
        
        cost_jobs = [format_to_job_config(rec) for rec in cost_recs]
        perf_jobs = [format_to_job_config(rec) for rec in perf_recs]
        
        cost_job_file = args.output_cost.replace('.json', '_job_config.json')
        perf_job_file = args.output_perf.replace('.json', '_job_config.json')
        
        Path(cost_job_file).write_text(json.dumps(cost_jobs, indent=2))
        Path(perf_job_file).write_text(json.dumps(perf_jobs, indent=2))
        
        log.info("Job config format written to %s and %s", cost_job_file, perf_job_file)
    
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
