#!/usr/bin/env python3
"""
Spark Event Log Processing & Recommendation Pipeline
====================================================
Two-stage pipeline:
1. Extract metrics from Spark event logs (S3) → Intermediate JSON
2. Generate EMR Serverless recommendations from extracted metrics

Usage:
    python3 pipeline_wrapper.py --input-bucket suthan-event-logs --input-prefix main/apps/
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Pipeline configuration
SPARK_PROCESSOR_SCRIPT = "spark_processor.py"
RECOMMENDER_SCRIPT = "emr_recommender_v16_s3.py"

# Default S3 locations
DEFAULT_INPUT_BUCKET = "suthan-event-logs"
DEFAULT_INPUT_PREFIX = "main/apps/"
DEFAULT_STAGING_BUCKET = "suthan-event-logs"
DEFAULT_STAGING_PREFIX = "pipeline-staging/"
DEFAULT_OUTPUT_FILE = "emr_recommendations.json"


def run_command(cmd, description):
    """Execute a command and handle errors."""
    import os
    print(f"\n{'='*80}")
    print(f"STEP: {description}")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}\n")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,
            text=True,
            env=os.environ.copy()  # Inherit environment variables
        )
        elapsed = time.time() - start_time
        print(f"\n✓ {description} completed in {elapsed:.1f}s")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} failed with exit code {e.returncode}")
        print(f"Error: {e}")
        return False


def update_spark_processor_config(input_bucket, input_prefix, staging_bucket, staging_prefix):
    """Update spark_processor.py configuration dynamically."""
    print(f"\n{'='*80}")
    print("CONFIGURATION: Updating spark_processor.py")
    print(f"{'='*80}")
    
    script_path = Path(SPARK_PROCESSOR_SCRIPT)
    if not script_path.exists():
        print(f"✗ Error: {SPARK_PROCESSOR_SCRIPT} not found")
        return False
    
    with open(script_path, 'r') as f:
        content = f.read()
    
    # Update configuration
    import re
    content = re.sub(
        r'INPUT_BUCKET = "[^"]*"',
        f'INPUT_BUCKET = "{input_bucket}"',
        content
    )
    content = re.sub(
        r'INPUT_PREFIX = "[^"]*"',
        f'INPUT_PREFIX = "{input_prefix}"',
        content
    )
    content = re.sub(
        r'OUTPUT_BUCKET = "[^"]*"',
        f'OUTPUT_BUCKET = "{staging_bucket}"',
        content
    )
    content = re.sub(
        r'OUTPUT_PREFIX = "[^"]*"',
        f'OUTPUT_PREFIX = "{staging_prefix}"',
        content
    )
    
    # Write updated config
    with open(script_path, 'w') as f:
        f.write(content)
    
    print(f"✓ Updated configuration:")
    print(f"  INPUT:   s3://{input_bucket}/{input_prefix}")
    print(f"  OUTPUT:  s3://{staging_bucket}/{staging_prefix}")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Spark Event Log Processing & EMR Recommendation Pipeline"
    )
    
    # Input configuration
    parser.add_argument(
        "--input-bucket",
        default=DEFAULT_INPUT_BUCKET,
        help=f"S3 bucket with event logs (default: {DEFAULT_INPUT_BUCKET})"
    )
    parser.add_argument(
        "--input-prefix",
        default=DEFAULT_INPUT_PREFIX,
        help=f"S3 prefix for event logs (default: {DEFAULT_INPUT_PREFIX})"
    )
    
    # Staging configuration
    parser.add_argument(
        "--staging-bucket",
        default=DEFAULT_STAGING_BUCKET,
        help=f"S3 bucket for intermediate JSON (default: {DEFAULT_STAGING_BUCKET})"
    )
    parser.add_argument(
        "--staging-prefix",
        default=DEFAULT_STAGING_PREFIX,
        help=f"S3 prefix for staging (default: {DEFAULT_STAGING_PREFIX})"
    )
    
    # Output configuration
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output file for recommendations (default: {DEFAULT_OUTPUT_FILE})"
    )
    
    # Processing options
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of largest jobs to analyze (default: 100)"
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region (default: us-east-1)"
    )
    parser.add_argument(
        "--skip-extraction",
        action="store_true",
        help="Skip extraction step (use existing staging data)"
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*80}")
    print("SPARK EVENT LOG PROCESSING & RECOMMENDATION PIPELINE")
    print(f"{'='*80}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nConfiguration:")
    print(f"  Input:   s3://{args.input_bucket}/{args.input_prefix}")
    print(f"  Staging: s3://{args.staging_bucket}/{args.staging_prefix}")
    print(f"  Output:  {args.output}")
    print(f"  Limit:   {args.limit} applications")
    
    pipeline_start = time.time()
    
    # Stage 1: Extract metrics from event logs
    if not args.skip_extraction:
        print(f"\n{'='*80}")
        print("STAGE 1: EXTRACT METRICS FROM EVENT LOGS")
        print(f"{'='*80}")
        
        # Update spark processor configuration
        if not update_spark_processor_config(
            args.input_bucket,
            args.input_prefix,
            args.staging_bucket,
            args.staging_prefix
        ):
            print("\n✗ Pipeline failed: Configuration update failed")
            sys.exit(1)
        
        # Run spark processor
        cmd = ["python3", SPARK_PROCESSOR_SCRIPT]
        if not run_command(cmd, "Metric Extraction"):
            print("\n✗ Pipeline failed: Metric extraction failed")
            sys.exit(1)
    else:
        print(f"\n⊘ Skipping extraction (using existing data in staging)")
    
    # Stage 2: Generate recommendations
    print(f"\n{'='*80}")
    print("STAGE 2: GENERATE EMR SERVERLESS RECOMMENDATIONS")
    print(f"{'='*80}")
    
    staging_path = f"s3://{args.staging_bucket}/{args.staging_prefix}"
    cmd = [
        "python3",
        RECOMMENDER_SCRIPT,
        "--s3-path", staging_path,
        "--region", args.region,
        "--limit", str(args.limit),
        "--output", args.output
    ]
    
    if not run_command(cmd, "Recommendation Generation"):
        print("\n✗ Pipeline failed: Recommendation generation failed")
        sys.exit(1)
    
    # Pipeline complete
    elapsed = time.time() - pipeline_start
    print(f"\n{'='*80}")
    print("PIPELINE COMPLETE")
    print(f"{'='*80}")
    print(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"\nOutputs:")
    print(f"  Staging JSON: s3://{args.staging_bucket}/{args.staging_prefix}")
    print(f"  Recommendations: {args.output}")
    print(f"  CSV Summary: {args.output.replace('.json', '.csv')}")
    print(f"\n✅ Pipeline completed successfully!\n")


if __name__ == "__main__":
    main()
