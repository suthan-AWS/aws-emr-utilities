# HBase Bootstrap Action to Prevent Service Restarts During Reconfiguration

## Overview

When you reconfigure an EMR cluster with HBase (e.g., changing `hbase-site.xml` properties via `modify-instance-groups`), EMR's Puppet automation restarts all HBase services (RegionServer, Master, Thrift, REST). This causes downtime and disrupts in-flight operations.

This bootstrap action prevents HBase services from restarting during reconfiguration while still applying the configuration changes to disk. The config files are updated, but services continue running with the previous configuration until manually restarted.

## How It Works

EMR uses Puppet to manage HBase services. The default `init.pp` manifest uses `subscribe` directives that trigger service restarts whenever config files (`hbase-site.xml`, `hbase-env.sh`, `log4j2.properties`, `hadoop-metrics2-hbase.properties`) change.

The modified `init.pp` replaces `subscribe` with `require` for all HBase services:
- `subscribe` = "restart this service when the config file changes"
- `require` = "ensure config files exist before starting, but don't restart on changes"

This affects all four HBase services: RegionServer, Master, Thrift Server, and REST Server.

## Artifacts

| File | Description |
|------|-------------|
| `replace_hbase_init_pp.sh` | Bootstrap action script — backs up original `init.pp` and replaces it with the modified version from S3 |
| `init.pp` | Modified Puppet manifest for HBase (tested on EMR 7.12) |

## Compatibility

| EMR Version | `init.pp` Version | Notes |
|-------------|-------------------|-------|
| 7.3 | Uses `log4j.properties` | Original version tested by customer |
| 7.12 | Uses `log4j2.properties` | Tested — `init.pp` in this repo is for 7.12 |

> **Important:** The `init.pp` file differs between EMR versions (e.g., `log4j.properties` vs `log4j2.properties`). Always verify the original `init.pp` on your target EMR version at `/var/aws/emr/bigtop-deploy/puppet/modules/hadoop_hbase/manifests/init.pp` before deploying.

## Setup

1. Upload the modified `init.pp` to your S3 bucket:
   ```bash
   aws s3 cp init.pp s3://YOUR-BUCKET/stop_hbase_restart/init.pp
   ```

2. Update the S3 path in `replace_hbase_init_pp.sh` to point to your bucket:
   ```bash
   sudo aws s3 cp s3://YOUR-BUCKET/stop_hbase_restart/init.pp /var/aws/emr/bigtop-deploy/puppet/modules/hadoop_hbase/manifests/init.pp
   ```

3. Upload the bootstrap script:
   ```bash
   aws s3 cp replace_hbase_init_pp.sh s3://YOUR-BUCKET/scripts/replace_hbase_init_pp.sh
   ```

4. Add the bootstrap action when creating your EMR cluster:
   ```bash
   aws emr create-cluster \
     --bootstrap-actions '[{"Name":"Stop HBase restart on Reconfiguration","Path":"s3://YOUR-BUCKET/scripts/replace_hbase_init_pp.sh"}]' \
     ...
   ```

## Test Results — EMR 7.12

Tested on 2026-03-12 with two EMR 7.12.0 clusters (m5.xlarge, 1 master + 1 core, HBase on S3).

Reconfiguration applied: added `hbase.rpc.timeout=120000` to `hbase-site.xml` via `modify-instance-groups`.

### Scenario 1 — Without Bootstrap Action

Cluster: `j-1LNTCECQS64Q0`

**Before Reconfiguration (Core Node):**
- RegionServer PID: `12489`
- RegionServer start time: `07:47`
- hbase-site.xml: `3687 bytes, Mar 12 07:47`

**After Reconfiguration (Core Node):**
- RegionServer PID: `15499` **(changed)**
- RegionServer start time: `07:52` **(changed)**
- hbase-site.xml: `3776 bytes, Mar 12 07:52` (updated)
- `hbase.rpc.timeout` property: ✅ present

**Observation:** HBase RegionServer was restarted (PID changed from 12489 → 15499). Config was applied.

### Scenario 2 — With Bootstrap Action

Cluster: `j-11WUXAJYKM9YA`

**Before Reconfiguration (Core Node):**
- RegionServer PID: `9457`
- RegionServer start time: `07:41`
- hbase-site.xml: `3690 bytes, Mar 12 07:48`

**After Reconfiguration (Core Node):**
- RegionServer PID: `9457` **(unchanged)**
- RegionServer start time: `07:41` **(unchanged)**
- hbase-site.xml: `3779 bytes, Mar 12 07:52` (updated)
- `hbase.rpc.timeout` property: ✅ present

**Observation:** HBase RegionServer was NOT restarted (PID remained 9457). Config was applied to disk.

### Verification

The bootstrap action was confirmed active on Cluster B:
- Backup file exists: `/var/aws/emr/bigtop-deploy/puppet/modules/hadoop_hbase/manifests/init.pp.bak`
- `subscribe` count in modified `init.pp`: **0** (all removed)

### Conclusion

The bootstrap action successfully prevents HBase service restarts during reconfiguration on EMR 7.12. Configuration changes are written to disk but services continue running uninterrupted.
