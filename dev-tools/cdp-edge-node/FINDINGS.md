# CDP Kerberos edge node — findings log

Every problem found while testing the DAGs on the Kerberized CDP edge node: symptom,
cause, fix, and the test that now covers it. Handover material for the client.
Ticket: [WF-520](https://linear.app/nexuscognitive/issue/WF-520).
Spec: `docs/superpowers/specs/2026-10-14-cdp-kerberos-edge-node-design.md` (section 5.3).

## DAG findings

Found by reading the code before the first end-to-end round. Each was pinned by a failing
unit test, then fixed; the end-to-end rounds on es-tenant-2 record whether it also showed
up on the node.

| ID | Found by | Symptom | Cause | Fix | Covered by |
|---|---|---|---|---|---|
| P1 | Code read | `cluster_login` runs under MapR's `source ~/.profile` on every cluster | `_login_shell(full_script)` called without `cluster_type` (`shared.py:748`) | Pass `cluster_type` to `_login_shell` | `TestClusterLoginShellWrapper` |
| P2 | Code read | Empty-source S3 prefix creation can fail silently and still log "Created S3 prefix" | Bare `exec_command`, `2>/dev/null \|\| true`, exit code ignored (`migration_dag_mapr_to_s3.py:1784`) | Login-shell wrapper, exit code checked, failure logged with stderr | `TestRunDistcpSsh::test_empty_source_prefix_runs_in_login_shell_and_reports_failure` |
| P3 | Code read | DAG 3 without a ticket fails late with a GSS error | `validate_prerequisites_folder_copy` has no cluster-auth check | Shared `_cluster_auth_check_cmd`, used by both pre-checks | `TestValidatePrerequisitesFolderCopy::test_missing_kerberos_ticket_fails_the_precheck`, `TestClusterAuthCheckCmd` |
| P4 | Code read | Kerberos runs log MapR wording and run `maprlogin` | MapR-only fallback and `MAPR_EFFECTIVE_USER=` key used for every auth method | `maprlogin` lookup only on MapR; `EFFECTIVE_USER` key (old key still parsed) | `TestClusterLoginSetup::test_effective_user_key_is_parsed`, `TestClusterLoginUserFallback` |
| P6 | End-to-end round 1 (es-tenant-2) | A run pointed at the CDP edge node with `dag_run.conf` (`ssh_conn_id`, `auth_method=kinit`) still used MapR's shell wrapper and no S3A committer flags | `cluster_type` was the one cluster setting with no per-run `conf_key` and no tenant-profile entry, so a tenant-wide `cluster_type=MapR` Variable always won | `cluster_type` takes `conf_key='cluster_type'` and is an accepted tenant-profile key | `TestClusterTypePerRun` |
| P5 | Code read | `CLUSTER_TYPE=CDP` didn't get the S3A committer flags HDP gets, and Kerberos reports showed a "Mapr Ticket File" line | Committer flags keyed on `HDP` only; ticket line unconditional | `CDP` treated like `HDP`; ticket line only for `auth_method=mapr` | `TestS3aCommitterOpts`, `TestReportTicketFileLine` |

## End-to-end round 1 (es-tenant-2, 2026-10-15)

Node: `cdp-edge.cdp-edge.es-tenant-2.svc.cluster.local` (image `cdp-edge-node:cdp-edge-2026-10-15`).
Personal `_cdp` DAGs, each run pointed at the node with `dag_run.conf`
`{"ssh_conn_id": "cdp_edge_ssh", "auth_method": "kinit", "cluster_type": "CDP", "service_account_user_id": "svc_migration"}`.

| Check | Run | Result |
|---|---|---|
| DAG 1 migration, 3 tables (partitioned, plain, empty) | `source_to_s3_migration_cdp` / `wf520_dag1_kerberos_3` | Every task succeeded except the report email (tenant SMTP, see E3). Pre-check logged `Default principal: svc_migration@CDP.LOCAL`; discovery through the SASL-secured metastore; DistCp from the Kerberized HDFS; report "CDP to S3 Migration Report": 3/3 successful, 3/3 passed validation, 0 row-count / partition / schema mismatches, size match 2/2. P1, P2, P4 and P5 behaved as fixed (login-shell wrapper, `Created S3 prefix` for the empty table, `EFFECTIVE_USER=svc_migration`, no "Mapr Ticket File" line). |
| DAG 3 folder copy, `/user/testdata/distcp_sizing_folder` (5 files, 5 MiB) | `folder_only_data_copy_cdp` / `wf520_dag3_kerberos_1` | Pre-check, cluster login and DistCp on the Kerberized node; COMPLETED, 5 files / 5,242,880 bytes; validation `VALIDATED` (destination exists, file count and size match). |
| No ticket (keytab moved aside, `kdestroy`) | `wf520_dag1_noticket_2`, `wf520_dag3_noticket_2` | Both pre-checks failed with `Cluster auth: No valid Kerberos TGT found` (DAG 3: P3 confirmed), and the runs stopped there. |

## End-to-end round 2: full regression suite (es-tenant-2, 2026-10-15)

`data-iceberg-migrator/notebooks/regression_test_suite.ipynb`, run tag `20261015T151923`, against the
CDP node through the `_cdp` DAGs (`REGRESSION_EXTRA_RUN_CONF` routes every run to the node,
`REGRESSION_DEST_DB_PREFIX=cdp_`): DAG 1 baseline `regression_20261015T151923_dag1_baseline`
(30 discovery groups) and DAG 2 baseline `regression_20261015T151923_dag2_baseline`.

**REGRESSION PASSED — 326 PASS, 0 FAIL, 0 ERROR**, 2 WARN, 3 MANUAL, 12 SKIPPED.

- WARN (2): `send_migration_report_email` and `send_iceberg_report_email` — tenant SMTP (E3).
- MANUAL (3): by design.
- SKIPPED (12): 9 fault cases off by config (fault run not part of this round), 2 DistCp sizing
  lines only logged at DEBUG, 1 destination already owned by another test (TC-25).

No new DAG findings. The first attempt of this round hit E4 (edge OOM), not a Kerberos problem.

Environment findings from these rounds (not DAG bugs, but they mislead a test run):

| ID | Symptom | Cause | Handling |
|---|---|---|---|
| E1 | The first runs silently used the MapR edge node (`ssh_conn_id=cluster_edge_ssh`, `auth_method=mapr`), and a "no ticket" run passed | Tenant-wide Airflow Variables (`cluster_ssh_conn_id`, `auth_method`, `service_account_user_id`, `cluster_type`, `migration_report_location`) outrank a per-DAG env file in `get_config()` | Point runs at the node with `dag_run.conf` (outranks Variables); P6 made that possible for `cluster_type`. Always confirm the pre-check log's `ssh_conn_id=` / `auth_method=` line. |
| E2 | `init_tracking_tables`: `user [amaluga] does not have [create] privilege on [iceberg/migration_tracking]` | Kyuubi Ranger authz needs `create` even for `CREATE DATABASE IF NOT EXISTS`; the run owner's grants are on a personal tracking database | Env file sets `MIGRATION_TRACKING_DATABASE` / `_LOCATION` to the owner's database |
| E3 | `send_migration_report_email` fails: `[Errno 111] Connection refused` | Tenant SMTP not reachable | Known tenant issue; the report itself is written to S3 |
| E4 | Regression suite: every `discover_tables_via_spark_ssh` task lost its SSH session (exit -1); the edge container was OOM-killed (8 Gi limit) | DAG 1 on `main` starts one `local[*]` PySpark discovery per source database at once; the per-run cap `MIGRATION_DISCOVERY_MAX_CONCURRENT` (PR #116) was merged to the WF-419 branch, not `main` | Test node raised to a 24 Gi limit. **For handover:** make sure #116 is in the delivered DAG, or a client edge node can be OOM-killed by a large Excel config. Not Kerberos-specific. |

## Trial build (local docker-compose, before deploying)

| ID | Symptom | Cause | Outcome |
|---|---|---|---|
| T1 | Seeding: `setup-test-data.sh` and `setup-distcp-test-data.sh` (shared with the MapR node) | — | Both pass on the Kerberized node as `svc_migration`, and pass again when re-run. Same results as the MapR node (26 OK, 1 deliberate WARN for the unregistered-partitions table; 7/7 DistCp sizing fixtures). |
| T2 | DistCp from Kerberized HDFS to S3A, with and without `-Dmapreduce.outputcommitter.factory.scheme.s3a=...S3ACommitterFactory -Dfs.s3a.committer.name=magic` | — | Both succeed with identical results (local S3 gateway). DistCp uses its own committer, so the flags the DAG adds for HDP/CDP have no effect on it. Kept for consistency; removing them for HDP and CDP is a separate cleanup. |

## Edge-node findings (fixed in the node, not the DAG)

Problems in the node itself, recorded so the log also shows what was ruled out.

| ID | Symptom | Cause | Fix |
|---|---|---|---|
| N1 | PySpark failed: `TypeError: an integer is required (got type bytes)` | PySpark 2.4.8 doesn't support Python 3.8 (Ubuntu 20.04's `python3`); the seeding scripts force `PYSPARK_PYTHON=python3` | Base image Ubuntu 18.04 (`python3` = 3.6), as on CDP 7.1.x's RHEL 7/8 edge nodes |
| N2 | HiveServer2 never opened port 10000 | Hive 3's `hive.metastore.event.db.notification.api.auth` rejects HiveServer2's user ("User root is not allowed to perform this API call") | Turned off in `hive-site.xml` |
| N3 | `docker exec` / `kubectl exec` sessions ran Spark without `PYSPARK_PYTHON` | The image put `/opt/*/bin` on `PATH`, bypassing the `/usr/bin` wrappers | No `/opt/*/bin` on the image `PATH`, as on CDP |
| N4 | beeline with a ticket: "Can't get Master Kerberos principal for use as renewer" | Kerberized job submission needs `yarn.resourcemanager.principal`; CDP always has it (it runs YARN), this node has no YARN | Set in `core-site.xml` |
| N5 | First beeline query right after a restart failed | HiveServer2 opens its port before it can serve sessions | The node only marks itself ready after a Kerberized beeline query succeeds |
| N6 | `setup-distcp-test-data.sh`: `Permission denied: user=svc_migration ... inode="/user"` | HDFS permissions are on (as on CDP); the MapR node runs with them off | The node creates `/user/testdata` owned by `svc_migration` on every boot |
| N7 | `test-kerberos.sh` failed at random | `cmd \| grep -q` under `pipefail` fails when grep exits early and `cmd` gets SIGPIPE | Output captured before grepping |
