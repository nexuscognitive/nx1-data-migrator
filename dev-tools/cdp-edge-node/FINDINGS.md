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
| P5 | Code read | `CLUSTER_TYPE=CDP` didn't get the S3A committer flags HDP gets, and Kerberos reports showed a "Mapr Ticket File" line | Committer flags keyed on `HDP` only; ticket line unconditional | `CDP` treated like `HDP`; ticket line only for `auth_method=mapr` | `TestS3aCommitterOpts`, `TestReportTicketFileLine` |

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
