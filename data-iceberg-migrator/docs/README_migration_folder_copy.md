# Folder Data Copy DAG (DAG 3)

DAG ID: `folder_only_data_copy`

Copies folders from the source cluster (MapR-FS or HDFS) to S3 with DistCp. It moves raw data only and creates no Hive metadata, so use it for data directories that have no Hive tables. For Hive tables, use the table migration DAG instead.

The DAG has no schedule. Trigger it manually and give it an Excel file that lists the folders to copy.

## 1. How a run works

1. **Pre-flight checks.** SSH connectivity to the edge node, cluster authentication (MapR ticket or Kerberos), and availability of `hadoop distcp` and `hadoop fs`. The run stops here if any check fails.
2. **Tracking setup.** Creates the tracking tables if they do not exist and opens a run record.
3. **Read the Excel file.** One copy job per valid row.
4. **Optional subfolder splitting.** Turns one folder into several smaller copy jobs (see section 4).
5. **Copy.** Each job runs DistCp from the edge node to S3. Up to 3 jobs run at the same time.
6. **Record results.** Writes one tracking row per copied folder.
7. **Validate.** Recounts files and bytes at the S3 destination and compares them with the source.
8. **Finalize, report, email.** Closes the run record, writes an HTML report to S3, and emails it if recipients are configured.

A failed folder does not stop the others. Every folder is copied, validated, and reported independently. A failed task is retried twice, five minutes apart.

## 2. Input: the Excel file

Set the file location with the `excel_file_path` parameter. Column names are not case-sensitive, and spaces are treated as underscores.

| Column | Required | Meaning |
|---|---|---|
| `source_path` | Yes | Full folder path on the source cluster |
| `target_bucket` | Yes | Destination S3 bucket. `s3://` is converted to `s3a://` |
| `dest_folder` | No | Folder inside the bucket. Defaults to the last part of `source_path` |
| `endpoint` | No | S3 endpoint URL for a non-default tenant. Credentials come from the matching `<hostname>_access_key` and `<hostname>_secret_key` Airflow Variables |
| `split_subfolders` | No | `true` or `false` for this row only. Blank follows the global setting (section 4) |

Rows without a `source_path` or `target_bucket` are skipped with a warning. The run fails if no valid row remains.

## 3. Run parameters

Set these when triggering the DAG.

| Parameter | Default | Meaning |
|---|---|---|
| `excel_file_path` | `s3a://config-bucket/folder_copy.xlsx` | Location of the Excel file |
| `tenant` | blank | Tenant or environment key from the `migration_tenant_profiles` Variable. Blank uses the global `cluster_*` Variables |
| `split_subfolders` | blank | Copy each subfolder as its own job. Blank follows the Airflow Variable or the Excel column, which default to off |
| `split_depth` | blank (1) | How many directory levels to split |
| `listing_strategy` | blank (`ls_r`) | How files and sizes are counted: `ls_r` or `count` |

## 4. Tunable settings

Each setting below is read in this order, and the first value found is used:

1. DAG parameter (where one exists)
2. Airflow Variable `folder_copy_<name>`
3. Environment variable `FOLDER_COPY_<NAME>`
4. Default

| Setting | Default | What it does |
|---|---|---|
| `listing_strategy` | `ls_r` | `ls_r` runs a recursive `hadoop fs -ls -R` plus `-du -s`. `count` runs a single `hadoop fs -count`, which is usually faster on very large folder trees. The same method is used for the source, the S3 counts before and after the copy, and validation. An unknown value falls back to `ls_r` with a warning |
| `split_subfolders` | off | Copies each immediate subfolder as a separate DistCp job. Use it for very large, deeply nested folders, so that no single listing has to walk the whole tree |
| `split_depth` | 1 | Levels of subfolders to split. A value below 1 is replaced by 1 |
| `max_split_entries` | 1000 | If splitting would create more jobs than this for one folder, the folder is copied whole |
| `max_loose_files` | 200 | If a split folder has more than this many files directly inside it, the folder is copied whole |
| `allow_delete` | off | Together with `distcp_preserve_delete` in the migration config, lets DistCp remove files from the destination that no longer exist in the source. See "Deleting" below |

Other settings, such as the S3 credentials, the tracking database, the report location, and the DistCp memory and strategy options, are shared with the other migration DAGs. See `docs/MIGRATION_AIRFLOW_VARIABLES.md`.

### Subfolder splitting

When splitting is on, each subfolder is copied to `<dest_folder>/<subfolder name>`, so the destination layout is the same as for a whole-folder copy. Files that sit directly in the split folder are copied together as one extra job. Each job has its own tracking row, validation result, and report line. The `parent_source_path` column links the rows back to the Excel row.

A folder is copied whole, with a log message, when any of these is true:

- The folder listing fails or cannot be read completely.
- An entry is a symlink or another non-regular entry.
- A name cannot be decoded reliably, for example one with a `?` or the Unicode replacement character.
- Deleting is enabled (see below).
- The number of jobs would exceed `max_split_entries`, or the loose files exceed `max_loose_files`.
- The total number of jobs in the run would exceed Airflow's `core.max_map_length` (default 1024).

### Deleting

By default, DistCp never deletes anything at the destination. Both of these must be on for DistCp to remove destination files that are missing from the source:

- `distcp_preserve_delete` in the migration config
- `folder_copy_allow_delete`

Folders copied with deleting enabled are never split, because a deletion can only be applied inside a single job. When a folder's loose files are copied as their own job, deleting is never applied to that job.

## 5. Copy and validation behavior

- **Incremental by default.** DistCp runs with `-update`. Running the DAG again on the same folders copies only new or changed files, so it is safe to re-run.
- **Sizing.** The number of mappers and the bandwidth per mapper are set from the size of each source folder. If the size cannot be read, the configured defaults are used.
- **Validation.** After the copy, the S3 destination is counted again. A folder is `VALIDATED` only when the destination exists, the file count matches the source exactly, and the size is within 1% of the source.
- **Missing or empty sources.** A missing source folder is reported as `SOURCE_NOT_FOUND` and an empty one as `EMPTY_SOURCE`. Neither is a failure. An empty source still gets an empty prefix in S3.

### Folder statuses

| Status | Meaning |
|---|---|
| `VALIDATED` | Copied, and the destination matches the source |
| `FAILED` | The copy failed |
| `VALIDATION_FAILED` | The copy finished, but the file count or size does not match |
| `VALIDATION_SKIPPED` | Validation was not run because the copy failed |
| `EMPTY_SOURCE` | The source folder has no files |
| `SOURCE_NOT_FOUND` | The source folder does not exist |
| `SKIPPED` | The folder was not processed |

### Run status

| Status | Meaning |
|---|---|
| `COMPLETED` | No folder failed |
| `COMPLETED_WITH_ERRORS` | At least one folder failed |
| `FAILED` | No folder results could be found for the run |

## 6. Outputs

### Tracking tables

Both tables are in the configured tracking database.

- `data_copy_runs`: one row per run, with start and end time, status, and folder totals.
- `data_copy_status`: one row per copied folder, with source and destination file counts and sizes, bytes copied, the YARN application ID, DistCp duration and throughput, the validation result, and any error message.

`data_copy_status` also holds troubleshooting information for each folder:

| Column | Meaning |
|---|---|
| `parent_source_path` | The Excel row a split folder belongs to |
| `failure_phase` | The step the copy stopped in (see section 7) |
| `yarn_app_created` | Whether DistCp was submitted to YARN |
| `script_exit_code` | Exit code of the copy script on the edge node |
| `s3_list_before_seconds`, `source_scan_seconds`, `s3_list_after_seconds`, `validation_listing_seconds` | Time spent in each counting step |
| `diagnostics_json` | Other details, such as edge node name, memory kill detection, and listing method |

Tracking rows are written after all copies finish, not one at a time, which keeps the number of table commits low. The tables are created and extended automatically, so no manual setup is needed.

### HTML report and email

The report is written to the configured report location as `<run id>_data_copy_report.html`. It contains:

- Run totals: folders, validated, failed, files, data size, and average throughput.
- Per-folder results with source and destination counts and sizes, validation result, and DistCp details.
- A **Phase Timings & Diagnostics** table with one row per folder: time in each step, listing method, whether YARN was reached, the failure phase, exit code, memory kill flag, and edge node and SSSD state.

If the `migration_email_recipients` Variable lists addresses (comma-separated), the report is emailed as an attachment. Without recipients, the email step is skipped.

## 7. Troubleshooting

Use `failure_phase` in the tracking table or the report to see where a folder failed.

| `failure_phase` | Meaning | What to check |
|---|---|---|
| `source_check`, `s3_list_before`, `source_scan`, `s3_list_after` | The edge script stopped while counting files. DistCp was not submitted if `yarn_app_created` is `false` | Folder size and listing time. Try `listing_strategy=count`, or turn on subfolder splitting |
| `distcp` | DistCp itself failed | The DistCp output in the task log and the YARN application |
| `script` | The edge script failed outside DistCp | The task log for the script output |
| `ssh_or_startup` | The SSH connection failed or the script never started | SSH connection and cluster login |
| `after_script` | The connection dropped after some steps had completed | The task log for the last step reached |
| `validation` | The copy succeeded but the destination did not match | The file count and size columns for that folder |

Other signs in the report and task log:

- **Memory kill suspected.** The output shows an out-of-memory or container kill. This is a resource problem, not a listing problem. Check the DistCp memory settings.
- **SSSD not running.** The edge node's group lookups can be wrong, which causes permission failures. Compare with a known good edge node before trusting timings from this one.
- **Long-running listings.** The task log prints the current step every 5 minutes while the script runs. If a run times out, the message names the step it was in.

## 8. Related docs

- `docs/MIGRATION_AIRFLOW_VARIABLES.md`: all Airflow Variables and environment variables, including the `folder_copy_*` settings.
- `docs/README.md`: overview of the migration DAGs.
