# Folder Data Copy DAG (DAG 3): Changes vs `main`

This document lists every change on branch `fix/folder-copy-dag` relative to `main` for DAG 3 (`migration_dag_folder_copy.py`) and its tests. Use it to review the PR and to know what behaviour changed.

## 1. Scope

| Item | Value |
|---|---|
| Branch | `fix/folder-copy-dag` (16 commits ahead of `main`, 0 behind) |
| Files | `data-iceberg-migrator/migration_dag_folder_copy.py`, `data-iceberg-migrator/tests/test_dag3_tasks.py`, `data-iceberg-migrator/tests/conftest.py` (+2 lines) |
| Folder-copy commits | `7609ed5` (deep nested folders, splitting, diagnostics), `d69604c` (edge-case fixes and tests) |
| Uncommitted | Batched tracking writes (section 4.8), and the test changes that go with them |
| Diff size vs `main` | DAG: 962 insertions, 99 deletions. Test file: 342 lines changed. |

Other files on the branch belong to DAG 1 or shared utilities and are not covered here: `migration_dag_mapr_to_s3.py`, `migrator_utils/migrations/shared.py`, `tests/test_dag1_*.py`, `tests/test_utils.py`, `tests/test_dag_integrity.py`, `env.migration.example`, and the README and docs files.

## 2. Summary

Before this branch, DAG 3 copied each Excel row as one DistCp job and recorded the result with a per-folder write. The branch adds:

1. **Diagnostics.** Phase timings, the failure phase, YARN app creation, memory-kill detection, and edge-node identity, stored per folder and shown in the report.
2. **Cheaper listings.** A selectable `listing_strategy` (`ls_r` or `count`) for counting files and sizes.
3. **Subfolder splitting.** Optional per-subfolder DistCp tasks for very large, deeply nested folders, with safety caps and fall-backs to a whole-folder copy.
4. **Safer parsing.** Date-anchored size parsing, owner/group names with spaces, a forced UTF-8 locale, and rejection of names that cannot be trusted.
5. **Delete safety.** `-delete` is never applied to a loose-files copy, and a folder is copied whole when delete is on.
6. **Batched tracking writes.** One batched insert and one batched MERGE instead of one write per folder (uncommitted).

## 3. Commit-level changes

### Commit `7609ed5`: deep nested folders

Adds the diagnostics, listing, and splitting features (sections 4.1 to 4.5 and 4.7).

### Commit `d69604c`: edge cases and tests

- Hardening of the listing and split code (sections 4.4 and 4.5), and the fixes for edge cases found in review.
- `conftest.py` gains a 2-line change to the shared SSH mock fixture.
- Test coverage added for the parsing, split, and setting helpers (section 6).

## 4. Changes by area

### 4.1 Edge-script phase markers and diagnostics

The generated edge script prints `PHASE_START <name> <timestamp>` and `PHASE_END <name> seconds=<n>` around each phase:

- `source_check`
- `s3_list_before`
- `source_scan`
- `distcp`
- `s3_list_after`

It also prints edge identity: `EDGE_HOST`, `EDGE_USER`, `EDGE_GROUPS`, `EDGE_SSSD_RUNNING`, `EDGE_MEM_AVAILABLE_MB`, and `LISTING_STRATEGY`.

Python side (`migration_dag_folder_copy.py`):

- `_PHASE_START_RE`, `_PHASE_END_RE`, `_MEMORY_KILL_RE`: parsers for the markers and for OOM or container-kill text.
- `_last_open_phase(output)`: the last phase that started and never ended. This names the phase a run died in.
- `_build_run_diagnostics(output, exit_code, yarn_ids, listing_strategy, shallow, extra)`: builds the diagnostic dict. Key fields:
  - `failure_phase`: the open phase, `distcp`, `script`, `after_script`, `ssh_or_startup`, or `None`.
  - `yarn_app_created`: `False` with a pre-DistCp `failure_phase` means DistCp was never submitted.
  - `memory_kill_suspected`: OOM text, or exit code 137.
  - `phase_seconds`, `edge_host`, `edge_groups`, `edge_sssd_running`, `edge_mem_available_mb`.
- `run_folder_distcp_ssh` logs the diagnostics, warns when SSSD is not running on the edge node, and warns on a suspected memory kill.
- Every return path of `run_folder_distcp_ssh` includes `**diag`, and the exception path also includes the diagnostics and the partial output (last 3000 characters).

### 4.2 Incremental SSH output reading

`_read_remote_command(channel, partial_sink, deadline_seconds, label, heartbeat_seconds=300)` replaces the blocking `stdout.read()` in `run_folder_distcp_ssh` only. The size probe, the child listing in `expand_folder_subfolders`, and the validation listing still read blocking.

- Output is drained as it arrives and appended to `partial_sink`, so the last phase reached is still known after a timeout or a dropped connection.
- A heartbeat log line every 5 minutes shows the current phase.
- A timeout raises `TimeoutError` with the last open phase in the message.

### 4.3 Listing strategy

A new `listing_strategy` setting selects how file counts and sizes are measured. It applies to the source scan, the S3 before and after counts, and the validation count.

| Value | Source and S3 | Meaning |
|---|---|---|
| `ls_r` (default) | `hadoop fs -ls -R` piped to `grep '^-' \| wc -l`, plus `hadoop fs -du -s` | The original behaviour |
| `count` | One `hadoop fs -count` call | Usually cheaper on large trees |

Implementation: `_listing_snippet(hadoop_cmd, path_expr, file_var, size_var, strategy, shallow)` generates the shell lines. It is used by `run_folder_distcp_ssh` and `validate_data_copy`, so validation uses the same listing as the copy.

Resolution order: DAG Param, then Airflow Variable `folder_copy_listing_strategy`, then environment variable `FOLDER_COPY_LISTING_STRATEGY`, then the default `ls_r`. An unknown value logs a warning and uses `ls_r`.

The pre-copy size probe (`_probe_source_size`, which sizes the DistCp mappers) always uses `hadoop fs -count`, whatever `listing_strategy` is set to.

### 4.4 Subfolder splitting

Each Excel row can be copied as one DistCp task per immediate subfolder instead of one task for the whole folder.

**Settings** (resolved by `_resolve_setting`: DAG Param, then Airflow Variable `folder_copy_<name>`, then environment variable `FOLDER_COPY_<NAME>`, then default):

| Setting | Default | Meaning |
|---|---|---|
| `split_subfolders` | off | Split each folder. Can also be set per Excel row with the optional `split_subfolders` column; a non-blank row value overrides the global setting for that row. |
| `split_depth` | 1 | How many directory levels to split. Values below 1 fall back to 1 with a warning. |
| `max_split_entries` | 1000 | Above this many generated tasks, the folder is copied whole. |
| `max_loose_files` | 200 | Above this many files directly inside a split folder, the folder is copied whole. |

**Code:**

- `_list_immediate_children(ssh, config, path)`: runs `hadoop fs -ls` under a UTF-8 locale and returns the subdirectories and `(file, size)` pairs.
- `_split_folder_config(ssh, config, cfg, depth, max_loose)`: builds one config per subfolder, recursing for `depth > 1`. Files that sit directly in the folder become one extra `shallow` entry, so no data is dropped.
- `expand_folder_subfolders(folder_configs, cluster_setup, **context)`: a new `@task` between parsing and DistCp. It passes rows through unchanged unless splitting is requested.

**Fail-safe rules.** The folder is copied whole when any of these holds:

- Listing fails, or any line of the listing cannot be parsed.
- The listing header count does not match the number of parsed items.
- An entry is a symlink or other non-regular entry.
- An entry name contains `?` or U+FFFD, which means the name was not decoded correctly.
- `-delete` is on (`distcp_preserve_delete` and `folder_copy_allow_delete`). DistCp `-delete` only runs inside a task, so a subfolder removed from the source would not be covered by any task.
- The generated task count would exceed `max_split_entries`.
- The running total across the whole DAG would exceed Airflow's `core.max_map_length` (default 1024). Each row not yet processed keeps one reserved slot.

Splitting never changes the destination layout. Subfolder copies go to `<dest_folder>/<subfolder name>`. Each task keeps its own tracking row. `parent_source_path` links the rows back to the Excel row.

### 4.5 Listing parser and locale

- `_LS_LINE_RE`: matches one `hadoop fs -ls` entry. Owner and group are matched loosely, so AD names with spaces, such as `domain users`, parse correctly. The size and date anchor identifies the fields.
- `_LS_HEADER_RE`: matches the `Found N items` header, used for the count check.
- `_list_immediate_children` runs `LC_ALL=C.UTF-8` and sets `-Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8` in `HADOOP_CLIENT_OPTS`. Without this, a C-locale edge node prints non-ASCII names as `?`, and the split would copy them to a different S3 key.
- Shallow size parsing: the size is the field just before the `YYYY-MM-DD` token. The old fixed-column parse broke on Hadoop 2.7 S3A listings, which have no owner or group columns.

### 4.6 Shallow (loose-files) copy and delete safety

When a split folder has loose files, the `shallow` entry copies only those files:

- Counts come from the split listing, so no source scan runs.
- A single file is used as the DistCp source with the target path set to the file, because DistCp `-update` treats a single file's target as a file path.
- `-delete` is never added to a shallow copy, because the destination also holds the subfolder copies that `-delete` would remove.
- `expand_folder_subfolders` copies a folder whole when delete is on (section 4.4).

### 4.7 Tracking table schema

Nine columns are added to `data_copy_status`, created or added by `init_folder_copy_tracking_tables`:

| Column | Type | Source |
|---|---|---|
| `parent_source_path` | STRING | Parent Excel row for split entries |
| `failure_phase` | STRING | `_build_run_diagnostics` or validation |
| `yarn_app_created` | BOOLEAN | Whether a YARN application ID was seen |
| `script_exit_code` | INT | Edge-script exit code |
| `s3_list_before_seconds` | DOUBLE | `s3_list_before` phase |
| `source_scan_seconds` | DOUBLE | `source_scan` phase |
| `s3_list_after_seconds` | DOUBLE | `s3_list_after` phase |
| `validation_listing_seconds` | DOUBLE | Timed destination listing in validation |
| `diagnostics_json` | STRING | Remaining diagnostics, capped at 4000 characters |

`_diagnostics_json` keeps the JSON valid when it is long. It shortens `edge_groups` first, and drops it if needed.

Existing tables get the new columns through the existing `ALTER TABLE ... ADD COLUMNS` path, so no manual migration is needed.

### 4.8 Batched tracking writes (uncommitted)

This replaces the per-folder writes with batched ones.

| Before | After |
|---|---|
| `record_data_copy_status`: one `INSERT` per folder, mapped | `record_data_copy_statuses`: one task, one multi-row `INSERT` per 100 folders (`TRACKING_WRITE_BATCH_SIZE`) |
| `update_data_copy_validation`: one `UPDATE` per folder, mapped | `update_data_copy_validations`: one task, one `MERGE` per 100 folders |

Details:

- `_status_row_sql` and `_validation_row_sql` build one `VALUES` tuple per folder. The validation tuple casts numeric and boolean columns explicitly, so NULL-only columns get a concrete type.
- `_chunks` splits the rows into batches.
- The `MERGE` matches on `(run_id, source_path, dest_bucket, dest_path)`. A duplicate key in one `MERGE` would fail it, so the task keeps only the last result per folder across the whole run, not per batch.
- Both tasks skip non-dict entries and return the valid results. `validate_data_copy` still maps over the recorded list.
- `execute_with_iceberg_retry` is used for each batch, so commit conflicts are retried.
- Each task logs its total duration once, after the last batch.

**Wiring.** The chain is now:

```
expand_folder_subfolders -> run_folder_distcp_ssh (mapped)
  -> record_data_copy_statuses (batched, all_done)
  -> validate_data_copy (mapped over the recorded list)
  -> update_data_copy_validations (batched, all_done)
  -> finalize_data_copy_run -> report -> email
```

**Trade-off.** Recording now happens after all copies finish, instead of after each folder. Validation can no longer overlap with copying, so the copy-to-validate phase may take longer. The number of Iceberg commits falls sharply. Benchmark both before choosing this as the default (see section 8).

### 4.9 DAG Params

Three Params are added to the DAG's `params`:

| Param | Type | Default | Purpose |
|---|---|---|---|
| `split_subfolders` | `null` or boolean | blank | Split folders. Blank follows the Variable or Excel row. |
| `split_depth` | `null` or integer | blank | Levels to split. Blank means 1. |
| `listing_strategy` | string, enum `''`, `ls_r`, `count` | `''` | Listing method. Blank follows the Variable, then the env var `FOLDER_COPY_LISTING_STRATEGY`, then `ls_r`. |

### 4.10 HTML report

`generate_data_copy_html_report` gains a **Phase Timings & Diagnostics** table with one row per folder:

- Source path and parent folder.
- S3 list before, source scan, DistCp only, S3 list after, and validation listing, in seconds.
- Listing strategy, with `(files only)` for shallow copies.
- YARN app created, failure phase, exit code, and memory-kill flag.
- Edge node and SSSD state.

The existing DistCp duration column covers the whole edge script including listings. The new DistCp-only column covers the DistCp command, which the section note explains.

### 4.11 Other

- Excel parsing: a blank or `nan` value in `split_subfolders` means "follow the global setting". Other values go through `_truthy`, which also accepts `1.0`, because pandas reads boolean-like columns as float.
- `_truthy`, `_sql_num`, `_sql_int`, `_resolve_setting`, `_int_setting`, `_allow_delete`, `_airflow_map_limit`: shared helpers.
- The `zip` in `expand_folder_subfolders` uses `strict=False` explicitly, for ruff B905.
- A log line that had an f-string with no placeholders was corrected, for ruff F541.

## 5. Behaviour changes at a glance

| Area | Before | After | Default effect |
|---|---|---|---|
| Copy per Excel row | One DistCp task | One task, or one per subfolder when splitting is on | Unchanged (splitting is off) |
| Listing | Recursive `ls -R` | `ls_r` (same) or `count` | Unchanged |
| Tracking writes | Per folder | Batched | Changed: fewer commits, recording after all copies |
| Failure reporting | Error message only | Phase, YARN, memory, edge, timings | Added fields |
| Shallow size parsing | Fixed column | Date-anchored | Fixes wrong sizes on some clusters |
| Output reading | Blocking `read()` | Incremental | Partial output kept on timeout |
| Tracking table | No diagnostic columns | 9 diagnostic columns added (section 4.7) | Added columns |

Removed names: `record_data_copy_status` and `update_data_copy_validation`. Nothing else in the repository referenced them except the DAG and its tests, which are updated.

## 6. Tests

`tests/test_dag3_tasks.py` changes:

- **Shared SSH mock wiring** (local to this file): `wire_channel(stdout, stderr)` drains the mock channel with `recv_ready`, `recv`, `recv_stderr_ready`, and `recv_stderr`. A bare `MagicMock` channel reports ready forever and makes the drain loop hang. `mock_ssh_stdout` and an overriding `mock_ssh_hook` fixture use it. Shared `helpers.py` and `conftest.py` are unchanged, apart from the 2-line `conftest.py` change on the branch.
- **Schema expectation:** `EXPECTED_SCHEMA_ADDITIONS` lists the 9 new columns.
- **New test classes and functions:**
  - `TestLsLineParsing`: owner and group with spaces, file names with spaces, symlink classification.
  - `TestListImmediateChildren`: parsing directories and files, header count mismatch, unrecognised line, symlink raising, `.snapshot` skipped but counted.
  - `TestSettingHelpers`: truthy values, integer settings and fall-backs, `folder_copy_allow_delete` precedence, diagnostics JSON length.
  - `TestExpandFolderSubfolders`: not requested, delete-on copies whole, delete-off still splits, map limit, reserved slots, split error falls back, invalid depth.
  - `TestRecordDataCopyStatus` (renamed to batch form): single and multi-folder inserts, failed rows, metric columns, chunking at the batch size, invalid entries skipped, empty input.
  - `TestUpdateDataCopyValidation` (batch form): validated and failed merges, one MERGE for many folders, duplicate keeps last, invalid input.

Last recorded result, from the batch change (not re-run for this doc update): `tests/test_dag3_tasks.py` **156 passed**. The full suite was not rerun after the latest changes.

`ruff check migration_dag_folder_copy.py`: **All checks passed**.

## 7. Known gaps

- **Real cluster behaviour is unverified.** The UTF-8 listing command, the date-anchored size parse on real S3A and HDFS listings, and the Spark SQL `MERGE` and multi-row `INSERT` have not run against a live cluster or Iceberg table.
- **Recording order.** Rows for copied folders are written only after all copies finish. A crash in between loses those rows from the tracking table, while the data is already copied.
- **Test name.** `test_map_limit_reserves_slot_for_unprocessed_rows` tests the whole-copy branch, so its name is misleading.
- **Missing tests.** No unit test yet covers the UTF-8 command prefix, the `?` and U+FFFD rejection path, or the awk size parse.
- **Full suite.** The last full-suite run timed out after 500 seconds, before the latest changes. Its cause is not yet known.

## 8. Next steps

1. Run the full test suite and find the cause of the earlier timeout.
2. Benchmark the old per-folder writes against the batched writes on the same dataset: wall-clock time, Iceberg snapshot count, and YARN application count.
3. Verify the listing and parsing changes on the edge node, using a folder that has non-ASCII names, owner and group names with spaces, and a Hadoop 2.7 S3A destination.
4. Decide whether to keep recording after all copies, or to batch per group.
5. Add the missing unit tests from section 7.

## 9. Related docs

- `docs/MIGRATION_AIRFLOW_VARIABLES.md`: Airflow Variables and environment variables, including the `folder_copy_*` settings.
- `docs/README.md`: DAG 3 overview.
