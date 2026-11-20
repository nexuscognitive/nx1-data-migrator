# DistCp Performance Tuning: Configuration & Recommendations

## 1. Purpose

Every table migrated by **DAG 1** (`source_to_s3_migration`) and every folder copied by
**DAG 3** (`folder_only_data_copy`) is copied with Hadoop `distcp`. Two parameters control how
each copy runs:

- **Mappers** (`-m`): how many parallel copy tasks run for the job.
- **Bandwidth** (`-bandwidth`): the MB/s throughput cap applied to *each* mapper.

Every DistCp call is its own YARN application, and several run at once: DAG 1 runs up to
`migration_distcp_copy_max_concurrent` batches per run, the folder copy runs 3 folders per run,
and several runs can overlap. So what the cluster sees is roughly
**mappers × bandwidth × concurrent jobs**. This document explains how to set that total
directly with a **queue budget**, and how the values are chosen without one. DAG 1's batching
([section 6](#6-batching-and-time-budgets-dag-1)) decides how long each copy may run.

Every value below is resolved in the order **Airflow Variable → env var (`env.shared` or the
copy's `env.<dag>_<suffix>`) → built-in default**. Runs started from the NX1 portal read the
`nx1_`-prefixed Variables the portal writes instead.

---

## 2. The queue budget

A budget is three numbers for the YARN queue migrations run in (`migration_distcp_queue_name`,
optional; empty means the user's default queue):

| Variable | Meaning |
| --- | --- |
| `migration_distcp_total_mappers` | Map containers all running DistCp jobs may use together |
| `migration_distcp_total_bandwidth_mbps` | MB/s all running DistCp mappers may use together |
| `migration_distcp_concurrent_jobs` | DistCp jobs running at once, across all runs of this copy of the DAGs |

Every job gets an equal share:

```
mappers per job      = total_mappers // concurrent_jobs
bandwidth per mapper = total_bandwidth_mbps // (concurrent_jobs × mappers per job)
```

Example: 40 mappers, 2000 MB/s, 4 concurrent jobs gives every job `-m 10 -bandwidth 50`, so the
queue never sees more than 40 mappers and 2000 MB/s from migrations.

**Set all three or none.** A run fails at its first task if only some are set, if
`total_mappers < concurrent_jobs` (a job would get no mapper), or if the bandwidth can't give
every mapper 1 MB/s.

**What the budget doesn't count:** YARN also starts one ApplicationMaster container per job, so
the queue needs `concurrent_jobs` more containers than `total_mappers`. And YARN never enforces
bandwidth: the total holds only because every job is started with its share.

A small table simply uses fewer mappers than its share: DistCp never launches more maps than its
copy listing has entries (in Hadoop 2.7, `DynamicInputFormat` uses `min(-m, chunks)`, and
`UniformSizeInputFormat` only splits between files).

---

## 3. Without a budget: fixed values

With no budget set, every job gets fixed values, as before:

| Set | Every job gets |
| --- | --- |
| `migration_distcp_mappers` **and** `migration_distcp_bandwidth` | that pair |
| neither | `migration_distcp_default_mappers` × `migration_distcp_default_bandwidth` (50 × 100 MB/s unless you change them) |
| only one of the pair | the run fails at its first task, naming the missing one |

These are per job, so the cluster-wide total is still that × concurrent jobs × overlapping runs.

### Checking which mode a run used

Every DistCp task logs one line:

```
[DistCp] Sizing mode: BUDGET — every job gets -m 10 -bandwidth 50: 40 mappers and 2000 MB/s shared by 4 concurrent DistCp jobs. | pool=migration_distcp_aleks
[DistCp] Sizing mode: FIXED — defaults: every job gets -m 50 -bandwidth 100 (...). No budget is set. | pool=default_pool
```

(`[FolderCopy] Sizing mode: …` in DAG 3.)

---

## 4. The pool: keeping the budget across runs

Dividing by `concurrent_jobs` only holds if no more than that many DistCp jobs run at once, across
every run in progress. An **Airflow pool** enforces it: each DistCp task (`run_distcp_ssh`,
`run_folder_distcp_ssh`) takes one slot, and a task that finds no free slot waits in the
scheduled state until one frees up. Waiting doesn't count against the 8-hour DistCp timeout.

- **Pool name:** `migration_distcp` plus the copy's suffix, e.g. `migration_distcp_aleks` for a
  copy deployed with `deploy.py --suffix aleks`. DAG 1 and DAG 3 of the same copy share it.
- **Create the pool before setting the budget.** A hand-deployed copy only uses its pool once
  `migration_distcp_concurrent_jobs` is set for it; until then it stays in `default_pool`. Airflow
  3 never schedules a task whose pool doesn't exist (the scheduler logs
  `Tasks using non-existent pool ... will not be scheduled` and the task waits forever), so:
  ```
  airflow pools set migration_distcp_<suffix> <concurrent_jobs> "DistCp budget for <suffix>"
  ```
  `deploy.py` prints this for the copy it deploys. Keep the pool's size equal to
  `migration_distcp_concurrent_jobs`.
- **The pool is chosen when Airflow parses the DAG.** A budget set after the last parse gets its
  pool on the next parse (minutes); a run started in between is divided but not capped.
- **The NX1 portal's copy** (DAG ids ending `_nx1`, pool `migration_distcp_nx1`) always uses its
  pool; aiapi creates and resizes it. Triggering an `_nx1` DAG by hand from the Airflow UI uses
  that pool with the *plain* budget Variables.
- The per-run caps still apply on top: `migration_distcp_copy_max_concurrent` (DAG 1) and 3 (DAG 3).
  The pool hands out slots by task priority, not per run, so a large run started first can make a
  later one wait.

---

## 5. Retired: auto-sizing

`migration_distcp_enable_auto_sizing`, `migration_distcp_target_bytes_per_mapper`,
`migration_distcp_min_mappers`, `migration_distcp_max_mappers`,
`migration_distcp_target_aggregate_mbps` and `migration_distcp_max_mapper_bandwidth` are no
longer read; a warning is logged if one is set. Auto-sizing targeted a total *per job*, so the
cluster still saw that times the number of concurrent jobs, and its scaling down by file count is
something DistCp already does. The queue budget replaces it.

---

## 6. Batching and time budgets (DAG 1)

DAG 1 doesn't copy one database per task any more. The `flatten_and_batch` task estimates every
table's copy time and packs the tables into batches; each batch is one `run_distcp_ssh` task,
so a stuck copy only affects its own batch. The full walkthrough is in
[How the S3 copy is split into batches](README.md#how-the-s3-copy-is-split-into-batches); the
parts that affect tuning are:

1. **Each table gets a cost estimate, in seconds:**
   `30 + size ÷ rate + 45 per DistCp job + 24 per partition directory + 0.002 per file`, where
   the rate is 150 MB/s, or what a job can copy under the budget when that is lower:
   the per-mapper bandwidth × its mappers, counting at most one mapper per file (DistCp never
   runs more maps than files, so a one-file table copies at one mapper's speed). Under a small
   share the estimate, and so every call timeout below, grows with it, so a slow but healthy
   copy isn't killed.
   A partition-filtered table with `migration_distcp_preserve_delete=true` runs one DistCp job
   per partition, so partition count can dominate size. The weights are fixed in code and not
   configurable: the plan depends on the *ratio* between size and partition count, which barely
   changes between clusters.
2. **One cost limit applies to every batch:** the smaller of **5.76 hours** (the largest batch
   whose budget can still stop new tables before the 8-hour task timeout) and **total estimated
   work ÷ (2 × `migration_distcp_copy_max_concurrent`)**, which spreads small runs across the
   available copy slots. This limit is derived, not configurable; the old
   `migration_distcp_max_batches` setting was retired because, at around 500 tables, it produced
   batches so large they were killed before writing their status rows.
3. **Batches are filled most-expensive-first**, up to `migration_distcp_max_tables_per_batch`
   (default **25**) tables each. A table whose own estimate exceeds the limit gets a batch to
   itself. A batch never mixes source databases.
4. **Every batch gets the same time budget: 7.2 hours** (nine tenths of the 8-hour task
   timeout). When the budget runs out, tables that haven't started are marked
   `batch budget exhausted before this table started` and picked up by the automatic retry.
   Earlier versions scaled each batch's budget from its own cost estimate; because the estimate
   ignores YARN queueing and SSH overhead, those budgets ran out mid-batch and skipped healthy
   tables. The flat budget removed that failure.
5. **Each DistCp call gets its own timeout:** 3 × the table's cost estimate, at least 30 minutes
   and at most `migration_distcp_call_timeout_max_seconds` (default **21600**, 6 hours). At a
   40 MB/s share a single table above roughly 860 GB reaches that ceiling, so raise it if your
   budget gives each job little bandwidth and you have very large tables. A
   table that needs longer simply takes more than one attempt: every copy runs `-update`, so
   each retry resumes where the last one stopped.

**Where to look:** the `flatten_and_batch` log prints the plan (`[Batching] DERIVED-CAP …`,
then one line per batch with its tables). Each copy logs
`[DistCp] cost estimate Xs vs actual Ys`; it is informational only, and there is nothing to
tune in response to it.

---

## 7. Choosing settings

1. **Get a queue for migrations** from the cluster's admins if you can, sized for the copy load.
   DistCp is submitted to it with `migration_distcp_queue_name`. NX1 never creates or changes a
   queue.
2. **Set a budget that fits it:** total mappers within the queue's containers (minus one
   ApplicationMaster per concurrent job), total bandwidth within what the link and source cluster
   take, and enough concurrent jobs to keep it busy. More concurrent jobs means smaller jobs; a
   few large tables copy faster with fewer, wider jobs.
3. **Create the pool** first (hand-deployed copies; the portal does its own).
4. **Check the first run's log:** `Sizing mode: BUDGET — …` with the pool you expect.

Without a budget, fixed values still work, but remember they are per job.

---

## 8. Configuration reference

Defaults apply when neither the Airflow Variable nor the env var is set.

### Sizing

| Airflow Variable | Env var | Default | Used when |
| --- | --- | --- | --- |
| `migration_distcp_total_mappers` | `MIGRATION_DISTCP_TOTAL_MAPPERS` | *(unset)* | Budget: map containers shared by all running jobs |
| `migration_distcp_total_bandwidth_mbps` | `MIGRATION_DISTCP_TOTAL_BANDWIDTH_MBPS` | *(unset)* | Budget: MB/s shared by all running mappers |
| `migration_distcp_concurrent_jobs` | `MIGRATION_DISTCP_CONCURRENT_JOBS` | *(unset)* | Budget: jobs at once across runs; also the pool size |
| `migration_distcp_mappers` | `MIGRATION_DISTCP_MAPPERS` | *(empty)* | No budget: set with `migration_distcp_bandwidth` to fix every job |
| `migration_distcp_bandwidth` | `MIGRATION_DISTCP_BANDWIDTH` | *(empty)* | No budget: set with `migration_distcp_mappers` to fix every job |
| `migration_distcp_default_mappers` | `MIGRATION_DISTCP_DEFAULT_MAPPERS` | `50` | No budget and no pair |
| `migration_distcp_default_bandwidth` | `MIGRATION_DISTCP_DEFAULT_BANDWIDTH` | `100` | No budget and no pair |

### Batching and concurrency (DAG 1)

| Airflow Variable | Env var | Default | Used when |
| --- | --- | --- | --- |
| `migration_distcp_copy_max_concurrent` | `MIGRATION_DISTCP_COPY_MAX_CONCURRENT` | `3` | Copies running at once per DAG run (the pool caps across runs); also sets the batch-count floor |
| `migration_distcp_max_tables_per_batch` | `MIGRATION_DISTCP_MAX_TABLES_PER_BATCH` | `25` | Most tables packed into one batch |
| `migration_distcp_call_timeout_max_seconds` | `MIGRATION_DISTCP_CALL_TIMEOUT_MAX_SECONDS` | `21600` | Ceiling on one DistCp call's timeout (6 hours) |

Lower-level DistCp settings (strategy, map memory, JVM options, `-delete` behaviour, checkpoint
location) are listed in [MIGRATION_AIRFLOW_VARIABLES.md](MIGRATION_AIRFLOW_VARIABLES.md#6-distcp-copy-behavior).

`migration_distcp_queue_name` (YARN queue, default unset) and `migration_distcp_skip_crc` (`-skipcrccheck`, default `true`) are also tuned there.

---

## 9. Summary

- **Set a queue budget** (total mappers, total bandwidth, concurrent jobs): every DistCp job gets
  an equal share, and a pool keeps no more than `concurrent_jobs` running across all runs.
- **Create the pool before setting the budget** on a hand-deployed copy: a missing pool makes
  DistCp tasks wait forever.
- **Without a budget,** every job gets fixed values: the forced pair, else 50 × 100 MB/s.
- Auto-sizing is retired.
- In DAG 1, tables are copied in cost-estimated batches, each with a flat 7.2-hour budget; a
  table that doesn't finish resumes on retry with `-update`.
