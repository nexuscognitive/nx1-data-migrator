# DistCp Performance Tuning: Configuration & Recommendations

## 1. Purpose

Every table migrated by **DAG 1** (`source_to_s3_migration`) and every folder copied by
**DAG 3** (`folder_only_data_copy`) is copied with Hadoop `distcp`. Two parameters control how
each copy runs:

- **Mappers** (`-m`): how many parallel copy tasks run for the table.
- **Bandwidth** (`-bandwidth`): the MB/s throughput cap applied to *each* mapper.

This document explains how those two values are chosen, what to expect for tables of
different sizes, and how to choose between the three sizing modes. In DAG 1, copies are also
grouped into cost-estimated batches; [section 6](#6-batching-and-time-budgets-dag-1) covers how
that works, because it decides how long a copy is allowed to run.

Every value below is resolved in the order **Airflow Variable → env var (`env.shared`) →
built-in default**.

---

## 2. Three sizing modes: Disabled, Forced, Auto

| Mode | How to select it | What every table gets |
| --- | --- | --- |
| **Disabled** (default) | `migration_distcp_enable_auto_sizing` unset or `false` | `migration_distcp_default_mappers` × `migration_distcp_default_bandwidth` (**50 × 100 MB/s**), whatever its size. Every other sizing knob, including the forced pair, is ignored. |
| **Forced** | Switch `true`, **and** both `migration_distcp_mappers` and `migration_distcp_bandwidth` set | That fixed mapper/bandwidth pair, whatever its size |
| **Auto** | Switch `true`, **and** both `migration_distcp_mappers` and `migration_distcp_bandwidth` empty | Mappers and bandwidth sized from the table's own size and file count ([section 3](#3-how-auto-sizing-works)) |

**Auto-sizing is opt-in.** The switch defaults to `false`, so an upgrade changes nothing until
someone sets `migration_distcp_enable_auto_sizing=true`. Disabled mode reproduces the DAG's
behaviour from before auto-sizing existed.

**The forced pair must be set together or left empty together.** Setting only one is a
configuration error: the DAG raises rather than guess. The error surfaces in the DistCp task,
after discovery has already run.

> **Trap: an empty Variable masks `env.shared`.** For the forced check, an empty or
> whitespace-only value counts as **unset**. But an empty Airflow Variable still *exists*, so it
> hides any value for the same key in the deployed `env.shared`; deleting the Variable exposes
> that value again. Deployments created from an older `env.shared.example` carry
> `MIGRATION_DISTCP_MAPPERS=50` and `MIGRATION_DISTCP_BANDWIDTH=100`. With those lines present,
> turning the switch on leaves the DAG in **Forced** mode at 50 × 100, not Auto. To get Auto,
> either delete both lines from `env.shared` and redeploy, or set both Airflow Variables to an
> empty string.

**Confirming the mode.** Each copy task logs one `[DistCp] Sizing mode:` line (DAG 3:
`[FolderCopy] Sizing mode:`):

| Log line starts with | Meaning |
| --- | --- |
| `DISABLED — migration_distcp_enable_auto_sizing is not 'true', so every table is pinned to -m 50 -bandwidth 100 …` | Switch off |
| `FORCED — every table pinned to -m … -bandwidth … by migration_distcp_mappers/migration_distcp_bandwidth …` | Switch on, forced pair set |
| `AUTO — sized per table from discovered size: … bytes/mapper, 1-100 mappers, 2000 MB/s aggregate, 500 MB/s per-mapper cap` | Switch on, sizing per table |

Each table then logs what it actually got, for example
`[DistCp] Sized db.table: 50 mappers x 100 MB/s (= 5000 MB/s aggregate) for 1150 bytes / 3 files`.

---

## 3. How auto-sizing works

When the switch is `true` and both forced values are empty, the DAG sizes each table from
**how much data it has to move** and **how many files that data is in**. When a partition
filter is active, both figures are the *filtered* totals, not the whole table's.

1. **Start from size.** Divide the table's size by `migration_distcp_target_bytes_per_mapper`
   (default **2 GiB**), rounding up. A 4.5 GiB table needs 3 mappers at this step, not 2: any
   partial chunk still needs a full mapper.
   *Rationale: this keeps each mapper's workload roughly constant, instead of a fixed mapper
   count that is overkill for a small table and too little for a huge one.*
2. **Clamp to a floor and a ceiling:** `migration_distcp_min_mappers` (default **1**) and
   `migration_distcp_max_mappers` (default **100**).
   *Rationale: a floor guarantees every table at least one mapper; a ceiling limits how many
   parallel connections one table's copy can open against the source cluster.*
3. **Cap to the file count.** Mappers never exceed the number of files in the table.
   *Rationale: on Hadoop 2.7 (no `-blocksperchunk`), a mapper needs at least one whole file; a
   mapper with nothing to copy just idles. A 200 GiB table stored as 3 files can't usefully run
   more than 3 mappers.*
4. **Split the aggregate budget, then cap each mapper.** `migration_distcp_target_aggregate_mbps`
   (default **2000 MB/s**) is divided evenly across the mappers chosen above (never below
   1 MB/s each). The result is then capped at `migration_distcp_max_mapper_bandwidth` (default
   **500 MB/s**).
   *Rationale: dividing a fixed budget keeps one table's total throughput roughly constant
   whether it runs 20 mappers or 100. The per-mapper cap exists because a table with very few
   files gets very few mappers, and without the cap the whole budget would land on one to three
   streams: one mapper told to sustain 2000 MB/s or more exceeded real link capacity and timed
   out connections. With the cap, such tables deliberately run below the aggregate target;
   mapper count stays bound by file count, it is never raised to use up the budget.*
5. **Fallback for an unusable size.** If the size probe returns 0 bytes but the table has
   files, the DAG doesn't size from that. It uses `migration_distcp_default_mappers` (**50**)
   and `migration_distcp_default_bandwidth` (**100**), and logs a warning. The fallback does
   **not** apply the file-count cap: a 10-file table still gets 50 mappers.
6. **Partition-filtered copies are sized per partition.** With a partition filter and
   `migration_distcp_preserve_delete=true` (the default), each matched partition is copied as
   its own DistCp job, and each job is sized separately with steps 1–5. Discovery records a file
   count per partition but not a size, so each partition's bytes are estimated as its share of
   the table's files. Detailed per-partition sizing is logged at debug level.

**Value checks.** Every numeric knob above must be a positive whole number. An empty value
falls back to the default. Zero, a negative number or a non-number fails the task when the
configuration is read, before any copy starts.

---

## 4. Worked examples

Auto mode with default settings. These values are asserted in the automated test suite
(`tests/test_distcp_sizing.py`).

| Table size | File count | → Mappers | → Bandwidth per mapper | Table throughput |
| --- | --- | --- | --- | --- |
| 500 MB | 6 | **1** | **500 MB/s** (capped) | 500 MB/s |
| 8 GB | 200,000 | **4** | **500 MB/s** | 2000 MB/s |
| 40 GB | 800 | **20** | **100 MB/s** | 2000 MB/s |
| 900 GB | 3 | **3** | **500 MB/s** (capped) | 1500 MB/s |
| 2 TB | 30,000 | **100** | **20 MB/s** | 2000 MB/s |
| *Any table, Disabled mode (default)* | *any* | **50** | **100 MB/s** | 5000 MB/s |

Rows worth calling out:

- **500 MB / 6 files:** size alone gives 1 mapper. Splitting 2000 MB/s across one mapper would
  ask a single stream for 2000 MB/s; the per-mapper cap holds it at 500 MB/s.
- **8 GB / 200,000 files:** small by size, made of a huge number of tiny files. Size gives 4
  mappers (8 GiB ÷ 2 GiB), and 200,000 files is nowhere near limiting, so it stays at 4. This is
  the common "many small files" pattern.
- **900 GB / 3 files:** large by size, stored as only 3 files. Size alone suggests about 450
  mappers, clamped to the ceiling of 100, then the file-count cap pulls it down to 3. Splitting
  2000 MB/s three ways would be 666 MB/s each; the per-mapper cap holds each at 500 MB/s, so the
  table runs at 1500 MB/s. A handful of very large files limits parallelism no matter how
  generous the other settings are.
- **Disabled mode:** every table, even one of a few kilobytes, gets 50 mappers at 100 MB/s, a
  nominal 5000 MB/s. This is the out-of-the-box behaviour, and the main reason to turn
  auto-sizing on for workloads with many small tables.

Tables with enough files reach the 2000 MB/s target; tables with few files stop at
*mappers × 500 MB/s* by design.

---

## 5. Does cluster load factor in?

**Not automatically.** Sizing looks only at each table's own size and file count. It can't see
how busy the source cluster is, what else is running on it, or its hardware.

**The aggregate target is per table, not per cluster.** DAG 1 runs several copies at the same
time, so the load on the source cluster is roughly *one table's throughput × the number of
copies running*. Copies per DAG run are capped by `migration_distcp_copy_max_concurrent`
(default **3**), and up to **5** DAG runs can be active at once. In Auto mode with defaults,
that's about 3 × 2000 MB/s per run; in Disabled mode, 3 × 5000 MB/s nominal.

**Recommendations:**

- **`migration_distcp_copy_max_concurrent`** (default 3) is the main **cluster-wide** lever:
  fewer copies at once means less total pressure. It can be changed as an Airflow Variable (the
  env var is the fallback) and also feeds batch sizing ([section 6](#6-batching-and-time-budgets-dag-1)).
- **`migration_distcp_max_mappers`** (default 100) limits how many parallel connections any one
  table's copy can open. On a source cluster shared with other workloads, consider 20–40.
- **`migration_distcp_target_aggregate_mbps`** (default 2000) limits one table's total
  throughput. If network or I/O capacity is limited or shared, lower this; it affects every
  table the same way.
- **`migration_distcp_max_mapper_bandwidth`** (default 500) should match the measured throughput
  of the link between the edge node and the destination.
- **Scheduling matters more than sizing.** Because the DAG can't see cluster load, the most
  effective control is *when* migrations run. If the source cluster has a quiet window
  (overnight, weekends), schedule large batches there instead of tuning values to compensate
  for a busy cluster.
- If the source cluster has hard limits (a cap on concurrent MapReduce tasks, or a shared
  bandwidth budget), map them onto `migration_distcp_copy_max_concurrent`,
  `migration_distcp_max_mappers` and `migration_distcp_target_aggregate_mbps` together.

---

## 6. Batching and time budgets (DAG 1)

DAG 1 doesn't copy one database per task any more. The `flatten_and_batch` task estimates every
table's copy time and packs the tables into batches; each batch is one `run_distcp_ssh` task,
so a stuck copy only affects its own batch. The full walkthrough is in
[How the S3 copy is split into batches](README.md#how-the-s3-copy-is-split-into-batches); the
parts that affect tuning are:

1. **Each table gets a cost estimate, in seconds:**
   `30 + size ÷ 150 MB/s + 45 per DistCp job + 24 per partition directory + 0.002 per file`.
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
   and at most `migration_distcp_call_timeout_max_seconds` (default **21600**, 6 hours). A
   table that needs longer simply takes more than one attempt: every copy runs `-update`, so
   each retry resumes where the last one stopped.

**Where to look:** the `flatten_and_batch` log prints the plan (`[Batching] DERIVED-CAP …`,
then one line per batch with its tables). Each copy logs
`[DistCp] cost estimate Xs vs actual Ys`; it is informational only, and there is nothing to
tune in response to it.

---

## 7. Choosing a mode

|  | **Disabled (default)** | **Auto (recommended for mixed workloads)** | **Forced** |
| --- | --- | --- | --- |
| **How to select** | Switch unset / `false` | Switch `true`, forced pair empty | Switch `true`, forced pair set |
| **Behaviour** | 50 × 100 MB/s for every table | Mappers and bandwidth scaled to each table's size and file count | Your fixed pair for every table |
| **Small tables** | Over-provisioned: a tiny table still gets 50 mappers (5000 MB/s nominal) | Proportionate | Depends on the pair you chose |
| **Large tables** | May be under-provisioned (50 mappers, 100 MB/s each) | Up to 100 mappers, bounded by the aggregate target and the 500 MB/s per-mapper cap | Depends on the pair; the per-mapper cap does **not** apply |
| **Predictability** | Fully predictable | Self-adjusting; check the `Sized …` log line per table | Fully predictable |
| **Use when** | Matching the pre-auto-sizing behaviour exactly | Databases with a mix of table sizes | You've validated a pair for your cluster, or you're troubleshooting and want sizing out of the picture |

**Recommendation:** for mixed-size workloads, turn auto-sizing on:

1. Set `migration_distcp_enable_auto_sizing=true`.
2. Make sure `migration_distcp_mappers` and `migration_distcp_bandwidth` are empty at **every**
   level ([section 2](#2-three-sizing-modes-disabled-forced-auto) explains the `env.shared` trap).
3. Set `migration_distcp_max_mappers`, `migration_distcp_target_aggregate_mbps` and
   `migration_distcp_max_mapper_bandwidth` to your cluster's and link's real headroom.
4. Confirm the first run's log shows `Sizing mode: AUTO` and sensible `Sized …` lines.

Use Forced only when you want identical behaviour for every table (for example, while debugging),
and remember it bypasses the per-mapper cap.

---

## 8. Configuration reference

Defaults apply when neither the Airflow Variable nor the env var is set.

### Sizing

| Airflow Variable | Env var | Default | Used when |
| --- | --- | --- | --- |
| `migration_distcp_enable_auto_sizing` | `MIGRATION_DISTCP_ENABLE_AUTO_SIZING` | `false` | Master switch. `false`: every table gets the default pair below. `true`: Forced or Auto mode. |
| `migration_distcp_mappers` | `MIGRATION_DISTCP_MAPPERS` | *(empty)* | Switch on: set together with `migration_distcp_bandwidth` to force fixed values |
| `migration_distcp_bandwidth` | `MIGRATION_DISTCP_BANDWIDTH` | *(empty)* | Switch on: set together with `migration_distcp_mappers` to force fixed values |
| `migration_distcp_target_bytes_per_mapper` | `MIGRATION_DISTCP_TARGET_BYTES_PER_MAPPER` | `2147483648` (2 GiB) | Auto: bytes per mapper before another mapper is added |
| `migration_distcp_min_mappers` | `MIGRATION_DISTCP_MIN_MAPPERS` | `1` | Auto: floor on mapper count |
| `migration_distcp_max_mappers` | `MIGRATION_DISTCP_MAX_MAPPERS` | `100` | Auto: ceiling on mapper count |
| `migration_distcp_target_aggregate_mbps` | `MIGRATION_DISTCP_TARGET_AGGREGATE_MBPS` | `2000` | Auto: total MB/s per table, split across its mappers |
| `migration_distcp_max_mapper_bandwidth` | `MIGRATION_DISTCP_MAX_MAPPER_BANDWIDTH` | `500` | Auto: ceiling on MB/s per mapper, applied after the split |
| `migration_distcp_default_mappers` | `MIGRATION_DISTCP_DEFAULT_MAPPERS` | `50` | Disabled mode, and Auto's fallback when the size probe is unusable |
| `migration_distcp_default_bandwidth` | `MIGRATION_DISTCP_DEFAULT_BANDWIDTH` | `100` | Disabled mode, and Auto's fallback when the size probe is unusable |

### Batching and concurrency (DAG 1)

| Airflow Variable | Env var | Default | Used when |
| --- | --- | --- | --- |
| `migration_distcp_copy_max_concurrent` | `MIGRATION_DISTCP_COPY_MAX_CONCURRENT` | `3` | Copies running at once per DAG run; also sets the batch-count floor |
| `migration_distcp_max_tables_per_batch` | `MIGRATION_DISTCP_MAX_TABLES_PER_BATCH` | `25` | Most tables packed into one batch |
| `migration_distcp_call_timeout_max_seconds` | `MIGRATION_DISTCP_CALL_TIMEOUT_MAX_SECONDS` | `21600` | Ceiling on one DistCp call's timeout (6 hours) |

Lower-level DistCp settings (strategy, map memory, JVM options, `-delete` behaviour, checkpoint
location) are listed in [MIGRATION_AIRFLOW_VARIABLES.md](MIGRATION_AIRFLOW_VARIABLES.md#6-distcp-copy-behavior).

---

## 9. Summary

- **Auto-sizing is off by default.** Every table gets 50 mappers × 100 MB/s until
  `migration_distcp_enable_auto_sizing=true`.
- When enabled, mappers and bandwidth are derived per table from its size and file count,
  aiming at a constant total throughput per table (2000 MB/s by default).
- File count can override size: a large table stored as few files gets fewer mappers than its
  size suggests. The 500 MB/s per-mapper cap then keeps those few mappers from being
  over-driven, so such tables deliberately run below the aggregate target.
- An empty Airflow Variable masks the deployed `env.shared`; check the `Sizing mode:` log line
  to see which mode actually ran.
- Sizing does **not** account for source-cluster load. Total pressure is roughly per-table
  throughput × concurrent copies, so `migration_distcp_copy_max_concurrent` and scheduling are
  the cluster-wide controls.
- In DAG 1, tables are copied in cost-estimated batches, each with a flat 7.2-hour budget; a
  table that doesn't finish resumes on retry with `-update`.
