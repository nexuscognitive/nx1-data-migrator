# Hive → Iceberg Migration Rulebook

How to migrate Hive tables to Iceberg with `migration_dag_iceberg.py`: which strategy to use, in which environment, in what order, and why.

---

## The rule

> **Snapshot is for testing. In-place is for cutover.**
> 

| Phase | Strategy | `inplace_migration` | Why |
| --- | --- | --- | --- |
| UAT / testing | **Snapshot** | `False` | Non-destructive, source stays live, drop and rebuild freely |
| Prod shadow validation | **Snapshot** | `False` | Validate against real data with zero risk |
| **Prod cutover** | **In-place** | `True` | Self-contained, zero-copy, keeps the table name, rollback via backup |
| Cutover of **text/CSV** | **In-place (CTAS)** | `True` | Full copy under the original name, gated by `iceberg_inplace_text_ctas` — see [Rule 6](#rule-6--text-tables-are-the-exception) |

---

## The two strategies

|  | **In-place** (`migrate` / CTAS) | **Snapshot** |
| --- | --- | --- |
| Excel setting | `inplace_migration = True` | `inplace_migration = False` |
| What it does | Converts the Hive table into Iceberg in place | Creates a separate Iceberg table in a destination DB |
| Data movement | None (zero-copy) for parquet/orc/avro; **full copy** for text (CTAS, gated by `iceberg_inplace_text_ctas`) | None for parquet/orc/avro; **full copy** for text (CTAS) |
| Source table after | Renamed to `<table>_backup_` | Untouched, stays live |
| Result location | Same DB; same location as source for parquet/orc/avro, new sibling location `<location>_iceberg` for text | Destination DB, new metadata location |
| Owns its data? | **Yes** | **No** for zero-copy (references source files); yes for CTAS |
| Formats | parquet / orc / avro, **and** text via CTAS (`EXTERNAL` tables only) | parquet / orc / avro, **and** text (via CTAS) |
| Sees later source writes? | n/a | **No** — frozen point-in-time |
| Re-run | SKIPPED (already Iceberg), never fails; an interrupted text swap auto-repairs on retry within the same run | Drop + rebuild; CTAS orphans the old copy |

### Where data and metadata actually live

| Case | Data files | Metadata | Self-contained? |
| --- | --- | --- | --- |
| **In-place — parquet/orc/avro** | Original location `L`, unmoved | Same location `L` | **Yes** |
| **In-place — text (CTAS)** | New sibling location `L_iceberg`, full copy | Same location `L_iceberg` | **Yes** |
| **Snapshot — parquet/orc/avro** | Source location `L_hive`, referenced not copied | Destination `L_ice` | **No** — depends on source data |
| **Snapshot — CTAS** (text/unknown/empty AVRO) | Destination `L_ice`, **new full copy** | Destination `L_ice` | **Yes** |

Snapshot CTAS lands at the catalog/warehouse default for the destination database

(`<warehouse root>/<destination_db>/<table>/`). In-place text CTAS lands at the sibling path `<original location>_iceberg` in the *same* database — no destination database is created.

### Why a snapshot can never be your production table

1. **Frozen in time.** Its metadata captures the source's files as of snapshot moment. Every later insert into the source is invisible to it, forever. The only refresh is a full rebuild.
2. **Doesn't own its data.** Zero-copy metadata points back at the *source's* files. Purge, overwrite, or lifecycle-expire the source and the Iceberg table's reads break.
3. **Writes diverge.** Writes to the Iceberg table go to the destination; writes to the source go to the source. Neither sees the other, and they are never reconciled.
4. **Wrong namespace.** It defaults to `{source_database}_iceberg`, so cutting over on it means repointing every consumer, dashboard and DAG.

In-place has none of these: it takes over the original location, owns both data and metadata there, and keeps the original database and table name.

---

## Flow

```
┌─ UAT ────────────────────────────────────────────────────────┐
│  U1  Snapshot   → stand up Iceberg copy beside Hive          │
│  U2  Validate   → OLAP / BI / pipelines, side-by-side  ─┐    │
│                       ↑ refresh + repeat until green ───┘    │
│  U3  In-place   → rehearse the real cutover                  │
└──────────────────────────────────────────────────────────────┘
                          ↓ sign-off
┌─ PROD ───────────────────────────────────────────────────────┐
│  P1  Snapshot   → read-only shadow check on real data        │
│  P2  Freeze     → stop all writers                           │
│  P3  In-place   → the cutover                                │
│  P4  Validate   → unfreeze in stages, soak 7–14 days         │
│  P5  Cleanup    → drop backups after the soak                │
└──────────────────────────────────────────────────────────────┘
```

**Don't skip U3.** Snapshot testing proves *your queries work on Iceberg*. It does not prove the in-place conversion works — different procedure, different failure modes. U3 is the only place you learn that before prod.

---

## UAT

### U1 — Snapshot

Excel config → S3, path passed as `excel_file_path`:

| database | table | inplace_migration | destination_iceberg_database |
| --- | --- | --- | --- |
| `sales_uat` | `*` | `F` | `sales_uat_iceberg` |
1. Check the UAT dataset is representative of prod in volume, partition count and format mix. A snapshot over 1% of prod won't surface the problems you hit at cutover.
2. Note which tables are text/CSV — they follow [Rule 6](#rule-6--text-tables-are-the-exception), not this flow.
3. Trigger the DAG, then read the HTML report and `migration_tracking.iceberg_migration_table_status`.

### U2 — Validate side-by-side

**Gate — all must pass:**

| Check | Pass criterion |
| --- | --- |
| Row count, schema, partition count | DAG reports `VALIDATED` for every table |
| OLAP correctness | Query suite run against `sales_uat.*` **and** `sales_uat_iceberg.*` returns identical results |
| BI / semantic layer | A copy of the model repointed at the Iceberg DB renders identical numbers |
| Pipeline reads | Read-only pipeline stages repointed at Iceberg produce identical output |
| Performance | Top-N heavy queries within accepted envelope |
| Engine compatibility | Every engine that touches these tables (Trino / Spark / Hive / BI drivers) reads Iceberg V2 |
- **Compare, don't just run.** The value is in the diff. "It didn't error" is not correctness.
- **Writes here are throwaway.** You can test write paths, but that data diverges from the source and dies at the next rebuild. Never validate a write-then-read-from-source scenario on a snapshot.
- **Refresh by rebuilding.** New source data won't appear in the snapshot. Re-run the DAG — it drops and rebuilds idempotently. Never top up a snapshot by hand.

### U3 — Rehearse the cutover

| database | table | inplace_migration | destination_iceberg_database |
| --- | --- | --- | --- |
| `sales_uat` | `*` | `T` | *(blank — same DB)* |
1. **Drop the U1 snapshot tables first** (`sales_uat_iceberg.*`) — see [Rule 2](#rule-2--drop-snapshot-tables-before-running-in-place-on-the-same-source).
2. Keep `iceberg_drop_backup = false` so `<table>_backup_` is retained.
3. Run the DAG. Record `migration_duration_seconds` per table — this sizes your prod freeze window.
4. Re-run the *same unmodified* U2 validation suite, now against `sales_uat.*`.
5. **Test rollback on at least one table**: restore from `<table>_backup_` and confirm it's queryable. An untested rollback isn't a rollback.
6. Run the DAG again unchanged — every table must record **SKIPPED**, not FAILED. This proves prod re-run safety.

**Exit criteria for prod approval**

- [ ]  Every table `VALIDATED` (row count, schema, partition count)
- [ ]  Full query / BI / pipeline suite passes on the in-place tables
- [ ]  Rollback demonstrated
- [ ]  Idempotent re-run demonstrated
- [ ]  Per-table durations recorded → freeze window sized
- [ ]  Text tables handled per Rule 6 and validated separately
- [ ]  Downstream owners signed off

---

## Production

### P1 — Shadow snapshot (read-only)

| database | table | inplace_migration | destination_iceberg_database |
| --- | --- | --- | --- |
| `sales_prod` | `*` | `F` | `sales_prod_iceberg` |
1. Run in a low-traffic window (metadata-only write, but count queries read the source).
2. Confirm every table `VALIDATED`; diff the query suite against the Hive tables.
3. **Point no production consumer at these tables** — they're frozen and get dropped in P3.

*Why bother when UAT is green:* prod has tables UAT doesn't — the empty AVRO table, the nested subdirectory, the partition with 400k files. Finding those in a snapshot is free.

Finding them mid-cutover costs your freeze window.

### P2 — Freeze

1. Pause every writer: ingestion DAGs, streaming sinks, ad-hoc inserts, compaction jobs.
2. Confirm no active sessions are writing to the target databases.
3. Take an HMS backup for those databases.
4. Announce the window, sized from U3 durations plus contingency.

*Why:* `migrate` renames the source and hands its location to the Iceberg table. A writer crossing that boundary writes files the Iceberg table doesn't know about —silent data loss.

### P3 — Cutover

| database | table | inplace_migration | destination_iceberg_database |
| --- | --- | --- | --- |
| `sales_prod` | `*` | `T` | *(blank)* |
1. **Drop the P1 snapshot tables** (metadata-only, no `PURGE`) — [Rule 2](#rule-2--drop-snapshot-tables-before-running-in-place-on-the-same-source).
2. `iceberg_drop_backup = false`. **Never `true` on the cutover run** — that backup is your rollback.
3. Trigger the DAG and monitor.

### P4 — Validate, unfreeze, soak

1. Every table `VALIDATED` on row count, schema and partition count.
2. Reconcile the inventory — each source table is migrated or explicitly accounted for as SKIPPED with a known reason.
3. Run the production smoke suite.
4. Unfreeze writers **one class at a time** — batch, then streaming, then ad-hoc — validating after each.
5. Soak **7–14 days**, covering at least one full weekly/monthly batch cycle.

**Roll back if:** any validation mismatch, or consumer breakage you can't fix forward inside the window. Restore `<table>_backup_` — it still points at the same data files.

**Rollback deadline:** rollback is clean only *before* writes resume against the Iceberg table. Once new Iceberg files exist at `L`, restoring the backup loses those writes. 

Decide inside the freeze window.

### P5 — Retire backups

After a clean soak: re-run with `iceberg_drop_backup = true`, or drop `<table>_backup_`manually. Backup drops are **always metadata-only, never `PURGE`** — the backup shares data files with the live Iceberg table, so a hand-written `DROP TABLE ... PURGE` here would destroy live data. Once backups are gone, so is the easy rollback.

---

## Standing rules

### Rule 1 — Never cut over on a snapshot

Point-in-time, doesn't own its data, wrong namespace. 

Only exception: text tables on the snapshot CTAS fallback ([Rule 6](#rule-6--text-tables-are-the-exception)) — unlike a zero-copy snapshot, that CTAS output is genuinely self-contained, so it's safe to cut over on directly. Prefer the in-place text CTAS route in Rule 6 when it's available; it needs no such exception.

### Rule 2 — Drop snapshot tables before running in-place on the same source

The highest-risk interaction in the whole migration, and easy to miss.

A zero-copy snapshot references data files at source location `L`. In-place converts the source at `L`, and the new Iceberg table **owns** those files. Any later Iceberg maintenance — `expire_snapshots`, `rewrite_data_files`, compaction, a row-level delete — may physically remove them. The stale snapshot still points there. Reads break with no warning and no clear error trail. The snapshot's job ends when validation ends. Drop it before the in-place run — in UAT (U3 step 1) and in prod (P3 step 1).

### Rule 3 — Never purge a snapshot's source

While a zero-copy snapshot exists, its source data is load-bearing. `DROP ... PURGE`, an S3 lifecycle rule, or a file overwrite on the source breaks the Iceberg table.

### Rule 4 — Freeze for in-place, not for snapshot

Snapshot is non-destructive and runs fine against a live source — you just get a point-in-time view. In-place renames the source and takes over its location. Freeze for P3, not for P1.

### Rule 5 — A green DAG is not a successful migration

Tasks use `trigger_rule = 'all_done'` and aggregate per-item failures instead of failing the run. The authoritative result is `iceberg_migration_table_status` and the HTML report. Read them every time.

### Rule 6 — Text tables are the exception

Iceberg's in-place `migrate` only registers Parquet/ORC/Avro data files — it rejects `LazySimpleSerDe` — so it can never convert a text table without a copy. Whether that copy still counts as "in-place" (same database, same table name) depends on `iceberg_inplace_text_ctas`.

**With `iceberg_inplace_text_ctas = true` (default) and `inplace_migration = T`:** in-place *can* migrate text, via CTAS instead of `migrate`:

1. CTAS copies the table into a staging table at the sibling location `<original location>_iceberg`, in the same database.
2. The copy is verified against the still-live source on row count and (Iceberg-normalized) schema — before anything is renamed.
3. Only once verification passes: the source is renamed to `<table>_backup_`, then the staging copy is renamed to `<table>`. The name and database never change.

This requires the table to be confirmed **EXTERNAL**. A text table that can't be positively confirmed EXTERNAL is skipped with `MANAGED_TEXT_INPLACE_UNSUPPORTED` — renaming a *managed* Hive table makes the metastore move its data directory, which on S3 is a second full copy on top of the one this migration already makes. Convert it to EXTERNAL, or fall back to the snapshot route below.

If verification fails — almost always a concurrent write — the staging table is dropped and the original is left untouched: `INPLACE_CTAS_VERIFY_FAILED`, status FAILED. Freeze writers and re-run.

**With `iceberg_inplace_text_ctas = false`, or `inplace_migration = F`:** text (and unknown-format) tables are skipped — `TEXT_FORMAT_INPLACE_UNSUPPORTED` — and the only path is Snapshot, which routes them through CTAS into the destination database:

1. Freeze writers to those tables **before** the CTAS run — the copy is point-in-time.
2. Run the snapshot DAG; validate row counts and schema.
3. Repoint consumers to the **destination database name** — this route doesn't keep the table's namespace. This repointing is the main cost of the snapshot text path.
4. Retire the source Hive tables per your retention policy.
5. **Clean up orphans.** CTAS re-runs drop metadata-only and write fresh files, leaving the previous copy unreferenced. Correctness is fine; storage isn't.

**Freeze writers for the in-place text route too.** It is the one exception to [Rule 4](#rule-4--freeze-for-in-place-not-for-snapshot): the copy is point-in-time and the verification gate uses strict row equality, so a single concurrent write fails an otherwise good copy.

**What the in-place text path does not give you:**

- **Only as faithful as Hive's serde.** Text values that don't parse as their declared column type already read as NULL through Hive's serde — on the source and the copy alike — so row-count-and-schema verification cannot see a difference no existing reader could see either.
- **Metadata is not carried over.** CTAS creates a brand-new table; the original's comment, column comments, owner and any custom `TBLPROPERTIES` are lost. "In place" preserves the name and database, not the rest of the table's metadata.
- **Storage doubles until cleanup.** Dropping `<table>_backup_` (P5) is metadata-only, as always — but for text, nothing else references those files afterward, so they become orphans on S3 that only a manual cleanup reclaims.
- **No concurrent runs on the same database.** If a second DAG 2 run starts while a first run's text copy is in flight, the second run's interrupted-swap repair will drop the first run's staging table with `PURGE`.

Cheaper alternative, still available: convert text tables to parquet/orc/avro **before** migrating, and they take the zero-copy in-place path with none of the above.

### Rule 7 — Migrate in waves, sized by measured durations

Never cut over the whole warehouse in one run. Order waves by blast radius: low-traffic non-critical → medium → business-critical, each with its own freeze, validation and soak. Size each window from `migration_duration_seconds` in the tracking tables, not from table size — in-place cost tracks file and partition **count**, not bytes.

---

## Quick reference

| Setting | UAT snapshot | UAT rehearsal | Prod shadow | **Prod cutover** | Post-soak |
| --- | --- | --- | --- | --- | --- |
| `inplace_migration` | `F` | `T` | `F` | `T` | `T` |
| `destination_iceberg_database` | `{db}_iceberg` | blank | `{db}_iceberg` | blank | blank |
| `iceberg_drop_backup` | n/a | `false` | n/a | **`false`** | `true` |
| Writers frozen | No | No | No | **Yes** | No |
| Drop snapshot tables first | n/a | **Yes** | n/a | **Yes** | n/a |

**Excel columns:** `database` (required) · `table` (supports `*` wildcards) · `inplace_migration` (`T`/`F`, default `F`) · `destination_iceberg_database` (defaults to `{database}_iceberg` for snapshot, same DB for in-place).

**Config resolution order** (highest first): Dag Trigger Parameter → Airflow Variable (`migration_iceberg_drop_backup`) → default.

**Results:** `migration_tracking.iceberg_migration_runs` (run level) · `migration_tracking.iceberg_migration_table_status` (per table) · HTML report written to S3 and emailed at the end of each run.

**Backups:** in-place migration always renames the source to `<table>_backup_` — via `system.migrate` for parquet/orc/avro, or directly as part of the verified swap for text CTAS. Discovery skips `*_backup_` / `*__BACKUP__`, so backups are never re-migrated. All backup drops are metadata-only, so they never delete data — though for text, that leaves the backup's files as orphans on S3 (Rule 6), since nothing else references them.
