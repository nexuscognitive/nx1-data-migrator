# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Monorepo of four independent Python 3.12 tools for data platform migration (MapR/HDFS → S3/Iceberg) and access control automation. Two are Airflow DAGs deployed to a shared Airflow environment; two are standalone CLI tools.

## Build & Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install ".[dev]"
```

## Lint

```bash
ruff check .           # lint entire repo
ruff format --check .  # check formatting
ruff format .          # auto-format
```

Ruff config is in `pyproject.toml`: target Python 3.12, line length 120, rules `E/W/F/I/B/SIM` with `E501/SIM108/SIM110/SIM115` ignored.

## Tests

Each module has its own test suite, `.coveragerc` (source + 80% threshold), and `pytest.ini` (`-v --timeout=60`). Run from each module's directory:

```bash
cd data-iceberg-migrator && pytest tests/              # no coverage
cd data-iceberg-migrator && pytest tests/ --cov        # with coverage

cd ranger-policies-generator && pytest tests/ --cov
```

code-scanner uses its own `requirements-test.txt` instead of the root `pyproject.toml`:

```bash
cd code-scanner && pip install -r requirements-test.txt && pytest tests/ --cov
```

Run a single test:

```bash
cd data-iceberg-migrator && pytest tests/test_dag1_tasks.py::TestParseExcel::test_basic_parse -v
```

## CI Pipeline

`.github/workflows/ci.yml` — triggers on push to `main`/`develop` and PRs targeting either branch:

```
lint (ruff check) ─┬─► test-data-iceberg-migrator ──────┬─► coverage-report (PR comment)
                   ├─► test-ranger-policies-generator ───┘
                   └─► test-code-scanner ────────────────┘
```

Lint must pass before any test job runs. Coverage report posts to PR with 80% green threshold.

## Architecture

### data-iceberg-migrator/

Single file `migration_dags_combined.py` (~5k lines) defining three Airflow DAGs:
- **mapr_to_s3_migration** — distcp from MapR/HDFS to S3 + Hive table creation
- **iceberg_migration** — convert S3 Hive tables to Iceberg format
- **data_copy_migration** — arbitrary folder copy via distcp

Key internals: `track_duration()` decorator for task timing via XCom, `execute_with_iceberg_retry()` with exponential backoff (6 retries), Excel-driven config (parsed via Spark). Tracking state stored in Iceberg tables in a `migration_tracking` database. SSH-based cluster operations with MapR/Kerberos auth.

### ranger-policies-generator/

Two files: `ranger_policies_generator_airflow3.py` (DAG + task functions) and `ranger_utils.py` (API wrappers).

Core classes in `ranger_utils.py`:
- **RangerPolicyManager** — wraps `apache-ranger` client; creates/updates policies with access type filtering against service definition, handles row-filter vs access-only policy split
- **KeycloakRoleManager** — wraps `python-keycloak`; manages realm roles and group-to-role assignments with exponential backoff on connect
- **Permissions** — maps READ/WRITE/ALL labels to Ranger access type lists

### code-scanner/

Standalone CLI (`code-scanner.py`). **SparkMigrationScanner** applies 40+ regex-based `ScanRule`s to detect Spark 2→3, HDFS→S3, JDK, and Python migration patterns. **ASTCodeExtractor** filters false positives in Python files by checking if matched lines are executable. Output formats: markdown, JSON, HTML.

### airflow-3-migration-assistant/

Standalone CLI (`airflow3_migration_assistant.py`). Scans Airflow 2 DAG files for 50+ import replacements and deprecated patterns, reports findings as `MigrationFinding` dataclasses. Supports dry-run (report only) and auto-fix (with backup) modes.

## Test Patterns

All test suites share a common approach:
- **conftest.py** installs lightweight stubs into `sys.modules` for heavy dependencies (Airflow, PySpark, apache-ranger, python-keycloak) so tests run without those packages installed.
- DAG module task functions are accessed via `.function` attribute (unwrapping the `@task` decorator).
- `data-iceberg-migrator/tests/helpers.py` provides `make_excel_bytes()`, `setup_spark_excel()`, and `mock_ssh_stdout()` shared across DAG test files.

## Configuration

Environment config uses `python-dotenv` with a two-layer pattern:
1. `env.shared` — base config loaded first
2. `env.<dag_stem>` — per-DAG overrides loaded second

Falls back to Airflow Variables when config directory is absent (production deployment). See `env.shared.example` and `env.migration.example` for all available settings.

## Conventions

- All DAG task functions return dicts with `status`, `errors`, `warnings` keys and optional `task_duration` (from `track_duration` decorator).
- HTML reports are self-contained with inline CSS (no external assets).
- Ruff handles all formatting and import sorting — no black/isort.
