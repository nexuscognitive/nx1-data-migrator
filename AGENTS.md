# nx1-data-migrator

Data platform migration tools (MapR/HDFS to S3/Iceberg) and Apache Ranger/Keycloak access control automation, orchestrated as Airflow DAGs.

## Stack

- Python 3.12
- Apache Airflow 2.7+ (DAG orchestration)
- PySpark 3.4+ (data migration and Iceberg operations)
- Apache Ranger SDK, python-keycloak (access control)
- pandas, openpyxl (Excel config parsing)
- python-dotenv (env file loading)
- boto3 (S3 deployment)
- ruff (linting and formatting)
- pytest, pytest-cov, pytest-mock, pytest-timeout (testing)

## Layout

```
.
├── data-iceberg-migrator/       # Airflow DAGs: MapR/HDFS -> S3, Hive -> Iceberg conversion
│   ├── migration_dag_mapr_to_s3.py      # DAG 1: MapR-FS/HDFS to S3 via DistCp
│   ├── migration_dag_iceberg.py         # DAG 2: Hive tables to Iceberg (in-place or snapshot)
│   ├── migration_dag_folder_copy.py     # DAG 3: folder-only data copy
│   ├── utils/migrations/shared.py       # Shared config, S3 helpers, retry logic
│   ├── utils/migrations/partition_utils.py
│   └── tests/
├── ranger-policies-generator/   # Airflow DAG: Ranger policies + Keycloak roles from Excel
│   ├── ranger_policies_generator_airflow3.py
│   ├── utils/migrations/ranger_utils.py
│   └── tests/
├── code-scanner/                # Standalone CLI: static analysis for Spark/HDFS/JDK/Python migration
│   ├── code-scanner.py
│   └── tests/
├── airflow-3-migration-assistant/  # Standalone CLI: scan/fix Airflow 2 DAGs for Airflow 3
│   └── airflow3_migration_assistant.py
├── deploy.py                    # Upload DAGs + utils to S3 for Airflow with suffix/owner customization
├── pyproject.toml               # Monorepo config: deps, ruff settings
├── env.shared.example           # Shared env template (S3, SSH, Spark, Ranger, Keycloak)
├── env.migration.example        # Per-DAG migration overrides template
└── env.ranger.example           # Per-DAG ranger overrides template
```

## Commands

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install ".[dev]"
```

### Lint

```bash
ruff check .
ruff format --check .
ruff format .          # auto-fix
```

### Test

Each component has its own test suite. Run from the component directory:

```bash
# data-iceberg-migrator
cd data-iceberg-migrator && pytest tests/
cd data-iceberg-migrator && pytest tests/ --cov

# ranger-policies-generator
cd ranger-policies-generator && pytest tests/
cd ranger-policies-generator && pytest tests/ --cov

# code-scanner (uses requirements-test.txt, not pyproject.toml)
cd code-scanner && pip install -r requirements-test.txt
cd code-scanner && pytest tests/
cd code-scanner && pytest tests/ --cov
```

### Deploy

```bash
python deploy.py --project migrator --dag iceberg --owner my-name --suffix dev1
python deploy.py --project ranger --dag ranger --owner my-name --suffix dev1 --dry-run
```

## Conventions

- Branch from `main`, PR back to `main`
- CI runs on every push to `main` and all PRs targeting `main`
- CI pipeline: lint -> test (3 parallel jobs) -> coverage-report (PR comment)
- Ruff config: `target-version = "py312"`, `line-length = 120`, rules `E W F I B SIM`
- 80% minimum test coverage enforced per component via `.coveragerc`
- pytest config per component in `pytest.ini`: `-v --timeout=60`
- Each DAG file is self-contained with its task definitions and DAG wiring at the bottom
- Shared utilities live in `utils/migrations/` within each component

## Patterns

### Configuration Loading

DAGs load config via a two-layer dotenv pattern:
1. `utils/migration_configs/env.shared` (base config)
2. `utils/migration_configs/env.{dag_stem}` (per-DAG overrides)

At runtime, `get_config()` in `shared.py` resolves each value from: Airflow Variable -> env var -> hardcoded default. This means env files set the env vars that `get_config()` reads.

### Task Decorator Pattern

All DAG tasks use `@task.pyspark(conn_id='spark_default')` for Spark tasks. The `@track_duration` decorator wraps tasks to inject `_task_duration` into the result dict via XCom.

### Error Handling

- `execute_with_iceberg_retry()`: retries Spark SQL up to 6 times with random backoff (10-50s) for Iceberg commit conflicts
- Tasks that process multiple items (tables/policies) continue on individual failures and aggregate results
- `_has_failures` and `_failure_summary` keys in result dicts propagate failure info through the DAG
- Tasks use `trigger_rule = 'all_done'` so downstream steps run even when upstream tasks partially fail
- Errors are tracked in Iceberg tracking tables for auditing

### Deployment

`deploy.py` uploads DAG files to S3, injecting per-user DAG ID suffixes and owner names. It also uploads `env.shared` and per-DAG env files to `utils/migration_configs/` on S3. S3 credentials resolve from env.shared, AWS env vars, `~/.aws/credentials`, or IAM role.

### Tracking and Reporting

Each DAG maintains Iceberg tracking tables for run-level and table-level status. HTML reports are generated at the end of each run, written to S3, and emailed via SMTP.

### Code Scanner and Migration Assistant

These are standalone CLI tools (not Airflow DAGs). They use argparse for CLI, dataclasses for data models, and produce HTML/markdown/JSON reports.

## Gotchas

- **Monorepo with independent test suites**: each component has its own `pytest.ini`, `.coveragerc`, and test directory. Running `pytest` from the repo root will not work correctly.
- **code-scanner uses `requirements-test.txt`**, not `pyproject.toml` dev deps. Install separately.
- **Env files are gitignored**: `env.*` except `*.example` are in `.gitignore`. Never commit actual env files.
- **deploy.py rewrites DAG content in memory**: it string-replaces `dag_id` and `owner` before uploading, so DAG files must contain the exact markers defined in `PROJECTS` dict (e.g. `dag_id='iceberg_migration'`, `'owner': 'data-migration'`).
- **S3 paths use `s3a://` prefix**: the codebase normalizes `s3://` and `s3n://` to `s3a://` via `normalize_s3()`. Always use `s3a://` in config.
- **Iceberg commit conflicts are expected**: concurrent Spark tasks writing to the same Iceberg table will conflict. This is handled by `execute_with_iceberg_retry()` — do not remove or bypass it.
- **CI only runs `ruff check`, not `ruff format --check`**: format violations will not fail CI. Run `ruff format --check .` locally before pushing.
- **Excel config drives migrations**: DAG behavior is controlled by Excel files on S3, not by code changes. Column names are normalized (lowercased, spaces to underscores) at parse time.
