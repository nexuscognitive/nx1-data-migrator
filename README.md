# nx1-data-migrator

Data platform migration tools (MapR/HDFS to S3/Iceberg) and access control automation.

## Repository Structure

| Directory                        | Description                                                                                                                 |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `data-iceberg-migrator/`         | Airflow DAGs for migrating Hive tables from MapR-FS/HDFS to S3 and converting to Iceberg format                             |
| `ranger-policies-generator/`     | Airflow DAG for automating Apache Ranger policies and Keycloak role mappings from Excel config                              |
| `code-scanner/`                  | Standalone CLI tool for static analysis of Spark, HDFS, JDK, and Python migration patterns                                  |
| `airflow-3-migration-assistant/` | Standalone CLI tool that scans Airflow 2 DAG files and reports or auto-applies changes required for Airflow 3 compatibility |
| `dev-tools/hadoop-edge-node/`    | Docker-based local Hadoop/Hive/Spark edge node for end-to-end DAG testing against the nx1 tenant platform                   |

## Deployment

`deploy.py` uploads DAGs and shared utilities to S3 for Airflow. It supports per-user DAG ID suffixes, owner customization, and env file management.

**Configuration** — set `DEPLOY_S3_BUCKET` and `DEPLOY_DAGS_PREFIX` in `env.shared` (see `env.shared.example`). S3 credentials are resolved automatically from `env.shared`, AWS env vars, `~/.aws/credentials`, or IAM role — no manual input needed if any of these are configured.

```bash
python deploy.py --project migrator --dag iceberg --owner my-name --suffix dev1
python deploy.py --project ranger --dag ranger --owner my-name --suffix dev1 --dry-run
```

Use `--dry-run` to preview the upload plan without uploading.

## Development Setup

**Python 3.12** is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install ".[dev]"
```

## Local End-to-End Testing

`dev-tools/hadoop-edge-node/` contains a Docker image that replicates a
Hadoop/Hive/PySpark edge node (Hadoop 2.10.2, Hive 2.3.9, Spark 2.4.8,
Python 2.7) matching the client environment, for full end-to-end DAG testing
via SSH without needing access to the real cluster.

### Architecture

```
nx1 Tenant (Kubernetes)              TCP tunnel          Your Laptop (WSL)
┌─────────────────────────┐         (bore / ngrok)      ┌──────────────────────┐
│  Airflow pod            │ ──SSH──────────────────────► │  Docker Edge Node    │
│  spark_default    ✅    │                              │                      │
│  smtp_default     ✅    │                              │  Hadoop  2.10.2      │
│  aws_default      ✅    │                              │  Hive    2.3.9       │
│  cluster_edge_ssh ➕    │ ◄── add manually             │  Spark   2.4.8       │
└─────────────────────────┘                              │  Python  2.7         │
         │                                               └──────────────────────┘
         │ PySpark / Iceberg
         ▼
S3 bucket (nx1poc tenant)
```

### 1. Build and start the edge node

All commands run in WSL, from the `dev-tools/hadoop-edge-node/` directory.

> **First build downloads ~1.5 GB and takes 20–40 min. Subsequent builds use Docker cache and finish in under 30 seconds.**

```bash
cd dev-tools/hadoop-edge-node

docker compose build
docker compose up -d
sleep 20
docker logs hadoop-edge-node --tail 5
# Expected: "Container is ready!"
```

### 2. Populate test data

Run once after starting (or after any full reset with `docker compose down -v`):

```bash
bash scripts/setup-test-data.sh
```

This creates **4 databases and 10 tables** covering all migration scenarios — see
[Test Data Reference](dev-tools/hadoop-edge-node/README.md#test-data-reference)
for the full table inventory.

### 3. Expose SSH via TCP tunnel

The Airflow pod runs in Kubernetes and cannot reach `localhost` directly.
Use [bore](https://github.com/ekzhang/bore) to expose port 2222 publicly:

```bash
# Install bore (one time)
curl -sSL https://github.com/ekzhang/bore/releases/download/v0.5.0/bore-v0.5.0-x86_64-unknown-linux-musl.tar.gz \
  | sudo tar -xz -C /usr/local/bin

# Start tunnel — keep this terminal open during testing
bore local 2222 --to bore.pub
# Prints: listening at bore.pub:XXXXX  ← note this port
```

### 4. Configure Airflow

The nx1 platform pre-builds `spark_default`, `smtp_default`, and `aws_default`.
Add one connection manually:

**Airflow UI → Admin → Connections → `cluster_edge_ssh`:**

| Field     | Value                                                                             |
| --------- | --------------------------------------------------------------------------------- |
| Conn Type | SSH                                                                               |
| Host      | `bore.pub`                                                                        |
| Port      | `XXXXX` ← from bore output                                                        |
| Username  | `root`                                                                            |
| Password  | `root`                                                                            |
| Extra     | `{"timeout": "30", "no_host_key_check": "true", "allow_host_key_change": "true"}` |

**Airflow UI → Admin → Variables** — add these:

| Key                                | Value for local testing                               |
| ---------------------------------- | ----------------------------------------------------- |
| `cluster_ssh_conn_id`              | `cluster_edge_ssh`                                    |
| `cluster_edge_temp_path`           | `/tmp/migration`                                      |
| `cluster_edge_discovery_temp_path` | `/tmp`                                                |
| `auth_method`                      | `none`                                                |
| `migration_distcp_mappers`         | `1`                                                   |
| `migration_distcp_bandwidth`       | `10`                                                  |
| `migration_spark_conn_id`          | `spark_default`                                       |
| `migration_smtp_conn_id`           | `smtp_default`                                        |
| `s3_listing_tool`                  | `hadoop`                                              |
| `migration_default_s3_bucket`      | `s3a://<your-tenant-bucket>/<path>`                   |
| `migration_tracking_database`      | `migration_tracking_<your_initials>`                  |
| `migration_tracking_location`      | `s3a://<bucket>/<path>/migration_tracking_<initials>` |
| `migration_report_location`        | `s3a://<bucket>/<path>/migration_reports_<initials>`  |
| `migration_email_recipients`       | `your@email.com`                                      |

### Common issues

| Symptom                               | Fix                                                                                                                                                                |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `validate_prerequisites` SSH timeout  | bore tunnel is not running — restart `bore local 2222 --to bore.pub` and update the Airflow connection port                                                        |
| `hadoop: command not found` over SSH  | `~/.profile` missing env — run: `docker exec -u root hadoop-edge-node bash -c "source /etc/profile.d/hadoop.sh && cat /etc/profile.d/hadoop.sh >> /root/.profile"` |
| `Database 'X' not found` in discovery | Test data not populated — run `bash scripts/setup-test-data.sh`                                                                                                    |
| `Hive metastore` error after restart  | Run: `docker exec -u root hadoop-edge-node bash -c "rm -rf /opt/hive/metastore_db && schematool -initSchema -dbType derby"` then re-run `setup-test-data.sh`       |
| `\r: command not found` in script     | Windows line endings — run: `sed -i 's/\r//' scripts/setup-test-data.sh`                                                                                           |
| bore port changed                     | Update `cluster_edge_ssh` connection port in Airflow UI after each bore restart                                                                                    |

## Running Tests

Each project has its own test suite. Run from the project directory:

```bash
cd data-iceberg-migrator
pytest tests/           # fast, no coverage
pytest tests/ --cov     # with coverage

cd ../ranger-policies-generator
pytest tests/
pytest tests/ --cov

cd ../code-scanner
pytest tests/
pytest tests/ --cov
```

Coverage settings (source module, 80% threshold) are in each project's `.coveragerc`. Test settings (`-v`, `--timeout=60`) are in each project's `pytest.ini`.

## CI / GitHub Actions

A unified workflow (`.github/workflows/ci.yml`) runs on every push to `main` and on all pull requests targeting `main`.

### Pipeline

```
lint (ruff check + format) ─┬─► test-data-iceberg-migrator ─┬─► coverage-report (PR comment)
                            └─► test-ranger-policies-generator ─┘
                            └─► test-code-scanner ──────────────┘
```

### Jobs

| Job                                | What it does                                                                           |
| ---------------------------------- | -------------------------------------------------------------------------------------- |
| **lint**                           | Runs `ruff check` and `ruff format --check` across the entire repo                     |
| **test-data-iceberg-migrator**     | Installs dev deps, runs pytest with coverage, uploads coverage artifact                |
| **test-ranger-policies-generator** | Same as above for the ranger project                                                   |
| **test-code-scanner**              | Installs `requirements-test.txt`, runs pytest with coverage, uploads coverage artifact |
| **coverage-report**                | Downloads coverage artifacts, posts a summary comment on the PR                        |

### Coverage

- Each test job enforces an **80% minimum** coverage threshold (configured in `.coveragerc`)
- On PRs, the `coverage-report` job posts a coverage summary as a PR comment
- Coverage XML artifacts are uploaded for each project
