"""
Shared utilities for all migration DAGs.

Contains configuration, S3 helpers, retry logic, and constants
used across mapr_to_s3, iceberg, folder_copy, and s3_metadata DAGs.
"""

import logging
import math
import os
import random
import time
from functools import wraps

from airflow.models import Variable

logger = logging.getLogger(__name__)

__all__ = [
    "SSH_COMMAND_TIMEOUT",
    "_login_shell",
    "apply_bucket_credentials",
    "build_s3_opts",
    "cell_str",
    "cluster_login",
    "compute_dest_path",
    "configure_spark_s3",
    "execute_with_iceberg_retry",
    "get_config",
    "hive_type_to_spark_ddl",
    "is_permanent_error",
    "normalize_s3",
    "permanent_fail",
    "track_duration",
    "validate_bucket_endpoint_pairs",
]

# =============================================================================
# Hive → Spark SQL DDL type converter
# =============================================================================
def hive_type_to_spark_ddl(hive_type: str) -> str:
    """
    Convert a Hive metastore type string to the Spark SQL DDL notation.

    Hive's DESCRIBE returns struct fields in colon-separated format:
        struct<field_name:type,...>
    Spark SQL DDL requires space-separated format:
        struct<field_name type,...>

    The regex replaces every colon that is surrounded by word characters
    (i.e., inside struct field definitions at any nesting depth) with a
    space.  Primitives and map<k,v>/array<t> contain no colons, so they
    are returned unchanged.  Parameterised types such as decimal(18,4)
    and varchar(255) contain no colons either, so they pass through
    correctly without any parenthesis-depth tracking.
    """
    import re
    return re.sub(r"(?<=\w):(?=\w)", " ", hive_type)


# =============================================================================
# Duration tracking decorator using XCom
# =============================================================================
def track_duration(func):
    """Decorator to automatically track task duration via result dict."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        from datetime import datetime as dt
        start_time = dt.utcnow()
        result = func(*args, **kwargs)
        end_time = dt.utcnow()
        duration = (end_time - start_time).total_seconds()

        if isinstance(result, dict):
            result['_task_duration'] = duration

        return result

    return wrapper

# Substrings that identify a transient Iceberg commit conflict in the wrapped
# PySpark exception message. Other errors (schema mismatches, INSERT arity bugs,
# parse errors, permission denials, etc.) are permanent — retrying them just
# wastes minutes and obscures the real failure.
_ICEBERG_COMMIT_CONFLICT_MARKERS = (
    "CommitFailedException",
    "Cannot commit:",
)


def _is_iceberg_commit_conflict(exc: Exception) -> bool:
    msg = str(exc)
    return any(marker in msg for marker in _ICEBERG_COMMIT_CONFLICT_MARKERS)


def execute_with_iceberg_retry(spark, sql: str, max_retries: int = 6, task_label: str = ""):
    """Execute Spark SQL with retry logic, scoped to Iceberg commit conflicts only."""
    status = False
    counter = 0
    last_exception = None

    while not status and counter < max_retries:
        try:
            spark.sql(sql)
            status = True
        except Exception as e:
            # Permanent errors fail immediately — retrying won't help.
            if not _is_iceberg_commit_conflict(e):
                logger.error(
                    f"[IcebergRetry] {task_label} non-retryable error (no commit conflict marker): {str(e)[:300]}"
                )
                raise
            last_exception = e
            counter += 1
            if counter < max_retries:
                sleep_secs = random.choice([10, 20, 30, 40, 50])
                logger.warning(
                    f"[IcebergRetry] {task_label} hit commit conflict "
                    f"(attempt {counter}/{max_retries}). Retrying in {sleep_secs}s... Error: {str(e)[:200]}"
                )
                time.sleep(sleep_secs)
            else:
                logger.error(f"[IcebergRetry] {task_label} failed after {max_retries} attempts.")

    if not status:
        raise last_exception

# =============================================================================
# HELPER: configure dual-S3 credentials on a Spark session
# =============================================================================

def configure_spark_s3(spark, config: dict):
    """ Configure Spark with per-bucket S3A credentials for source and destination. """
    src_endpoint   = config.get('s3_source_endpoint')   or config.get('s3_endpoint', '')
    src_access_key = config.get('s3_source_access_key') or config.get('s3_access_key', '')
    src_secret_key = config.get('s3_source_secret_key') or config.get('s3_secret_key', '')

    dest_endpoint   = config.get('s3_dest_endpoint')   or config.get('s3_endpoint', '')
    dest_access_key = config.get('s3_dest_access_key') or config.get('s3_access_key', '')
    dest_secret_key = config.get('s3_dest_secret_key') or config.get('s3_secret_key', '')

    if src_endpoint:
        spark.conf.set("fs.s3a.endpoint", src_endpoint)
    if src_access_key:
        spark.conf.set("fs.s3a.access.key", src_access_key)
    if src_secret_key:
        spark.conf.set("fs.s3a.secret.key", src_secret_key)

    config['_src_endpoint']    = src_endpoint
    config['_src_access_key']  = src_access_key
    config['_src_secret_key']  = src_secret_key
    config['_dest_endpoint']   = dest_endpoint
    config['_dest_access_key'] = dest_access_key
    config['_dest_secret_key'] = dest_secret_key


def apply_bucket_credentials(spark, bucket_url: str, endpoint: str, access_key: str, secret_key: str):
    """Apply per-bucket S3A credentials given an s3a://bucket-name/... URL."""
    if not bucket_url.startswith('s3a://') or not (access_key or endpoint):
        return
    bucket_name = bucket_url.split('/')[2]
    if endpoint:
        spark.conf.set(f"fs.s3a.bucket.{bucket_name}.endpoint", endpoint)
    if access_key:
        spark.conf.set(f"fs.s3a.bucket.{bucket_name}.access.key", access_key)
    if secret_key:
        spark.conf.set(f"fs.s3a.bucket.{bucket_name}.secret.key", secret_key)


def compute_dest_path(source_location: str, dest_database: str, table_name: str,
                      dest_bucket: str, source_s3_prefix: str, dest_s3_prefix: str) -> str:
    """ Compute the destination S3 path for a table. """
    if source_s3_prefix and dest_s3_prefix and source_location.startswith(source_s3_prefix):
        relative = source_location[len(source_s3_prefix):].lstrip('/')
        return f"{dest_s3_prefix.rstrip('/')}/{relative}"
    return f"{dest_bucket.rstrip('/')}/{dest_database}/{table_name}"


def cell_str(val, default=''):
    """Safely convert a pandas cell value to a stripped string, handling NaN/None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return str(val).strip() or default


def normalize_s3(path: str) -> str:
    """Normalize S3 path prefixes to s3a://."""
    if not path:
        return path
    if path.startswith('s3n://'):
        return 's3a://' + path[6:]
    if path.startswith('s3://'):
        return 's3a://' + path[5:]
    if not path.startswith('s3a://'):
        return 's3a://' + path
    return path

# =============================================================================
# SHARED CONFIGURATION
# =============================================================================

def get_config() -> dict:
    """Shared configuration for all migration DAGs."""
    try:
        from airflow.operators.python import get_current_context
        _ctx = get_current_context()
        _run_id = _ctx["run_id"]
        _dag_run = _ctx.get("dag_run")
        _dag_run_conf = (_dag_run.conf if _dag_run and hasattr(_dag_run, 'conf') else {}) or {}
    except Exception:
        _run_id = None
        _dag_run_conf = {}

    def _var(base_key: str, env_var: str, default: str) -> str:
        if _run_id:
            try:
                scoped = Variable.get(f"{base_key}__{_run_id}", default_var=None)
                if scoped is not None:
                    return scoped
            except Exception:
                pass
        return Variable.get(base_key, default_var=os.getenv(env_var, default))


    dag_owner = _var('migration_dag_owner', 'MIGRATION_DAG_OWNER', '') \
                or _dag_run_conf.get('dag_owner', '') \
                or _dag_run_conf.get('spark_user', '') \
                or 'data-migration'

    config = {
        # SSH Configuration (for MapR migration)
        'ssh_conn_id': _var('cluster_ssh_conn_id','CLUSTER_SSH_CONN_ID', 'cluster_edge_ssh'),
        'edge_temp_path': _var('cluster_edge_temp_path', 'CLUSTER_EDGE_TEMP_PATH', '/tmp/migration'),

        # S3 Configuration
        'default_s3_bucket': _var('migration_default_s3_bucket', 'MIGRATION_DEFAULT_S3_BUCKET', 's3a://data-lake'),
        's3_endpoint': _var('s3_endpoint', 'S3_ENDPOINT', ''),
        's3_access_key': _var('s3_access_key', 'S3_ACCESS_KEY', ''),
        's3_secret_key': _var('s3_secret_key', 'S3_SECRET_KEY', ''),

        # DistCp Configuration
        'distcp_mappers': _var('migration_distcp_mappers', 'MIGRATION_DISTCP_MAPPERS', '50'),
        'distcp_bandwidth': _var('migration_distcp_bandwidth', 'MIGRATION_DISTCP_BANDWIDTH', '100'),
        'distcp_preserve_delete': str(
            _var(
                'migration_distcp_preserve_delete',
                'MIGRATION_DISTCP_PRESERVE_DELETE', 'true'
            )
        ).strip().lower() in ('1', 'true', 'yes', 'y', 'on'),

        # Spark Configuration
        'spark_conn_id': _var('migration_spark_conn_id', 'MIGRATION_SPARK_CONN_ID', 'spark_default'),

        # Tracking Configuration
        'tracking_database': _var('migration_tracking_database', 'MIGRATION_TRACKING_DATABASE', 'migration_tracking'),
        'tracking_location': _var('migration_tracking_location', 'MIGRATION_TRACKING_LOCATION', 's3a://data-lake/migration_tracking'),
        'report_output_location': _var('migration_report_location', 'MIGRATION_REPORT_LOCATION', 's3a://data-lake/migration_reports'),

        # Cluster type for display/reporting purposes ('MapR' or 'HDP')
        'cluster_type': _var('cluster_type', 'CLUSTER_TYPE', 'MapR'),
        # Cluster Authentication ('mapr', 'kinit', or 'none')
        'auth_method': _var('auth_method', 'AUTH_METHOD', 'mapr'),  # 'mapr' or 'kinit'
        'mapr_user': _var('mapr_user', 'MAPR_USER', ''),
        'mapr_ticketfile_location': _var('mapr_ticketfile_location', 'MAPR_TICKETFILE_LOCATION', '/tmp/maprticket_${USER}'),
        # HDFS nameservice (required for HDFS HA clusters; leave empty for MapR)
        'hdfs_nameservice': _var('hdfs_nameservice', 'HDFS_NAMESERVICE', ''),

        # Listing tool
        's3_listing_tool': Variable.get('s3_listing_tool', default_var=os.getenv('S3_LISTING_TOOL', 'hadoop')),

        # S3 source credentials
        's3_source_endpoint': _var('s3_source_endpoint', 'S3_SOURCE_ENDPOINT', ''),
        's3_source_access_key': _var('s3_source_access_key', 'S3_SOURCE_ACCESS_KEY', ''),
        's3_source_secret_key': _var('s3_source_secret_key', 'S3_SOURCE_SECRET_KEY', ''),

        # S3 destination credentials
        's3_dest_endpoint': _var('s3_dest_endpoint','S3_DEST_ENDPOINT', ''),
        's3_dest_access_key': _var('s3_dest_access_key', 'S3_DEST_ACCESS_KEY', ''),
        's3_dest_secret_key': _var('s3_dest_secret_key', 'S3_DEST_SECRET_KEY', ''),

        # Email / SMTP Configuration
        'smtp_conn_id': _var('migration_smtp_conn_id', 'MIGRATION_SMTP_CONN_ID', 'smtp_default'),
        'email_recipients': _var('migration_email_recipients', 'MIGRATION_EMAIL_RECIPIENTS', ''),

        # Path structure: when True (default), dest path is {bucket}/{database}/{table}.
        # When False, dest path is {bucket}/{table} (database folder omitted).
        'include_db_in_path': str(
            _var('migration_include_db_in_path', 'MIGRATION_INCLUDE_DB_IN_PATH', 'true')
        ).strip().lower() in ('1', 'true', 'yes', 'y', 'on'),

        'owner': dag_owner,
    }

    if dag_owner and dag_owner != 'data-migration':
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                spark.conf.set('spark.sql.kyuubi.session.user', dag_owner)
                logger.debug(f"[get_config] Set spark.sql.kyuubi.session.user={dag_owner}")
        except Exception:
            pass

    logger.debug(f"[get_config] dag_owner={dag_owner!r} run_id={_run_id!r}")
    return config

# SSH timeout: 24 hours
SSH_COMMAND_TIMEOUT = 86400


def _login_shell(cmd: str, cluster_type: str = 'MapR') -> str:
    """Wrap a shell command for execution over SSH.

    - MapR: sources ``~/.profile`` directly (customer-tested approach).
    - HDP (and any non-MapR cluster): uses ``bash -l`` (login shell) so
      /etc/profile.d/*.sh is sourced, ensuring JAVA_HOME, SPARK_HOME,
      HADOOP_HOME, HADOOP_CONF_DIR and PATH are set.
    """
    if cluster_type.upper() == 'MAPR':
        return f"source ~/.profile 2>/dev/null || true\n{cmd}"
    return f"bash -l <<'__LOGIN_SHELL_EOF__'\n{cmd}\n__LOGIN_SHELL_EOF__\n"


def cluster_login(run_id: str) -> dict:
    """SSH to edge node, perform cluster login (MapR or Kerberos), create temp dir.

    This is the core logic — DAG files wrap it with @task to make it an Airflow task.
    """
    from airflow.providers.ssh.hooks.ssh import SSHHook

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = f"{config['edge_temp_path']}/{run_id}"

    auth_method = config.get('auth_method', 'mapr')
    mapr_user = config.get('mapr_user', '')
    mapr_ticketfile = config.get('mapr_ticketfile_location', '/tmp/maprticket_${USER}')

    auth_script_parts = []

    auth_script_parts.append(f"""
echo "=== Cluster Authentication ({auth_method}) ==="

if [ "{auth_method}" = "mapr" ]; then
    MAPR_TICKETFILE_LOCATION="{mapr_ticketfile}"
    export MAPR_TICKETFILE_LOCATION

    if maprlogin print 2>/dev/null | grep -q "{mapr_user}"; then
        echo "Using existing valid MapR ticket"
    else
        echo "ERROR: No valid MapR ticket found"
        echo "Please ensure a valid MapR ticket exists before running this DAG"
        exit 1
    fi

elif [ "{auth_method}" = "kinit" ]; then
    echo "Kerberos authentication handled via login shell"

elif [ "{auth_method}" = "none" ]; then
    echo "No authentication required (auth_method=none)"

else
    echo "ERROR: Unknown auth_method: {auth_method}"
    exit 1
fi

echo "Authentication successful"
""")

    auth_script_parts.append(f"""
echo "=== Creating temp directory ==="
mkdir -p {temp_dir}
chmod 755 {temp_dir}

echo "CLUSTER_LOGIN_SUCCESS"
echo "TEMP_DIR={temp_dir}"
""")
    full_script = "set -e\n" + "\n".join(auth_script_parts)
    with ssh.get_conn() as client:
        _, stdout, stderr = client.exec_command(_login_shell(full_script), timeout=300)
        output = stdout.read().decode()
        error = stderr.read().decode()
        exit_code = stdout.channel.recv_exit_status()

        logger.info("=== Cluster Login Output ===")
        logger.info(output)

        if exit_code != 0:
            logger.error("=== Cluster Login Errors ===")
            logger.error(error)
            raise Exception(
                f"Cluster login setup failed with exit code {exit_code}\n"
                f"Error: {error}\n"
                f"Output: {output[-500:]}"
            )

        if "CLUSTER_LOGIN_SUCCESS" not in output:
            raise Exception(
                f"Cluster login setup incomplete - success marker not found\n"
                f"Output: {output[-500:]}"
            )

    return {'temp_dir': temp_dir, 'run_id': run_id}


def _s3a_committer_opts(config: dict) -> str:
    """Return S3A magic-committer JVM flags for HDP clusters.

    MapR has its own S3 connector and doesn't use FileOutputCommitter, so no
    flags are needed there. HDP 3.x uses vanilla S3A whose default v1 committer
    tries an atomic rename on S3 (unsupported), causing CommitterEventHandler
    failures. The magic committer bypasses that with S3 multipart uploads.
    """
    if config.get('cluster_type', 'MapR').upper() == 'HDP':
        return (
            " -Dmapreduce.outputcommitter.factory.scheme.s3a="
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
            " -Dfs.s3a.committer.name=magic"
        )
    return ""


def build_s3_opts(dest_bucket_url: str, config: dict, dest_endpoint: str = '') -> str:
    """Build per-bucket Hadoop S3A JVM options scoped to the destination bucket name.

    Resolution order:
      Case 1 — dest_endpoint is provided (from the Excel 'endpoint' column):
        - Endpoint  : used directly from dest_endpoint.
        - Credentials: looked up via Airflow Variable '<endpoint-hostname>_access_key/secret_key'
                       or env var '<ENDPOINT_HOSTNAME>_ACCESS_KEY/SECRET_KEY'.
        - Emitted as fs.s3a.bucket.<name>.* so multi-tenant rows in one DistCp command
          carry isolated credentials per bucket.
        The endpoint hostname is used as the credential slug so that two buckets with the
        same name on different tenant managers are always disambiguated by their endpoint.

      Case 2 — no dest_endpoint (original single-tenant behaviour, unchanged):
        - Uses the global config keys s3_endpoint / s3_access_key / s3_secret_key.
        - Emitted as unscoped fs.s3a.* properties, exactly as before this feature.
        - If those are also empty, Hadoop uses its own credential chain (e.g. IAM role).

    Credential Variable naming (Case 1):
      Slug = hostname of dest_endpoint, lowercased, e.g. "s3.tenant-a.example.com"
      Hyphens and dots are kept in the Airflow Variable name; env var uses underscores.

      Airflow Variable                          Environment variable
      ----------------------------------------- -------------------------------------------
      s3.tenant-a.example.com_access_key        S3_TENANT_A_EXAMPLE_COM_ACCESS_KEY  (masked)
      s3.tenant-a.example.com_secret_key        S3_TENANT_A_EXAMPLE_COM_SECRET_KEY  (masked)
    """
    from urllib.parse import urlparse

    raw = (dest_bucket_url or '').strip()
    for prefix in ('s3a://', 's3n://', 's3://'):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break
    bucket_name = urlparse(f's3a://{raw}').netloc.lower().strip()

    if not bucket_name:
        logger.warning(f"[build_s3_opts] Could not extract bucket name from '{dest_bucket_url}' — falling back to global credentials")

    endpoint = (dest_endpoint or '').strip()

    if endpoint and bucket_name:
        ep_hostname = urlparse(endpoint).hostname or urlparse(endpoint).netloc or endpoint
        ep_hostname = ep_hostname.lower().strip()
        env_slug = ep_hostname.upper().replace('.', '_').replace('-', '_')

        access_key = (Variable.get(f'{ep_hostname}_access_key',
                                   default_var=os.getenv(f'{env_slug}_ACCESS_KEY', ''))
                      or config.get('s3_access_key') or '')
        secret_key = (Variable.get(f'{ep_hostname}_secret_key',
                                   default_var=os.getenv(f'{env_slug}_SECRET_KEY', ''))
                      or config.get('s3_secret_key') or '')

        s3_opts = f" -Dfs.s3a.bucket.{bucket_name}.endpoint={endpoint}"
        if access_key:
            s3_opts += f" -Dfs.s3a.bucket.{bucket_name}.access.key={access_key}"
        if secret_key:
            s3_opts += f" -Dfs.s3a.bucket.{bucket_name}.secret.key={secret_key}"
        s3_opts += _s3a_committer_opts(config)
        return s3_opts

    global_endpoint   = config.get('s3_endpoint')   or ''
    global_access_key = config.get('s3_access_key') or ''
    global_secret_key = config.get('s3_secret_key') or ''

    s3_opts = ""
    if global_endpoint:
        s3_opts += f" -Dfs.s3a.endpoint={global_endpoint}"
    if global_access_key:
        s3_opts += f" -Dfs.s3a.access.key={global_access_key}"
    if global_secret_key:
        s3_opts += f" -Dfs.s3a.secret.key={global_secret_key}"
    s3_opts += _s3a_committer_opts(config)
    return s3_opts


def validate_bucket_endpoint_pairs(grouped: dict, config: dict) -> None:
    """Pre-flight check: verify each (bucket, endpoint) pair in the Excel config is reachable"""
    try:
        from urllib.parse import urlparse as _urlparse

        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        logger.warning(
            "[ValidateBucketEndpoint] boto3 not available — skipping pre-flight validation"
        )
        return

    errors = []
    checked: set = set()

    for (src_db, _dest_db, bucket_val, endpoint_val, _partition_filter), _group in grouped.items():
        if not endpoint_val:
            continue

        pair = (bucket_val, endpoint_val)
        if pair in checked:
            continue
        checked.add(pair)

        raw = bucket_val.strip()
        for prefix in ('s3a://', 's3n://', 's3://'):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                break
        bucket_name = _urlparse(f's3a://{raw}').netloc.lower().strip()

        if not bucket_name:
            errors.append(
                f"  - src_db={src_db}: could not extract bucket name from '{bucket_val}'"
            )
            continue

        ep_hostname = (
            _urlparse(endpoint_val).hostname
            or _urlparse(endpoint_val).netloc
            or endpoint_val
        )
        ep_hostname = ep_hostname.lower().strip()
        env_slug = ep_hostname.upper().replace('.', '_').replace('-', '_')

        access_key = (
            Variable.get(f'{ep_hostname}_access_key',
                         default_var=os.getenv(f'{env_slug}_ACCESS_KEY', ''))
            or config.get('s3_access_key') or ''
        )
        secret_key = (
            Variable.get(f'{ep_hostname}_secret_key',
                         default_var=os.getenv(f'{env_slug}_SECRET_KEY', ''))
            or config.get('s3_secret_key') or ''
        )

        try:
            s3 = boto3.client(
                's3',
                endpoint_url=endpoint_val,
                aws_access_key_id=access_key or None,
                aws_secret_access_key=secret_key or None,
            )
            s3.head_bucket(Bucket=bucket_name)
            logger.info(
                f"[ValidateBucketEndpoint] ✓ bucket='{bucket_name}' "
                f"reachable at endpoint='{endpoint_val}'"
            )
        except ClientError as exc:
            code = exc.response.get('Error', {}).get('Code', '')
            if code in ('403', 'AccessDenied'):
                logger.warning(
                    f"[ValidateBucketEndpoint] bucket='{bucket_name}' at "
                    f"endpoint='{endpoint_val}' returned 403 — bucket exists but "
                    f"credentials may lack full access. Proceeding."
                )
            elif code in ('404', 'NoSuchBucket'):
                errors.append(
                    f"  - src_db={src_db}: bucket='{bucket_name}' does NOT exist at "
                    f"endpoint='{endpoint_val}' (HTTP 404) — "
                    f"likely a bucket/endpoint mismatch in the Excel config"
                )
            else:
                errors.append(
                    f"  - src_db={src_db}: bucket='{bucket_name}' at "
                    f"endpoint='{endpoint_val}' returned unexpected S3 error "
                    f"{code}: {exc}"
                )
        except Exception as exc:
            errors.append(
                f"  - src_db={src_db}: could not reach endpoint='{endpoint_val}' "
                f"for bucket='{bucket_name}': {exc}"
            )

    if errors:
        raise Exception(
            f"[ParseExcel] Bucket/endpoint validation failed for {len(errors)} "
            f"pair(s). Fix the Excel config and re-trigger the DAG:\n"
            + "\n".join(errors)
        )



def _list_iceberg_tables(spark, base_path):
    """List subdirectories under base_path that contain a metadata/ folder."""
    try:
        from py4j.java_gateway import java_import

        java_import(spark._jvm, 'org.apache.hadoop.fs.*')
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(base_path),
            spark._jsc.hadoopConfiguration()
        )
        base = spark._jvm.org.apache.hadoop.fs.Path(base_path)
        if not fs.exists(base):
            return []

        status_list = fs.listStatus(base)
        tables = []
        for i in range(len(status_list)):
            if not status_list[i].isDirectory():
                continue
            name = status_list[i].getPath().getName()
            metadata_dir = spark._jvm.org.apache.hadoop.fs.Path(
                f"{base_path}/{name}/metadata"
            )
            if fs.exists(metadata_dir):
                tables.append(name)
        return sorted(tables)
    except Exception as e:
        logger.warning(f"[iceberg_helpers] Could not list tables at {base_path}: {e}")
        return []


def _match_tokens(table_names, tokens):
    """Match table names against token patterns using fnmatch."""
    import fnmatch as _fnmatch

    if '*' in tokens:
        return table_names
    seen = set()
    matched = []
    for tok in tokens:
        for t in table_names:
            if t not in seen and _fnmatch.fnmatch(t, tok):
                seen.add(t)
                matched.append(t)
    return matched

def _parse_transform(transform_str):
    """Parse an Iceberg partition transform string into (transform, param)."""
    if '[' in transform_str:
        name, rest = transform_str.split('[', 1)
        param = int(rest.rstrip(']'))
        return name, param
    return transform_str, None


def _extract_partition_spec(metadata):
    """Extract partition spec from Iceberg metadata.

    Returns (spec_fields, is_partitioned) where spec_fields is a list of dicts:
        {'source_column': str, 'transform': str, 'name': str, 'param': int | None}
    """
    default_spec_id = metadata.get('default-spec-id', 0)
    specs = metadata.get('partition-specs', [])

    spec = None
    for s in specs:
        if s.get('spec-id') == default_spec_id:
            spec = s
            break
    if spec is None and specs:
        spec = specs[-1]

    if not spec or not spec.get('fields'):
        return [], False

    current_schema_id = metadata.get('current-schema-id', 0)
    schemas = metadata.get('schemas', [])
    schema = None
    for s in schemas:
        if s.get('schema-id') == current_schema_id:
            schema = s
            break
    if schema is None and schemas:
        schema = schemas[-1]

    field_id_to_name = {}
    if schema:
        for f in schema.get('fields', []):
            field_id_to_name[f['id']] = f['name']

    spec_fields = []
    for pf in spec.get('fields', []):
        source_id = pf.get('source-id')
        source_column = field_id_to_name.get(source_id, f'field_{source_id}')
        transform, param = _parse_transform(pf.get('transform', 'identity'))
        spec_fields.append({
            'source_column': source_column,
            'transform': transform,
            'name': pf.get('name', source_column),
            'param': param,
        })

    return spec_fields, len(spec_fields) > 0


def _extract_row_count(metadata):
    """Extract total row count from the current snapshot summary."""
    current_snapshot_id = metadata.get('current-snapshot-id')
    if current_snapshot_id is None:
        return 0

    for snap in metadata.get('snapshots', []):
        if snap.get('snapshot-id') == current_snapshot_id:
            summary = snap.get('summary', {})
            return int(summary.get('total-records', 0))

    return 0


def _read_iceberg_metadata(spark, table_path):
    """Read and parse the latest Iceberg metadata.json from S3."""
    import json as _json

    from py4j.java_gateway import java_import

    java_import(spark._jvm, 'org.apache.hadoop.fs.*')

    metadata_file = _resolve_metadata_file(spark, table_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(table_path),
        spark._jsc.hadoopConfiguration()
    )

    reader = spark._jvm.java.io.BufferedReader(
        spark._jvm.java.io.InputStreamReader(
            fs.open(spark._jvm.org.apache.hadoop.fs.Path(metadata_file)), "UTF-8"
        )
    )
    try:
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
    finally:
        reader.close()

    return _json.loads('\n'.join(lines))


def _extract_schema(metadata):
    """Extract schema from Iceberg metadata as list of {name, type} dicts."""
    current_schema_id = metadata.get('current-schema-id', 0)
    schemas = metadata.get('schemas', [])

    schema = None
    for s in schemas:
        if s.get('schema-id') == current_schema_id:
            schema = s
            break
    if schema is None and schemas:
        schema = schemas[-1]
    if schema is None:
        return []

    return [
        {'name': field['name'], 'type': _map_iceberg_type(field['type'])}
        for field in schema.get('fields', [])
    ]

def _rebase_table_path(table_path: str, from_prefix: str, to_prefix: str) -> str:
    """Return table_path with its leading from_prefix replaced by to_prefix.

    Raises ValueError if table_path does not start with from_prefix, which
    would indicate a misconfigured Excel row (dest path outside dest_prefix).
    """
    base = from_prefix.rstrip('/')
    if not table_path.startswith(base):
        raise ValueError(
            f"table_path {table_path!r} does not start with prefix {base!r} — "
            "check that source_s3_prefix / dest_s3_prefix in the Excel config "
            "match the actual table locations"
        )
    return to_prefix.rstrip('/') + table_path[len(base):]


def _resolve_metadata_file(spark, table_path):
    """Resolve the path to the latest Iceberg metadata.json file for a table."""
    from py4j.java_gateway import java_import

    java_import(spark._jvm, 'org.apache.hadoop.fs.*')

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(table_path),
        spark._jsc.hadoopConfiguration()
    )

    metadata_dir = f"{table_path}/metadata"
    hint_path = spark._jvm.org.apache.hadoop.fs.Path(
        f"{metadata_dir}/version-hint.text"
    )

    if fs.exists(hint_path):
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(hint_path))
        )
        version = reader.readLine().strip()
        reader.close()
        return f"{metadata_dir}/v{version}.metadata.json"

    status_list = fs.listStatus(
        spark._jvm.org.apache.hadoop.fs.Path(metadata_dir)
    )
    metadata_files = []
    for i in range(len(status_list)):
        name = status_list[i].getPath().getName()
        if name.endswith('.metadata.json'):
            metadata_files.append(name)
    if not metadata_files:
        raise FileNotFoundError(f"No metadata.json files found in {metadata_dir}")
    import re as _re

    def _version_key(name):
        # handles v{N}.metadata.json and {N:05d}-{uuid}.metadata.json
        m = _re.match(r'(?:v)?(\d+)', name)
        return int(m.group(1)) if m else -1

    latest = sorted(metadata_files, key=_version_key)[-1]
    return f"{metadata_dir}/{latest}"

# =============================================================================
# Iceberg metadata helpers
# =============================================================================

ICEBERG_TYPE_MAP = {
    'boolean': 'BOOLEAN',
    'int': 'INT',
    'long': 'BIGINT',
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'date': 'DATE',
    'time': 'STRING',
    'timestamp': 'TIMESTAMP',
    'timestamptz': 'TIMESTAMP',
    'string': 'STRING',
    'binary': 'BINARY',
    'uuid': 'STRING',
}


def _map_iceberg_type(iceberg_type):
    """Map an Iceberg type to a Spark SQL type string.

    Handles primitives plus nested struct / list / map recursively, emitting
    the same forms Spark's DESCRIBE returns for an Iceberg-backed table:
      STRUCT<a:INT,b:STRING>   ←  iceberg struct<a:int,b:string>
      ARRAY<INT>               ←  iceberg list<int>
      MAP<STRING,INT>          ←  iceberg map<string,int>

    Casing doesn't matter because validate_dest_tables lowercases both sides
    before comparison; the structural form must match exactly or every
    complex-typed column flags a false schema mismatch.
    """
    if isinstance(iceberg_type, dict):
        kind = str(iceberg_type.get('type', '')).lower()
        if kind == 'struct':
            inner = ','.join(
                f"{f['name']}:{_map_iceberg_type(f['type'])}"
                for f in iceberg_type.get('fields', [])
            )
            return f"STRUCT<{inner}>"
        if kind == 'list':
            return f"ARRAY<{_map_iceberg_type(iceberg_type.get('element', 'string'))}>"
        if kind == 'map':
            key_t = _map_iceberg_type(iceberg_type.get('key', 'string'))
            val_t = _map_iceberg_type(iceberg_type.get('value', 'string'))
            return f"MAP<{key_t},{val_t}>"
        return 'STRING'

    t = str(iceberg_type).lower()
    if t in ICEBERG_TYPE_MAP:
        return ICEBERG_TYPE_MAP[t]
    if t.startswith('decimal'):
        return t.upper()
    if t.startswith('fixed'):
        return 'BINARY'
    return 'STRING'

# =============================================================================
# ERROR CLASSIFICATION — permanent vs transient
# =============================================================================

_PERMANENT_ERROR_MARKERS: dict[str, tuple[str, ...]] = {

    # ── parse_*_excel ──────────────────────────
    # Every parse failure is a data-quality / config problem.
    "parse_excel": (
        "no valid rows",
        "missing required column",
        "valueerror",
        "zipfile.badzipfile",
        "not a zip file",
        "xlrd",
        "source_path",
        "target_bucket",
    ),

    # ── migrate_tables_to_iceberg ─────────────────────────────────────────
    # Iceberg CommitFailedException → transient (handled inside execute_with_iceberg_retry).
    # Corrupt data / unreadable files → permanent.
    "iceberg_migrate": (
        "corrupt parquet",
        "corruptrecordexception",
        "parquet.io.parquetdecodingexception",
        "footer is missing",
        "invalid parquet file",
        "not a parquet file",
        "could not read footer",
        "table_or_view_not_found",
        "cannot be found. verify the spelling",
    ),

    # ── send_*_report_email ──────────────────────────────────────────────
    # SMTP transient timeout → retried (not listed). Bad creds / missing
    # report file → permanent.
    "send_email": (
        "no recipients configured",
        "no such key",
        "smtpauthenticationerror",
        "authentication failed",
    ),
}


def is_permanent_error(task_category: str, exc: Exception) -> bool:
    """ Return True if *exc* matches a known permanent-failure pattern. """
    markers = _PERMANENT_ERROR_MARKERS.get(task_category, ())
    if not markers:
        return False
    msg = str(exc).lower()
    return any(m.lower() in msg for m in markers)


def permanent_fail(task_label: str, exc: Exception) -> None:
    """ Raise AirflowFailException so Airflow marks the task FAILED immediately without consuming remaining retries. """
    from airflow.exceptions import AirflowFailException

    logger.error(
        "[PermanentFail] %s — permanent error, retries suppressed: %s: %s",
        task_label, type(exc).__name__, str(exc)[:400],
    )
    raise AirflowFailException(
        f"{task_label} failed permanently ({type(exc).__name__}): {str(exc)[:400]}"
    ) from exc
