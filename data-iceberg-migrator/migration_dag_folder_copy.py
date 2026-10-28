"""
DAG 3: Folder-Only Data Copy

Copies folders from MapR/HDFS to S3 via DistCp — no Hive metadata.
For migrating raw data directories that don't have associated Hive tables.

Excel columns: source_path | target_bucket | dest_folder | endpoint | split_subfolders (optional)

Performance / diagnostics knobs (DAG Param > Airflow Variable folder_copy_<name> > env FOLDER_COPY_<NAME>):
  - listing_strategy : 'ls_r' (default, `hadoop fs -ls -R` + `-du -s`) or 'count' (`hadoop fs -count`)
  - split_subfolders : copy immediate subfolders as separate DistCp tasks instead of one big folder
  - split_depth      : how many directory levels to split when split_subfolders is on (default 1)
  - max_split_entries: safety cap on generated tasks; above it the folder is copied unsplit (default 1000)
  - max_loose_files  : safety cap on loose files that sit directly in a split folder (default 200)
"""

import contextlib
import json
import logging
import os
import re
import shlex
import sys
import time
from datetime import datetime, timedelta
from html import escape as _html_escape
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))

from migrator_utils.migrations.shared import (
    DISTCP_CHECK_CMD,
    SSH_COMMAND_TIMEOUT,
    _cluster_auth_check_cmd,
    _login_shell,
    build_s3_opts,
    cluster_login,
    distcp_jvm_opts,
    distcp_sizing_mode,
    execute_with_iceberg_retry,
    get_config,
    is_permanent_error,
    normalize_s3,
    permanent_fail,
    size_distcp_job,
    sql_quote,
)

_dag_stem = Path(__file__).stem
logger = logging.getLogger(__name__)

_dag_dir = Path(__file__).resolve().parent
_config_dir = str(_dag_dir / 'migrator_utils' / 'migration_configs')
if os.path.isdir(_config_dir):
    load_dotenv(os.path.join(_config_dir, 'env.shared'))
    load_dotenv(os.path.join(_config_dir, f'env.{_dag_stem}'), override=True)
else:
    logger.warning(f"Config directory {_config_dir} not found — env files not loaded, using Airflow Variables / defaults")

def _resolve_dag_owner() -> str:
    """Owner shown in Airflow, and the task pod's HADOOP_USER_NAME.

    A pod_mutation_hook in nx1-full-package turns the dag.owners column this
    feeds into HADOOP_USER_NAME/SPARK_USER, so this is the principal Ranger
    authorizes. Blank or comma-bearing values fall through to the literal
    deploy.py rewrites via --owner -- the hook splits dag.owners on ','.

    Never reads an nx1_ key: parse time has no dag_run to identify an origin
    by. One value per DAG, refreshed each parse; the per-run owner is separate,
    resolved by get_config() into spark.sql.kyuubi.session.user.
    """
    try:
        from airflow.models import Variable
        owner = (Variable.get('migration_dag_owner', default_var='') or '').strip()
        if owner and ',' not in owner:
            return owner
    except Exception:
        pass
    return 'data-migration'

default_args = {
    "owner": _resolve_dag_owner(),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def _folder_copy_dag_failure_callback(context) -> None:
    try:
        dag_run = None
        task_instance = None
        try:
            dag_run = context.get('dag_run')
            task_instance = context.get('task_instance')
        except Exception:
            pass
        run_ref = getattr(dag_run, 'run_id', None)
        task_ref = getattr(task_instance, 'task_id', None)
        logger.error(
            f"[FolderCopy] DAG failure callback — dag_run_id={run_ref} task_id={task_ref}"
        )

        tracking_run_id = None
        try:
            creator_ti = dag_run.get_task_instance('create_data_copy_run')
            if creator_ti is not None:
                tracking_run_id = creator_ti.xcom_pull(
                    task_ids='create_data_copy_run', key='return_value'
                )
        except Exception as e:
            logger.warning(
                f"[FolderCopy] Failure callback could not read the tracking run_id XCom: {e}"
            )
        if not tracking_run_id:
            logger.warning(
                "[FolderCopy] Failure callback found no tracking run_id — "
                "the run failed before create_data_copy_run wrote a record."
            )
            return

        logger.warning(
            f"[FolderCopy] Run {tracking_run_id} ended without a report. "
            f"data_copy_runs still shows status RUNNING for {tracking_run_id} — "
            f"re-run the DAG or reconcile the tracking row manually."
        )
    except Exception:
        with contextlib.suppress(Exception):
            logger.error("[FolderCopy] DAG failure callback raised and was suppressed")


# =============================================================================
# FOLDER-COPY DIAGNOSTICS, LISTING AND SUBFOLDER HELPERS
# =============================================================================

_LISTING_STRATEGIES = ('ls_r', 'count')

# Markers printed by the generated edge-node script (see run_folder_distcp_ssh).
_PHASE_START_RE = re.compile(r'^PHASE_START (\w+)', re.MULTILINE)
_PHASE_END_RE = re.compile(r'^PHASE_END (\w+) seconds=(\d+)', re.MULTILINE)
_MEMORY_KILL_RE = re.compile(
    r'beyond physical memory limits|Container killed by|OutOfMemoryError|GC overhead limit exceeded',
    re.IGNORECASE,
)
# One `hadoop fs -ls` entry (MapR-FS and HDFS): perms, replication, owner, group, size, date, time, path.
# Owner and group may contain spaces on AD/SSSD-joined clusters (e.g. "domain users"), so they are
# matched loosely; the size/date anchor after them is what identifies the fields.
_LS_LINE_RE = re.compile(
    r'^(?P<kind>[-dlbcps])[rwxsStT-]{9}[+.@]?\s+\S+\s+.+?\s+(?P<size>\d+)\s+'
    r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?\s+(?P<path>.+?)\s*$'
)
_LS_HEADER_RE = re.compile(r'^Found (\d+) items?$')


def _truthy(value) -> bool:
    # '1.0' is included because pandas reads an all-numeric/boolean Excel column as float.
    return str(value).strip().lower() in ('true', '1', '1.0', 'yes', 'y', 'on')


def _sql_num(value) -> str:
    """SQL literal for an optional DOUBLE value."""
    if value is None or value == '':
        return 'NULL'
    try:
        return repr(float(value))
    except (TypeError, ValueError):
        return 'NULL'


def _sql_int(value) -> str:
    """SQL literal for an optional INT value."""
    if value is None or value == '':
        return 'NULL'
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return 'NULL'


def _resolve_setting(name: str, context: dict = None, default=None):
    """DAG Param > Airflow Variable folder_copy_<name> > env FOLDER_COPY_<NAME> > default.

    Params make A/B runs on the same data possible without redeploying (e.g. the
    same folder with listing_strategy=ls_r and then =count).
    """
    value = None
    try:
        params = (context or {}).get('params') or {}
        value = params.get(name)
    except Exception:
        value = None
    if value is not None and str(value).strip() != '':
        return value
    try:
        from airflow.models import Variable
        value = Variable.get(f'folder_copy_{name}', default_var=None)
    except Exception:
        value = None
    if value is not None and str(value).strip() != '':
        return value
    value = os.environ.get(f'FOLDER_COPY_{name.upper()}')
    if value is not None and value.strip() != '':
        return value
    return default


def _resolve_listing_strategy(context: dict = None) -> str:
    value = str(_resolve_setting('listing_strategy', context, default='ls_r')).strip().lower()
    if value not in _LISTING_STRATEGIES:
        logger.warning(
            f"[FolderCopy] Unknown listing_strategy {value!r} — using 'ls_r'. "
            f"Valid values: {', '.join(_LISTING_STRATEGIES)}"
        )
        return 'ls_r'
    return value


def _allow_delete(config: dict) -> bool:
    """folder_copy_allow_delete: config key > Airflow Variable > env FOLDER_COPY_ALLOW_DELETE."""
    raw = config.get('folder_copy_allow_delete')
    if raw is None:
        try:
            from airflow.models import Variable
            raw = Variable.get('folder_copy_allow_delete', default_var=None)
        except Exception:
            raw = None
    if raw is None:
        raw = os.environ.get('FOLDER_COPY_ALLOW_DELETE')
    return _truthy(raw)


def _int_setting(name: str, context: dict, default: int, minimum: int) -> int:
    """Integer setting that falls back to `default` with a warning instead of failing the task."""
    raw = _resolve_setting(name, context, default=default)
    try:
        value = float(str(raw).strip())
        if not value.is_integer() or value < minimum:
            raise ValueError('not an integer >= %d' % minimum)
        return int(value)
    except (TypeError, ValueError):
        logger.warning(f"[FolderSplit] Invalid {name}={raw!r} — using default {default}")
        return default


def _airflow_map_limit() -> int:
    """Airflow's core.max_map_length: the most dynamic-task mappings one DAG run may create."""
    try:
        from airflow.configuration import conf
        return conf.getint('core', 'max_map_length', fallback=1024)
    except Exception:
        return 1024


def _diagnostics_json(diag: dict, limit: int = 4000) -> str:
    """JSON for the diagnostics column that always parses, even when it is long.

    edge_groups (one entry per AD group) is the field that can grow large, so it is
    shortened first and dropped only if the rest still does not fit.
    """
    text = json.dumps(diag)
    if len(text) <= limit:
        return text
    groups = str(diag.get('edge_groups') or '')
    keep = max(0, len(groups) - (len(text) - limit) - 3)
    diag = {**diag, 'edge_groups': (groups[:keep] + '...') if keep else None}
    text = json.dumps(diag)
    if len(text) > limit:
        diag = {**diag, 'edge_groups': None}
        text = json.dumps(diag)
    if len(text) > limit:
        text = json.dumps({'truncated': True})
    return text


def _listing_snippet(
    hadoop_cmd: str,
    path_expr: str,
    file_var: str,
    size_var: str,
    strategy: str = 'ls_r',
    shallow: bool = False,
) -> str:
    """Shell lines that set `file_var` / `size_var` for `path_expr`.

    ls_r    : the original behaviour — a recursive `-ls -R` plus a `-du -s`.
    count   : one `-count` call (file count and content size together).
    shallow : direct children only (used for the loose files of a split folder).

    `path_expr` is inserted verbatim, so the caller supplies its own quoting.
    Plain placeholders (not an f-string) keep the awk braces readable.
    """
    if shallow:
        template = (
            "_SHALLOW_LS=$(@HC@ -ls @PATH@ 2>/dev/null | grep '^-')\n"
            "@FV@=$(printf '%s\\n' \"$_SHALLOW_LS\" | grep -c '^-')\n"
            # Size is the field just before the YYYY-MM-DD date: a fixed column breaks when
            # owner/group are omitted (Hadoop 2.7 S3A) or contain spaces.
            "@SV@=$(printf '%s\\n' \"$_SHALLOW_LS\" | awk '/^-/ {for (i = 2; i < NF; i++) if ($i ~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$/) {s += $(i-1); break}} END {print s+0}')\n"
        )
    elif strategy == 'count':
        template = (
            "_COUNT_LINE=$(@HC@ -count @PATH@ 2>/dev/null | tail -1)\n"
            "@FV@=$(echo \"$_COUNT_LINE\" | awk '{print $2}')\n"
            "@SV@=$(echo \"$_COUNT_LINE\" | awk '{print $3}')\n"
        )
    else:
        template = (
            "@FV@=$(@HC@ -ls -R @PATH@ 2>/dev/null | grep '^-' | wc -l)\n"
            "@SV@=$(@HC@ -du -s @PATH@ 2>/dev/null | awk '{print $1}')\n"
        )
    return (
        template.replace('@HC@', hadoop_cmd)
        .replace('@PATH@', path_expr)
        .replace('@FV@', file_var)
        .replace('@SV@', size_var)
    )


def _last_open_phase(output: str):
    """Name of the most recent PHASE_START with no matching PHASE_END, or None."""
    ended = {name for name, _ in _PHASE_END_RE.findall(output or '')}
    open_phases = [p for p in _PHASE_START_RE.findall(output or '') if p not in ended]
    return open_phases[-1] if open_phases else None


def _read_remote_command(
    channel,
    partial_sink: list,
    deadline_seconds: float,
    label: str = '',
    heartbeat_seconds: float = 300.0,
) -> tuple:
    """Drain an SSH exec channel incrementally; return (stdout, stderr, exit_code).

    `stdout.read()` returns nothing until the command ends, so a command that
    stalls (e.g. the recursive S3 listing) loses every line it already printed.
    Chunks are appended to `partial_sink` as they arrive, so the caller can still
    see which PHASE_START marker was the last one reached after a timeout or an
    SSH drop, and a heartbeat shows the same thing in the task log while it runs.
    """
    err_chunks = []
    started = time.monotonic()
    last_beat = started

    def _drain() -> bool:
        got = False
        while channel.recv_ready():
            partial_sink.append(channel.recv(65536).decode('utf-8', errors='replace'))
            got = True
        while channel.recv_stderr_ready():
            err_chunks.append(channel.recv_stderr(65536).decode('utf-8', errors='replace'))
            got = True
        return got

    while True:
        got = _drain()
        if channel.exit_status_ready():
            _drain()
            break
        now = time.monotonic()
        if now - started > deadline_seconds:
            raise TimeoutError(
                f"{label} still running after {deadline_seconds:.0f}s; last phase reached: "
                f"{_last_open_phase(''.join(partial_sink)) or 'none (no phase marker seen)'}"
            )
        if now - last_beat >= heartbeat_seconds:
            logger.info(
                f"[FolderDistCp] {label} running for {now - started:.0f}s — current phase: "
                f"{_last_open_phase(''.join(partial_sink)) or 'n/a'}"
            )
            last_beat = now
        if not got:
            time.sleep(1)
    return ''.join(partial_sink), ''.join(err_chunks), channel.recv_exit_status()


def _build_run_diagnostics(
    output: str,
    exit_code,
    yarn_ids: list,
    listing_strategy: str,
    shallow: bool = False,
    extra: dict = None,
) -> dict:
    """Turn the edge-script output into the fields that separate the failure modes.

    - failure_phase        : the phase that was running when it stopped (an unmatched
                             PHASE_START), 'distcp' for a DistCp error, else None.
    - yarn_app_created     : False + a pre-DistCp failure_phase = the run died before
                             DistCp was submitted (the listing/discovery problem).
    - memory_kill_suspected: container-kill / OOM text or exit 137 (the separate
                             1 GB-container issue).
    - edge_*               : edge node identity, SSSD state and groups, so the
                             bad-edge-node permission failure can be ruled in or out.
    """
    output = output or ''
    ended = {name: int(sec) for name, sec in _PHASE_END_RE.findall(output)}

    def _kv(key: str) -> str:
        match = re.search(rf'^{key}=(.*?)\r?$', output, re.MULTILINE)
        return match.group(1).strip() if match else ''

    distcp_exit_raw = _kv('DISTCP_EXIT_CODE')
    distcp_exit = int(distcp_exit_raw) if distcp_exit_raw.lstrip('-').isdigit() else None
    open_phase = _last_open_phase(output)

    if open_phase:
        failure_phase = open_phase
    elif distcp_exit not in (None, 0):
        failure_phase = 'distcp'
    elif exit_code not in (0, None):
        failure_phase = 'script'
    elif exit_code is None:
        # No exit status: the SSH session died or Python raised. If phases had already
        # completed the script itself got further than startup.
        failure_phase = 'after_script' if ended else 'ssh_or_startup'
    else:
        failure_phase = None

    diag = {
        'failure_phase': failure_phase,
        'yarn_app_created': bool(yarn_ids),
        'script_exit_code': exit_code,
        'distcp_exit_code': distcp_exit,
        'memory_kill_suspected': bool(_MEMORY_KILL_RE.search(output)) or exit_code == 137 or distcp_exit == 137,
        'phase_seconds': ended,
        'listing_strategy': listing_strategy,
        'shallow': shallow,
        'edge_host': _kv('EDGE_HOST'),
        'edge_groups': _kv('EDGE_GROUPS'),
        'edge_sssd_running': _kv('EDGE_SSSD_RUNNING'),
        'edge_mem_available_mb': _kv('EDGE_MEM_AVAILABLE_MB'),
    }
    if extra:
        diag.update(extra)
    return diag


def _list_immediate_children(ssh, config: dict, path: str) -> tuple:
    """Return (sorted subdirectory paths, sorted [(file path, size)]) directly under `path`."""
    # Force a UTF-8 locale: the edge's login shell may be C/POSIX, which prints non-ASCII
    # names as '?', and the split would then copy them to a different S3 key.
    cmd = (
        'LC_ALL=C.UTF-8 HADOOP_CLIENT_OPTS="-Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 '
        f'$HADOOP_CLIENT_OPTS" hadoop fs -ls "{path}"'
    )
    with ssh.get_conn() as client:
        _, stdout, stderr = client.exec_command(
            _login_shell(cmd, config.get('cluster_type', 'MapR')),
            timeout=SSH_COMMAND_TIMEOUT,
        )
        out = stdout.read().decode(errors='replace')
        err = stderr.read().decode(errors='replace')
        rc = stdout.channel.recv_exit_status()
    if rc != 0:
        raise RuntimeError(f"`hadoop fs -ls {path}` exited {rc}: {err.strip()[:300]}")

    # Fail safe: any line we cannot classify raises, so the caller copies the folder whole
    # instead of silently leaving entries out of the split.
    dirs, files = [], []
    expected = None
    parsed = 0
    for raw_line in out.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        header = _LS_HEADER_RE.match(line)
        if header:
            expected = int(header.group(1))
            continue
        match = _LS_LINE_RE.match(line)
        if not match:
            raise ValueError(f"unrecognised `hadoop fs -ls` line under {path}: {line[:200]!r}")
        parsed += 1
        entry_path = match.group('path')
        kind = match.group('kind')
        # '?' is what the shell prints for undecodable bytes, and U+FFFD is what decoding
        # yields. Either way the name is not the real one, so the split cannot be trusted.
        if any(ch in entry_path for ch in ('?', '�')):
            raise ValueError(f"{path} has an entry with an unverifiable name ({entry_path!r}); cannot split safely")
        if kind == 'd':
            if os.path.basename(entry_path.rstrip('/')) != '.snapshot':
                dirs.append(entry_path)
        elif kind == '-':
            files.append((entry_path, int(match.group('size'))))
        else:
            raise ValueError(f"{path} contains a non-regular entry ({entry_path}); cannot split safely")
    if expected is not None and expected != parsed:
        raise ValueError(f"`hadoop fs -ls {path}` reported {expected} items but {parsed} were parsed")
    return sorted(dirs), sorted(files)


def _split_folder_config(ssh, config: dict, cfg: dict, depth: int, max_loose: int) -> list:
    """Replace one folder config by one config per subfolder (plus one for loose files).

    A folder with no subfolders is returned unchanged: there is nothing to split.
    Files that sit directly in the folder become a single 'shallow' entry that
    copies just those files, so splitting never drops data. Raises ValueError when
    the loose files exceed `max_loose` (the caller then copies the folder unsplit).
    """
    dirs, files = _list_immediate_children(ssh, config, cfg['source_path'])
    if not dirs:
        return [cfg]
    if len(files) > max_loose:
        raise ValueError(
            f"{cfg['source_path']} has {len(files)} files directly inside it "
            f"(limit {max_loose}); they cannot be copied as a separate batch"
        )

    top_parent = cfg.get('parent_source_path') or cfg['source_path']
    base_dest = str(cfg['dest_folder']).rstrip('/')
    entries = []
    for child_path in dirs:
        child = {
            **cfg,
            'source_path': child_path,
            'dest_folder': f"{base_dest}/{os.path.basename(child_path.rstrip('/'))}",
            'parent_source_path': top_parent,
            'split_role': 'subfolder',
            'split_subfolders': False,
        }
        if depth > 1:
            entries.extend(_split_folder_config(ssh, config, child, depth - 1, max_loose))
        else:
            entries.append(child)
    if files:
        entries.append({
            **cfg,
            'shallow': True,
            'source_files': [p for p, _ in files],
            'source_files_bytes': sum(size for _, size in files),
            'parent_source_path': top_parent,
            'split_role': 'loose_files',
            'split_subfolders': False,
        })
    return entries


@task
def cluster_login_setup(run_id: str) -> dict:
    """SSH to edge, perform cluster login (MapR or Kerberos), create temp dir."""
    return cluster_login(run_id)

# =============================================================================
# DAG 3: FOLDER-ONLY DATA COPY TASKS
# =============================================================================

@task
def validate_prerequisites_folder_copy() -> dict:
    """Validate SSH connectivity and Hadoop DistCp availability before starting the folder copy."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    checks = {
        'ssh_connectivity': False,
        'cluster_auth': False,
        'hadoop_distcp_available': False,
        'hadoop_fs_available': False,
    }
    errors = []

    logger.info("=" * 60)
    logger.info("[FolderCopy] STARTING PRE-DAG VALIDATION")
    logger.info("=" * 60)

    try:
        with ssh.get_conn() as client:

            # 1. SSH connectivity
            logger.info("[1/4] Testing SSH connectivity...")
            _, stdout, stderr = client.exec_command(_login_shell('echo "SSH_TEST_OK"', config.get('cluster_type', 'MapR')), timeout=30)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()
            if exit_code == 0 and "SSH_TEST_OK" in output:
                checks['ssh_connectivity'] = True
                logger.info("SSH connectivity: PASSED")
            else:
                msg = f"SSH command failed with exit code {exit_code}"
                errors.append(f"SSH: {msg}")
                logger.error(f"SSH connectivity: FAILED - {msg}")

            # 2. Cluster authentication (MapR ticket or Kerberos TGT), same check as DAG 1
            auth_method = config.get('auth_method', 'mapr')
            logger.info(f"[2/4] Testing cluster authentication (auth_method={auth_method})...")
            auth_cmd = _cluster_auth_check_cmd(auth_method, config.get('service_account_user_id', ''))
            _, stdout, stderr = client.exec_command(_login_shell(auth_cmd, config.get('cluster_type', 'MapR')), timeout=30)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()
            if exit_code == 0 and "CLUSTER_AUTH_OK" in output:
                checks['cluster_auth'] = True
                logger.info(f"Cluster auth ({auth_method}): PASSED")
            else:
                msg = output.replace("CLUSTER_AUTH_FAIL: ", "").strip() or f"Auth check failed (exit={exit_code})"
                errors.append(f"Cluster auth: {msg}")
                logger.error(f"Cluster auth ({auth_method}): FAILED - {msg}")

            # 3. Hadoop DistCp
            logger.info("[3/4] Testing hadoop distcp availability...")
            _, stdout, stderr = client.exec_command(
                _login_shell(DISTCP_CHECK_CMD, config.get('cluster_type', 'MapR')), timeout=60
            )
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()
            if "DISTCP_OK" in output:
                checks['hadoop_distcp_available'] = True
                logger.info("Hadoop DistCp: PASSED")
            else:
                msg = "hadoop distcp not found or not executable"
                errors.append(f"DistCp: {msg}")
                logger.error(f"Hadoop DistCp: FAILED - {msg}")

            # 4. Hadoop FS
            logger.info("[4/4] Testing Hadoop FS commands...")
            hdfs_nameservice = config.get('hdfs_nameservice', '')
            fs_root = f"hdfs://{hdfs_nameservice}/" if hdfs_nameservice else "/"
            test_cmd = f"if hadoop fs -ls {fs_root} > /dev/null 2>&1; then echo HADOOP_FS_OK; else echo HADOOP_FS_FAIL; fi"
            _, stdout, stderr = client.exec_command(_login_shell(test_cmd, config.get('cluster_type', 'MapR')), timeout=60)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()
            if "HADOOP_FS_OK" in output:
                checks['hadoop_fs_available'] = True
                logger.info("Hadoop FS: PASSED")
            else:
                msg = "hadoop fs -ls / failed"
                errors.append(f"Hadoop FS: {msg}")
                logger.error(f"Hadoop FS: FAILED - {msg}")

    except Exception as e:
        msg = f"SSH connection failed: {str(e)}"
        errors.append(f"SSH: {msg}")
        errors.append("Cluster auth: Skipped due to SSH failure")
        errors.append("DistCp: Skipped due to SSH failure")
        errors.append("Hadoop FS: Skipped due to SSH failure")
        logger.error(f"SSH connectivity: FAILED - {msg}")

    logger.info("=" * 60)
    logger.info("[FolderCopy] VALIDATION SUMMARY")
    logger.info("=" * 60)

    if errors:
        logger.error("SOME PRE-DAG CHECKS FAILED:")
        for e in errors:
            logger.warning(f"  - {e}")
        raise Exception(
            f"Pre-DAG validation failed — {len(errors)} check(s) failed:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    logger.info("ALL PRE-DAG CHECKS PASSED")
    return checks


@task.pyspark(conn_id='spark_default')
def init_folder_copy_tracking_tables(spark) -> dict:
    """Create tracking tables for folder-only data copy if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")
    # Run-level table: one row per DAG run
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_runs (
            run_id          STRING,
            excel_file_path STRING,
            started_at      TIMESTAMP,
            completed_at    TIMESTAMP,
            status          STRING,
            total_folders   INT,
            successful_folders INT,
            failed_folders  INT,
            error_message   STRING,
            created_at      TIMESTAMP,
            dag_run_id      STRING,
            service_account_user_id STRING,
            service_account_source  STRING,
            skipped_folders INT
        )
        USING iceberg
        LOCATION '{tracking_loc}/data_copy_runs'
    """)
    # Folder-level table: one row per source/dest folder pair
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_status (
            run_id            STRING,
            source_path       STRING,
            dest_bucket       STRING,
            dest_path         STRING,
            status            STRING,
            started_at        TIMESTAMP,
            completed_at      TIMESTAMP,
            source_file_count BIGINT,
            source_size_bytes BIGINT,
            dest_file_count   BIGINT,
            dest_size_bytes   BIGINT,
            files_copied      BIGINT,
            bytes_copied      BIGINT,
            is_incremental    BOOLEAN,
            file_count_match  BOOLEAN,
            size_match        BOOLEAN,
            error_message     STRING,
            updated_at        TIMESTAMP,
            yarn_application_id STRING,
            distcp_started_at   TIMESTAMP,
            distcp_completed_at TIMESTAMP,
            distcp_duration_seconds DOUBLE,
            distcp_bytes_copied BIGINT,
            distcp_files_copied BIGINT,
            throughput_mbps     DOUBLE
        )
        USING iceberg
        LOCATION '{tracking_loc}/data_copy_status'
    """)

    for _col_name, _col_type in (
        ("yarn_application_id", "STRING"),
        ("distcp_started_at", "TIMESTAMP"),
        ("distcp_completed_at", "TIMESTAMP"),
        ("distcp_duration_seconds", "DOUBLE"),
        ("distcp_bytes_copied", "BIGINT"),
        ("distcp_files_copied", "BIGINT"),
        ("throughput_mbps", "DOUBLE"),
        # Phase timing / failure-mode diagnostics
        ("parent_source_path", "STRING"),
        ("failure_phase", "STRING"),
        ("yarn_app_created", "BOOLEAN"),
        ("script_exit_code", "INT"),
        ("s3_list_before_seconds", "DOUBLE"),
        ("source_scan_seconds", "DOUBLE"),
        ("s3_list_after_seconds", "DOUBLE"),
        ("validation_listing_seconds", "DOUBLE"),
        ("diagnostics_json", "STRING"),
    ):
        try:
            spark.sql(
                f"ALTER TABLE {tracking_db}.data_copy_status "
                f"ADD COLUMN {_col_name} {_col_type}"
            )
            logger.info(f"[FolderCopy] Added column {_col_name} to data_copy_status")
        except Exception as _alter_exc:
            logger.debug(
                f"[FolderCopy] ALTER TABLE {tracking_db}.data_copy_status "
                f"ADD COLUMN {_col_name} {_col_type} did not apply: {_alter_exc}"
            )

    for _col_name, _col_type in (
        ("dag_run_id", "STRING"),
        ("service_account_user_id", "STRING"),
        ("service_account_source", "STRING"),
        ("skipped_folders", "INT"),
    ):
        try:
            spark.sql(
                f"ALTER TABLE {tracking_db}.data_copy_runs "
                f"ADD COLUMN {_col_name} {_col_type}"
            )
            logger.info(f"[FolderCopy] Added column {_col_name} to data_copy_runs")
        except Exception as _alter_exc:
            logger.debug(
                f"[FolderCopy] ALTER TABLE {tracking_db}.data_copy_runs "
                f"ADD COLUMN {_col_name} {_col_type} did not apply: {_alter_exc}"
            )

    for _table_name, _expected in (
        ('data_copy_status', (
            'yarn_application_id',
            'distcp_started_at',
            'distcp_completed_at',
            'distcp_duration_seconds',
            'distcp_bytes_copied',
            'distcp_files_copied',
            'throughput_mbps',
            'parent_source_path',
            'failure_phase',
            'yarn_app_created',
            'script_exit_code',
            's3_list_before_seconds',
            'source_scan_seconds',
            's3_list_after_seconds',
            'validation_listing_seconds',
            'diagnostics_json',
        )),
        ('data_copy_runs', (
            'dag_run_id',
            'service_account_user_id',
            'service_account_source',
            'skipped_folders',
        )),
    ):
        try:
            described = spark.sql(f"DESCRIBE {tracking_db}.{_table_name}").collect()
            present = set()
            for _row in described:
                try:
                    present.add(str(_row['col_name']).strip().lower())
                except Exception:
                    continue
            missing = sorted(c for c in _expected if c.lower() not in present)
        except Exception as _desc_exc:
            logger.debug(
                f"[FolderCopy] Could not DESCRIBE {tracking_db}.{_table_name}: {_desc_exc}"
            )
            missing = []
        if missing:
            logger.warning(
                f"[FolderCopy] Tracking table {tracking_db}.{_table_name} is missing "
                f"expected column(s) after schema evolution: {', '.join(missing)}. "
                f"Subsequent INSERT/UPDATE statements referencing them will fail — "
                f"check ALTER TABLE privileges on {tracking_db}."
            )

    logger.info("[FolderCopy] Tracking tables initialized: data_copy_runs, data_copy_status")
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_data_copy_run(excel_file_path: str, spark, dag_run_id: str = '') -> str:
    """Create a run record in data_copy_runs at the start of a folder-only copy DAG run."""
    import uuid
    from datetime import datetime
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = f"folder_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    sa_user = str(config.get('service_account_user_id') or '').strip()
    sa_source = 'config:service_account_user_id' if sa_user else 'pending (.profile fallback)'
    esc_excel = sql_quote(str(excel_file_path or ''))
    esc_dag_run_id = sql_quote(str(dag_run_id or ''))
    esc_sa_user = sql_quote(sa_user)
    esc_sa_source = sql_quote(sa_source)

    spark.sql(f"""
        INSERT INTO {tracking_db}.data_copy_runs (
            run_id, excel_file_path, started_at, completed_at,
            status, total_folders, successful_folders, failed_folders,
            error_message, created_at,
            dag_run_id, service_account_user_id, service_account_source,
            skipped_folders
        ) VALUES (
            '{run_id}',
            '{esc_excel}',
            current_timestamp(),
            NULL,
            'RUNNING',
            NULL, NULL, NULL, NULL,
            current_timestamp(),
            '{esc_dag_run_id}',
            '{esc_sa_user}',
            '{esc_sa_source}',
            NULL
        )
    """)
    logger.info(f"[FolderCopy] Created run record: {run_id}")
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_folder_copy_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read folder copy Excel config from S3.

    Expected columns:
      - source_path   (required) : Full source cluster path (MapR-FS or HDFS)
      - target_bucket (required) : S3 bucket, normalised to s3a://
      - dest_folder   (optional) : Destination folder inside the bucket;
                                   defaults to the basename of source_path
      - endpoint      (optional) : S3 endpoint URL for non-default tenants;
                                   credentials looked up via <hostname>_access_key/secret_key Variables
    Returns a list of dicts, one per valid row.
    """
    import os
    from io import BytesIO

    import pandas as ps

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')

    # Normalise column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    configs = []
    skipped = 0
    for _, row in df.iterrows():
        source_path = str(row.get('source_path', '') or '').strip()
        if not source_path:
            skipped += 1
            continue

        raw_bucket = str(row.get('target_bucket', '') or '').strip()
        if not raw_bucket or raw_bucket.lower() in ('nan', 'none'):
            logger.warning(f"[FolderCopy] Skipping row — missing target_bucket for source_path={source_path!r}")
            skipped += 1
            continue

        if re.match(r'(?i)^s3:(?!//)', raw_bucket):
            raw_bucket = raw_bucket[3:].strip()

        dest_bucket = normalize_s3(raw_bucket)
        scheme, _, bucket_body = dest_bucket.partition('://')
        bucket_body = re.sub(r'/{2,}', '/', bucket_body).strip('/')
        dest_bucket = f"{scheme}://{bucket_body}" if bucket_body else dest_bucket.rstrip('/')

        raw_val = row.get('dest_folder', '')
        dest_folder = '' if ps.isna(raw_val) else str(raw_val).strip().strip('/')
        dest_folder = re.sub(r'/{2,}', '/', dest_folder).strip('/')
        dest_folder = dest_folder or os.path.basename(source_path.rstrip('/'))

        if dest_folder and dest_bucket.endswith(f"/{dest_folder}"):
            dest_bucket = dest_bucket[: -(len(dest_folder) + 1)].rstrip('/')

        raw_endpoint = row.get('endpoint', '')
        dest_endpoint = '' if (raw_endpoint is None or str(raw_endpoint).strip().lower() in ('', 'nan', 'none')) else str(raw_endpoint).strip()

        # Optional per-row override of the global split_subfolders setting.
        # Blank/missing = follow the global setting (None).
        raw_split = row.get('split_subfolders', '')
        if raw_split is None or str(raw_split).strip().lower() in ('', 'nan', 'none'):
            split_subfolders = None
        else:
            split_subfolders = _truthy(raw_split)

        config_entry = {
            'run_id': run_id,
            'source_path': source_path,
            'dest_bucket': dest_bucket,
            'dest_folder': dest_folder,
            'dest_endpoint': dest_endpoint,
            'split_subfolders': split_subfolders,
        }
        logger.info(
            f"[FolderCopy] Parsed: {source_path} -> {dest_bucket}/{dest_folder}"
        )
        configs.append(config_entry)

    logger.info(
        f"[FolderCopy] parse_folder_copy_excel: {len(configs)} folders to copy, {skipped} rows skipped"
    )
    if not configs:
        raise ValueError("[FolderCopy] No valid rows found in Excel — check source_path and target_bucket columns")

    return configs


def _probe_source_size(ssh, source_path: str, config: dict) -> tuple[int, int]:
    """Probe a source path with `hadoop fs -count`, returning (content_size, file_count)."""
    # (0, 1) is the "size probe unusable" pair size_distcp_job routes to the
    # configured defaults. This DAG has no discovery task to fall back on, and a
    # probe failure must not fail the copy.
    unusable = (0, 1)
    cmd = f'hadoop fs -count "{source_path}"'
    try:
        with ssh.get_conn() as client:
            _, stdout, stderr = client.exec_command(
                _login_shell(cmd, config.get('cluster_type', 'MapR')),
                timeout=SSH_COMMAND_TIMEOUT,
            )
            output = stdout.read().decode()
            exit_code = stdout.channel.recv_exit_status()
            error_output = stderr.read().decode()

        if exit_code != 0:
            logger.warning(
                f"[FolderCopy] Size probe for {source_path} exited {exit_code} — "
                f"falling back to configured DistCp defaults. {error_output.strip()[:300]}"
            )
            return unusable

        for line in reversed(output.splitlines()):
            fields = line.split()
            if len(fields) < 4:
                continue
            try:
                return int(fields[2]), int(fields[1])
            except ValueError:
                continue

        logger.warning(
            f"[FolderCopy] Could not parse `hadoop fs -count` output for {source_path} — "
            f"falling back to configured DistCp defaults. Output: {output.strip()[:300]}"
        )
        return unusable
    except Exception as exc:
        logger.warning(
            f"[FolderCopy] Size probe for {source_path} failed ({exc}) — "
            f"falling back to configured DistCp defaults."
        )
        return unusable


@task
def expand_folder_subfolders(folder_configs: list, cluster_setup: dict = None, **context) -> list:
    """Optionally split each folder into one DistCp task per subfolder.

    Subfolder-wise copy is the mitigation for very large, deeply nested folders:
    each task lists and copies only its own subtree, so no single listing has to
    walk the whole folder. It is off unless split_subfolders is set (DAG Param,
    Variable folder_copy_split_subfolders, or the per-row Excel column), and any
    folder that cannot be split safely is copied whole, exactly as before.

    Each subfolder keeps its own tracking row, validation and report line;
    parent_source_path ties them back to the Excel row.
    """
    global_split = _truthy(_resolve_setting('split_subfolders', context, default=False))
    wanted = [
        global_split if cfg.get('split_subfolders') is None else bool(cfg.get('split_subfolders'))
        for cfg in folder_configs
    ]
    if not any(wanted):
        logger.info("[FolderSplit] Subfolder splitting not requested — copying folders whole")
        return folder_configs

    depth = _int_setting('split_depth', context, default=1, minimum=1)
    max_entries = _int_setting('max_split_entries', context, default=1000, minimum=1)
    max_loose = _int_setting('max_loose_files', context, default=200, minimum=0)
    map_limit = _airflow_map_limit()

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    # DistCp -delete only runs inside each task, so it cannot remove a subfolder or file
    # that vanished from the source: no task would cover it any more. Copy whole instead.
    delete_on = config.get('distcp_preserve_delete') is True and _allow_delete(config)

    expanded = []
    for index, (cfg, split_this) in enumerate(zip(folder_configs, wanted, strict=False)):
        if not split_this:
            expanded.append(cfg)
            continue
        source_path = cfg['source_path']
        if delete_on:
            logger.warning(
                f"[FolderSplit] {source_path} copied whole — folder_copy_allow_delete is on and "
                f"splitting would stop DistCp -delete from removing deleted source content"
            )
            expanded.append(cfg)
            continue
        try:
            entries = _split_folder_config(ssh, config, cfg, depth, max_loose)
        except Exception as exc:
            logger.error(
                f"[FolderSplit] Could not split {source_path} ({exc}) — copying it whole"
            )
            expanded.append(cfg)
            continue
        if len(entries) > max_entries:
            logger.error(
                f"[FolderSplit] {source_path} would become {len(entries)} tasks, over the "
                f"max_split_entries limit of {max_entries} — copying it whole. "
                f"Lower split_depth or raise the limit (and Airflow core.max_map_length)."
            )
            expanded.append(cfg)
            continue
        # Airflow fails the whole task when the mapped list exceeds core.max_map_length,
        # so keep a running total. Every row not yet processed still needs one slot.
        remaining = len(folder_configs) - index - 1
        if len(expanded) + len(entries) + remaining > map_limit:
            logger.error(
                f"[FolderSplit] Splitting {source_path} would exceed core.max_map_length "
                f"({map_limit}) across this run — copying it whole"
            )
            expanded.append(cfg)
            continue
        if len(entries) == 1 and entries[0] is cfg:
            logger.info(f"[FolderSplit] {source_path} has no subfolders — nothing to split")
        else:
            loose = sum(1 for e in entries if e.get('split_role') == 'loose_files')
            logger.info(
                f"[FolderSplit] {source_path} -> {len(entries) - loose} subfolder task(s)"
                + (" + 1 loose-files task" if loose else "")
                + f" (depth={depth})"
            )
        expanded.extend(entries)

    if len(expanded) > map_limit:
        logger.warning(
            f"[FolderSplit] {len(expanded)} copy tasks generated, over core.max_map_length "
            f"({map_limit}); the mapped task will fail even without splitting"
        )
    return expanded


@task
def run_folder_distcp_ssh(folder_config: dict, cluster_setup: dict = None, **context) -> dict:
    """Copy a single source folder to S3 via SSH DistCp with -update for incremental runs."""
    from datetime import datetime as _dt

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    run_id = folder_config['run_id']
    source_path = folder_config['source_path']
    dest_bucket = folder_config['dest_bucket']
    dest_folder = folder_config['dest_folder']
    dest_endpoint = folder_config.get('dest_endpoint', '')
    s3_dest = f"{dest_bucket}/{dest_folder}"

    # 'shallow' = copy only the explicit files that sat directly in a split folder.
    shallow = bool(folder_config.get('shallow'))
    source_files = list(folder_config.get('source_files') or []) if shallow else []
    source_files_bytes = int(folder_config.get('source_files_bytes') or 0) if shallow else 0
    listing_strategy = _resolve_listing_strategy(context)
    diag_extra = {
        'parent_source_path': folder_config.get('parent_source_path'),
        'split_role': folder_config.get('split_role'),
    }
    partial_output: list = []
    diag: dict = {}

    logger.info(
        f"[FolderCopy] Sizing mode: {distcp_sizing_mode(config)} | "
        f"listing_strategy={listing_strategy} | shallow={shallow}"
    )
    if shallow:
        # The parent folder holds the rest of the data; only these files are in scope.
        probe_size, probe_files = source_files_bytes, max(len(source_files), 1)
    else:
        probe_size, probe_files = _probe_source_size(ssh, source_path, config)
    mappers, bandwidth = size_distcp_job(probe_size, probe_files, config)
    logger.info(
        f"[FolderCopy] Sized {source_path}: {mappers} mappers x {bandwidth} MB/s "
        f"(= {mappers * bandwidth} MB/s aggregate) for "
        f"{probe_size} bytes / {probe_files} files"
    )

    jvm_opts = distcp_jvm_opts(config)
    # shlex.quote leaves a bare word like 'dynamic' untouched, so the default
    # command is byte-identical to the pre-branch hardcoded -strategy dynamic.
    strategy = shlex.quote(config['distcp_strategy'])
    client_java_opts = str(config.get('distcp_client_java_opts') or '').strip()
    client_opts_export = (
        f'export HADOOP_CLIENT_OPTS={shlex.quote(client_java_opts)}\n'
        if client_java_opts
        else ''
    )

    cluster_setup = cluster_setup if isinstance(cluster_setup, dict) else {}
    distcp_log_dir = (
        cluster_setup.get('distcp_log_dir')
        or cluster_setup.get('temp_dir')
        or '/tmp'
    )
    log_name = re.sub(r'[^A-Za-z0-9_.-]', '_', str(dest_folder or 'folder')).strip('_') or 'folder'

    # Never delete for a loose-files copy: its destination also holds the subfolder
    # copies, which DistCp -delete would remove as "not in the source".
    delete_flag = ''
    if config.get('distcp_preserve_delete') is True and _allow_delete(config) and not shallow:
        delete_flag = ' -delete'

    s3_opts = build_s3_opts(dest_bucket, config, dest_endpoint)

    dest_count_snippet = _listing_snippet(
        f'hadoop fs{s3_opts}', '"$location"', 'FILE_COUNT', 'TOTAL_SIZE', listing_strategy, shallow
    )
    if shallow:
        # Counts come from the split step's directory listing, so no source scan is needed.
        src_metrics_block = (
            f'SRC_FILE_COUNT={len(source_files)}\n'
            f'SRC_TOTAL_SIZE={source_files_bytes}\n'
        )
        distcp_sources = ' '.join(shlex.quote(p) for p in source_files)
        # DistCp -update treats a single source file's target as a file path, not a directory.
        distcp_target = (
            f"{s3_dest}/{os.path.basename(source_files[0])}" if len(source_files) == 1 else s3_dest
        )
    else:
        src_metrics_block = _listing_snippet(
            'hadoop fs', f'"{source_path}"', 'SRC_FILE_COUNT', 'SRC_TOTAL_SIZE', listing_strategy, False
        )
        distcp_sources = f'"{source_path}"'
        distcp_target = s3_dest

    cmd = f'''set +e
{client_opts_export}
phase_start() {{ PHASE_T0=$(date +%s); echo "PHASE_START $1 $(date -u +%Y-%m-%dT%H:%M:%SZ)"; }}
phase_end() {{ echo "PHASE_END $1 seconds=$(( $(date +%s) - PHASE_T0 ))"; }}

echo "EDGE_HOST=$(hostname)"
echo "EDGE_USER=$(id -un 2>/dev/null)"
echo "EDGE_GROUPS=$(id -Gn 2>/dev/null | tr ' ' ',')"
if command -v pgrep >/dev/null 2>&1; then
    if pgrep -x sssd >/dev/null 2>&1; then echo "EDGE_SSSD_RUNNING=true"; else echo "EDGE_SSSD_RUNNING=false"; fi
else
    echo "EDGE_SSSD_RUNNING=unknown"
fi
echo "EDGE_MEM_AVAILABLE_MB=$(free -m 2>/dev/null | awk '/^Mem:/ {{print $7}}')"
echo "LISTING_STRATEGY={listing_strategy}"

calculate_s3_metrics() {{
    local location=$1
    if ! hadoop fs{s3_opts} -test -d "$location" 2>/dev/null; then
        echo "S3_FILE_COUNT=0"
        echo "S3_TOTAL_SIZE=0"
        return
    fi
{dest_count_snippet}    [ -z "$FILE_COUNT" ] && FILE_COUNT=0
    [ -z "$TOTAL_SIZE" ] && TOTAL_SIZE=0
    echo "S3_FILE_COUNT=$FILE_COUNT"
    echo "S3_TOTAL_SIZE=$TOTAL_SIZE"
}}

emit_metrics() {{
    echo "===DISTCP_METRICS_START==="
    echo "INCREMENTAL=$1"
    echo "SRC_FILE_COUNT=$2"
    echo "SRC_TOTAL_SIZE=$3"
    echo "S3_FILE_COUNT_BEFORE=$4"
    echo "S3_TOTAL_SIZE_BEFORE=$5"
    echo "S3_FILE_COUNT_AFTER=$6"
    echo "S3_TOTAL_SIZE_AFTER=$7"
    echo "BYTES_COPIED=$8"
    echo "FILES_COPIED=$9"
    echo "===DISTCP_METRICS_END==="
}}

echo "=== Source existence check ==="
phase_start source_check
SRC_TEST_ERR=$(hadoop fs -test -d "{source_path}" 2>&1)
SRC_TEST_RC=$?
phase_end source_check
echo "SOURCE_TEST_RC=$SRC_TEST_RC"
if [ "$SRC_TEST_RC" -eq 1 ]; then
    echo "SOURCE_NOT_FOUND=true"
    emit_metrics false 0 0 0 0 0 0 0 0
    exit 0
elif [ "$SRC_TEST_RC" -ne 0 ]; then
    echo "SOURCE_TEST_ERROR=$SRC_TEST_ERR"
    exit $SRC_TEST_RC
fi

INCR=false
hadoop fs{s3_opts} -test -d "{s3_dest}" 2>/dev/null && INCR=true
echo "INCREMENTAL=$INCR"

echo "=== S3 metrics BEFORE distcp ==="
phase_start s3_list_before
S3_BEFORE=$(calculate_s3_metrics "{s3_dest}")
phase_end s3_list_before
S3_FILE_COUNT_BEFORE=$(echo "$S3_BEFORE" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=$(echo "$S3_BEFORE" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_BEFORE" ] && S3_FILE_COUNT_BEFORE=0
[ -z "$S3_TOTAL_SIZE_BEFORE" ] && S3_TOTAL_SIZE_BEFORE=0
echo "S3_FILE_COUNT_BEFORE=$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=$S3_TOTAL_SIZE_BEFORE"

echo "=== Source metrics ==="
phase_start source_scan
{src_metrics_block}phase_end source_scan
[ -z "$SRC_FILE_COUNT" ] && SRC_FILE_COUNT=0
[ -z "$SRC_TOTAL_SIZE" ] && SRC_TOTAL_SIZE=0
echo "SRC_FILE_COUNT=$SRC_FILE_COUNT"
echo "SRC_TOTAL_SIZE=$SRC_TOTAL_SIZE"

if [ "$SRC_FILE_COUNT" -eq 0 ]; then
    echo "EMPTY_SOURCE=true"
    hadoop fs{s3_opts} -mkdir -p "{s3_dest}" 2>/dev/null || true
    emit_metrics "$INCR" 0 "$SRC_TOTAL_SIZE" "$S3_FILE_COUNT_BEFORE" "$S3_TOTAL_SIZE_BEFORE" "$S3_FILE_COUNT_BEFORE" "$S3_TOTAL_SIZE_BEFORE" 0 0
    exit 0
fi

echo "=== Running distcp ==="
phase_start distcp
DISTCP_OUTPUT=$(hadoop distcp{s3_opts}{jvm_opts} -update{delete_flag} -m {mappers} -bandwidth {bandwidth} -strategy {strategy} \\
    -log {distcp_log_dir}/distcp_{log_name}.log {distcp_sources} "{distcp_target}" 2>&1)
DISTCP_EXIT=$?
phase_end distcp
echo "$DISTCP_OUTPUT"
echo "DISTCP_EXIT_CODE=$DISTCP_EXIT"

BYTES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Bytes Copied" | grep -oE '[0-9,]+' | tail -1 | tr -d ',')
FILES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -iE "Files Copied|Number of files copied" | grep -oE '[0-9,]+' | tail -1 | tr -d ',')
[ -z "$BYTES_COPIED" ] && BYTES_COPIED=0
[ -z "$FILES_COPIED" ] && FILES_COPIED=0

echo "=== S3 metrics AFTER distcp ==="
phase_start s3_list_after
S3_AFTER=$(calculate_s3_metrics "{s3_dest}")
phase_end s3_list_after
S3_FILE_COUNT_AFTER=$(echo "$S3_AFTER" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=$(echo "$S3_AFTER" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_AFTER" ] && S3_FILE_COUNT_AFTER=0
[ -z "$S3_TOTAL_SIZE_AFTER" ] && S3_TOTAL_SIZE_AFTER=0
echo "S3_FILE_COUNT_AFTER=$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=$S3_TOTAL_SIZE_AFTER"

emit_metrics "$INCR" "$SRC_FILE_COUNT" "$SRC_TOTAL_SIZE" "$S3_FILE_COUNT_BEFORE" "$S3_TOTAL_SIZE_BEFORE" "$S3_FILE_COUNT_AFTER" "$S3_TOTAL_SIZE_AFTER" "$BYTES_COPIED" "$FILES_COPIED"

[ "$DISTCP_EXIT" -ne 0 ] && exit $DISTCP_EXIT
exit 0
'''

    started_at = _dt.utcnow()
    started_at_str = started_at.strftime('%Y-%m-%d %H:%M:%S')
    distcp_reported = False

    try:
        with ssh.get_conn() as client:
            _, stdout, stderr = client.exec_command(
                _login_shell(cmd, config.get('cluster_type', 'MapR')),
                timeout=SSH_COMMAND_TIMEOUT,
                get_pty=True,
            )
            # Incremental read: if the script stalls or the connection drops, the
            # phase markers printed so far are kept in partial_output.
            output, error_output, exit_code = _read_remote_command(
                stdout.channel,
                partial_output,
                deadline_seconds=SSH_COMMAND_TIMEOUT,
                label=f"{source_path} -> {s3_dest}",
            )

            logger.info(f"=== [FolderDistCp] {source_path} -> {s3_dest} (FULL stdout) ===")
            logger.info(output)
            if error_output.strip():
                logger.info(f"=== [FolderDistCp] {source_path} -> {s3_dest} (FULL stderr) ===")
                logger.info(error_output)

            combined_output = output + "\n" + error_output
            yarn_application_ids = re.findall(r"application_\d+_\d+", combined_output)
            yarn_application_ids = list(dict.fromkeys(yarn_application_ids))
            yarn_application_id = yarn_application_ids[-1] if yarn_application_ids else None
            if yarn_application_ids:
                logger.info(
                    f"[FolderDistCp] YARN Application IDs for {source_path}: {yarn_application_ids}"
                )
                logger.info(
                    f"[FolderDistCp] Last YARN Application ID for {source_path}: {yarn_application_id}"
                )
            else:
                logger.warning(
                    f"[FolderDistCp] No YARN Application ID found for {source_path}"
                )

            diag = _build_run_diagnostics(
                output, exit_code, yarn_application_ids, listing_strategy, shallow, diag_extra
            )
            logger.info(
                f"[FolderDistCp] Diagnostics for {source_path}: phase_seconds={diag['phase_seconds']} | "
                f"yarn_app_created={diag['yarn_app_created']} | failure_phase={diag['failure_phase']} | "
                f"exit_code={exit_code} | memory_kill_suspected={diag['memory_kill_suspected']} | "
                f"edge_host={diag['edge_host']} sssd={diag['edge_sssd_running']} "
                f"groups={diag['edge_groups']} mem_available_mb={diag['edge_mem_available_mb']}"
            )
            if diag['edge_sssd_running'] == 'false':
                logger.warning(
                    f"[FolderDistCp] SSSD is not running on edge node {diag['edge_host']} — "
                    f"group lookups can be wrong there (permission failures), and timings from "
                    f"this node should not be compared with a validated node."
                )
            if diag['memory_kill_suspected']:
                logger.warning(
                    f"[FolderDistCp] Memory kill / OOM signature in the output for {source_path} — "
                    f"a resource problem, separate from the listing phase."
                )

            metrics = {
                'INCREMENTAL': 'false',
                'SRC_FILE_COUNT': 0,
                'SRC_TOTAL_SIZE': 0,
                'S3_FILE_COUNT_BEFORE': 0,
                'S3_TOTAL_SIZE_BEFORE': 0,
                'S3_FILE_COUNT_AFTER': 0,
                'S3_TOTAL_SIZE_AFTER': 0,
                'BYTES_COPIED': 0,
                'FILES_COPIED': 0,
            }
            m_start = output.find("===DISTCP_METRICS_START===")
            m_end = output.find("===DISTCP_METRICS_END===")
            if m_start != -1 and m_end != -1:
                metrics_block = output[m_start + len("===DISTCP_METRICS_START==="):m_end]
            else:
                logger.warning(
                    f"[FolderDistCp] Metrics block not found for {source_path} — "
                    f"falling back to scanning full output"
                )
                metrics_block = output
            for line in metrics_block.splitlines():
                line = line.strip()
                if '=' not in line:
                    continue
                key, _, val = line.partition('=')
                key = key.strip()
                if key not in metrics:
                    continue
                val = val.strip()
                if key == 'INCREMENTAL':
                    metrics[key] = val
                else:
                    try:
                        metrics[key] = int(val or 0)
                    except ValueError:
                        logger.warning(
                            f"[FolderDistCp] Could not parse metric '{key}={val}' for {source_path}"
                        )

            completed_at = _dt.utcnow()
            duration = max((completed_at - started_at).total_seconds(), 0.0)
            completed_at_str = completed_at.strftime('%Y-%m-%d %H:%M:%S')

            source_test_rc = None
            source_test_error = ''
            for line in output.splitlines():
                line = line.strip()
                if line.startswith('SOURCE_TEST_RC='):
                    try:
                        source_test_rc = int(line.split('=', 1)[1] or 0)
                    except ValueError:
                        source_test_rc = None
                elif line.startswith('SOURCE_TEST_ERROR='):
                    source_test_error = line.split('=', 1)[1].strip()

            if source_test_rc == 1:
                logger.error(f"[FolderDistCp] SOURCE_NOT_FOUND: {source_path}")
                return {
                    **diag,
                    'run_id': run_id,
                    'source_path': source_path,
                    'dest_bucket': dest_bucket,
                    'dest_path': dest_folder,
                    'dest_endpoint': dest_endpoint,
                    'status': 'SOURCE_NOT_FOUND',
                    'started_at': started_at_str,
                    'completed_at': completed_at_str,
                    'source_file_count': 0,
                    'source_size_bytes': 0,
                    'dest_file_count': 0,
                    'dest_size_bytes': 0,
                    'files_copied': 0,
                    'bytes_copied': 0,
                    'is_incremental': False,
                    'file_count_match': False,
                    'size_match': False,
                    'error': f"Source path does not exist: {source_path}",
                    'yarn_application_id': yarn_application_id,
                    'yarn_application_ids': yarn_application_ids,
                    'distcp_duration_seconds': duration,
                    'distcp_bytes_copied': 0,
                    'distcp_files_copied': 0,
                    'throughput_mbps': 0.0,
                }

            is_incr = str(metrics['INCREMENTAL']).lower() == 'true'
            src_files = metrics['SRC_FILE_COUNT']
            src_bytes = metrics['SRC_TOTAL_SIZE']
            dest_files = metrics['S3_FILE_COUNT_AFTER']
            dest_bytes = metrics['S3_TOTAL_SIZE_AFTER']
            transferred_files = max(dest_files - metrics['S3_FILE_COUNT_BEFORE'], 0)
            transferred_bytes = max(dest_bytes - metrics['S3_TOTAL_SIZE_BEFORE'], 0)
            distcp_bytes = metrics['BYTES_COPIED']
            distcp_files = metrics['FILES_COPIED']
            throughput = (distcp_bytes / 1048576.0 / duration) if duration > 0 else 0.0

            if 'EMPTY_SOURCE=true' in output:
                logger.warning(
                    f"[FolderDistCp] EMPTY_SOURCE: {source_path} has 0 files — "
                    f"S3 prefix created at {s3_dest}"
                )
                return {
                    **diag,
                    'run_id': run_id,
                    'source_path': source_path,
                    'dest_bucket': dest_bucket,
                    'dest_path': dest_folder,
                    'dest_endpoint': dest_endpoint,
                    'status': 'EMPTY_SOURCE',
                    'started_at': started_at_str,
                    'completed_at': completed_at_str,
                    'source_file_count': src_files,
                    'source_size_bytes': src_bytes,
                    'dest_file_count': dest_files,
                    'dest_size_bytes': dest_bytes,
                    'files_copied': transferred_files,
                    'bytes_copied': transferred_bytes,
                    'is_incremental': is_incr,
                    'file_count_match': False,
                    'size_match': False,
                    'error': None,
                    'yarn_application_id': yarn_application_id,
                    'yarn_application_ids': yarn_application_ids,
                    'distcp_duration_seconds': duration,
                    'distcp_bytes_copied': distcp_bytes,
                    'distcp_files_copied': distcp_files,
                    'throughput_mbps': throughput,
                }

            if exit_code != 0:
                if source_test_rc is not None and source_test_rc not in (0, 1):
                    error_msg = (
                        f"Source existence check failed (exit {source_test_rc}): "
                        f"{source_test_error or error_output[:1000] or output[-1000:]}"
                    )[:2000]
                else:
                    error_msg = (
                        f"DistCp failed for {source_path} -> {s3_dest} "
                        f"(exit {exit_code}): {error_output[:1000] or output[-1000:]}"
                    )[:2000]
                logger.error(f"[FolderDistCp] FAILED: {error_msg}")
                result = {
                    **diag,
                    'run_id': run_id,
                    'source_path': source_path,
                    'dest_bucket': dest_bucket,
                    'dest_path': dest_folder,
                    'dest_endpoint': dest_endpoint,
                    'status': 'FAILED',
                    'started_at': started_at_str,
                    'completed_at': completed_at_str,
                    'source_file_count': src_files,
                    'source_size_bytes': src_bytes,
                    'dest_file_count': dest_files,
                    'dest_size_bytes': dest_bytes,
                    'files_copied': transferred_files,
                    'bytes_copied': transferred_bytes,
                    'is_incremental': is_incr,
                    'file_count_match': False,
                    'size_match': False,
                    'error': error_msg,
                    'yarn_application_id': yarn_application_id,
                    'yarn_application_ids': yarn_application_ids,
                    'distcp_duration_seconds': duration,
                    'distcp_bytes_copied': distcp_bytes,
                    'distcp_files_copied': distcp_files,
                    'throughput_mbps': throughput,
                }
                context['ti'].xcom_push(key='return_value', value=result)
                distcp_reported = True
                raise Exception(
                    f"DistCp failed for {source_path} -> {s3_dest}: {error_msg}"
                )

            logger.info(
                f"[FolderDistCp] COMPLETED: {source_path} -> {s3_dest} | "
                f"incremental={is_incr} | "
                f"files_copied={transferred_files} | bytes_copied={transferred_bytes} | "
                f"duration={duration:.1f}s | "
                f"throughput={throughput:.2f} MB/s"
            )
            return {
                **diag,
                'run_id': run_id,
                'source_path': source_path,
                'dest_bucket': dest_bucket,
                'dest_path': dest_folder,
                'dest_endpoint': dest_endpoint,
                'status': 'COMPLETED',
                'started_at': started_at_str,
                'completed_at': completed_at_str,
                'source_file_count': src_files,
                'source_size_bytes': src_bytes,
                'dest_file_count': dest_files,
                'dest_size_bytes': dest_bytes,
                'files_copied': transferred_files,
                'bytes_copied': transferred_bytes,
                'is_incremental': is_incr,
                'file_count_match': src_files == dest_files,
                'size_match': abs(src_bytes - dest_bytes) <= max(1, int(src_bytes * 0.01)),
                'error': None,
                'yarn_application_id': yarn_application_id,
                'yarn_application_ids': yarn_application_ids,
                'distcp_duration_seconds': duration,
                'distcp_bytes_copied': distcp_bytes,
                'distcp_files_copied': distcp_files,
                'throughput_mbps': throughput,
            }

    except Exception as e:
        if distcp_reported:
            raise
        completed_at = _dt.utcnow()
        error_msg = str(e)[:2000]
        logger.error(f"[FolderDistCp] ERROR: {source_path} -> {s3_dest}: {error_msg}")
        partial_text = ''.join(partial_output)
        if partial_text:
            logger.error(
                f"[FolderDistCp] Output captured before the failure (last 3000 chars):\n"
                f"{partial_text[-3000:]}"
            )
        # A failure before DistCp was ever submitted (no YARN app id) names the phase
        # it died in: the PHASE_START that never got its PHASE_END.
        partial_yarn_ids = list(dict.fromkeys(re.findall(r"application_\d+_\d+", partial_text)))
        diag = _build_run_diagnostics(
            partial_text, None, partial_yarn_ids, listing_strategy, shallow, diag_extra
        )
        logger.error(
            f"[FolderDistCp] Failed in phase {diag['failure_phase']!r} "
            f"(yarn_app_created={diag['yarn_app_created']}, phase_seconds={diag['phase_seconds']}, "
            f"edge_host={diag['edge_host'] or 'unknown'})"
        )
        result = {
            **diag,
            'run_id': run_id,
            'source_path': source_path,
            'dest_bucket': dest_bucket,
            'dest_path': dest_folder,
            'dest_endpoint': dest_endpoint,
            'status': 'FAILED',
            'started_at': started_at_str,
            'completed_at': completed_at.strftime('%Y-%m-%d %H:%M:%S'),
            'source_file_count': 0,
            'source_size_bytes': 0,
            'dest_file_count': 0,
            'dest_size_bytes': 0,
            'files_copied': 0,
            'bytes_copied': 0,
            'is_incremental': False,
            'file_count_match': False,
            'size_match': False,
            'error': error_msg,
            'yarn_application_id': None,
            'yarn_application_ids': [],
            'distcp_duration_seconds': max((completed_at - started_at).total_seconds(), 0.0),
            'distcp_bytes_copied': 0,
            'distcp_files_copied': 0,
            'throughput_mbps': 0.0,
        }
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(
            f"DistCp failed for {source_path} -> {s3_dest}: {error_msg}"
        ) from e


@task.pyspark(conn_id='spark_default')
def record_data_copy_status(distcp_result: dict, spark) -> dict:
    """Insert a row into data_copy_status for the completed/failed folder copy."""
    if not isinstance(distcp_result, dict) or 'run_id' not in distcp_result:
        logger.warning(
            f"[record_data_copy_status] Skipping invalid input: {type(distcp_result)}"
        )
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id        = distcp_result['run_id']
    source_path   = sql_quote(distcp_result['source_path'])
    dest_bucket   = sql_quote(distcp_result['dest_bucket'])
    dest_path     = sql_quote(distcp_result['dest_path'])
    status        = sql_quote(str(distcp_result['status']))
    started_at    = sql_quote(str(distcp_result.get('started_at', '') or ''))
    completed_at  = sql_quote(str(distcp_result.get('completed_at', '') or ''))
    error_msg     = sql_quote(distcp_result.get('error') or '', 2000)

    src_file_count  = distcp_result.get('source_file_count', 0) or 0
    src_size_bytes  = distcp_result.get('source_size_bytes', 0) or 0
    dest_file_count = distcp_result.get('dest_file_count', 0) or 0
    dest_size_bytes = distcp_result.get('dest_size_bytes', 0) or 0
    files_copied    = distcp_result.get('files_copied', 0) or 0
    bytes_copied    = distcp_result.get('bytes_copied', 0) or 0
    is_incremental  = str(distcp_result.get('is_incremental', False)).lower()
    file_count_match = str(distcp_result.get('file_count_match', False)).lower()
    size_match       = str(distcp_result.get('size_match', False)).lower()

    yarn_ids = distcp_result.get('yarn_application_ids') or (
        [distcp_result['yarn_application_id']] if distcp_result.get('yarn_application_id') else []
    )
    yarn_app_id = sql_quote(','.join(str(x) for x in yarn_ids))
    distcp_duration = float(distcp_result.get('distcp_duration_seconds', 0.0) or 0.0)
    distcp_bytes = int(distcp_result.get('distcp_bytes_copied', 0) or 0)
    distcp_files = int(distcp_result.get('distcp_files_copied', 0) or 0)
    throughput_mbps = float(distcp_result.get('throughput_mbps', 0.0) or 0.0)

    phase_seconds = distcp_result.get('phase_seconds') or {}
    parent_source_path = sql_quote(str(distcp_result.get('parent_source_path') or ''))
    failure_phase = sql_quote(str(distcp_result.get('failure_phase') or ''))
    yarn_app_created = str(bool(distcp_result.get('yarn_app_created'))).lower()
    script_exit_code = _sql_int(distcp_result.get('script_exit_code'))
    s3_list_before_s = _sql_num(phase_seconds.get('s3_list_before'))
    source_scan_s = _sql_num(phase_seconds.get('source_scan'))
    s3_list_after_s = _sql_num(phase_seconds.get('s3_list_after'))
    diagnostics_json = sql_quote(_diagnostics_json({
        'phase_seconds': phase_seconds,
        'listing_strategy': distcp_result.get('listing_strategy'),
        'shallow': distcp_result.get('shallow'),
        'split_role': distcp_result.get('split_role'),
        'distcp_exit_code': distcp_result.get('distcp_exit_code'),
        'memory_kill_suspected': distcp_result.get('memory_kill_suspected'),
        'edge_host': distcp_result.get('edge_host'),
        'edge_groups': distcp_result.get('edge_groups'),
        'edge_sssd_running': distcp_result.get('edge_sssd_running'),
        'edge_mem_available_mb': distcp_result.get('edge_mem_available_mb'),
    }))

    execute_with_iceberg_retry(spark, f"""
        INSERT INTO {tracking_db}.data_copy_status (
            run_id, source_path, dest_bucket, dest_path,
            status, started_at, completed_at,
            source_file_count, source_size_bytes,
            dest_file_count, dest_size_bytes,
            files_copied, bytes_copied,
            is_incremental, file_count_match, size_match,
            error_message, updated_at,
            yarn_application_id, distcp_started_at, distcp_completed_at,
            distcp_duration_seconds, distcp_bytes_copied, distcp_files_copied,
            throughput_mbps,
            parent_source_path, failure_phase, yarn_app_created, script_exit_code,
            s3_list_before_seconds, source_scan_seconds, s3_list_after_seconds,
            validation_listing_seconds, diagnostics_json
        ) VALUES (
            '{run_id}',
            '{source_path}',
            '{dest_bucket}',
            '{dest_path}',
            '{status}',
            CAST('{started_at}' AS TIMESTAMP),
            CAST('{completed_at}' AS TIMESTAMP),
            {src_file_count},
            {src_size_bytes},
            {dest_file_count},
            {dest_size_bytes},
            {files_copied},
            {bytes_copied},
            {is_incremental},
            {file_count_match},
            {size_match},
            '{error_msg}',
            current_timestamp(),
            '{yarn_app_id}',
            CAST('{started_at}' AS TIMESTAMP),
            CAST('{completed_at}' AS TIMESTAMP),
            {distcp_duration},
            {distcp_bytes},
            {distcp_files},
            {throughput_mbps},
            '{parent_source_path}',
            '{failure_phase}',
            {yarn_app_created},
            {script_exit_code},
            {s3_list_before_s},
            {source_scan_s},
            {s3_list_after_s},
            NULL,
            '{diagnostics_json}'
        )
    """, task_label=f"record_data_copy_status:{source_path}")

    logger.info(
        f"[FolderCopy] Recorded status={status} for {source_path} -> {dest_bucket}/{dest_path}"
    )
    return distcp_result


@task
def validate_data_copy(copy_status: dict, **context) -> dict:
    """Re-verify the S3 destination after copy: recount files/bytes and update data_copy_status."""
    if not isinstance(copy_status, dict) or 'run_id' not in copy_status:
        logger.warning(
            f"[validate_data_copy] Skipping invalid input: {type(copy_status)}"
        )
        return {}

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    source_path = copy_status['source_path']
    dest_bucket = copy_status['dest_bucket']
    dest_path   = copy_status['dest_path']
    dest_endpoint = copy_status.get('dest_endpoint', '')
    s3_dest     = f"{dest_bucket}/{dest_path}"

    s3_opts = build_s3_opts(dest_bucket, config, dest_endpoint)

    # If the copy itself failed, skip SSH validation and mark as VALIDATION_SKIPPED
    upstream_status = copy_status.get('status')
    if upstream_status == 'FAILED':
        logger.warning(f"[FolderValidate] Skipping validation — copy FAILED for {source_path}")
        result = {
            **copy_status,
            'validation_status': 'VALIDATION_SKIPPED',
            'validation_error': copy_status.get('error'),
        }
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(f"Validation skipped — upstream copy FAILED for {source_path}")

    if upstream_status in ('SOURCE_NOT_FOUND', 'EMPTY_SOURCE', 'SKIPPED'):
        logger.warning(
            f"[FolderValidate] Passing through status={upstream_status} for {source_path} "
            f"— no SSH validation performed"
        )
        result = {
            **copy_status,
            'validation_status': upstream_status,
            'validation_error': copy_status.get('error'),
        }
        ti = context.get('ti')
        if ti is not None:
            ti.xcom_push(key='return_value', value=result)
        return result

    # Same listing the copy used, so validation timings are comparable with the copy's.
    listing_strategy = copy_status.get('listing_strategy') or _resolve_listing_strategy(context)
    shallow = bool(copy_status.get('shallow'))
    dest_count_snippet = _listing_snippet(
        f'hadoop fs{s3_opts}', f'"{s3_dest}"', 'DEST_FILE_COUNT', 'DEST_TOTAL_SIZE', listing_strategy, shallow
    )

    cmd = f"""
if ! hadoop fs{s3_opts} -test -d "{s3_dest}" 2>/dev/null; then
    echo "DEST_EXISTS=false"
    echo "DEST_FILE_COUNT=0"
    echo "DEST_TOTAL_SIZE=0"
else
    echo "DEST_EXISTS=true"
{dest_count_snippet}    [ -z "$DEST_FILE_COUNT" ] && DEST_FILE_COUNT=0
    [ -z "$DEST_TOTAL_SIZE" ] && DEST_TOTAL_SIZE=0
    echo "DEST_FILE_COUNT=$DEST_FILE_COUNT"
    echo "DEST_TOTAL_SIZE=$DEST_TOTAL_SIZE"
fi
"""

    dest_exists = False
    dest_file_count = 0
    dest_size_bytes = 0
    validation_error = None
    validation_listing_seconds = None

    try:
        with ssh.get_conn() as client:
            _listing_t0 = time.monotonic()
            _, stdout, stderr = client.exec_command(_login_shell(cmd, config.get('cluster_type', 'MapR')), timeout=SSH_COMMAND_TIMEOUT)
            output = stdout.read().decode()
            stderr.read()
            stdout.channel.recv_exit_status()
            validation_listing_seconds = round(time.monotonic() - _listing_t0, 1)
            logger.info(
                f"[FolderValidate] Destination listing for {s3_dest} took "
                f"{validation_listing_seconds}s (strategy={listing_strategy}, shallow={shallow})"
            )

            for line in output.split('\n'):
                line = line.strip()
                if line == 'DEST_EXISTS=true':
                    dest_exists = True
                elif line.startswith('DEST_FILE_COUNT='):
                    dest_file_count = int(line.split('=', 1)[1] or 0)
                elif line.startswith('DEST_TOTAL_SIZE='):
                    dest_size_bytes = int(line.split('=', 1)[1] or 0)

            if not dest_exists:
                validation_error = f"S3 destination does not exist after copy: {s3_dest}"
                logger.error(f"[FolderValidate] {validation_error}")

    except Exception as e:
        validation_error = f"Validation SSH error: {str(e)[:1000]}"
        logger.error(f"[FolderValidate] {validation_error}")

    src_file_count = copy_status.get('source_file_count', 0) or 0
    src_size_bytes = copy_status.get('source_size_bytes', 0) or 0

    file_count_match = dest_file_count == src_file_count
    size_match = abs(src_size_bytes - dest_size_bytes) <= max(1, int(src_size_bytes * 0.01))

    validation_status = 'VALIDATED' if (dest_exists and file_count_match and size_match and not validation_error) else 'VALIDATION_FAILED'

    logger.info(
        f"[FolderValidate] {source_path} -> {s3_dest} | "
        f"dest_exists={dest_exists} | file_count_match={file_count_match} | "
        f"size_match={size_match} | status={validation_status}"
    )

    result = {
        **copy_status,
        'dest_file_count': dest_file_count,
        'dest_size_bytes': dest_size_bytes,
        'file_count_match': file_count_match,
        'size_match': size_match,
        'validation_status': validation_status,
        'validation_error': validation_error,
        'validation_listing_seconds': validation_listing_seconds,
    }
    if validation_status != 'VALIDATED':
        result['failure_phase'] = copy_status.get('failure_phase') or 'validation'
        result['validation_error'] = (
            validation_error or 'file count or size mismatch'
        )
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(
            f"Validation {validation_status} for {source_path} -> {s3_dest}: "
            f"{validation_error or 'file count or size mismatch'}"
        )
    return result


@task.pyspark(conn_id='spark_default')
def update_data_copy_validation(validation_result: dict, spark) -> dict:
    """Update data_copy_status with final validation metrics and status."""
    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning(
            f"[update_data_copy_validation] Skipping invalid input: {type(validation_result)}"
        )
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id      = validation_result['run_id']
    source_path = sql_quote(validation_result['source_path'])
    dest_bucket = sql_quote(validation_result['dest_bucket'])
    dest_path   = sql_quote(validation_result['dest_path'])

    validation_status = sql_quote(str(
        validation_result.get('validation_status', 'VALIDATION_FAILED')
    ))
    dest_file_count   = validation_result.get('dest_file_count', 0) or 0
    dest_size_bytes   = validation_result.get('dest_size_bytes', 0) or 0
    file_count_match  = str(validation_result.get('file_count_match', False)).lower()
    size_match        = str(validation_result.get('size_match', False)).lower()
    val_error         = sql_quote(str(validation_result.get('validation_error') or ''), 2000)

    yarn_ids = validation_result.get('yarn_application_ids') or (
        [validation_result['yarn_application_id']]
        if validation_result.get('yarn_application_id') else []
    )
    yarn_app_id = sql_quote(','.join(str(x) for x in yarn_ids))
    distcp_duration = float(validation_result.get('distcp_duration_seconds', 0.0) or 0.0)
    distcp_bytes = int(validation_result.get('distcp_bytes_copied', 0) or 0)
    distcp_files = int(validation_result.get('distcp_files_copied', 0) or 0)
    throughput_mbps = float(validation_result.get('throughput_mbps', 0.0) or 0.0)
    val_list_secs = _sql_num(validation_result.get('validation_listing_seconds'))
    failure_phase = sql_quote(str(validation_result.get('failure_phase') or ''))

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.data_copy_status
        SET status           = '{validation_status}',
            dest_file_count  = {dest_file_count},
            dest_size_bytes  = {dest_size_bytes},
            file_count_match = {file_count_match},
            size_match       = {size_match},
            yarn_application_id = CASE
                                 WHEN '{yarn_app_id}' != '' THEN '{yarn_app_id}'
                                 ELSE yarn_application_id
                               END,
            distcp_duration_seconds = {distcp_duration},
            distcp_bytes_copied     = {distcp_bytes},
            distcp_files_copied     = {distcp_files},
            throughput_mbps         = {throughput_mbps},
            validation_listing_seconds = {val_list_secs},
            failure_phase    = CASE
                                 WHEN '{failure_phase}' != '' THEN '{failure_phase}'
                                 ELSE failure_phase
                               END,
            error_message    = CASE
                                 WHEN '{val_error}' != '' THEN '{val_error}'
                                 ELSE error_message
                               END,
            updated_at       = current_timestamp()
        WHERE run_id     = '{run_id}'
          AND source_path = '{source_path}'
          AND dest_bucket = '{dest_bucket}'
          AND dest_path   = '{dest_path}'
    """, task_label=f"update_data_copy_validation:{source_path}")

    logger.info(f"[FolderValidate] Updated tracking: {source_path} -> {validation_status}")
    return validation_result


@task.pyspark(conn_id='spark_default')
def finalize_data_copy_run(run_id: str, spark, cluster_setup: dict = None) -> dict:
    """Aggregate folder-level counts and mark the data_copy_runs record as COMPLETED."""
    config = get_config()
    tracking_db = config['tracking_database']

    total = successful = failed = skipped = not_found = 0
    overall_status = 'FAILED'

    try:
        stats = spark.sql(f"""
            SELECT
                COUNT(*)                                                              AS total_folders,
                SUM(CASE WHEN status IN ('VALIDATED', 'EMPTY_SOURCE', 'SOURCE_NOT_FOUND')  THEN 1 ELSE 0 END) AS successful_folders,
                SUM(CASE WHEN status IN ('FAILED', 'VALIDATION_FAILED', 'VALIDATION_SKIPPED') THEN 1 ELSE 0 END) AS failed_folders,
                SUM(CASE WHEN status IN ('SOURCE_NOT_FOUND')                               THEN 1 ELSE 0 END) AS not_found_folders,
                SUM(CASE WHEN status IN ('SKIPPED')                                        THEN 1 ELSE 0 END) AS skipped_folders
            FROM {tracking_db}.data_copy_status
            WHERE run_id = '{run_id}'
        """).collect()

        if not stats or int(stats[0]['total_folders'] or 0) == 0:
            logger.warning(
                f"[FolderCopy] No folder records found for run_id '{run_id}'. "
                f"Upstream tasks (excel parse / distcp / record) likely failed before "
                f"writing any records."
            )
            overall_status = 'FAILED'
        else:
            total      = int(stats[0]['total_folders'] or 0)
            successful = int(stats[0]['successful_folders'] or 0)
            failed     = int(stats[0]['failed_folders'] or 0)
            not_found  = int(stats[0]['not_found_folders'] or 0)
            skipped    = int(stats[0]['skipped_folders'] or 0)
            overall_status = 'COMPLETED' if failed == 0 else 'COMPLETED_WITH_ERRORS'
    except Exception as e:
        logger.error(
            f"[FolderCopy] Failed to query data_copy_status for run_id '{run_id}': {str(e)}"
        )
        total = successful = failed = skipped = not_found = 0
        overall_status = 'FAILED'

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.data_copy_runs
        SET status             = '{overall_status}',
            completed_at       = current_timestamp(),
            total_folders      = {total},
            successful_folders = {successful},
            failed_folders     = {failed},
            skipped_folders    = {skipped}
        WHERE run_id = '{run_id}'
    """, task_label="finalize_data_copy_run")

    if isinstance(cluster_setup, dict) and cluster_setup.get('service_account_user_id'):
        _sa = sql_quote(str(cluster_setup['service_account_user_id']))
        _sa_src = sql_quote(str(cluster_setup.get('service_account_source') or 'unknown'))
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.data_copy_runs
            SET service_account_user_id = '{_sa}',
                service_account_source  = '{_sa_src}'
            WHERE run_id = '{run_id}'
        """, task_label="finalize_data_copy_run:update_service_account")
        logger.info(f"[FolderCopy] Service account recorded: {_sa} ({_sa_src})")

    logger.info(
        f"[FolderCopy] Run {run_id} finalized: status={overall_status} | "
        f"total={total} | successful={successful} | failed={failed} | "
        f"not_found={not_found} | skipped={skipped}"
    )
    return {
        'run_id': run_id,
        'status': overall_status,
        'total_folders': total,
        'successful_folders': successful,
        'failed_folders': failed,
        'not_found_folders': not_found,
        'skipped_folders': skipped,
    }


@task.pyspark(conn_id='spark_default')
def generate_data_copy_html_report(finalize_result: dict, run_id: str, spark,
                                   cluster_setup: dict = None, **context) -> dict:
    """Generate HTML report for folder-only data copy run and write to S3."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.data_copy_runs
        WHERE run_id = '{run_id}'
    """).collect()
    run_row = run_info[0] if run_info else None

    folders = spark.sql(f"""
        SELECT * FROM {tracking_db}.data_copy_status
        WHERE run_id = '{run_id}'
        ORDER BY source_path
    """).collect()

    non_distcp_statuses = ('EMPTY_SOURCE', 'SOURCE_NOT_FOUND', 'SKIPPED')
    copied_rows = [f for f in folders if (f.status or '') not in non_distcp_statuses]

    badge_classes = {
        'VALIDATED': 'status-completed',
        'COMPLETED': 'status-completed',
        'FAILED': 'status-failed',
        'VALIDATION_FAILED': 'status-failed',
        'VALIDATION_SKIPPED': 'status-failed',
        'EMPTY_SOURCE': 'status-empty',
        'SOURCE_NOT_FOUND': 'status-not-found',
        'SKIPPED': 'status-skipped',
    }

    total_folders     = len(folders)
    validated         = sum(1 for f in folders if f.status == 'VALIDATED')
    failed            = sum(1 for f in folders if (f.status or '') in ('FAILED', 'VALIDATION_FAILED', 'VALIDATION_SKIPPED'))
    source_not_found  = sum(1 for f in folders if (f.status or '') == 'SOURCE_NOT_FOUND')
    empty_source      = sum(1 for f in folders if (f.status or '') == 'EMPTY_SOURCE')
    skipped           = sum(1 for f in folders if (f.status or '') == 'SKIPPED')
    successful        = validated + empty_source + source_not_found
    incremental       = sum(1 for f in folders if f.is_incremental)
    total_bytes       = sum(f.dest_size_bytes or 0 for f in folders)
    total_files       = sum(f.dest_file_count or 0 for f in folders)
    total_gb          = total_bytes / (1024 ** 3)

    size_match_count      = sum(1 for f in copied_rows if f.size_match)
    size_mismatch_count   = len(copied_rows) - size_match_count
    fcount_match_count    = sum(1 for f in copied_rows if f.file_count_match)
    fcount_mismatch_count = len(copied_rows) - fcount_match_count
    total_src_gb  = sum(f.source_size_bytes or 0 for f in copied_rows) / (1024 ** 3)
    total_dest_gb = sum(f.dest_size_bytes or 0 for f in copied_rows) / (1024 ** 3)
    size_diff_pct = (
        abs(total_src_gb - total_dest_gb) / total_src_gb * 100 if total_src_gb > 0 else 0.0
    )

    _throughputs = [
        float(getattr(f, 'throughput_mbps', 0) or 0)
        for f in folders
        if float(getattr(f, 'throughput_mbps', 0) or 0) > 0
    ]
    avg_throughput = (sum(_throughputs) / len(_throughputs)) if _throughputs else 0.0

    run_status = (run_row.status if run_row else 'UNKNOWN')
    dag_run_id = str(getattr(run_row, 'dag_run_id', '') or '') if run_row else ''

    if isinstance(cluster_setup, dict):
        sa_user = str(cluster_setup.get('service_account_user_id') or '').strip()
        sa_source = str(cluster_setup.get('service_account_source') or '').strip()
    else:
        sa_user, sa_source = '', ''
    if not sa_user and run_row is not None:
        sa_user = str(getattr(run_row, 'service_account_user_id', '') or '').strip()
        sa_source = str(getattr(run_row, 'service_account_source', '') or '').strip()
    if not sa_user:
        sa_user = str(config.get('service_account_user_id') or '').strip()
        sa_source = 'config:service_account_user_id' if sa_user else 'unknown'
    sa_display = sa_user or 'not configured'
    sa_source = sa_source or 'unknown'

    def _status_row(f, colspan):
        status_val = f.status or ''
        source_cell = _html_escape(str(f.source_path or ''))
        if status_val == 'EMPTY_SOURCE':
            return f"""
                <tr>
                    <td>{source_cell}</td>
                    <td colspan="{colspan}">
                        <span class="status-badge status-empty">EMPTY_SOURCE</span>
                    </td>
                </tr>
"""
        return f"""
                <tr>
                    <td>{source_cell}</td>
                    <td colspan="{colspan}">
                        <span class="status-badge status-not-found">SOURCE_NOT_FOUND</span>
                    </td>
                </tr>
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Folder Data Copy Report - {run_id}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 8px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .summary-card.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .summary-card.warning {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        .summary-card.info {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}
        th {{
            background-color: #34495e;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status-badge {{
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            display: inline-block;
        }}
        .status-completed {{
            background-color: #d4edda;
            color: #155724;
        }}
        .status-failed {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .status-skipped {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .status-empty {{
            background-color: #e8f4fd;
            color: #1a6fa3;
        }}
        .status-not-found {{
            background-color: #e2e3e5;
            color: #6c757d;
        }}
        .status-warning {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .metric {{
            font-weight: bold;
            color: #2980b9;
        }}
        .duration {{
            color: #7f8c8d;
            font-size: 12px;
        }}
        .validation-pass {{
            color: #27ae60;
            font-weight: bold;
        }}
        .validation-fail {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .validation-warn {{
            color: #856404;
            background-color: #fff3cd;
            font-weight: bold;
            padding: 2px 6px;
            border-radius: 4px;
        }}
        .timestamp {{
            color: #95a5a6;
            font-size: 12px;
        }}
        td.status-cell {{
            max-width: 320px;
        }}
        .status-reason {{
            margin-top: 6px;
            color: #4d5656;
            font-size: 12px;
            line-height: 1.4;
            white-space: normal;
            word-break: break-word;
        }}
        .section-divider {{
            margin: 40px 0;
            border-top: 2px dashed #ecf0f1;
        }}
    </style>
</head>
<body>
<div class="container">
    <h1>Folder Data Copy Report</h1>
    <div class="timestamp">
        Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
        Run ID: <strong>{run_id}</strong><br>
        DAG Run: <strong>{_html_escape(dag_run_id)}</strong><br>
        Service Account: <strong>{_html_escape(sa_display)}</strong> <span style="opacity:0.7">({_html_escape(sa_source)})</span>
    </div>

    <h2>Copy Summary</h2>
    <div class="summary-grid">
        <div class="summary-card">
            <h3>TOTAL FOLDERS</h3>
            <p class="value">{total_folders}</p>
        </div>
        <div class="summary-card success">
            <h3>SUCCESSFUL</h3>
            <p class="value">{successful}</p>
        </div>
        <div class="summary-card warning">
            <h3>FAILED</h3>
            <p class="value">{failed}</p>
        </div>
        <div class="summary-card">
            <h3>SOURCE NOT FOUND</h3>
            <p class="value">{source_not_found}</p>
        </div>
        <div class="summary-card info">
            <h3>EMPTY SOURCE</h3>
            <p class="value">{empty_source}</p>
        </div>
        <div class="summary-card">
            <h3>SKIPPED</h3>
            <p class="value">{skipped}</p>
        </div>
        <div class="summary-card info">
            <h3>TOTAL DATA</h3>
            <p class="value">{total_gb:.6f} GB</p>
        </div>
        <div class="summary-card info">
            <h3>TOTAL FILES</h3>
            <p class="value">{total_files:,}</p>
        </div>
        <div class="summary-card">
            <h3>INCREMENTAL RUNS</h3>
            <p class="value">{incremental}</p>
        </div>
        <div class="summary-card info">
            <h3>AVG THROUGHPUT (MB/s)</h3>
            <p class="value">{avg_throughput:.2f}</p>
        </div>
    </div>

    <div class="section-divider"></div>

    <h2>Data Validation Summary</h2>
    <div class="summary-grid">
        <div class="summary-card {'success' if size_mismatch_count == 0 else 'warning'}">
            <h3>SIZE MATCH</h3>
            <p class="value">{size_match_count} / {size_match_count + size_mismatch_count}</p>
        </div>
        <div class="summary-card {'success' if fcount_mismatch_count == 0 else 'warning'}">
            <h3>FILE COUNT MATCH</h3>
            <p class="value">{fcount_match_count} / {fcount_match_count + fcount_mismatch_count}</p>
        </div>
        <div class="summary-card info">
            <h3>SOURCE SIZE</h3>
            <p class="value">{total_src_gb:.6f} GB</p>
        </div>
        <div class="summary-card info">
            <h3>DEST SIZE</h3>
            <p class="value">{total_dest_gb:.6f} GB</p>
        </div>
        <div class="summary-card {'success' if size_diff_pct < 1.0 else 'warning'}">
            <h3>SIZE DELTA</h3>
            <p class="value">{size_diff_pct:.2f}%</p>
        </div>
    </div>

    <div class="section-divider"></div>

    <h2>Folder Copy Details</h2>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Destination</th>
                <th>Status</th>
                <th>Incremental</th>
                <th>DistCp Duration</th>
                <th>YARN App ID</th>
                <th>Total Duration</th>
            </tr>
        </thead>
        <tbody>
"""

    for f in folders:
        status = f.status or ''
        if status in ('EMPTY_SOURCE', 'SOURCE_NOT_FOUND'):
            html += _status_row(f, 6)
            continue

        badge_cls = badge_classes.get(status, 'status-warning')
        _reason = str(f.error_message or '')
        badge_title_attr = f' title="{_html_escape(_reason)}"' if _reason else ''
        status_reason_html = (
            f'\n                        <div class="status-reason">{_html_escape(_reason[:200])}</div>'
            if _reason else ''
        )
        _dur = float(getattr(f, 'distcp_duration_seconds', 0) or 0)
        _started = getattr(f, 'started_at', None)
        _completed = getattr(f, 'completed_at', None)
        _total_dur = _dur
        if _started is not None and _completed is not None:
            try:
                _total_dur = max((_completed - _started).total_seconds(), 0.0)
            except Exception:
                _total_dur = _dur
        dur_cell = f"{_dur:.1f}s" if _dur else 'N/A'
        if f.is_incremental:
            dur_cell += " <span style='background-color: #fff3cd; padding: 2px 6px; border-radius: 4px; font-size: 10px;'>INCREMENTAL</span>"

        _yarn_raw = str(getattr(f, 'yarn_application_id', '') or '')
        _yarn_ids = [x for x in _yarn_raw.split(',') if x]
        if len(_yarn_ids) > 1:
            _yarn_list = "".join(
                f"<div style='font-family:monospace;color:#666;font-size:11px;'>{_html_escape(i)}</div>"
                for i in _yarn_ids
            )
            yarn_cell = (
                f"<details style='font-size:11px;color:#666;'>"
                f"<summary style='cursor:pointer;color:#2980b9;'>{len(_yarn_ids)} YARN app IDs</summary>"
                f"{_yarn_list}</details>"
            )
        elif _yarn_ids:
            yarn_cell = f"<small style='font-family:monospace;color:#666;'>{_html_escape(_yarn_ids[0])}</small>"
        else:
            yarn_cell = 'N/A'

        html += f"""
                <tr>
                    <td>{_html_escape(str(f.source_path or ''))}</td>
                    <td>{_html_escape(str(f.dest_bucket or ''))}/{_html_escape(str(f.dest_path or ''))}</td>
                    <td class="status-cell">
                        <span class="status-badge {badge_cls}"{badge_title_attr}>{status}</span>{status_reason_html}
                    </td>
                    <td style="text-align:center">{'✓' if f.is_incremental else ''}</td>
                    <td class="duration">{dur_cell}</td>
                    <td>{yarn_cell}</td>
                    <td class="metric">{_total_dur:.1f}s</td>
                </tr>
"""

    html += """
        </tbody>
    </table>

    <div class="section-divider"></div>

    <h2>Data Validation Results</h2>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Source Size (GB)</th>
                <th>S3 Size Before (GB)</th>
                <th>S3 Size After (GB)</th>
                <th>S3 Size Transferred (GB)</th>
                <th>Size Match</th>
                <th>Source Files</th>
                <th>S3 Files Before</th>
                <th>S3 Files After</th>
                <th>S3 Files Transferred</th>
                <th>File Count Match</th>
            </tr>
        </thead>
        <tbody>
"""

    for f in folders:
        status = f.status or ''
        if status in ('EMPTY_SOURCE', 'SOURCE_NOT_FOUND'):
            html += _status_row(f, 10)
            continue

        src_gb = (f.source_size_bytes or 0) / (1024 ** 3)
        dest_gb = (f.dest_size_bytes or 0) / (1024 ** 3)
        transferred_gb = (f.bytes_copied or 0) / (1024 ** 3)
        before_gb = max(dest_gb - transferred_gb, 0.0)
        files_after = f.dest_file_count or 0
        files_transferred = f.files_copied or 0
        files_before = max(files_after - files_transferred, 0)

        size_match_class = 'validation-pass' if f.size_match else 'validation-fail'
        size_match_icon = '✓ PASS' if f.size_match else '✗ FAIL'
        count_match_class = 'validation-pass' if f.file_count_match else 'validation-fail'
        count_match_icon = '✓ PASS' if f.file_count_match else '✗ FAIL'

        html += f"""
                <tr>
                    <td>{_html_escape(str(f.source_path or ''))}</td>
                    <td class="metric">{src_gb:.6f}</td>
                    <td class="metric">{before_gb:.6f}</td>
                    <td class="metric">{dest_gb:.6f}</td>
                    <td class="metric">{transferred_gb:.6f}</td>
                    <td class="{size_match_class}">{size_match_icon}</td>
                    <td class="metric">{(f.source_file_count or 0):,}</td>
                    <td class="metric">{files_before:,}</td>
                    <td class="metric">{files_after:,}</td>
                    <td class="metric">{files_transferred:,}</td>
                    <td class="{count_match_class}">{count_match_icon}</td>
                </tr>
"""

    html += """
        </tbody>
    </table>

    <div class="section-divider"></div>

    <h2>Performance Metrics</h2>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Data Volume</th>
                <th>DistCp Speed</th>
                <th>Files/Second</th>
                <th>End-to-End Duration</th>
            </tr>
        </thead>
        <tbody>
"""

    for f in folders:
        if (f.status or '') == 'SOURCE_NOT_FOUND':
            continue
        _dur = float(getattr(f, 'distcp_duration_seconds', 0) or 0)
        _started = getattr(f, 'started_at', None)
        _completed = getattr(f, 'completed_at', None)
        _total_dur = _dur
        if _started is not None and _completed is not None:
            try:
                _total_dur = max((_completed - _started).total_seconds(), 0.0)
            except Exception:
                _total_dur = _dur
        data_gb = (f.dest_size_bytes or 0) / (1024 ** 3)
        speed_mbps = float(getattr(f, 'throughput_mbps', 0) or 0)
        files_per_sec = (f.dest_file_count or 0) / (_dur or 1)
        html += f"""
                <tr>
                    <td>{_html_escape(str(f.source_path or ''))}</td>
                    <td class="metric">{data_gb:.6f} GB</td>
                    <td class="metric">{speed_mbps:.6f} MB/s</td>
                    <td class="metric">{files_per_sec:,.0f}</td>
                    <td class="metric">{_total_dur:.1f}s ({_total_dur / 60:.1f}m)</td>
                </tr>
"""

    html += """
        </tbody>
    </table>

    <div class="section-divider"></div>

    <h2>Phase Timings &amp; Diagnostics</h2>
    <p class="timestamp">
        Seconds spent per phase before/after DistCp. A row with <em>YARN app created = no</em> and a
        pre-DistCp failure phase (source_check, s3_list_before, source_scan) stopped before DistCp was
        submitted. DistCp duration here is the DistCp command only; the DistCp Duration column above
        covers the whole edge script, listings included.
    </p>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Parent Folder</th>
                <th>S3 List Before (s)</th>
                <th>Source Scan (s)</th>
                <th>DistCp Only (s)</th>
                <th>S3 List After (s)</th>
                <th>Validation List (s)</th>
                <th>Listing</th>
                <th>YARN App Created</th>
                <th>Failure Phase</th>
                <th>Exit Code</th>
                <th>Memory Kill?</th>
                <th>Edge Node (SSSD)</th>
            </tr>
        </thead>
        <tbody>
"""

    def _secs(value):
        try:
            return f"{float(value):,.0f}" if value is not None else 'N/A'
        except (TypeError, ValueError):
            return 'N/A'

    for f in folders:
        try:
            _diag = json.loads(getattr(f, 'diagnostics_json', None) or '{}')
        except Exception:
            _diag = {}
        _phases = _diag.get('phase_seconds') or {}
        _fphase = getattr(f, 'failure_phase', None) or ''
        _yarn_created = getattr(f, 'yarn_app_created', None)
        _exit = getattr(f, 'script_exit_code', None)
        _edge = _diag.get('edge_host') or ''
        _sssd = _diag.get('edge_sssd_running') or ''
        html += f"""
                <tr>
                    <td>{_html_escape(str(f.source_path or ''))}</td>
                    <td>{_html_escape(str(getattr(f, 'parent_source_path', '') or ''))}</td>
                    <td class="metric">{_secs(getattr(f, 's3_list_before_seconds', None))}</td>
                    <td class="metric">{_secs(getattr(f, 'source_scan_seconds', None))}</td>
                    <td class="metric">{_secs(_phases.get('distcp'))}</td>
                    <td class="metric">{_secs(getattr(f, 's3_list_after_seconds', None))}</td>
                    <td class="metric">{_secs(getattr(f, 'validation_listing_seconds', None))}</td>
                    <td>{_html_escape(str(_diag.get('listing_strategy') or ''))}{' (files only)' if _diag.get('shallow') else ''}</td>
                    <td>{'yes' if _yarn_created else ('no' if _yarn_created is not None else 'N/A')}</td>
                    <td>{_html_escape(_fphase) or '&mdash;'}</td>
                    <td>{_html_escape(str(_exit)) if _exit is not None else 'N/A'}</td>
                    <td>{'<span class="validation-warn">suspected</span>' if _diag.get('memory_kill_suspected') else 'no'}</td>
                    <td>{_html_escape(_edge) or 'N/A'}{(' (SSSD ' + _html_escape(str(_sssd)) + ')') if _sssd else ''}</td>
                </tr>
"""

    html += """
        </tbody>
    </table>

    <div style="margin-top:50px; padding-top:20px; border-top:1px solid #ecf0f1; color:#95a5a6; font-size:12px;">
        <p>This report was automatically generated by the Folder Data Copy DAG.</p>
    </div>
</div>
</body>
</html>
"""

    portal_run_id = context.get('params', {}).get('run_id') or run_id
    report_filename = f"{portal_run_id}_data_copy_report.html"
    report_path = f"{report_location}/{report_filename}"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )
    out = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    stream = fs.create(out, True)
    stream.write(html.encode('utf-8'))
    stream.close()

    logger.info(f"[FolderCopy] HTML report written to {report_path}")
    return {
        'report_path': report_path,
        'overall_status': run_status,
        'total_folders': total_folders,
        'successful_folders': successful,
    }


@task.pyspark(conn_id='spark_default')
def send_data_copy_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send the folder data copy HTML report via SMTP."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id   = config.get('smtp_conn_id', 'smtp_default')
    recipients_raw = config.get('email_recipients', '')

    if not recipients_raw or not recipients_raw.strip():
        logger.warning("[FolderCopy] No recipients configured in 'migration_email_recipients' variable. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients  = [r.strip() for r in recipients_raw.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')
    overall_status = report_result.get('overall_status', 'UNKNOWN')
    total_folders = report_result.get('total_folders', 0)
    successful_folders = report_result.get('successful_folders', 0)

    try:
        logger.info(f"[FolderCopy] Reading HTML report from S3: {report_path}")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path),
            hadoop_conf
        )
        s3_path_obj = spark._jvm.org.apache.hadoop.fs.Path(report_path)
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(s3_path_obj), "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_data_copy_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=(
                f"Folder Data Copy Report - {run_id} - {overall_status} "
                f"({successful_folders}/{total_folders})"
            ),
            html_content=f"<p>Please find the Folder Data Copy report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[FolderCopy] Report email sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[FolderCopy] Failed to send report email: {str(e)}")
        if is_permanent_error("send_email", e):
            permanent_fail("send_data_copy_report_email", e)
        raise Exception(f"Failed to send Folder Data Copy report email: {str(e)}") from e


# =============================================================================
# DAG 3 DEFINITION: FOLDER-ONLY DATA COPY
# =============================================================================

with DAG(
    dag_id='folder_only_data_copy',
    default_args=default_args,
    description='Copy folders from source cluster (MapR/HDP) to S3 via DistCp — no Hive metadata',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=['migration', 'source-cluster', 's3', 'folder-copy'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/folder_copy.xlsx',
            type='string',
            description='S3 path to Excel config file (columns: source_path, target_bucket, dest_folder)'
        ),
        'tenant': Param(
            default=None,
            type=['null', 'string'],
            description=(
                'Tenant/environment key from the migration_tenant_profiles Variable '
                '(e.g. tenant_1, tenant_2). Blank = use the global cluster_* Variables.'
            )
        ),
        'split_subfolders': Param(
            default=None,
            type=['null', 'boolean'],
            description=(
                'Copy each subfolder as its own DistCp task (for very large, deeply nested folders). '
                'Blank = use the Variable folder_copy_split_subfolders / the per-row Excel column '
                '(default: off).'
            )
        ),
        'split_depth': Param(
            default=None,
            type=['null', 'integer'],
            description='Directory levels to split when split_subfolders is on. Blank = 1.'
        ),
        'listing_strategy': Param(
            default='',
            type='string',
            enum=['', 'ls_r', 'count'],
            description=(
                "How file counts/sizes are measured on source and S3: 'ls_r' = recursive ls + du "
                "(original), 'count' = hadoop fs -count. Blank = Variable "
                "folder_copy_listing_strategy, else 'ls_r'."
            )
        ),
    },
    render_template_as_native_obj=True,
    on_failure_callback=_folder_copy_dag_failure_callback,
) as dag_folder_copy:

    # Pre-flight checks
    t_fc_prereq  = validate_prerequisites_folder_copy()

    # Initialize tracking tables and create a run record
    t_fc_init   = init_folder_copy_tracking_tables()
    t_fc_run_id = create_data_copy_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}"
    )

    # Cluster authentication — receives tracking run_id
    t_fc_cluster = cluster_login_setup(run_id=t_fc_run_id)

    # Parse Excel — one dict per folder row
    t_fc_excel = parse_folder_copy_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_fc_run_id
    )

    # Optional: split each folder into per-subfolder copies. Passes the rows through
    # untouched (no SSH) unless split_subfolders is enabled.
    t_fc_expand = expand_folder_subfolders(
        folder_configs=t_fc_excel,
        cluster_setup=t_fc_cluster
    )

    # Per-folder: copy → record → validate → update validation (dynamically mapped)
    # max_active_tis_per_dagrun=3 caps concurrent DistCp YARN jobs
    t_fc_distcp = run_folder_distcp_ssh.override(
        trigger_rule='all_done', max_active_tis_per_dagrun=3
    ).partial(cluster_setup=t_fc_cluster).expand(folder_config=t_fc_expand)

    t_fc_record = record_data_copy_status.override(
        trigger_rule='all_done'
    ).expand(distcp_result=t_fc_distcp)

    t_fc_copy_validate = validate_data_copy.override(
        trigger_rule='all_done'
    ).expand(copy_status=t_fc_record)

    t_fc_val_status = update_data_copy_validation.override(
        trigger_rule='all_done'
    ).expand(validation_result=t_fc_copy_validate)

    # Finalize run — waits for all per-folder validation to finish (via dependency chain)
    t_fc_final = finalize_data_copy_run.override(trigger_rule='all_done')(
        run_id=t_fc_run_id,
        cluster_setup=t_fc_cluster
    )

    # Report and email
    t_fc_report = generate_data_copy_html_report.override(trigger_rule='all_done')(
        run_id=t_fc_run_id,
        finalize_result=t_fc_final,
        cluster_setup=t_fc_cluster,
        params="{{ params }}"
    )

    t_fc_email = send_data_copy_report_email.override(trigger_rule='all_done')(
        report_result=t_fc_report,
        run_id=t_fc_run_id
    )

    # Dependency chain — prereqs → init → run_id → excel → cluster → work
    # Per-folder chain is linear: distcp → record_status → validate → update_validation
    t_fc_prereq >> t_fc_init >> t_fc_run_id >> t_fc_cluster >> t_fc_excel
    t_fc_excel >> t_fc_expand >> t_fc_distcp >> t_fc_record >> t_fc_copy_validate >> t_fc_val_status
    t_fc_val_status >> t_fc_final >> t_fc_report >> t_fc_email
