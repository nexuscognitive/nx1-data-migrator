"""Shared test helpers for building mock data."""

from io import BytesIO
from unittest.mock import MagicMock


def make_excel_bytes(rows):
    """Create a real in-memory Excel file from a list of row dicts."""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    if rows:
        ws.append(list(rows[0].keys()))
        for row in rows:
            ws.append(list(row.values()))
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def setup_spark_excel(mock_spark, excel_bytes):
    """Wire mock_spark.read.format('binaryFile') to return given bytes."""
    content_row = MagicMock()
    content_row.content = excel_bytes
    df_mock = MagicMock()
    df_mock.select.return_value.first.return_value = content_row
    mock_spark.read.format.return_value.load.return_value = df_mock


def mock_ssh_stdout(exit_code=0, output=b''):
    """Create a mock SSH stdout object with given exit code and output."""
    s = MagicMock()
    s.channel.recv_exit_status.return_value = exit_code
    s.read.return_value = output
    return s


def distcp_call(discovery, table_names=None, budget=0.0, cluster_setup=None,
                pulled=None, **overrides):
    """Build kwargs for run_distcp_ssh.function.__wrapped__ from a discovery dict.

    The task now receives a descriptor and pulls its group's discovery from
    XCom, so every test needs a `ti` whose xcom_pull returns that group. Pass
    `pulled` to make the pull return something other than `discovery` — that is
    how the identity-assertion tests inject a mismatched group.
    """
    tables = discovery.get('tables', [])
    if table_names is not None:
        tables = [t for t in tables if t.get('source_table') in table_names]

    descriptor = {
        'group_map_index': discovery.get('_map_index', 0),
        'table_keys': [
            [t['source_table'], t.get('partition_filter') or ''] for t in tables
        ],
        'batch_index': 0,
        'batch_cost_secs': 0.0,
        'batch_budget_secs': budget,
        'run_id': discovery.get('run_id'),
        'source_database': discovery.get('source_database'),
    }
    descriptor.update(overrides)

    ti = MagicMock()
    ti.xcom_pull.return_value = discovery if pulled is None else pulled

    return {
        'batch': descriptor,
        'cluster_setup': cluster_setup or {'temp_dir': '/tmp/test', 'run_id': 'r'},
        'source_task_id': 'record_discovered_tables',
        'ti': ti,
    }


def pool_wiring(dag_file: str, task_name: str) -> tuple[str, str] | None:
    """(dag variable, base dag id) from `<task>.override(pool=distcp_pool(<dag>.dag_id, '<base>'))`.

    Read from the DAG source because the conftest's fake Airflow ignores
    override() keyword arguments.
    """
    import ast
    from pathlib import Path
    tree = ast.parse((Path(__file__).resolve().parent.parent / dag_file).read_text())
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'override' and isinstance(node.func.value, ast.Name)
                and node.func.value.id == task_name):
            continue
        pool = next((kw.value for kw in node.keywords if kw.arg == 'pool'), None)
        if (isinstance(pool, ast.Call) and getattr(pool.func, 'id', None) == 'distcp_pool'
                and len(pool.args) == 2 and isinstance(pool.args[0], ast.Attribute)
                and pool.args[0].attr == 'dag_id' and isinstance(pool.args[1], ast.Constant)):
            return pool.args[0].value.id, pool.args[1].value
    return None
