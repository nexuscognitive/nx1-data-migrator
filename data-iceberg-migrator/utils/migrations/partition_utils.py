# -*- coding: utf-8 -*-
"""
Pure-Python partition filtering and SQL clause helpers.
"""
import fnmatch
import re

try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote


def _normalize_filter_expr(filter_expr):
    """
    Convert path-style filter 'year=2024/month=1' to SQL-style
    'year=2024 AND month=1'. If already SQL-style, return as-is.
    Only triggers when '/' is present but no AND/OR/comparison operators.
    """
    if not filter_expr:
        return filter_expr
    if '/' in filter_expr and not re.search(r'\b(AND|OR)\b|[><!=]', filter_expr, re.IGNORECASE):
        segments = filter_expr.split('/')
        conditions = []
        for seg in segments:
            seg = seg.strip()
            if seg == '*':
                return filter_expr
            if '=' in seg:
                k, _, v = seg.partition('=')
                v = v.strip()
                if not (v.startswith("'") and v.endswith("'")):
                    v = "'" + v + "'"
                conditions.append(k.strip() + '=' + v)
        if conditions:
            return ' AND '.join(conditions)
    return filter_expr

_LAST_N_RE = re.compile(r'^last_n_partitions\s*=\s*(\d+)$', re.IGNORECASE)
_GLOB_RE = re.compile(r'[\*\?]')
_SQL_PRED_RE = re.compile(
    r"(\w+)\s*(>=|<=|!=|=|>|<)\s*'([^']*)'$"
    r"|(\w+)\s*(>=|<=|!=|=|>|<)\s*(\S+)$",
)


def _classify_term(term):
    """
    Return ('last_n', N) | ('glob', pattern) | ('sql', expr) | ('unknown', term).
    """
    term = term.strip()
    m = _LAST_N_RE.match(term)
    if m:
        return ('last_n', int(m.group(1)))

    if _GLOB_RE.search(term):
        return ('glob', term)

    if _SQL_PRED_RE.search(term) or re.search(r'\b(AND|OR)\b|[><!=]', term, re.IGNORECASE):
        return ('sql', term)

    return ('unknown', term)


def _eval_filter(kv, expr):
    """
    Evaluate a simple SQL filter expression against a partition kv dict.
    Supports: AND, OR, =, !=, >, >=, <, <=
    Values are compared as strings, with numeric coercion attempted.
    """
    expr = expr.strip()

    or_parts = re.split(r'\bOR\b', expr, flags=re.IGNORECASE)
    if len(or_parts) > 1:
        return any(_eval_filter(kv, part.strip()) for part in or_parts)

    and_parts = re.split(r'\bAND\b', expr, flags=re.IGNORECASE)
    if len(and_parts) > 1:
        return all(_eval_filter(kv, part.strip()) for part in and_parts)

    expr_stripped = expr.strip().strip('()')
    m = re.match(
        r"(\w+)\s*(>=|<=|!=|=|>|<)\s*'([^']*)'$"
        r"|(\w+)\s*(>=|<=|!=|=|>|<)\s*(\S+)$",
        expr_stripped
    )
    if not m:
        return False

    if m.group(1):
        col, op, val = m.group(1), m.group(2), m.group(3)
    else:
        col, op, val = m.group(4), m.group(5), m.group(6)

    actual = kv.get(col)
    if actual is None:
        return False

    try:
        a, b = float(actual), float(val)
    except (ValueError, TypeError):
        a, b = str(actual), str(val)

    if op == '=':
        return a == b
    if op == '!=':
        return a != b
    if op == '>':
        return a > b
    if op == '>=':
        return a >= b
    if op == '<':
        return a < b
    if op == '<=':
        return a <= b
    return False


def apply_partition_filter(partitions, filter_expr):
    if not filter_expr:
        return list(partitions)

    # Split on commas to get individual terms, then classify each
    raw_terms = [t.strip() for t in filter_expr.split(',') if t.strip()]
    if len(raw_terms) == 1:
        raw_terms = [_normalize_filter_expr(raw_terms[0])]

    classified = [_classify_term(t) for t in raw_terms]

    # Collect matched partitions per term, then union preserving order
    matched = set()

    for kind, value in classified:
        if kind == 'last_n':
            n = value
            tail = partitions[-n:] if n < len(partitions) else list(partitions)
            for p in tail:
                matched.add(p)

        elif kind == 'glob':
            for p in partitions:
                if fnmatch.fnmatch(p, value):
                    matched.add(p)

        elif kind == 'sql':
            sql_expr = _normalize_filter_expr(value)
            for p in partitions:
                kv = {}
                for segment in p.split('/'):
                    if '=' in segment:
                        k, _, v = segment.partition('=')
                        kv[k.strip()] = unquote(v.strip().strip("'\""))
                if _eval_filter(kv, sql_expr):
                    matched.add(p)

    return [p for p in partitions if p in matched]


def partitions_to_where_clause(partitions):
    if not partitions:
        return "1=0"

    clauses = []
    for part_str in partitions:
        conditions = []
        for segment in part_str.strip().split('/'):
            if '=' in segment:
                k, _, v = segment.partition('=')
                v_clean = unquote(v.strip()).replace("'", "''")
                conditions.append("`%s`='%s'" % (k.strip(), v_clean))
        if conditions:
            clauses.append("(" + " AND ".join(conditions) + ")")

    return " OR ".join(clauses) if clauses else "1=1"
