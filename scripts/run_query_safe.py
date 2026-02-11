"""
run_query_safe.py - Execute SELECT queries with safety limits.

Safety measures:
- ONLY allows SELECT/WITH statements (blocks all DML/DDL)
- Wraps query with row limit (ROWNUM/LIMIT/FETCH FIRST)
- Sets statement timeout
- Formatted output via tabulate or built-in formatter

Usage:
    python run_query_safe.py --db DWH --sql "SELECT * FROM SCHEMA.TABLE WHERE id = 1"
    python run_query_safe.py --db DWH --file query.sql --limit 50 --timeout 60
    python run_query_safe.py --db DWH --file query.sql --count-only
    python run_query_safe.py --db DWH --sql "SELECT ..." --format json
"""

import argparse
import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type, is_select_only, safe_execute


# ============================================================================
# Core
# ============================================================================

def run_query(sql: str, db_alias: str = "DWH", row_limit: int = 100,
              timeout_seconds: int = 30, count_only: bool = False) -> dict:
    """
    Execute a SELECT query safely.

    Returns:
        dict with: columns, rows, row_count, execution_time_ms, truncated
    """
    sql = sql.strip().rstrip(";")

    if not is_select_only(sql):
        raise ValueError(
            "CHỈ cho phép SELECT/WITH statements.\n"
            "Các lệnh INSERT/UPDATE/DELETE/DROP/ALTER/... bị chặn vì lý do an toàn."
        )

    db_type = get_db_type(db_alias)

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()

        if count_only:
            return _count_query(cursor, sql, db_type, timeout_seconds)

        # Set timeout
        if db_type == "mysql":
            cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {timeout_seconds * 1000}")
        elif db_type == "postgresql":
            cursor.execute(f"SET statement_timeout = '{timeout_seconds * 1000}'")
        elif db_type == "sqlserver":
            # SQL Server: timeout is set at connection level or via OPTION clause
            pass

        # Wrap with limit
        wrapped = _wrap_with_limit(sql, row_limit, db_type)

        start = time.time()
        cursor.execute(wrapped)
        rows = cursor.fetchall()
        elapsed_ms = round((time.time() - start) * 1000, 2)

        # Get column names
        columns = []
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]

        return {
            "columns": columns,
            "rows": [list(r) for r in rows],
            "row_count": len(rows),
            "execution_time_ms": elapsed_ms,
            "truncated": len(rows) >= row_limit,
            "row_limit": row_limit,
        }


def _count_query(cursor, sql: str, db_type: str, timeout: int) -> dict:
    """Execute COUNT(*) wrapper to get total row count."""
    if db_type == "mysql":
        cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {timeout * 1000}")
    elif db_type == "postgresql":
        cursor.execute(f"SET statement_timeout = '{timeout * 1000}'")
    elif db_type == "sqlserver":
        pass  # Timeout handled at connection level

    count_sql = f"SELECT COUNT(*) FROM ({sql}) count_wrapper"
    if db_type == "oracle":
        count_sql = f"SELECT COUNT(*) FROM ({sql})"
    elif db_type == "sqlserver":
        count_sql = f"SELECT COUNT(*) FROM ({sql}) AS count_wrapper"

    start = time.time()
    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    elapsed_ms = round((time.time() - start) * 1000, 2)

    return {
        "columns": ["COUNT"],
        "rows": [[count]],
        "row_count": 1,
        "total_rows": count,
        "execution_time_ms": elapsed_ms,
        "truncated": False,
    }


def _wrap_with_limit(sql: str, limit: int, db_type: str) -> str:
    sql_clean = sql.rstrip().rstrip(";")
    if db_type == "oracle":
        return f"SELECT * FROM ({sql_clean}) WHERE ROWNUM <= {limit}"
    elif db_type in ("mysql", "postgresql"):
        return f"SELECT * FROM ({sql_clean}) AS _limited LIMIT {limit}"
    elif db_type == "sqlserver":
        return f"SELECT TOP {limit} * FROM ({sql_clean}) AS _limited"
    return sql_clean


# ============================================================================
# Formatters
# ============================================================================

def format_text(result: dict) -> str:
    lines = []

    columns = result.get("columns", [])
    rows = result.get("rows", [])

    if not columns:
        lines.append("Query executed but returned no columns.")
        return "\n".join(lines)

    # Calculate column widths
    widths = [len(str(c)) for c in columns]
    for row in rows:
        for i, val in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(str(val) if val is not None else "NULL"))

    # Cap widths at 50 chars for readability
    widths = [min(w, 50) for w in widths]

    # Header
    header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(columns))
    sep = "-+-".join("-" * w for w in widths)
    lines.append(header)
    lines.append(sep)

    # Rows
    for row in rows:
        vals = []
        for i, val in enumerate(row):
            s = str(val) if val is not None else "NULL"
            if len(s) > 50:
                s = s[:47] + "..."
            if i < len(widths):
                vals.append(s.ljust(widths[i]))
            else:
                vals.append(s)
        lines.append(" | ".join(vals))

    # Footer
    lines.append(f"\n--- {result['row_count']} rows returned in {result['execution_time_ms']}ms ---")
    if result.get("truncated"):
        lines.append(f"(Kết quả bị giới hạn tại {result.get('row_limit', '?')} rows. Dùng --count-only để xem tổng.)")
    if result.get("total_rows") is not None:
        lines.append(f"Tổng số dòng: {result['total_rows']:,}")

    return "\n".join(lines)


def format_json(result: dict) -> str:
    return json.dumps(result, indent=2, default=str, ensure_ascii=False)


def format_markdown(result: dict) -> str:
    columns = result.get("columns", [])
    rows = result.get("rows", [])

    if not columns:
        return "No data returned."

    lines = []
    lines.append("| " + " | ".join(str(c) for c in columns) + " |")
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for row in rows:
        vals = [str(v) if v is not None else "NULL" for v in row]
        # Escape pipes
        vals = [v.replace("|", "\\|")[:50] for v in vals]
        lines.append("| " + " | ".join(vals) + " |")

    lines.append(f"\n_{result['row_count']} rows, {result['execution_time_ms']}ms_")
    if result.get("truncated"):
        lines.append(f"_(Giới hạn {result.get('row_limit', '?')} rows)_")

    return "\n".join(lines)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SELECT queries safely with limits.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", help="SQL query string")
    group.add_argument("--file", help="Path to .sql file")
    parser.add_argument("--db", default="DWH", help="Database alias (default: DWH)")
    parser.add_argument("--limit", type=int, default=100, help="Row limit (default: 100)")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds (default: 30)")
    parser.add_argument("--count-only", action="store_true", help="Only return total row count")
    parser.add_argument("--format", choices=["text", "json", "markdown"], default="text")
    args = parser.parse_args()

    sql = args.sql
    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            sql = f.read()

    try:
        result = run_query(
            sql, db_alias=args.db, row_limit=args.limit,
            timeout_seconds=args.timeout, count_only=args.count_only,
        )
        if args.format == "json":
            print(format_json(result))
        elif args.format == "markdown":
            print(format_markdown(result))
        else:
            print(format_text(result))
    except Exception as e:
        print(f"Lỗi: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
