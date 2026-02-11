"""
explain_query.py - Run EXPLAIN PLAN safely across Oracle, MySQL, PostgreSQL, SQL Server.

Analyzes execution plans and highlights potential issues:
- Full Table Scans
- High cost operations
- Missing index usage
- Cartesian products

Usage:
    python explain_query.py --db DWH --sql "SELECT * FROM SCHEMA.TABLE WHERE id = 1"
    python explain_query.py --db DWH --file query.sql
    python explain_query.py --db DWH --file query.sql --format json
"""

import argparse
import json
import sys
import os
import re
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type, is_select_only


# ============================================================================
# Oracle EXPLAIN
# ============================================================================

def _oracle_explain(cursor, sql: str) -> dict:
    """Run EXPLAIN PLAN FOR on Oracle and retrieve plan from DBMS_XPLAN."""
    stmt_id = f"agent_{uuid.uuid4().hex[:12]}"

    # Execute EXPLAIN PLAN
    cursor.execute(f"EXPLAIN PLAN SET STATEMENT_ID = '{stmt_id}' FOR {sql}")

    # Retrieve plan using DBMS_XPLAN
    cursor.execute(f"""
        SELECT PLAN_TABLE_OUTPUT
        FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '{stmt_id}', 'ALL'))
    """)
    plan_lines = [r[0] for r in cursor.fetchall()]

    # Clean up plan table
    try:
        cursor.execute(f"DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '{stmt_id}'")
        cursor.connection.commit()
    except Exception:
        pass

    # Analyze for issues
    issues = _analyze_oracle_plan(plan_lines)

    return {
        "db_type": "oracle",
        "plan": plan_lines,
        "issues": issues,
    }


def _analyze_oracle_plan(plan_lines: list[str]) -> list[dict]:
    """Detect potential performance issues in Oracle execution plan."""
    issues = []
    full_text = "\n".join(plan_lines)

    if "TABLE ACCESS FULL" in full_text:
        tables = re.findall(r"TABLE ACCESS FULL\s*\|\s*(\S+)", full_text)
        for t in tables:
            issues.append({
                "severity": "WARNING",
                "type": "FULL_TABLE_SCAN",
                "message": f"Full Table Scan trên bảng {t}. Cân nhắc thêm index hoặc filter partition.",
            })

    if "MERGE JOIN CARTESIAN" in full_text:
        issues.append({
            "severity": "CRITICAL",
            "type": "CARTESIAN_PRODUCT",
            "message": "Cartesian product detected! Kiểm tra lại JOIN conditions.",
        })

    if "HASH JOIN" in full_text:
        # Not always bad, but note it
        issues.append({
            "severity": "INFO",
            "type": "HASH_JOIN",
            "message": "Hash Join detected. OK cho bảng lớn, nhưng kiểm tra memory.",
        })

    # Check cost
    cost_matches = re.findall(r"\|\s*(\d+)\s*\|", full_text)
    if cost_matches:
        max_cost = max(int(c) for c in cost_matches)
        if max_cost > 100000:
            issues.append({
                "severity": "WARNING",
                "type": "HIGH_COST",
                "message": f"Cost cao ({max_cost:,}). Cân nhắc tối ưu hóa query.",
            })

    if not issues:
        issues.append({
            "severity": "OK",
            "type": "NO_ISSUES",
            "message": "Không phát hiện vấn đề rõ ràng trong execution plan.",
        })

    return issues


# ============================================================================
# MySQL EXPLAIN
# ============================================================================

def _mysql_explain(cursor, sql: str) -> dict:
    cursor.execute(f"EXPLAIN FORMAT=JSON {sql}")
    row = cursor.fetchone()
    plan_json = json.loads(row[0]) if row else {}

    # Also get traditional EXPLAIN
    cursor.execute(f"EXPLAIN {sql}")
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, r)) for r in cursor.fetchall()]

    issues = _analyze_mysql_plan(rows, plan_json)

    return {
        "db_type": "mysql",
        "plan": rows,
        "plan_json": plan_json,
        "issues": issues,
    }


def _analyze_mysql_plan(rows: list[dict], plan_json: dict) -> list[dict]:
    issues = []
    for r in rows:
        if r.get("type") == "ALL":
            issues.append({
                "severity": "WARNING",
                "type": "FULL_TABLE_SCAN",
                "message": f"Full table scan trên {r.get('table')} ({r.get('rows', '?')} rows). Cần index.",
            })
        if r.get("Extra") and "Using filesort" in str(r["Extra"]):
            issues.append({
                "severity": "INFO",
                "type": "FILESORT",
                "message": f"Filesort trên {r.get('table')}. Cân nhắc index cho ORDER BY.",
            })
        if r.get("Extra") and "Using temporary" in str(r["Extra"]):
            issues.append({
                "severity": "INFO",
                "type": "TEMP_TABLE",
                "message": f"Temporary table trên {r.get('table')}. Cân nhắc tối ưu GROUP BY.",
            })

    if not issues:
        issues.append({
            "severity": "OK", "type": "NO_ISSUES",
            "message": "Không phát hiện vấn đề rõ ràng.",
        })
    return issues


# ============================================================================
# PostgreSQL EXPLAIN
# ============================================================================

def _pg_explain(cursor, sql: str) -> dict:
    """Run EXPLAIN (without ANALYZE to avoid actual execution)."""
    cursor.execute(f"EXPLAIN (FORMAT TEXT, COSTS true, VERBOSE true) {sql}")
    plan_lines = [r[0] for r in cursor.fetchall()]

    # Also get JSON format for structured analysis
    cursor.execute(f"EXPLAIN (FORMAT JSON, COSTS true) {sql}")
    plan_json = cursor.fetchone()[0]

    issues = _analyze_pg_plan(plan_lines)

    return {
        "db_type": "postgresql",
        "plan": plan_lines,
        "plan_json": plan_json,
        "issues": issues,
    }


def _analyze_pg_plan(plan_lines: list[str]) -> list[dict]:
    issues = []
    full_text = "\n".join(plan_lines)

    if "Seq Scan" in full_text:
        tables = re.findall(r"Seq Scan on (\S+)", full_text)
        for t in tables:
            issues.append({
                "severity": "WARNING",
                "type": "FULL_TABLE_SCAN",
                "message": f"Sequential Scan trên {t}. Cân nhắc thêm index.",
            })

    if "Nested Loop" in full_text:
        cost_match = re.findall(r"Nested Loop.*?cost=[\d.]+\.\.([\d.]+)", full_text)
        for c in cost_match:
            if float(c) > 100000:
                issues.append({
                    "severity": "WARNING",
                    "type": "NESTED_LOOP_HIGH_COST",
                    "message": f"Nested Loop cost cao ({c}). Kiểm tra join conditions.",
                })

    if not issues:
        issues.append({
            "severity": "OK", "type": "NO_ISSUES",
            "message": "Không phát hiện vấn đề rõ ràng.",
        })
    return issues


# ============================================================================
# SQL Server EXPLAIN (SHOWPLAN)
# ============================================================================

def _sqlserver_explain(cursor, sql: str) -> dict:
    """Run SHOWPLAN on SQL Server."""
    # Turn on SHOWPLAN_TEXT or SHOWPLAN_XML
    cursor.execute("SET SHOWPLAN_TEXT ON")
    
    try:
        cursor.execute(sql)
        plan_lines = [r[0] for r in cursor.fetchall()]
    finally:
        cursor.execute("SET SHOWPLAN_TEXT OFF")

    issues = _analyze_sqlserver_plan(plan_lines)

    return {
        "db_type": "sqlserver",
        "plan": plan_lines,
        "issues": issues,
    }


def _analyze_sqlserver_plan(plan_lines: list[str]) -> list[dict]:
    """Detect potential performance issues in SQL Server execution plan."""
    issues = []
    full_text = "\n".join(plan_lines)

    if "Table Scan" in full_text or "Clustered Index Scan" in full_text:
        issues.append({
            "severity": "WARNING",
            "type": "FULL_TABLE_SCAN",
            "message": "Table Scan hoặc Clustered Index Scan detected. Cân nhắc thêm index.",
        })

    if "Nested Loops" in full_text:
        issues.append({
            "severity": "INFO",
            "type": "NESTED_LOOPS",
            "message": "Nested Loops join detected. OK cho bảng nhỏ, kiểm tra nếu bảng lớn.",
        })

    if "Hash Match" in full_text:
        issues.append({
            "severity": "INFO",
            "type": "HASH_JOIN",
            "message": "Hash Match join detected. Bình thường cho bảng lớn, kiểm tra memory.",
        })

    if "Sort" in full_text:
        issues.append({
            "severity": "INFO",
            "type": "SORT_OPERATION",
            "message": "Sort operation detected. Cân nhắc index cho ORDER BY.",
        })

    if not issues:
        issues.append({
            "severity": "OK",
            "type": "NO_ISSUES",
            "message": "Không phát hiện vấn đề rõ ràng trong execution plan.",
        })

    return issues


# ============================================================================
# Public API
# ============================================================================

_EXPLAIN_FUNCS = {
    "oracle": _oracle_explain,
    "mysql": _mysql_explain,
    "postgresql": _pg_explain,
    "sqlserver": _sqlserver_explain,
}


def explain_query(sql: str, db_alias: str = "DWH") -> dict:
    """
    Run EXPLAIN PLAN on a SQL query.

    Returns dict with: db_type, plan (list of lines or rows), issues (list of dicts).
    """
    if not is_select_only(sql):
        raise ValueError("Chỉ cho phép EXPLAIN trên SELECT/WITH statements.")

    db_type = get_db_type(db_alias)
    func = _EXPLAIN_FUNCS[db_type]

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()
        return func(cursor, sql)


# ============================================================================
# Formatters
# ============================================================================

def format_text(result: dict) -> str:
    lines = []
    lines.append("=" * 90)
    lines.append(f"EXPLAIN PLAN ({result['db_type']})")
    lines.append("=" * 90)

    # Plan output
    plan = result.get("plan", [])
    if isinstance(plan, list) and plan and isinstance(plan[0], str):
        for line in plan:
            lines.append(line)
    elif isinstance(plan, list):
        # MySQL rows (list of dicts)
        for row in plan:
            lines.append(json.dumps(row, default=str))
    else:
        lines.append(str(plan))

    # Issues
    lines.append("\n" + "=" * 90)
    lines.append("PHÂN TÍCH VẤN ĐỀ")
    lines.append("=" * 90)
    for issue in result.get("issues", []):
        severity = issue["severity"]
        icon = {"CRITICAL": "[!!!]", "WARNING": "[!]", "INFO": "[i]", "OK": "[OK]"}.get(severity, "[?]")
        lines.append(f"\n  {icon} [{issue['type']}] {issue['message']}")

    return "\n".join(lines)


def format_json(result: dict) -> str:
    return json.dumps(result, indent=2, default=str, ensure_ascii=False)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run EXPLAIN PLAN safely on a SQL query.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", help="SQL query string")
    group.add_argument("--file", help="Path to .sql file")
    parser.add_argument("--db", default="DWH", help="Database alias (default: DWH)")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    args = parser.parse_args()

    sql = args.sql
    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            sql = f.read()

    # Strip trailing semicolons (some drivers don't like them)
    sql = sql.strip().rstrip(";")

    try:
        result = explain_query(sql, args.db)
        if args.format == "json":
            print(format_json(result))
        else:
            print(format_text(result))
    except Exception as e:
        print(f"Lỗi: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
