"""
sample_data.py - Get sample rows and data profile from a database table.

Two modes:
1. Sample mode (default): Fetch N sample rows for quick inspection
2. Profile mode (--profile): Column-level statistics (distinct count, nulls, min/max, top values)

Usage:
    python sample_data.py --db DWH --schema OWNER --table TABLE1
    python sample_data.py --db DWH --schema OWNER --table TABLE1 --rows 20
    python sample_data.py --db DWH --schema OWNER --table TABLE1 --profile
    python sample_data.py --db DWH --schema OWNER --table TABLE1 --profile --format json
"""

import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type


# ============================================================================
# Sample data
# ============================================================================

def get_sample(schema: str, table_name: str, db_alias: str = "DWH",
               rows: int = 10) -> dict:
    """Fetch N sample rows from a table."""
    db_type = get_db_type(db_alias)

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()

        if db_type == "oracle":
            sql = f'SELECT * FROM "{schema}"."{table_name}" WHERE ROWNUM <= :lim'
            cursor.execute(sql, {"lim": rows})
        elif db_type == "mysql":
            sql = f"SELECT * FROM `{schema}`.`{table_name}` LIMIT %s"
            cursor.execute(sql, (rows,))
        elif db_type == "postgresql":
            sql = f'SELECT * FROM "{schema}"."{table_name}" LIMIT %s'
            cursor.execute(sql, (rows,))

        columns = [desc[0] for desc in cursor.description]
        data = [list(r) for r in cursor.fetchall()]

    return {
        "schema": schema, "table": table_name, "db_type": db_type,
        "columns": columns, "rows": data, "row_count": len(data),
    }


# ============================================================================
# Data profile
# ============================================================================

def get_profile(schema: str, table_name: str, db_alias: str = "DWH") -> dict:
    """Get column-level statistics: distinct count, null count, min, max, top values."""
    db_type = get_db_type(db_alias)

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()

        # Get column list first
        if db_type == "oracle":
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = :s AND TABLE_NAME = :t
                ORDER BY COLUMN_ID
            """, {"s": schema, "t": table_name})
        elif db_type == "mysql":
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (schema, table_name))
        elif db_type == "postgresql":
            cursor.execute("""
                SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (schema, table_name))

        columns_info = [(r[0], r[1]) for r in cursor.fetchall()]

        # Get total row count
        if db_type == "oracle":
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
        elif db_type == "mysql":
            cursor.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table_name}`")
        elif db_type == "postgresql":
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')

        total_rows = cursor.fetchone()[0]

        # Profile each column
        profiles = []
        for col_name, col_type in columns_info:
            profile = _profile_column(cursor, schema, table_name, col_name, col_type,
                                      db_type, total_rows)
            profiles.append(profile)

    return {
        "schema": schema, "table": table_name, "db_type": db_type,
        "total_rows": total_rows, "column_count": len(columns_info),
        "profiles": profiles,
    }


def _profile_column(cursor, schema: str, table_name: str, col_name: str,
                     col_type: str, db_type: str, total_rows: int) -> dict:
    """Profile a single column."""
    profile = {"column": col_name, "data_type": col_type}

    # Quote identifiers
    if db_type == "oracle":
        fqn = f'"{schema}"."{table_name}"'
        col_q = f'"{col_name}"'
    elif db_type == "mysql":
        fqn = f"`{schema}`.`{table_name}`"
        col_q = f"`{col_name}`"
    elif db_type == "postgresql":
        fqn = f'"{schema}"."{table_name}"'
        col_q = f'"{col_name}"'

    try:
        # Distinct count + null count
        cursor.execute(f"SELECT COUNT(DISTINCT {col_q}), SUM(CASE WHEN {col_q} IS NULL THEN 1 ELSE 0 END) FROM {fqn}")
        r = cursor.fetchone()
        profile["distinct_count"] = r[0]
        profile["null_count"] = r[1] or 0
        profile["null_pct"] = round((r[1] or 0) / total_rows * 100, 2) if total_rows > 0 else 0

        # Min/Max (skip for LOB types)
        skip_types = ("CLOB", "BLOB", "NCLOB", "LONG", "RAW", "LONGRAW",
                       "blob", "text", "mediumtext", "longtext", "bytea", "json", "jsonb")
        if not any(st in col_type.upper() for st in [s.upper() for s in skip_types]):
            cursor.execute(f"SELECT MIN({col_q}), MAX({col_q}) FROM {fqn}")
            r = cursor.fetchone()
            profile["min"] = str(r[0]) if r[0] is not None else None
            profile["max"] = str(r[1]) if r[1] is not None else None

        # Top 5 values (by frequency)
        if db_type == "oracle":
            cursor.execute(f"""
                SELECT {col_q}, COUNT(*) AS cnt
                FROM {fqn}
                WHERE {col_q} IS NOT NULL
                GROUP BY {col_q}
                ORDER BY cnt DESC
                FETCH FIRST 5 ROWS ONLY
            """)
        elif db_type == "mysql":
            cursor.execute(f"""
                SELECT {col_q}, COUNT(*) AS cnt
                FROM {fqn}
                WHERE {col_q} IS NOT NULL
                GROUP BY {col_q}
                ORDER BY cnt DESC
                LIMIT 5
            """)
        elif db_type == "postgresql":
            cursor.execute(f"""
                SELECT {col_q}, COUNT(*) AS cnt
                FROM {fqn}
                WHERE {col_q} IS NOT NULL
                GROUP BY {col_q}
                ORDER BY cnt DESC
                LIMIT 5
            """)

        profile["top_values"] = [
            {"value": str(r[0]), "count": r[1]}
            for r in cursor.fetchall()
        ]

    except Exception as e:
        profile["error"] = str(e)

    return profile


# ============================================================================
# Formatters
# ============================================================================

def format_sample_text(result: dict) -> str:
    lines = []
    lines.append("=" * 90)
    lines.append(f"SAMPLE DATA: {result['schema']}.{result['table']}  ({result['row_count']} rows)")
    lines.append("=" * 90)

    columns = result["columns"]
    rows = result["rows"]

    if not columns:
        lines.append("No data.")
        return "\n".join(lines)

    # Column widths
    widths = [len(str(c)) for c in columns]
    for row in rows:
        for i, val in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], min(len(str(val) if val is not None else "NULL"), 40))

    widths = [min(w, 40) for w in widths]

    # Header
    lines.append("\n" + " | ".join(str(c)[:40].ljust(widths[i]) for i, c in enumerate(columns)))
    lines.append("-+-".join("-" * w for w in widths))

    for row in rows:
        vals = []
        for i, val in enumerate(row):
            s = str(val) if val is not None else "NULL"
            if len(s) > 40:
                s = s[:37] + "..."
            if i < len(widths):
                vals.append(s.ljust(widths[i]))
        lines.append(" | ".join(vals))

    return "\n".join(lines)


def format_profile_text(result: dict) -> str:
    lines = []
    lines.append("=" * 90)
    lines.append(f"DATA PROFILE: {result['schema']}.{result['table']}")
    lines.append(f"Total rows: {result['total_rows']:,}  |  Columns: {result['column_count']}")
    lines.append("=" * 90)

    for p in result["profiles"]:
        lines.append(f"\n  {p['column']}  ({p['data_type']})")
        lines.append(f"    Distinct: {p.get('distinct_count', '?'):,}  |  "
                      f"Nulls: {p.get('null_count', '?'):,} ({p.get('null_pct', '?')}%)")
        if "min" in p and p["min"] is not None:
            lines.append(f"    Min: {p['min']}  |  Max: {p['max']}")
        if p.get("top_values"):
            top = ", ".join(f"{v['value']}({v['count']})" for v in p["top_values"][:3])
            lines.append(f"    Top values: {top}")
        if p.get("error"):
            lines.append(f"    [Error] {p['error']}")

    return "\n".join(lines)


def format_json(result: dict) -> str:
    return json.dumps(result, indent=2, default=str, ensure_ascii=False)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get sample data or column profile from a table.")
    parser.add_argument("--db", default="DWH", help="Database alias (default: DWH)")
    parser.add_argument("--schema", "-s", required=True, help="Schema/owner name")
    parser.add_argument("--table", "-t", required=True, help="Table name")
    parser.add_argument("--rows", type=int, default=10, help="Sample rows (default: 10)")
    parser.add_argument("--profile", action="store_true", help="Run data profiling mode")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    args = parser.parse_args()

    try:
        if args.profile:
            result = get_profile(args.schema, args.table, args.db)
            if args.format == "json":
                print(format_json(result))
            else:
                print(format_profile_text(result))
        else:
            result = get_sample(args.schema, args.table, args.db, args.rows)
            if args.format == "json":
                print(format_json(result))
            else:
                print(format_sample_text(result))
    except Exception as e:
        print(f"Lỗi: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
