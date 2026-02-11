"""
find_relationships.py - Find foreign key constraints and potential join paths between tables.

Two strategies:
1. FK constraints from data dictionary (ALL_CONSTRAINTS / INFORMATION_SCHEMA)
2. Naming convention heuristics (e.g., TABLE1_ID in TABLE2 likely joins to TABLE1.ID)

Usage:
    python find_relationships.py --db DWH --schema OWNER1 --table TABLE1
    python find_relationships.py --db DWH --tables TABLE1,TABLE2
    python find_relationships.py --db DWH --schema OWNER1 --table TABLE1 --format json
"""

import argparse
import json
import sys
import os
import re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type


# ============================================================================
# Oracle
# ============================================================================

def _oracle_find_fk(cursor, schema: str, table_name: str) -> list[dict]:
    """Find FK relationships involving the given table (both directions)."""
    results = []

    # FKs FROM this table (this table references others)
    cursor.execute("""
        SELECT
            c.CONSTRAINT_NAME,
            c.TABLE_NAME AS FK_TABLE,
            cc.COLUMN_NAME AS FK_COLUMN,
            r.TABLE_NAME AS PK_TABLE,
            rc.COLUMN_NAME AS PK_COLUMN,
            c.OWNER AS FK_SCHEMA,
            r.OWNER AS PK_SCHEMA
        FROM ALL_CONSTRAINTS c
        JOIN ALL_CONS_COLUMNS cc ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND cc.OWNER = c.OWNER
        JOIN ALL_CONSTRAINTS r ON r.CONSTRAINT_NAME = c.R_CONSTRAINT_NAME AND r.OWNER = c.R_OWNER
        JOIN ALL_CONS_COLUMNS rc ON rc.CONSTRAINT_NAME = r.CONSTRAINT_NAME AND rc.OWNER = r.OWNER
            AND rc.POSITION = cc.POSITION
        WHERE c.CONSTRAINT_TYPE = 'R'
          AND c.OWNER = :schema AND c.TABLE_NAME = :tname
        ORDER BY c.CONSTRAINT_NAME, cc.POSITION
    """, {"schema": schema, "tname": table_name})

    for r in cursor.fetchall():
        results.append({
            "constraint_name": r[0],
            "direction": "OUTGOING",
            "from_schema": r[5], "from_table": r[1], "from_column": r[2],
            "to_schema": r[6], "to_table": r[3], "to_column": r[4],
        })

    # FKs TO this table (other tables reference this table)
    cursor.execute("""
        SELECT
            c.CONSTRAINT_NAME,
            c.TABLE_NAME AS FK_TABLE,
            cc.COLUMN_NAME AS FK_COLUMN,
            r.TABLE_NAME AS PK_TABLE,
            rc.COLUMN_NAME AS PK_COLUMN,
            c.OWNER AS FK_SCHEMA,
            r.OWNER AS PK_SCHEMA
        FROM ALL_CONSTRAINTS c
        JOIN ALL_CONS_COLUMNS cc ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND cc.OWNER = c.OWNER
        JOIN ALL_CONSTRAINTS r ON r.CONSTRAINT_NAME = c.R_CONSTRAINT_NAME AND r.OWNER = c.R_OWNER
        JOIN ALL_CONS_COLUMNS rc ON rc.CONSTRAINT_NAME = r.CONSTRAINT_NAME AND rc.OWNER = r.OWNER
            AND rc.POSITION = cc.POSITION
        WHERE c.CONSTRAINT_TYPE = 'R'
          AND r.OWNER = :schema AND r.TABLE_NAME = :tname
        ORDER BY c.CONSTRAINT_NAME, cc.POSITION
    """, {"schema": schema, "tname": table_name})

    for r in cursor.fetchall():
        results.append({
            "constraint_name": r[0],
            "direction": "INCOMING",
            "from_schema": r[5], "from_table": r[1], "from_column": r[2],
            "to_schema": r[6], "to_table": r[3], "to_column": r[4],
        })

    return results


def _oracle_find_naming_hints(cursor, schema: str, table_name: str) -> list[dict]:
    """Find potential joins based on column naming conventions."""
    hints = []

    # Get columns of target table that look like foreign keys (ending in _ID, _CODE, _KEY)
    cursor.execute("""
        SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS
        WHERE OWNER = :schema AND TABLE_NAME = :tname
        ORDER BY COLUMN_ID
    """, {"schema": schema, "tname": table_name})
    columns = [r[0] for r in cursor.fetchall()]

    fk_patterns = []
    for col in columns:
        # e.g., CUSTOMER_ID -> look for CUSTOMER table with ID column
        for suffix in ("_ID", "_CODE", "_KEY", "_NO", "_NUM"):
            if col.endswith(suffix):
                prefix = col[: -len(suffix)]
                if prefix and prefix != table_name:
                    fk_patterns.append((col, prefix, suffix.lstrip("_")))

    # Check if those tables exist
    for fk_col, prefix, pk_suffix in fk_patterns:
        cursor.execute("""
            SELECT t.TABLE_NAME, t.OWNER
            FROM ALL_TABLES t
            WHERE t.OWNER = :schema
              AND (t.TABLE_NAME = :exact
                   OR t.TABLE_NAME LIKE :pattern
                   OR t.TABLE_NAME LIKE :pattern2)
        """, {
            "schema": schema,
            "exact": prefix,
            "pattern": f"%{prefix}%",
            "pattern2": f"{prefix}%",
        })
        matches = cursor.fetchall()
        for m in matches:
            # Check if the target table has a matching PK column
            cursor.execute("""
                SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS
                WHERE OWNER = :schema AND TABLE_NAME = :tname
                  AND COLUMN_NAME IN (:pk1, :pk2, :pk3)
            """, {
                "schema": schema, "tname": m[0],
                "pk1": pk_suffix, "pk2": f"{m[0]}_{pk_suffix}", "pk3": fk_col,
            })
            pk_cols = [r[0] for r in cursor.fetchall()]
            if pk_cols:
                hints.append({
                    "type": "NAMING_CONVENTION",
                    "from_table": table_name, "from_column": fk_col,
                    "to_schema": m[1], "to_table": m[0], "to_column": pk_cols[0],
                    "confidence": "MEDIUM",
                })

    return hints


# ============================================================================
# MySQL
# ============================================================================

def _mysql_find_fk(cursor, schema: str, table_name: str) -> list[dict]:
    results = []

    # FKs FROM this table
    cursor.execute("""
        SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME,
               REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
          AND REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
    """, (schema, table_name))

    for r in cursor.fetchall():
        results.append({
            "constraint_name": r[0], "direction": "OUTGOING",
            "from_schema": schema, "from_table": r[1], "from_column": r[2],
            "to_schema": r[3], "to_table": r[4], "to_column": r[5],
        })

    # FKs TO this table
    cursor.execute("""
        SELECT CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
               REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s
        ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
    """, (schema, table_name))

    for r in cursor.fetchall():
        results.append({
            "constraint_name": r[0], "direction": "INCOMING",
            "from_schema": r[1], "from_table": r[2], "from_column": r[3],
            "to_schema": schema, "to_table": table_name, "to_column": r[4],
        })

    return results


def _mysql_find_naming_hints(cursor, schema: str, table_name: str) -> list[dict]:
    # Simplified: similar logic can be added later
    return []


# ============================================================================
# PostgreSQL
# ============================================================================

def _pg_find_fk(cursor, schema: str, table_name: str) -> list[dict]:
    results = []

    cursor.execute("""
        SELECT
            tc.constraint_name,
            kcu.table_schema AS fk_schema, kcu.table_name AS fk_table, kcu.column_name AS fk_column,
            ccu.table_schema AS pk_schema, ccu.table_name AS pk_table, ccu.column_name AS pk_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ((kcu.table_schema = %s AND kcu.table_name = %s)
               OR (ccu.table_schema = %s AND ccu.table_name = %s))
        ORDER BY tc.constraint_name
    """, (schema, table_name, schema, table_name))

    for r in cursor.fetchall():
        direction = "OUTGOING" if r[2] == table_name else "INCOMING"
        results.append({
            "constraint_name": r[0], "direction": direction,
            "from_schema": r[1], "from_table": r[2], "from_column": r[3],
            "to_schema": r[4], "to_table": r[5], "to_column": r[6],
        })

    return results


def _pg_find_naming_hints(cursor, schema: str, table_name: str) -> list[dict]:
    return []


# ============================================================================
# Find join path between two tables
# ============================================================================

def _find_join_path(cursor, schema: str, table1: str, table2: str, db_type: str) -> list[dict]:
    """Find potential join path between two specific tables."""
    # Get columns of both tables
    if db_type == "oracle":
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME FROM ALL_TAB_COLUMNS
            WHERE OWNER = :s AND TABLE_NAME IN (:t1, :t2)
            ORDER BY TABLE_NAME, COLUMN_ID
        """, {"s": schema, "t1": table1, "t2": table2})
    elif db_type == "mysql":
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN (%s, %s)
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """, (schema, table1, table2))
    elif db_type == "postgresql":
        cursor.execute("""
            SELECT c.relname, a.attname
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname IN (%s, %s)
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY c.relname, a.attnum
        """, (schema, table1, table2))

    cols = {}
    for r in cursor.fetchall():
        cols.setdefault(r[0], []).append(r[1])

    paths = []
    cols1 = set(cols.get(table1, []))
    cols2 = set(cols.get(table2, []))

    # Exact column name match
    common = cols1 & cols2
    for c in common:
        paths.append({
            "type": "COMMON_COLUMN",
            "join": f"{table1}.{c} = {table2}.{c}",
            "confidence": "HIGH" if c.endswith(("_ID", "_KEY", "_CODE")) else "MEDIUM",
        })

    # Naming convention: TABLE1_ID in TABLE2
    for c2 in cols2:
        for suffix in ("_ID", "_CODE", "_KEY"):
            if c2.startswith(table1) and c2.endswith(suffix):
                pk_guess = suffix.lstrip("_")
                if pk_guess in cols1 or c2 in cols1:
                    pk = pk_guess if pk_guess in cols1 else c2
                    paths.append({
                        "type": "NAMING_CONVENTION",
                        "join": f"{table1}.{pk} = {table2}.{c2}",
                        "confidence": "MEDIUM",
                    })

    for c1 in cols1:
        for suffix in ("_ID", "_CODE", "_KEY"):
            if c1.startswith(table2) and c1.endswith(suffix):
                pk_guess = suffix.lstrip("_")
                if pk_guess in cols2 or c1 in cols2:
                    pk = pk_guess if pk_guess in cols2 else c1
                    paths.append({
                        "type": "NAMING_CONVENTION",
                        "join": f"{table1}.{c1} = {table2}.{pk}",
                        "confidence": "MEDIUM",
                    })

    return paths


# ============================================================================
# Public API
# ============================================================================

_FK_FUNCS = {
    "oracle": (_oracle_find_fk, _oracle_find_naming_hints),
    "mysql": (_mysql_find_fk, _mysql_find_naming_hints),
    "postgresql": (_pg_find_fk, _pg_find_naming_hints),
}


def find_relationships(schema: str, table_name: str, db_alias: str = "DWH") -> dict:
    """Find all relationships for a table (FK constraints + naming hints)."""
    db_type = get_db_type(db_alias)
    fk_func, hint_func = _FK_FUNCS[db_type]

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()
        fks = fk_func(cursor, schema, table_name)
        hints = hint_func(cursor, schema, table_name)

    return {
        "schema": schema, "table": table_name, "db_type": db_type,
        "foreign_keys": fks, "naming_hints": hints,
    }


def find_join_between(schema: str, tables: list[str], db_alias: str = "DWH") -> dict:
    """Find potential join paths between specified tables."""
    db_type = get_db_type(db_alias)
    all_paths = []

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()
        # Check all pairs
        for i in range(len(tables)):
            for j in range(i + 1, len(tables)):
                paths = _find_join_path(cursor, schema, tables[i], tables[j], db_type)
                for p in paths:
                    p["table1"] = tables[i]
                    p["table2"] = tables[j]
                    all_paths.append(p)

    return {"schema": schema, "tables": tables, "join_paths": all_paths}


# ============================================================================
# Formatters
# ============================================================================

def format_text(result: dict) -> str:
    lines = []

    if "foreign_keys" in result:
        lines.append("=" * 90)
        lines.append(f"RELATIONSHIPS: {result['schema']}.{result['table']}")
        lines.append("=" * 90)

        fks = result["foreign_keys"]
        lines.append(f"\n--- Foreign Key Constraints ({len(fks)}) ---")
        if fks:
            for fk in fks:
                arrow = "->" if fk["direction"] == "OUTGOING" else "<-"
                lines.append(
                    f"  [{fk['direction']}] {fk['from_schema']}.{fk['from_table']}.{fk['from_column']} "
                    f"{arrow} {fk['to_schema']}.{fk['to_table']}.{fk['to_column']}  "
                    f"({fk['constraint_name']})"
                )
        else:
            lines.append("  Không tìm thấy FK constraint nào.")

        hints = result.get("naming_hints", [])
        lines.append(f"\n--- Naming Convention Hints ({len(hints)}) ---")
        if hints:
            for h in hints:
                lines.append(
                    f"  [{h['confidence']}] {h['from_table']}.{h['from_column']} "
                    f"-> {h['to_schema']}.{h['to_table']}.{h['to_column']}"
                )
        else:
            lines.append("  Không tìm thấy naming hint nào.")

    if "join_paths" in result:
        lines.append("=" * 90)
        lines.append(f"JOIN PATHS giữa: {', '.join(result['tables'])}")
        lines.append("=" * 90)

        paths = result["join_paths"]
        if paths:
            for p in paths:
                lines.append(f"  [{p['confidence']}] {p['join']}  ({p['type']})")
        else:
            lines.append("  Không tìm thấy join path trực tiếp nào.")

    return "\n".join(lines)


def format_json(result: dict) -> str:
    return json.dumps(result, indent=2, default=str, ensure_ascii=False)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find FK relationships and join paths between tables.")
    parser.add_argument("--db", default="DWH", help="Database alias (default: DWH)")
    parser.add_argument("--schema", "-s", required=True, help="Schema/owner name")
    parser.add_argument("--table", "-t", default=None, help="Single table to find relationships for")
    parser.add_argument("--tables", default=None,
                        help="Comma-separated table names to find join paths between")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    args = parser.parse_args()

    if not args.table and not args.tables:
        parser.error("Provide --table or --tables")

    try:
        if args.table:
            result = find_relationships(args.schema, args.table, args.db)
        else:
            tables = [t.strip() for t in args.tables.split(",")]
            result = find_join_between(args.schema, tables, args.db)

        if args.format == "json":
            print(format_json(result))
        else:
            print(format_text(result))
    except Exception as e:
        print(f"Lỗi: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
