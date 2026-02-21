"""
check_table.py - Inspect table structure, indexes, partitions, statistics, and COMMENTS.

Supports Oracle, MySQL, PostgreSQL, SQL Server via db_connector.

Usage:
    python check_table.py SCHEMA TABLE_NAME
    python check_table.py SCHEMA TABLE_NAME --db DWH
    python check_table.py SCHEMA TABLE_NAME --format markdown
    python check_table.py SCHEMA TABLE_NAME --format json
"""

import argparse
import json
import os
import sys
import traceback

# Allow importing sibling module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type


def _nullable_display(nullable: bool) -> str:
    return "YES" if nullable else "NO"


def _stats_dict(num_rows, blocks, avg_row_len, last_analyzed) -> dict:
    return {
        "num_rows": num_rows,
        "blocks": blocks,
        "avg_row_len": avg_row_len,
        "last_analyzed": str(last_analyzed) if last_analyzed else None,
    }


# ============================================================================
# Oracle
# ============================================================================

def _oracle_column_type(row) -> str:
    """Build Oracle data type string from ALL_TAB_COLUMNS row."""
    data_type, data_length, precision, scale = row[1], row[2], row[3], row[4]
    if data_type in ("VARCHAR2", "CHAR"):
        return f"{data_type}({data_length})"
    if data_type == "NUMBER" and precision:
        if scale and scale > 0:
            return f"{data_type}({precision},{scale})"
        return f"{data_type}({precision})"
    return data_type


def _oracle_table_info(cursor, schema: str, table_name: str) -> dict:
    info = {"schema": schema, "table": table_name, "db_type": "oracle"}

    cursor.execute(
        "SELECT COMMENTS FROM ALL_TAB_COMMENTS WHERE OWNER = :s AND TABLE_NAME = :t",
        {"s": schema, "t": table_name},
    )
    row = cursor.fetchone()
    info["table_comment"] = row[0] if row and row[0] else ""

    cursor.execute("""
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.DATA_LENGTH, c.DATA_PRECISION,
               c.DATA_SCALE, c.NULLABLE, c.DATA_DEFAULT,
               NVL(cc.COMMENTS, '') AS COMMENTS
        FROM ALL_TAB_COLUMNS c
        LEFT JOIN ALL_COL_COMMENTS cc
            ON cc.OWNER = c.OWNER AND cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME
        WHERE c.OWNER = :s AND c.TABLE_NAME = :t
        ORDER BY c.COLUMN_ID
    """, {"s": schema, "t": table_name})
    info["columns"] = [
        {
            "name": row[0],
            "data_type": _oracle_column_type(row),
            "nullable": row[5] == "Y",
            "default": str(row[6]) if row[6] else None,
            "comment": row[7],
        }
        for row in cursor.fetchall()
    ]

    # Indexes
    cursor.execute("""
        SELECT i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS,
               LISTAGG(c.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY c.COLUMN_POSITION) AS COLS
        FROM ALL_INDEXES i
        JOIN ALL_IND_COLUMNS c
            ON i.INDEX_NAME = c.INDEX_NAME AND i.TABLE_OWNER = c.TABLE_OWNER
        WHERE i.TABLE_OWNER = :s AND i.TABLE_NAME = :t
        GROUP BY i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS
    """, {"s": schema, "t": table_name})
    info["indexes"] = [
        {"name": r[0], "type": r[1], "unique": r[2], "columns": r[3]}
        for r in cursor.fetchall()
    ]

    # Partitions
    cursor.execute("""
        SELECT PARTITION_NAME, PARTITION_POSITION, HIGH_VALUE, NUM_ROWS, COMPRESSION
        FROM ALL_TAB_PARTITIONS
        WHERE TABLE_OWNER = :s AND TABLE_NAME = :t
        ORDER BY PARTITION_POSITION
    """, {"s": schema, "t": table_name})
    info["partitions"] = [
        {"name": r[0], "position": r[1], "high_value": str(r[2]) if r[2] else None,
         "num_rows": r[3], "compression": r[4]}
        for r in cursor.fetchall()
    ]

    cursor.execute("""
        SELECT NUM_ROWS, BLOCKS, AVG_ROW_LEN, LAST_ANALYZED
        FROM ALL_TABLES WHERE OWNER = :s AND TABLE_NAME = :t
    """, {"s": schema, "t": table_name})
    stats_row = cursor.fetchone()
    info["statistics"] = _stats_dict(*stats_row) if stats_row else {}

    return info


# ============================================================================
# MySQL
# ============================================================================

def _mysql_table_info(cursor, schema: str, table_name: str) -> dict:
    info = {"schema": schema, "table": table_name, "db_type": "mysql"}

    # Table comment
    cursor.execute("""
        SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """, (schema, table_name))
    row = cursor.fetchone()
    info["table_comment"] = row[0] if row and row[0] else ""

    # Columns + comments
    cursor.execute("""
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (schema, table_name))
    info["columns"] = [
        {"name": r[0], "data_type": r[1], "nullable": r[2] == "YES",
         "default": r[3], "comment": r[4] or ""}
        for r in cursor.fetchall()
    ]

    # Indexes
    cursor.execute("""
        SELECT INDEX_NAME, INDEX_TYPE, NON_UNIQUE,
               GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS COLS
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        GROUP BY INDEX_NAME, INDEX_TYPE, NON_UNIQUE
    """, (schema, table_name))
    info["indexes"] = [
        {"name": r[0], "type": r[1], "unique": "UNIQUE" if r[2] == 0 else "NONUNIQUE", "columns": r[3]}
        for r in cursor.fetchall()
    ]

    # Partitions
    cursor.execute("""
        SELECT PARTITION_NAME, PARTITION_ORDINAL_POSITION, PARTITION_DESCRIPTION,
               TABLE_ROWS, PARTITION_METHOD
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND PARTITION_NAME IS NOT NULL
        ORDER BY PARTITION_ORDINAL_POSITION
    """, (schema, table_name))
    info["partitions"] = [
        {"name": r[0], "position": r[1], "high_value": r[2], "num_rows": r[3], "compression": r[4]}
        for r in cursor.fetchall()
    ]

    cursor.execute("""
        SELECT TABLE_ROWS, DATA_LENGTH, AVG_ROW_LENGTH, UPDATE_TIME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """, (schema, table_name))
    stats_row = cursor.fetchone()
    info["statistics"] = _stats_dict(*stats_row) if stats_row else {}

    return info


# ============================================================================
# PostgreSQL
# ============================================================================

def _pg_table_info(cursor, schema: str, table_name: str) -> dict:
    info = {"schema": schema, "table": table_name, "db_type": "postgresql"}

    # Table comment
    cursor.execute("""
        SELECT obj_description(c.oid) FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
    """, (schema, table_name))
    row = cursor.fetchone()
    info["table_comment"] = row[0] if row and row[0] else ""

    # Columns + comments
    cursor.execute("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod),
               NOT a.attnotnull, pg_get_expr(d.adbin, d.adrelid),
               col_description(a.attrelid, a.attnum)
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """, (schema, table_name))
    info["columns"] = [
        {"name": r[0], "data_type": r[1], "nullable": r[2],
         "default": r[3], "comment": r[4] or ""}
        for r in cursor.fetchall()
    ]

    # Indexes
    cursor.execute("""
        SELECT i.relname, am.amname,
               CASE WHEN ix.indisunique THEN 'UNIQUE' ELSE 'NONUNIQUE' END,
               string_agg(a.attname, ', ' ORDER BY array_position(ix.indkey, a.attnum))
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname = %s AND t.relname = %s
        GROUP BY i.relname, am.amname, ix.indisunique
    """, (schema, table_name))
    info["indexes"] = [
        {"name": r[0], "type": r[1], "unique": r[2], "columns": r[3]}
        for r in cursor.fetchall()
    ]

    info["partitions"] = []  # PostgreSQL partitions need different handling

    cursor.execute("""
        SELECT n_live_tup, pg_total_relation_size(c.oid),
               NULL, NULL
        FROM pg_stat_user_tables s
        JOIN pg_class c ON c.relname = s.relname
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
        WHERE s.schemaname = %s AND s.relname = %s
    """, (schema, table_name))
    stats_row = cursor.fetchone()
    info["statistics"] = _stats_dict(*stats_row) if stats_row else {}

    return info


# ============================================================================
# SQL Server
# ============================================================================

def _sqlserver_table_info(cursor, schema: str, table_name: str) -> dict:
    info = {"schema": schema, "table": table_name, "db_type": "sqlserver"}

    # Table comment (extended properties)
    cursor.execute("""
        SELECT CAST(value AS NVARCHAR(MAX))
        FROM sys.extended_properties ep
        JOIN sys.tables t ON ep.major_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? 
            AND ep.minor_id = 0 AND ep.name = 'MS_Description'
    """, (schema, table_name))
    row = cursor.fetchone()
    info["table_comment"] = row[0] if row and row[0] else ""

    # Columns + comments
    cursor.execute("""
        SELECT c.name,
               TYPE_NAME(c.user_type_id) + 
               CASE 
                   WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'nvarchar', 'char', 'nchar') 
                       THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' 
                                      ELSE CAST(c.max_length AS VARCHAR) END + ')'
                   WHEN TYPE_NAME(c.user_type_id) IN ('decimal', 'numeric')
                       THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                   ELSE ''
               END AS data_type,
               c.is_nullable,
               OBJECT_DEFINITION(c.default_object_id) AS default_value,
               CAST(ep.value AS NVARCHAR(MAX)) AS description
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.extended_properties ep 
            ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
        WHERE s.name = ? AND t.name = ?
        ORDER BY c.column_id
    """, (schema, table_name))
    info["columns"] = [
        {"name": r[0], "data_type": r[1], "nullable": bool(r[2]),
         "default": r[3], "comment": r[4] or ""}
        for r in cursor.fetchall()
    ]

    # Indexes
    cursor.execute("""
        SELECT i.name, i.type_desc,
               CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE 'NONUNIQUE' END,
               STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal)
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE s.name = ? AND t.name = ?
        GROUP BY i.name, i.type_desc, i.is_unique
    """, (schema, table_name))
    info["indexes"] = [
        {"name": r[0], "type": r[1], "unique": r[2], "columns": r[3]}
        for r in cursor.fetchall()
    ]

    # Partitions
    cursor.execute("""
        SELECT p.partition_number, p.rows, ps.name AS partition_scheme
        FROM sys.partitions p
        JOIN sys.tables t ON p.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
        LEFT JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
        WHERE s.name = ? AND t.name = ? AND i.index_id <= 1
        ORDER BY p.partition_number
    """, (schema, table_name))
    partition_rows = cursor.fetchall()
    info["partitions"] = [
        {"name": f"Partition_{row[0]}", "position": row[0], "num_rows": row[1],
         "high_value": None, "compression": row[2]}
        for row in partition_rows
        if row[2]  # partition_scheme present only when table is partitioned
    ]

    cursor.execute("""
        SELECT SUM(p.rows) AS num_rows,
               SUM(a.total_pages) * 8 AS total_space_kb,
               SUM(a.total_pages) * 8 / NULLIF(SUM(p.rows), 0) AS avg_row_kb,
               MAX(stats_date(i.object_id, i.index_id)) AS last_stats_update
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.indexes i ON t.object_id = i.object_id
        JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        JOIN sys.allocation_units a ON p.partition_id = a.container_id
        WHERE s.name = ? AND t.name = ?
        GROUP BY t.object_id
    """, (schema, table_name))
    stats_row = cursor.fetchone()
    info["statistics"] = _stats_dict(*stats_row) if stats_row else {}

    return info


# ============================================================================
# Formatters
# ============================================================================

def format_text(info: dict) -> str:
    lines = []
    sep = "=" * 90

    lines.append(sep)
    lines.append(f"CẤU TRÚC BẢNG {info['schema']}.{info['table']}  ({info['db_type']})")
    lines.append(sep)
    if info.get("table_comment"):
        lines.append(f"Mô tả: {info['table_comment']}")

    lines.append(f"\nTổng số cột: {len(info['columns'])}\n")
    lines.append(f"{'STT':<5} {'TÊN CỘT':<35} {'KIỂU DỮ LIỆU':<25} {'NULL?':<6} {'MÔ TẢ CỘT'}")
    lines.append("-" * 120)
    for i, col in enumerate(info["columns"], 1):
        comment = col.get("comment") or ""
        lines.append(
            f"{i:<5} {col['name']:<35} {col['data_type']:<25} "
            f"{_nullable_display(col['nullable']):<6} {comment}"
        )

    lines.append(f"\n{sep}\nINDEXES\n{sep}")
    if info["indexes"]:
        for idx in info["indexes"]:
            lines.append(f"\n  Index: {idx['name']}")
            lines.append(f"  Type: {idx['type']}  |  Unique: {idx['unique']}")
            lines.append(f"  Columns: {idx['columns']}")
    else:
        lines.append("\nKhông có index nào.")

    lines.append(f"\n{sep}\nPARTITIONS\n{sep}")
    if info["partitions"]:
        lines.append(f"\nTổng số partitions: {len(info['partitions'])}\n")
        lines.append(f"{'PARTITION':<30} {'POS':<5} {'NUM_ROWS':<15} {'COMPRESS':<10}")
        lines.append("-" * 60)
        for part in info["partitions"][:10]:
            lines.append(
                f"{part.get('name') or '':<30} {str(part.get('position', '')):<5} "
                f"{str(part.get('num_rows', 'N/A')):<15} {part.get('compression', 'NONE'):<10}"
            )
        if len(info["partitions"]) > 10:
            lines.append(f"\n... và {len(info['partitions']) - 10} partitions khác")
    else:
        lines.append("\nBảng không được phân vùng.")

    lines.append(f"\n{sep}\nTHỐNG KÊ\n{sep}")
    stats = info.get("statistics", {})
    if stats:
        row_count = stats.get("num_rows")
        lines.append(
            f"\nSố dòng (ước tính): {row_count:,}" if row_count else "\nSố dòng: Chưa có thống kê"
        )
        block_count = stats.get("blocks")
        lines.append(f"Số blocks: {block_count:,}" if block_count else "Số blocks: N/A")
        avg_row_len = stats.get("avg_row_len")
        lines.append(
            f"Độ dài TB mỗi dòng: {avg_row_len} bytes" if avg_row_len else "Độ dài TB: N/A"
        )
        last_analyzed = stats.get("last_analyzed")
        lines.append(f"Lần phân tích cuối: {last_analyzed}" if last_analyzed else "Chưa phân tích")

    return "\n".join(lines)


def format_markdown(info: dict) -> str:
    lines = []
    lines.append(f"# {info['schema']}.{info['table']}\n")
    if info.get("table_comment"):
        lines.append(f"> {info['table_comment']}\n")
    lines.append(f"**Database type:** {info['db_type']}\n")

    lines.append("## Columns\n")
    lines.append("| # | Column | Type | Nullable | Comment |")
    lines.append("|---|--------|------|----------|---------|")
    for i, col in enumerate(info["columns"], 1):
        comment = (col.get("comment") or "").replace("|", "\\|")
        lines.append(
            f"| {i} | {col['name']} | {col['data_type']} | "
            f"{_nullable_display(col['nullable'])} | {comment} |"
        )

    lines.append("\n## Indexes\n")
    if info["indexes"]:
        lines.append("| Name | Type | Unique | Columns |")
        lines.append("|------|------|--------|---------|")
        for idx in info["indexes"]:
            lines.append(f"| {idx['name']} | {idx['type']} | {idx['unique']} | {idx['columns']} |")
    else:
        lines.append("_No indexes._")

    lines.append("\n## Statistics\n")
    stats = info.get("statistics", {})
    if stats:
        lines.append(f"- **Rows:** {stats.get('num_rows', 'N/A')}")
        lines.append(f"- **Blocks:** {stats.get('blocks', 'N/A')}")
        lines.append(f"- **Avg row length:** {stats.get('avg_row_len', 'N/A')}")
        lines.append(f"- **Last analyzed:** {stats.get('last_analyzed', 'N/A')}")

    return "\n".join(lines)


def format_json(info: dict) -> str:
    return json.dumps(info, indent=2, default=str, ensure_ascii=False)


# ============================================================================
# Dispatcher
# ============================================================================

_INFO_FUNCS = {
    "oracle": _oracle_table_info,
    "mysql": _mysql_table_info,
    "postgresql": _pg_table_info,
    "sqlserver": _sqlserver_table_info,
}

_FORMATTERS = {
    "text": format_text,
    "markdown": format_markdown,
    "json": format_json,
}


def get_table_info(schema: str, table_name: str, db_alias: str = "DWH",
                   fmt: str = "text") -> str:
    db_type = get_db_type(db_alias)
    info_func = _INFO_FUNCS[db_type]

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()
        info = info_func(cursor, schema, table_name)

    formatter = _FORMATTERS.get(fmt, format_text)
    return formatter(info)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Inspect table structure, indexes, partitions, statistics, and column comments."
    )
    parser.add_argument("schema", help="Schema/owner name")
    parser.add_argument("table", help="Table name")
    parser.add_argument("--db", default="DWH", help="Database alias (default: DWH)")
    parser.add_argument("--format", choices=["text", "markdown", "json"], default="text",
                        help="Output format (default: text)")
    args = parser.parse_args()

    try:
        output = get_table_info(args.schema, args.table, args.db, args.format)
        print(output)
    except Exception as e:
        print(f"Lỗi: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
