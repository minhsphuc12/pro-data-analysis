"""
search_schema.py - Search database metadata for tables and columns by name or comment.

Critical for data discovery: most DWH columns have comments that describe their business meaning.
This script queries ALL_COL_COMMENTS and ALL_TAB_COMMENTS (Oracle), INFORMATION_SCHEMA (MySQL/PG),
or sys tables + extended properties (SQL Server).

Usage:
    python search_schema.py --keyword "khach hang" --db DWH
    python search_schema.py --keyword "customer" --search-in comments,names --schema OWNER1
    python search_schema.py --keyword "revenue|doanh thu" --db DWH --regex
    python search_schema.py --keyword "CUST" --search-in names --db DWH --format json
"""

import argparse
import json
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type


# ============================================================================
# Oracle
# ============================================================================

def _oracle_search(cursor, keyword: str, schema: str | None,
                   search_in: list[str], use_regex: bool, limit: int) -> list[dict]:
    results = []
    kw_upper = keyword.upper()

    # Search table names + table comments
    if "names" in search_in or "comments" in search_in:
        sql = """
            SELECT t.OWNER, t.TABLE_NAME, tc.COMMENTS AS TABLE_COMMENT
            FROM ALL_TABLES t
            LEFT JOIN ALL_TAB_COMMENTS tc
                ON tc.OWNER = t.OWNER AND tc.TABLE_NAME = t.TABLE_NAME
            WHERE 1=1
        """
        params = {}
        if schema:
            sql += " AND t.OWNER = :schema"
            params["schema"] = schema.upper()

        cursor.execute(sql, params)
        tables = cursor.fetchall()

        matched_tables = set()
        for r in tables:
            owner, tname, tcomment = r[0], r[1], r[2] or ""
            match = False
            if "names" in search_in:
                match = match or _match(kw_upper, tname.upper(), use_regex)
            if "comments" in search_in:
                match = match or _match(keyword, tcomment, use_regex)
            if match:
                matched_tables.add((owner, tname))

    # Search column names + column comments
    sql = """
        SELECT c.OWNER, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE,
               c.DATA_LENGTH, c.DATA_PRECISION, c.DATA_SCALE, c.NULLABLE,
               NVL(cc.COMMENTS, '') AS COL_COMMENT,
               NVL(tc.COMMENTS, '') AS TBL_COMMENT
        FROM ALL_TAB_COLUMNS c
        LEFT JOIN ALL_COL_COMMENTS cc
            ON cc.OWNER = c.OWNER AND cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME
        LEFT JOIN ALL_TAB_COMMENTS tc
            ON tc.OWNER = c.OWNER AND tc.TABLE_NAME = c.TABLE_NAME
        WHERE 1=1
    """
    params = {}
    if schema:
        sql += " AND c.OWNER = :schema"
        params["schema"] = schema.upper()

    sql += " ORDER BY c.OWNER, c.TABLE_NAME, c.COLUMN_ID"
    cursor.execute(sql, params)

    count = 0
    for r in cursor:
        if count >= limit:
            break
        owner, tname, cname = r[0], r[1], r[2]
        dtype, dlen, dprec, dscale, nullable = r[3], r[4], r[5], r[6], r[7]
        col_comment, tbl_comment = r[8], r[9]

        match = False
        if "names" in search_in:
            match = match or _match(kw_upper, cname.upper(), use_regex)
            match = match or _match(kw_upper, tname.upper(), use_regex)
        if "comments" in search_in:
            match = match or _match(keyword, col_comment, use_regex)
            match = match or _match(keyword, tbl_comment, use_regex)

        if match:
            # Format data type
            if dtype in ("VARCHAR2", "CHAR"):
                type_str = f"{dtype}({dlen})"
            elif dtype == "NUMBER" and dprec:
                type_str = f"{dtype}({dprec},{dscale})" if dscale and dscale > 0 else f"{dtype}({dprec})"
            else:
                type_str = dtype

            results.append({
                "schema": owner,
                "table": tname,
                "table_comment": tbl_comment,
                "column": cname,
                "data_type": type_str,
                "nullable": nullable == "Y",
                "column_comment": col_comment,
            })
            count += 1

    return results


# ============================================================================
# MySQL
# ============================================================================

def _mysql_search(cursor, keyword: str, schema: str | None,
                  search_in: list[str], use_regex: bool, limit: int) -> list[dict]:
    results = []

    sql = """
        SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_TYPE,
               c.IS_NULLABLE, c.COLUMN_COMMENT,
               COALESCE(t.TABLE_COMMENT, '') AS TBL_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN INFORMATION_SCHEMA.TABLES t
            ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
        WHERE 1=1
    """
    params = []
    if schema:
        sql += " AND c.TABLE_SCHEMA = %s"
        params.append(schema)

    sql += " ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION"
    cursor.execute(sql, params or None)

    count = 0
    for r in cursor:
        if count >= limit:
            break
        tschema, tname, cname, ctype, nullable, col_comment, tbl_comment = r

        match = False
        if "names" in search_in:
            match = match or _match(keyword.upper(), cname.upper(), use_regex)
            match = match or _match(keyword.upper(), tname.upper(), use_regex)
        if "comments" in search_in:
            match = match or _match(keyword, col_comment or "", use_regex)
            match = match or _match(keyword, tbl_comment or "", use_regex)

        if match:
            results.append({
                "schema": tschema, "table": tname, "table_comment": tbl_comment or "",
                "column": cname, "data_type": ctype,
                "nullable": nullable == "YES", "column_comment": col_comment or "",
            })
            count += 1

    return results


# ============================================================================
# PostgreSQL
# ============================================================================

def _pg_search(cursor, keyword: str, schema: str | None,
               search_in: list[str], use_regex: bool, limit: int) -> list[dict]:
    results = []

    sql = """
        SELECT n.nspname, c.relname, a.attname,
               format_type(a.atttypid, a.atttypmod),
               NOT a.attnotnull,
               COALESCE(col_description(a.attrelid, a.attnum), '') AS col_comment,
               COALESCE(obj_description(c.oid), '') AS tbl_comment
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('r', 'v', 'm')
    """
    params = []
    if schema:
        sql += " AND n.nspname = %s"
        params.append(schema)
    else:
        sql += " AND n.nspname NOT IN ('pg_catalog', 'information_schema')"

    sql += " ORDER BY n.nspname, c.relname, a.attnum"
    cursor.execute(sql, params or None)

    count = 0
    for r in cursor:
        if count >= limit:
            break
        nsp, tname, cname, ctype, nullable, col_comment, tbl_comment = r

        match = False
        if "names" in search_in:
            match = match or _match(keyword.lower(), cname.lower(), use_regex)
            match = match or _match(keyword.lower(), tname.lower(), use_regex)
        if "comments" in search_in:
            match = match or _match(keyword, col_comment, use_regex)
            match = match or _match(keyword, tbl_comment, use_regex)

        if match:
            results.append({
                "schema": nsp, "table": tname, "table_comment": tbl_comment,
                "column": cname, "data_type": ctype,
                "nullable": nullable, "column_comment": col_comment,
            })
            count += 1

    return results


# ============================================================================
# SQL Server
# ============================================================================

def _sqlserver_search(cursor, keyword: str, schema: str | None,
                      search_in: list[str], use_regex: bool, limit: int) -> list[dict]:
    results = []

    sql = """
        SELECT s.name AS schema_name, 
               t.name AS table_name,
               c.name AS column_name,
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
               CAST(ep_col.value AS NVARCHAR(MAX)) AS col_comment,
               CAST(ep_tbl.value AS NVARCHAR(MAX)) AS tbl_comment
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.extended_properties ep_col 
            ON ep_col.major_id = c.object_id AND ep_col.minor_id = c.column_id 
            AND ep_col.name = 'MS_Description'
        LEFT JOIN sys.extended_properties ep_tbl
            ON ep_tbl.major_id = t.object_id AND ep_tbl.minor_id = 0 
            AND ep_tbl.name = 'MS_Description'
        WHERE 1=1
    """
    params = []
    if schema:
        sql += " AND s.name = ?"
        params.append(schema)

    sql += " ORDER BY s.name, t.name, c.column_id"
    cursor.execute(sql, params or None)

    count = 0
    for r in cursor:
        if count >= limit:
            break
        schema_name, tname, cname, ctype, nullable, col_comment, tbl_comment = r

        match = False
        if "names" in search_in:
            match = match or _match(keyword.upper(), cname.upper(), use_regex)
            match = match or _match(keyword.upper(), tname.upper(), use_regex)
        if "comments" in search_in:
            match = match or _match(keyword, col_comment or "", use_regex)
            match = match or _match(keyword, tbl_comment or "", use_regex)

        if match:
            results.append({
                "schema": schema_name, 
                "table": tname, 
                "table_comment": tbl_comment or "",
                "column": cname, 
                "data_type": ctype,
                "nullable": bool(nullable), 
                "column_comment": col_comment or "",
            })
            count += 1

    return results


# ============================================================================
# Helpers
# ============================================================================

def _match(pattern: str, text: str, use_regex: bool) -> bool:
    if not text:
        return False
    if use_regex:
        return bool(re.search(pattern, text, re.IGNORECASE))
    return pattern.lower() in text.lower()


_SEARCH_FUNCS = {
    "oracle": _oracle_search,
    "mysql": _mysql_search,
    "postgresql": _pg_search,
    "sqlserver": _sqlserver_search,
}


def search_schema(keyword: str, db_alias: str = "DWH", schema: str | None = None,
                  search_in: list[str] | None = None, use_regex: bool = False,
                  limit: int = 200) -> list[dict]:
    """Search database metadata for matching tables/columns."""
    if search_in is None:
        search_in = ["names", "comments"]
    db_type = get_db_type(db_alias)
    func = _SEARCH_FUNCS[db_type]

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()
        return func(cursor, keyword, schema, search_in, use_regex, limit)


# ============================================================================
# Formatters
# ============================================================================

def format_text(results: list[dict]) -> str:
    if not results:
        return "Không tìm thấy kết quả nào."

    lines = []
    lines.append(f"Tìm thấy {len(results)} kết quả:\n")
    lines.append(
        f"{'SCHEMA':<20} {'TABLE':<35} {'COLUMN':<35} {'TYPE':<20} {'COLUMN COMMENT'}"
    )
    lines.append("-" * 150)

    current_table = None
    for r in results:
        table_key = f"{r['schema']}.{r['table']}"
        if table_key != current_table:
            current_table = table_key
            if r["table_comment"]:
                lines.append(f"\n-- {table_key}: {r['table_comment']}")

        lines.append(
            f"{r['schema']:<20} {r['table']:<35} {r['column']:<35} "
            f"{r['data_type']:<20} {r['column_comment']}"
        )

    return "\n".join(lines)


def format_json(results: list[dict]) -> str:
    return json.dumps(results, indent=2, ensure_ascii=False)


def format_markdown(results: list[dict]) -> str:
    if not results:
        return "Không tìm thấy kết quả nào."

    lines = [f"Tìm thấy **{len(results)}** kết quả:\n"]
    lines.append("| Schema | Table | Column | Type | Column Comment |")
    lines.append("|--------|-------|--------|------|----------------|")
    for r in results:
        cc = (r["column_comment"] or "").replace("|", "\\|")
        lines.append(f"| {r['schema']} | {r['table']} | {r['column']} | {r['data_type']} | {cc} |")
    return "\n".join(lines)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Search database metadata (table/column names and comments)."
    )
    parser.add_argument("--keyword", "-k", required=True, help="Search keyword or regex pattern")
    parser.add_argument("--db", default="DWH", help="Database alias (default: DWH)")
    parser.add_argument("--schema", "-s", default=None, help="Filter by schema/owner")
    parser.add_argument("--search-in", default="comments,names",
                        help="Comma-separated: names,comments (default: both)")
    parser.add_argument("--regex", action="store_true", help="Use regex matching")
    parser.add_argument("--limit", type=int, default=200, help="Max results (default: 200)")
    parser.add_argument("--format", choices=["text", "json", "markdown"], default="text",
                        help="Output format")
    args = parser.parse_args()

    search_in = [s.strip() for s in args.search_in.split(",")]

    try:
        results = search_schema(
            keyword=args.keyword, db_alias=args.db, schema=args.schema,
            search_in=search_in, use_regex=args.regex, limit=args.limit,
        )
        if args.format == "json":
            print(format_json(results))
        elif args.format == "markdown":
            print(format_markdown(results))
        else:
            print(format_text(results))
    except Exception as e:
        print(f"Lỗi: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
