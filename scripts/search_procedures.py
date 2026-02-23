"""
search_procedures.py - Search for Oracle procedure/package in DWH by table name or content string.

Query ALL_SOURCE to find OBJECT (PROCEDURE, PACKAGE, PACKAGE BODY, FUNCTION) containing
the table name or specified string in the source code.

Only supports Oracle (DWH). Requires environment variables: DWH_TYPE=oracle, DWH_USERNAME, DWH_PASSWORD, DWH_DSN.

Usage:
    python search_procedures.py --table DIM_CUSTOMER
    python search_procedures.py --text "INSERT INTO"
    python search_procedures.py --table FACT_ORDER --schema APP_OWNER
    python search_procedures.py --text "COMMIT" --type PACKAGE BODY --format json
    python search_procedures.py --table CUST --regex --limit 50
    python search_procedures.py --table X --show-query   # print SQL to stderr to debug no results
"""

import argparse
import json
import re
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type


# ============================================================================
# Oracle: search ALL_SOURCE
# ============================================================================

def _match(text: str, pattern: str, use_regex: bool) -> bool:
    if not text or not pattern:
        return False
    if use_regex:
        return bool(re.search(pattern, text, re.IGNORECASE))
    return pattern.upper() in text.upper()


def _oracle_build_text_filter(
    table_name: str | None,
    text_content: str | None,
    use_regex: bool,
    params: dict,
) -> str:
    """Build SQL condition for TEXT match (LIKE or REGEXP_LIKE). Appends bind names to params."""
    conditions = []
    if table_name:
        if use_regex:
            key = "re_table"
            params[key] = table_name
            conditions.append(f"REGEXP_LIKE(TEXT, :{key}, 'i')")
        else:
            key = "table_val"
            params[key] = table_name.strip().upper()
            conditions.append(f"UPPER(TEXT) LIKE '%' || :{key} || '%'")
    if text_content:
        if use_regex:
            key = "re_text"
            params[key] = text_content
            conditions.append(f"REGEXP_LIKE(TEXT, :{key}, 'i')")
        else:
            key = "text_val"
            params[key] = text_content.strip().upper()
            conditions.append(f"UPPER(TEXT) LIKE '%' || :{key} || '%'")
    if not conditions:
        return "1=0"
    return "(" + ") OR (".join(conditions) + ")"


def _to_oracle_literal(value: str) -> str:
    """Escape and quote a string for use as Oracle literal (single-quoted)."""
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _executable_sql(sql: str, params: dict) -> str:
    """Substitute bind parameters with Oracle literals so the query is copy-paste executable."""
    out = sql
    # Replace longer param names first to avoid :typ1 matching inside :typ10
    for key in sorted(params.keys(), key=lambda k: -len(k)):
        val = params[key]
        literal = _to_oracle_literal(val) if isinstance(val, str) else (str(val) if val is not None else "NULL")
        out = out.replace(f":{key}", literal)
    return out


def _print_query_debug(sql: str, params: dict, stream=None):
    """Print executable SQL (bind vars substituted) to stream (default stderr) for debugging."""
    if stream is None:
        stream = sys.stderr
    executable = _executable_sql(sql, params)
    stream.write("-- Executable query (copy-paste to run)\n")
    stream.write(executable.strip())
    stream.write("\n\n")
    stream.flush()


def _oracle_search_procedures(
    cursor,
    table_name: str | None,
    text_content: str | None,
    schema: str | None,
    object_types: list[str],
    use_regex: bool,
    limit_objects: int,
    limit_lines_per_object: int,
    show_query: bool = False,
) -> list[dict]:
    """
    Find objects (procedure/package/function) in ALL_SOURCE containing table_name or text_content.
    Step 1: Query only rows where TEXT matches (filter in DB). Step 2: In Python, group by
    (OWNER, NAME, TYPE), apply both-criteria filter, then build output.
    """
    if not table_name and not text_content:
        return []

    valid_types = ["PROCEDURE", "PACKAGE", "PACKAGE BODY", "FUNCTION"]
    types_to_use = [t for t in object_types if t in valid_types] or valid_types
    placeholders = ", ".join(f":typ{i}" for i in range(len(types_to_use)))
    params: dict = {f"typ{i}": t for i, t in enumerate(types_to_use)}
    if schema:
        params["schema"] = schema.upper()

    text_condition = _oracle_build_text_filter(table_name, text_content, use_regex, params)

    sql = f"""
        SELECT OWNER, NAME, TYPE, LINE, TEXT
        FROM ALL_SOURCE
        WHERE TYPE IN ({placeholders})
          AND ({text_condition})
    """
    if schema:
        sql += " AND OWNER = :schema"
    sql += " ORDER BY OWNER, NAME, TYPE, LINE"

    if show_query:
        _print_query_debug(sql, params)

    cursor.execute(sql, params)

    # Group by (owner, name, type); track which criterion each line matched and which line numbers
    object_has_table: dict[tuple, bool] = defaultdict(bool)
    object_has_text: dict[tuple, bool] = defaultdict(bool)
    matching_line_nums: dict[tuple, list[int]] = defaultdict(list)
    all_keys: set[tuple] = set()

    for row in cursor:
        owner, name, otype, line, text = row[0], row[1], row[2], row[3], row[4] or ""
        key = (owner, name, otype)
        all_keys.add(key)

        match_table = _match(text, table_name, use_regex) if table_name else True
        match_text = _match(text, text_content, use_regex) if text_content else True

        if match_table:
            object_has_table[key] = True
        if match_text:
            object_has_text[key] = True
        if match_table or match_text:
            matching_line_nums[key].append(line)

    # Keep only objects that have table match (if requested) AND text match (if requested)
    filtered_keys: list[tuple] = []
    for key in sorted(all_keys, key=lambda x: (x[0], x[1], x[2])):
        if table_name and not object_has_table[key]:
            continue
        if text_content and not object_has_text[key]:
            continue
        filtered_keys.append(key)
        if len(filtered_keys) >= limit_objects:
            break

    if not filtered_keys:
        return []

    # Step 2: Fetch full source for these objects (whole package/procedure for lineage)
    in_parts = []
    params2: dict = {}
    for i, (owner, name, otype) in enumerate(filtered_keys):
        in_parts.append(f"(:o{i}, :n{i}, :t{i})")
        params2[f"o{i}"] = owner
        params2[f"n{i}"] = name
        params2[f"t{i}"] = otype
    in_clause = ", ".join(in_parts)
    sql_full = f"""
        SELECT OWNER, NAME, TYPE, LINE, TEXT
        FROM ALL_SOURCE
        WHERE (OWNER, NAME, TYPE) IN ({in_clause})
        ORDER BY OWNER, NAME, TYPE, LINE
    """
    if show_query:
        sys.stderr.write("-- Query for full source (step 2):\n")
        _print_query_debug(sql_full, params2)

    cursor.execute(sql_full, params2)
    full_by_key: dict[tuple, list[dict]] = defaultdict(list)
    for row in cursor:
        owner, name, otype, line, text = row[0], row[1], row[2], row[3], row[4] or ""
        full_by_key[(owner, name, otype)].append({"line": line, "text": text.rstrip()})

    results = []
    for key in filtered_keys:
        owner, name, otype = key
        full_lines = full_by_key.get(key, [])
        if limit_lines_per_object > 0 and len(full_lines) > limit_lines_per_object:
            full_lines = full_lines[: limit_lines_per_object]
        results.append({
            "schema": owner,
            "name": name,
            "type": otype,
            "match_count": len(matching_line_nums.get(key, [])),
            "matching_line_numbers": matching_line_nums.get(key, [])[: 100],
            "lines": full_lines,
        })

    return results


def search_procedures(
    table_name: str | None = None,
    text_content: str | None = None,
    db_alias: str = "DWH_ADMIN",
    schema: str | None = None,
    object_types: list[str] | None = None,
    use_regex: bool = False,
    limit_objects: int = 100,
    limit_lines_per_object: int = 0,
    show_query: bool = False,
) -> list[dict]:
    """
    Find procedure/package/function in DWH containing a table name or string.

    Only supports Oracle. If db_alias is not Oracle, raises ValueError.

    Args:
        table_name: Table name to search in source (e.g. DIM_CUSTOMER).
        text_content: Any string to search in source (e.g. "INSERT INTO", "COMMIT").
        db_alias: Connection alias (default DWH).
        schema: Filter by owner/schema.
        object_types: Object types (PROCEDURE, PACKAGE, PACKAGE BODY, FUNCTION). Default is all.
        use_regex: If True, table_name and text_content are treated as regex.
        limit_objects: Maximum number of objects to return.
        limit_lines_per_object: Max number of matching lines per object.
        show_query: If True, print the SQL and bind parameters to stderr (for debugging).

    Returns:
        List[dict], each element: schema, name, type, match_count, matching_line_numbers, lines (full source).
    """
    db_type = get_db_type(db_alias)
    if db_type != "oracle":
        raise ValueError("search_procedures only supports Oracle. Use db_alias with DWH_TYPE=oracle.")

    if not object_types:
        object_types = ["PROCEDURE", "PACKAGE", "PACKAGE BODY", "FUNCTION"]

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()
        return _oracle_search_procedures(
            cursor,
            table_name=table_name,
            text_content=text_content,
            schema=schema,
            object_types=object_types,
            use_regex=use_regex,
            limit_objects=limit_objects,
            limit_lines_per_object=limit_lines_per_object,
            show_query=show_query,
        )


# ============================================================================
# Formatters
# ============================================================================

def format_text(results: list[dict]) -> str:
    if not results:
        return "No matching procedure/package found."

    lines = []
    lines.append(f"Found {len(results)} matching objects (full source below):\n")
    for r in results:
        lines.append(f"  [{r['type']}] {r['schema']}.{r['name']}  ({r['match_count']} lines reference search term)")
        source = "\n".join(ln["text"] for ln in r["lines"])
        lines.append(source)
        lines.append("")
    return "\n".join(lines)


def format_json(results: list[dict]) -> str:
    return json.dumps(results, indent=2, ensure_ascii=False)


def format_markdown(results: list[dict]) -> str:
    if not results:
        return "No matching procedure/package found."

    out = [f"Found **{len(results)}** matching objects (full source).\n"]
    for r in results:
        out.append(f"### `{r['schema']}.{r['name']}` ({r['type']}) — {r['match_count']} lines reference search term\n")
        out.append("```")
        source = "\n".join(ln["text"] for ln in r["lines"])
        out.append(source)
        out.append("```\n")
    return "\n".join(out)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Search for Oracle procedure/package in DWH by table name or content string."
    )
    parser.add_argument("--table", "-t", default=None, help="Table name to search in source (e.g. DIM_CUSTOMER)")
    parser.add_argument("--text", "-e", default=None, dest="text_content",
                        help="Any string to search in source (e.g. INSERT INTO, COMMIT)")
    parser.add_argument("--db", default="DWH_ADMIN", help="Database alias (default: DWH)")
    parser.add_argument("--schema", "-s", default=None, help="Filter by owner/schema")
    parser.add_argument("--type", dest="object_types", default="PROCEDURE,PACKAGE,PACKAGE BODY,FUNCTION",
                        help="Object types, separated by commas (default: PROCEDURE,PACKAGE,PACKAGE BODY,FUNCTION)")
    parser.add_argument("--regex", action="store_true", help="Treat --table and --text as regex")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of objects (default: 100)")
    parser.add_argument("--limit-lines", type=int, default=0,
                        help="Max lines of full source per object; 0 = no limit (default: 0)")
    parser.add_argument("--show-query", action="store_true",
                        help="Print the SQL and bind parameters to stderr (to debug no results)")
    parser.add_argument("--format", choices=["text", "json", "markdown"], default="text",
                        help="Output format")
    args = parser.parse_args()

    if not args.table and not args.text_content:
        parser.error("Need at least one of --table or --text.")

    object_types = [s.strip().upper() for s in args.object_types.split(",") if s.strip()]

    try:
        results = search_procedures(
            table_name=args.table or None,
            text_content=args.text_content or None,
            db_alias=args.db,
            schema=args.schema or None,
            object_types=object_types,
            use_regex=args.regex,
            limit_objects=args.limit,
            limit_lines_per_object=args.limit_lines,
            show_query=args.show_query,
        )
        if args.format == "json":
            print(format_json(results))
        elif args.format == "markdown":
            print(format_markdown(results))
        else:
            print(format_text(results))
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
