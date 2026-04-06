"""
search_views.py - Fetch Oracle VIEW definition/text by view name (substring or exact).

Oracle notes:
- Uses ALL_VIEWS.TEXT (LONG) to retrieve the view "select text".
- If --ddl is provided, the script will also try DBMS_METADATA.GET_DDL.
  If DBMS_METADATA is not available/privileged, it will fall back to:
    CREATE OR REPLACE VIEW <owner>.<view_name> AS <TEXT>
"""

import argparse
import json
import sys
import os
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type


def _lob_to_str(val: Any) -> str:
    """Best-effort conversion for Oracle LONG/CLOB/Lob-like objects."""
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    # oracledb may return a LOB object with .read()
    if hasattr(val, "read"):
        return val.read()
    return str(val)


def _to_oracle_literal(value: Any) -> str:
    """Escape and quote a string for use as Oracle literal."""
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _executable_sql(sql: str, params: dict[str, Any]) -> str:
    """Substitute bind parameters with Oracle literals (for debug only)."""
    out = sql
    for key in sorted(params.keys(), key=lambda k: -len(k)):
        out = out.replace(f":{key}", _to_oracle_literal(params[key]))
    return out


def _print_query_debug(sql: str, params: dict[str, Any], stream=None) -> None:
    """Print executable SQL (bind vars substituted) to stream."""
    if stream is None:
        stream = sys.stderr
    stream.write("-- Executable query (copy-paste to run)\n")
    stream.write(_executable_sql(sql, params).strip())
    stream.write("\n\n")
    stream.flush()


def _oracle_fetch_view_by_name(
    cursor,
    view_name: str,
    owner: str | None,
    exact: bool = False,
    show_query: bool = False,
) -> list[dict]:
    """
    Fetch VIEW rows from ALL_VIEWS by view name.

    When exact=False, matches any VIEW whose name contains the given text.
    Matching is case-insensitive (uses UPPER(VIEW_NAME)) so quoted mixed-case
    identifiers in the data dictionary still match.

    Returns list of dict: schema, name, type='VIEW', text.
    """
    needle = view_name.strip().upper()
    if not needle:
        return []

    sql = """
        SELECT OWNER, VIEW_NAME, TEXT
        FROM ALL_VIEWS
        WHERE """
    if exact:
        sql += "UPPER(VIEW_NAME) = :view_name"
        params: dict[str, Any] = {"view_name": needle}
    else:
        sql += "INSTR(UPPER(VIEW_NAME), :name_substr) > 0"
        params = {"name_substr": needle}
    if owner:
        sql += " AND OWNER = :owner"
        params["owner"] = owner.strip().upper()
    sql += " ORDER BY OWNER, VIEW_NAME"

    if show_query:
        _print_query_debug(sql, params)

    cursor.execute(sql, params)

    results: list[dict] = []
    for row in cursor:
        r_owner, r_view_name, r_text = row[0], row[1], row[2]
        results.append(
            {
                "schema": str(r_owner),
                "name": str(r_view_name),
                "type": "VIEW",
                "text": _lob_to_str(r_text),
            }
        )
    return results


def _oracle_fetch_view_ddl(cursor, view_name: str, owner: str, show_query: bool = False) -> str | None:
    """
    Try to fetch full DDL using DBMS_METADATA.GET_DDL.
    Returns DDL string or None.
    """
    sql = """
        SELECT DBMS_METADATA.GET_DDL('VIEW', :view_name, :owner) AS DDL
        FROM DUAL
    """
    params: dict[str, Any] = {
        "view_name": view_name.strip().upper(),
        "owner": owner.strip().upper(),
    }
    if show_query:
        _print_query_debug(sql, params)

    cursor.execute(sql, params)
    row = cursor.fetchone()
    if not row:
        return None
    return _lob_to_str(row[0])


def search_views(
    view_name: str | None = None,
    object_name: str | None = None,
    db_alias: str = "DWH",
    schema: str | None = None,
    exact: bool = False,
    ddl: bool = False,
    show_query: bool = False,
) -> list[dict]:
    """
    Fetch Oracle VIEW definition/text by view name (substring match by default).

    Args:
        view_name: substring to find in VIEW_NAME (case-insensitive), or exact name if exact=True
        object_name: e.g. SALES or OWNER.SALES (overrides view_name/schema); same matching rules
        db_alias: connection alias (default DWH)
        schema: filter by owner/schema (used only when object_name has no owner)
        exact: if True, require VIEW_NAME equal to the given string (after trim/upper)
        ddl: also try to return full DDL (CREATE OR REPLACE VIEW ...)
        show_query: print SQL debug to stderr
    """
    db_type = get_db_type(db_alias)
    if db_type != "oracle":
        raise ValueError("search_views only supports Oracle. Use db_alias with DWH_TYPE=oracle.")

    obj = object_name.strip() if object_name else None
    if obj:
        if "." in obj:
            object_owner, obj_view_name = obj.split(".", 1)
        else:
            object_owner, obj_view_name = schema, obj
    else:
        obj_view_name = view_name.strip() if view_name else None
        object_owner = schema

    if not obj_view_name:
        raise ValueError("Need at least one of view_name or object_name.")

    with get_connection(db_alias) as conn:
        cursor = conn.cursor()

        results = _oracle_fetch_view_by_name(
            cursor=cursor,
            view_name=obj_view_name,
            owner=object_owner,
            exact=exact,
            show_query=show_query,
        )

        if ddl:
            for r in results:
                # Try DBMS_METADATA first (preferred full DDL)
                ddl_text: str | None = None
                try:
                    ddl_text = _oracle_fetch_view_ddl(
                        cursor=cursor,
                        view_name=r["name"],
                        owner=r["schema"],
                        show_query=show_query,
                    )
                except Exception:
                    ddl_text = None

                if not ddl_text:
                    # Fallback: build CREATE statement from view select text.
                    view_sql = r.get("text", "").rstrip()
                    ddl_text = f"CREATE OR REPLACE VIEW {r['schema']}.{r['name']} AS\n{view_sql}\n"

                r["ddl"] = ddl_text

    return results


def format_text(results: list[dict]) -> str:
    if not results:
        return "No matching view found."

    lines: list[str] = []
    lines.append(f"Found {len(results)} view(s) (VIEW TEXT below):\n")
    for r in results:
        lines.append(f"  [VIEW] {r['schema']}.{r['name']}")
        lines.append(r.get("text", "").rstrip())
        if "ddl" in r:
            lines.append("\n-- DDL\n" + r["ddl"].rstrip())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def format_json(results: list[dict]) -> str:
    return json.dumps(results, indent=2, ensure_ascii=False)


def format_markdown(results: list[dict]) -> str:
    if not results:
        return "No matching view found."

    out: list[str] = [f"Found **{len(results)}** view(s).\n"]
    for r in results:
        out.append(f"### `{r['schema']}.{r['name']}`\n")
        out.append("```sql")
        out.append((r.get("ddl") or r.get("text") or "").rstrip())
        out.append("```\n")
    return "\n".join(out).rstrip() + "\n"


def format_sql(results: list[dict]) -> str:
    """
    Plain .sql-style output: comment headers, then the view text/DDL as-is.
    Suitable for redirect to a file (e.g. --format sql > out.sql).
    """
    if not results:
        return "-- No matching view found.\n"

    blocks: list[str] = [f"-- search_views: {len(results)} view(s)\n\n"]
    for r in results:
        blocks.append("-- " + ("-" * 76) + "\n")
        blocks.append(f"-- Object: {r['schema']}.{r['name']}\n")
        blocks.append("-- " + ("-" * 76) + "\n")
        body = (r.get("ddl") or r.get("text") or "").rstrip()
        blocks.append(body + "\n\n")
    return "".join(blocks).rstrip() + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Oracle VIEW definition/text by view name (substring in name by default)."
    )
    parser.add_argument(
        "--view",
        "-v",
        default=None,
        help="Text to find in view name (e.g. SALES matches VW_SALES). Use --exact for full name only.",
    )
    parser.add_argument(
        "--name",
        "-n",
        default=None,
        dest="object_name",
        help="Object as text in view name or OWNER.NAME (overrides --view/--schema). Same matching as --view.",
    )
    parser.add_argument(
        "--exact",
        action="store_true",
        help="Require exact VIEW_NAME match (trimmed, case-insensitive) instead of substring search.",
    )
    parser.add_argument("--db", default="DWH", help="Database alias (default: DWH)")
    parser.add_argument("--schema", "-s", default=None, help="Owner/schema filter for VIEWs")
    parser.add_argument("--ddl", action="store_true", help="Also return full DDL (CREATE OR REPLACE VIEW ...)")
    parser.add_argument("--show-query", action="store_true", help="Print SQL debug to stderr")
    parser.add_argument(
        "--format",
        choices=["text", "json", "markdown", "sql"],
        default="text",
        help="Output format (sql: commented headers + definition/DDL)",
    )
    args = parser.parse_args()

    try:
        results = search_views(
            view_name=args.view,
            object_name=args.object_name,
            db_alias=args.db,
            schema=args.schema,
            exact=args.exact,
            ddl=args.ddl,
            show_query=args.show_query,
        )
        if args.format == "json":
            print(format_json(results))
        elif args.format == "markdown":
            print(format_markdown(results))
        elif args.format == "sql":
            print(format_sql(results), end="")
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

