"""
report_missing_column_comments.py - Báo cáo số cột thiếu comment theo từng nguồn DB.

Lấy từ mọi source (trừ HRM, NEXUSTI): Source db, Schema, Table, tổng số cột,
số cột thiếu comment, % thiếu comment và bin 5% (>=95%, >=90%, ...).
Xuất 1 file Excel: sheet đầu gộp toàn bộ dòng chi tiết từ mọi source, sau đó mỗi source 1 sheet.

Usage:
    python scripts/report_missing_column_comments.py
    python scripts/report_missing_column_comments.py --output reports/column_comments_audit.xlsx
    python scripts/report_missing_column_comments.py --exclude HRM,NEXUSTI,SCORING
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_connector import get_connection, get_db_type, list_available_connections


# Sources to exclude (user request: HRM, Nexusti và "mấy thằng ất")
DEFAULT_EXCLUDE = {"HRM", "NEXUSTI"}


def _pct_missing_bin(pct: float) -> str:
    """Bin 5%: >=95%, >=90%, >=85%, ... >=0%."""
    if pct >= 100:
        return ">=95%"
    bucket = (int(pct) // 5) * 5
    return f">={bucket}%"


def _fetch_oracle(cursor, source: str) -> list[tuple]:
    sql = """
        SELECT c.OWNER, c.TABLE_NAME,
               COUNT(*) AS total_cols,
               SUM(CASE WHEN NVL(TRIM(cc.COMMENTS), '') = '' OR cc.COMMENTS IS NULL THEN 1 ELSE 0 END) AS missing_comment
        FROM ALL_TAB_COLUMNS c
        LEFT JOIN ALL_COL_COMMENTS cc
            ON cc.OWNER = c.OWNER AND cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME
        GROUP BY c.OWNER, c.TABLE_NAME
        ORDER BY c.OWNER, c.TABLE_NAME
    """
    cursor.execute(sql)
    return [(source, r[0], r[1], r[2], r[3]) for r in cursor.fetchall()]


def _fetch_mysql(cursor, source: str, database: str | None) -> list[tuple]:
    if database:
        sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME,
                   COUNT(*) AS total_cols,
                   SUM(CASE WHEN COALESCE(TRIM(COLUMN_COMMENT), '') = '' THEN 1 ELSE 0 END) AS missing_comment
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            GROUP BY TABLE_SCHEMA, TABLE_NAME
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        cursor.execute(sql, (database,))
    else:
        sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME,
                   COUNT(*) AS total_cols,
                   SUM(CASE WHEN COALESCE(TRIM(COLUMN_COMMENT), '') = '' THEN 1 ELSE 0 END) AS missing_comment
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            GROUP BY TABLE_SCHEMA, TABLE_NAME
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        cursor.execute(sql)
    return [(source, r[0], r[1], r[2], r[3]) for r in cursor.fetchall()]


def _fetch_postgresql(cursor, source: str) -> list[tuple]:
    sql = """
        SELECT n.nspname, c.relname,
               COUNT(*) AS total_cols,
               SUM(CASE WHEN COALESCE(TRIM(col_description(a.attrelid, a.attnum)), '') = '' THEN 1 ELSE 0 END) AS missing_comment
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY n.nspname, c.relname
        ORDER BY n.nspname, c.relname
    """
    cursor.execute(sql)
    return [(source, r[0], r[1], r[2], r[3]) for r in cursor.fetchall()]


def _fetch_sqlserver(cursor, source: str) -> list[tuple]:
    sql = """
        SELECT s.name, t.name,
               COUNT(*) AS total_cols,
               SUM(CASE WHEN ep.value IS NULL OR LTRIM(RTRIM(CAST(ep.value AS NVARCHAR(MAX)))) = '' THEN 1 ELSE 0 END) AS missing_comment
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.extended_properties ep
            ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
        GROUP BY s.name, t.name
        ORDER BY s.name, t.name
    """
    cursor.execute(sql)
    return [(source, r[0], r[1], r[2], r[3]) for r in cursor.fetchall()]


def fetch_source_metadata(source: str) -> pd.DataFrame:
    """Lấy metadata (schema, table, total_cols, missing_comment) cho một source."""
    db_type = get_db_type(source)
    rows = []

    with get_connection(source) as conn:
        cursor = conn.cursor()
        if db_type == "oracle":
            rows = _fetch_oracle(cursor, source)
        elif db_type == "mysql":
            database = os.environ.get(f"{source}_DATABASE")
            rows = _fetch_mysql(cursor, source, database)
        elif db_type == "postgresql":
            rows = _fetch_postgresql(cursor, source)
        elif db_type == "sqlserver":
            rows = _fetch_sqlserver(cursor, source)
        else:
            raise ValueError(f"Unsupported db_type: {db_type}")

    if not rows:
        return pd.DataFrame(columns=[
            "Source db", "Schema", "Table", "Number of Column in Total",
            "Number of Column missing comment", "% Missing Comment", "% Missing Comment (bin 5%)"
        ])

    df = pd.DataFrame(rows, columns=[
        "Source db", "Schema", "Table", "Number of Column in Total", "Number of Column missing comment"
    ])
    df["% Missing Comment"] = (
        df["Number of Column missing comment"].astype(float) / df["Number of Column in Total"].astype(float) * 100
    ).round(2)
    df["% Missing Comment (bin 5%)"] = df["% Missing Comment"].apply(_pct_missing_bin)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Báo cáo số cột thiếu comment theo từng nguồn DB, xuất Excel (1 sheet/source)."
    )
    parser.add_argument(
        "--output", "-o",
        default="column_comments_audit.xlsx",
        help="Đường dẫn file Excel đầu ra (default: column_comments_audit.xlsx)",
    )
    parser.add_argument(
        "--exclude",
        default=",".join(DEFAULT_EXCLUDE),
        help=f"Danh sách source loại trừ, cách nhau bởi dấu phẩy (default: {','.join(DEFAULT_EXCLUDE)})",
    )
    args = parser.parse_args()

    exclude_set = {s.strip().upper() for s in args.exclude.split(",") if s.strip()}
    connections = list_available_connections()
    sources = [c["alias"] for c in connections if c["alias"].upper() not in exclude_set]

    if not sources:
        print("Không có source nào (sau khi loại trừ).", file=sys.stderr)
        sys.exit(1)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_dfs: list[tuple[str, pd.DataFrame]] = []
    for source in sources:
        try:
            df = fetch_source_metadata(source)
            all_dfs.append((source, df))
            print(f"  {source}: {len(df)} bảng.")
        except Exception as e:
            print(f"  {source}: LỖI - {e}", file=sys.stderr)

    # First sheet: concatenated detail from all sources
    combined = pd.concat([df for _, df in all_dfs if not df.empty], ignore_index=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        combined.to_excel(writer, sheet_name="All_Sources", index=False)
        for source, df in all_dfs:
            sheet_name = source[:31].replace("[", "").replace("]", "").replace(":", "_").replace("*", "").replace("?", "").replace("/", "_").replace("\\", "_")
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\nĐã ghi: {out_path.absolute()}")


if __name__ == "__main__":
    main()
