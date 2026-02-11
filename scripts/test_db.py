#!/usr/bin/env python3
"""Quick test one DB by alias. Usage: python scripts/test_db.py <ALIAS> (default: DWH)."""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from scripts.db_connector import get_connection, get_db_type

PING_SQL = {
    "oracle": "SELECT 1 FROM DUAL",
    "mysql": "SELECT 1",
    "postgresql": "SELECT 1",
    "sqlserver": "SELECT 1",
}

def main():
    alias = (sys.argv[1] if len(sys.argv) > 1 else "DWH").strip().upper()
    if not alias:
        print("Usage: python scripts/test_db.py <ALIAS>")
        print("Example: python scripts/test_db.py LOS")
        return 1
    try:
        db_type = get_db_type(alias)
    except ValueError as e:
        print(e)
        return 1
    ping = PING_SQL.get(db_type, "SELECT 1")
    print(f"Connecting to {alias} ({db_type})...")
    try:
        with get_connection(alias) as conn:
            cur = conn.cursor()
            cur.execute(ping)
            row = cur.fetchone()
            cur.close()
        print(f"OK: {alias} -> {row}")
        return 0
    except Exception as e:
        print(f"FAIL: {alias} -> {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
