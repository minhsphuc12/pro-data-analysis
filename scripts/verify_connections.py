#!/usr/bin/env python3
"""
Verify database connections defined in .env.

Usage:
  python verify_connections.py              # verify all configured DBs
  python verify_connections.py SOURCE         # verify only LOS
  python verify_connections.py SOURCE_A SOURCE_B   # verify multiple

Uses db_connector: only Oracle, MySQL, PostgreSQL, SQL Server are tested.
MongoDB (NOTIFY_HUB) is skipped (not supported by db_connector).
"""
import sys
import signal
from pathlib import Path

# Ensure project root is on path so load_dotenv finds .env
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from scripts.db_connector import list_available_connections, get_connection, get_db_type

CONNECT_TIMEOUT_SEC = 15
PING_SQL = {
    "oracle": "SELECT 1 FROM DUAL",
    "mysql": "SELECT 1",
    "postgresql": "SELECT 1",
    "sqlserver": "SELECT 1",
}


class TimeoutException(Exception):
    pass


def _test_one(alias: str, db_type: str) -> str | None:
    """Return None on success, error message on failure."""
    ping = PING_SQL.get(db_type, "SELECT 1")
    try:
        with get_connection(alias) as conn:
            cur = conn.cursor()
            cur.execute(ping)
            cur.fetchone()
            cur.close()
        return None
    except Exception as e:
        return str(e)


def _run_one(alias: str, db_type: str) -> tuple[bool, str | None]:
    """Run _test_one with timeout. Returns (success, error_message)."""
    def handler(signum, frame):
        raise TimeoutException()

    try:
        old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(CONNECT_TIMEOUT_SEC)
        try:
            err = _test_one(alias, db_type)
            return (err is None, err)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    except TimeoutException:
        return (False, f"timeout ({CONNECT_TIMEOUT_SEC}s)")
    except Exception as e:
        return (False, str(e))


def main():
    # Optional: test only these aliases (e.g. python verify_connections.py SOURCE)
    only_aliases = [a.strip().upper() for a in sys.argv[1:] if a.strip()]

    connections = list_available_connections()
    if not connections:
        print("No database connections found in .env (expected *_TYPE, *_HOST or *_DSN).")
        return 1

    if only_aliases:
        # Resolve aliases to connection dicts; skip unknown
        alias_set = set(only_aliases)
        by_alias = {c["alias"].upper(): c for c in connections}
        connections = [by_alias[a] for a in only_aliases if a in by_alias]
        unknown = alias_set - {c["alias"].upper() for c in connections}
        if unknown:
            print(f"Unknown alias(es): {', '.join(sorted(unknown))}")
            print(f"Available: {', '.join(sorted(c['alias'] for c in list_available_connections()))}")
            return 1
        if not connections:
            return 1
        print(f"Verifying {len(connections)} connection(s) (timeout {CONNECT_TIMEOUT_SEC}s each)...\n")
    else:
        print(f"Verifying {len(connections)} connection(s) (timeout {CONNECT_TIMEOUT_SEC}s each)...\n")

    ok = 0
    fail = 0

    for c in sorted(connections, key=lambda x: x["alias"].lower()):
        alias = c["alias"]
        db_type = c["type"]
        success, err = _run_one(alias, db_type)
        if success:
            print(f"  OK   {alias:<20} ({db_type})")
            ok += 1
        else:
            print(f"  FAIL {alias:<20} ({db_type}): {err}")
            fail += 1

    print()
    print(f"Result: {ok} OK, {fail} FAIL")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
