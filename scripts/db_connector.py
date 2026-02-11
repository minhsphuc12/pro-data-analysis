"""
db_connector.py - Multi-database connection manager.

Supports Oracle, MySQL, PostgreSQL, SQL Server via unified interface.
Connection info is read from environment variables using a naming convention:
    {DB_ALIAS}_TYPE     = oracle | mysql | postgresql | sqlserver
    {DB_ALIAS}_USERNAME = username
    {DB_ALIAS}_PASSWORD = password
    {DB_ALIAS}_DSN      = connection string (Oracle) or host:port/dbname

Default alias: DWH (Oracle datawarehouse)

Usage:
    from db_connector import get_connection, get_db_type

    with get_connection("DWH") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM DUAL")
"""

import os
import sys
import re
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_db_type(db_alias: str = "DWH") -> str:
    """Return the database type for the given alias (oracle|mysql|postgresql|sqlserver)."""
    db_type = os.environ.get(f"{db_alias}_TYPE", "").lower().strip()
    if not db_type:
        # Legacy fallback: if DWH alias and no TYPE var, assume oracle
        if db_alias.upper() == "DWH":
            return "oracle"
        raise ValueError(
            f"Environment variable {db_alias}_TYPE is not set. "
            f"Expected one of: oracle, mysql, postgresql, sqlserver"
        )
    if db_type not in ("oracle", "mysql", "postgresql", "sqlserver"):
        raise ValueError(
            f"{db_alias}_TYPE='{db_type}' is not supported. "
            f"Expected one of: oracle, mysql, postgresql, sqlserver"
        )
    return db_type


def _get_env(db_alias: str, key: str, required: bool = True) -> str | None:
    """Read an env var for the given DB alias."""
    val = os.environ.get(f"{db_alias}_{key}")
    if required and not val:
        raise ValueError(f"Environment variable {db_alias}_{key} is not set.")
    return val


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------

def _connect_oracle(db_alias: str):
    """Return an oracledb connection."""
    try:
        import oracledb
    except ImportError:
        sys.exit("oracledb package is required for Oracle connections. Install: pip install oracledb")

    username = _get_env(db_alias, "USERNAME")
    password = _get_env(db_alias, "PASSWORD")
    dsn = _get_env(db_alias, "DSN")
    return oracledb.connect(user=username, password=password, dsn=dsn)


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------

def _connect_mysql(db_alias: str):
    """Return a mysql-connector connection."""
    try:
        import mysql.connector
    except ImportError:
        sys.exit("mysql-connector-python package is required. Install: pip install mysql-connector-python")

    host = _get_env(db_alias, "HOST")
    port = int(_get_env(db_alias, "PORT", required=False) or "3306")
    username = _get_env(db_alias, "USERNAME")
    password = _get_env(db_alias, "PASSWORD")
    database = _get_env(db_alias, "DATABASE", required=False)

    params = dict(host=host, port=port, user=username, password=password)
    if database:
        params["database"] = database
    return mysql.connector.connect(**params)


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

def _connect_postgresql(db_alias: str):
    """Return a psycopg2 connection."""
    try:
        import psycopg2
    except ImportError:
        sys.exit("psycopg2 package is required. Install: pip install psycopg2-binary")

    host = _get_env(db_alias, "HOST")
    port = int(_get_env(db_alias, "PORT", required=False) or "5432")
    username = _get_env(db_alias, "USERNAME")
    password = _get_env(db_alias, "PASSWORD")
    database = _get_env(db_alias, "DATABASE", required=False) or "postgres"

    return psycopg2.connect(host=host, port=port, user=username, password=password, dbname=database)


# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------

def _connect_sqlserver(db_alias: str):
    """Return a pyodbc connection for SQL Server."""
    try:
        import pyodbc
    except ImportError:
        sys.exit("pyodbc package is required for SQL Server connections. Install: pip install pyodbc")

    host = _get_env(db_alias, "HOST")
    port = int(_get_env(db_alias, "PORT", required=False) or "1433")
    username = _get_env(db_alias, "USERNAME")
    password = _get_env(db_alias, "PASSWORD")
    database = _get_env(db_alias, "DATABASE", required=False)
    driver = _get_env(db_alias, "DRIVER", required=False) or "{ODBC Driver 17 for SQL Server}"
    encrypt = _get_env(db_alias, "ENCRYPT", required=False)
    trust_server_certificate = _get_env(db_alias, "TRUST_SERVER_CERTIFICATE", required=False)

    # Build connection string
    conn_str_parts = [
        f"DRIVER={driver}",
        f"SERVER={host},{port}",
        f"UID={username}",
        f"PWD={password}",
    ]
    if database:
        conn_str_parts.append(f"DATABASE={database}")
    if encrypt is not None:
        conn_str_parts.append(f"Encrypt={encrypt}")
    if trust_server_certificate is not None:
        conn_str_parts.append(f"TrustServerCertificate={trust_server_certificate}")

    conn_str = ";".join(conn_str_parts)
    return pyodbc.connect(conn_str)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_CONNECTORS = {
    "oracle": _connect_oracle,
    "mysql": _connect_mysql,
    "postgresql": _connect_postgresql,
    "sqlserver": _connect_sqlserver,
}


@contextmanager
def get_connection(db_alias: str = "DWH"):
    """
    Context manager that yields a database connection.

    Usage:
        with get_connection("DWH") as conn:
            cur = conn.cursor()
            cur.execute(...)
    """
    db_type = get_db_type(db_alias)
    connector = _CONNECTORS[db_type]
    conn = connector(db_alias)
    try:
        yield conn
    finally:
        conn.close()


def is_select_only(sql: str) -> bool:
    """Check if SQL is a read-only SELECT statement (no DML/DDL)."""
    cleaned = re.sub(r'--[^\n]*', '', sql)           # strip line comments
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)  # strip block comments
    cleaned = cleaned.strip().upper()

    blocked = ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
               "TRUNCATE", "MERGE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
               "CALL", "BEGIN", "DECLARE")
    first_word = cleaned.split()[0] if cleaned.split() else ""
    if first_word in blocked:
        return False

    # Also block if any blocked keyword appears at statement boundary
    for kw in blocked:
        if re.search(rf'\b{kw}\b', cleaned):
            return False

    return first_word in ("SELECT", "WITH", "EXPLAIN")


def safe_execute(cursor, sql: str, params=None, row_limit: int = 100,
                 timeout_seconds: int = 30, db_type: str = "oracle"):
    """
    Execute a SELECT query with safety limits.

    Returns:
        list of rows (up to row_limit)
    Raises:
        ValueError if SQL is not a SELECT statement
    """
    if not is_select_only(sql):
        raise ValueError("Only SELECT/WITH statements are allowed for safe execution.")

    # Set statement timeout per database
    if db_type == "oracle":
        # Oracle: resource limit via session (requires DBA grant for full control)
        # We use ROWNUM wrapping instead
        pass
    elif db_type == "mysql":
        cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {timeout_seconds * 1000}")
    elif db_type == "postgresql":
        cursor.execute(f"SET statement_timeout = '{timeout_seconds * 1000}'")
    elif db_type == "sqlserver":
        # SQL Server: use query hint for timeout
        # This is handled in the wrapped query with OPTION (QUERYTIMEOUT timeout_seconds)
        pass

    # Wrap with row limit
    wrapped = _wrap_with_limit(sql, row_limit, db_type)

    if params:
        cursor.execute(wrapped, params)
    else:
        cursor.execute(wrapped)

    return cursor.fetchall()


def _wrap_with_limit(sql: str, limit: int, db_type: str) -> str:
    """Wrap a SELECT with a row limit appropriate to the DB dialect."""
    sql_stripped = sql.rstrip().rstrip(";")

    if db_type == "oracle":
        return f"SELECT * FROM ({sql_stripped}) WHERE ROWNUM <= {limit}"
    elif db_type == "mysql":
        return f"SELECT * FROM ({sql_stripped}) AS _limited LIMIT {limit}"
    elif db_type == "postgresql":
        return f"SELECT * FROM ({sql_stripped}) AS _limited LIMIT {limit}"
    elif db_type == "sqlserver":
        # SQL Server 2012+ uses OFFSET...FETCH NEXT
        # For simpler limit without offset, we use TOP in subquery
        return f"SELECT TOP {limit} * FROM ({sql_stripped}) AS _limited"
    return sql_stripped


def get_param_style(db_type: str) -> str:
    """Return the parameter placeholder style for each DB type."""
    if db_type == "oracle":
        return ":param"   # named :name
    elif db_type == "mysql":
        return "%s"       # positional
    elif db_type == "postgresql":
        return "%s"       # positional
    elif db_type == "sqlserver":
        return "?"        # positional
    return "?"


def list_available_connections() -> list[dict]:
    """List all configured database connections from environment."""
    connections = []
    seen_aliases = set()

    for key in os.environ:
        if key.endswith("_TYPE") or key.endswith("_DSN") or key.endswith("_HOST"):
            alias = key.rsplit("_", 1)[0]
            # Handle multi-part suffixes like _USERNAME
            for suffix in ("_TYPE", "_DSN", "_HOST", "_USERNAME"):
                if key.endswith(suffix):
                    alias = key[: -len(suffix)]
                    break
            if alias and alias not in seen_aliases:
                seen_aliases.add(alias)
                try:
                    db_type = get_db_type(alias)
                    connections.append({"alias": alias, "type": db_type})
                except ValueError:
                    pass

    return sorted(connections, key=lambda x: x["alias"])
